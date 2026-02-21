package gc

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/agenthands/blobcas/internal/testkit"
	"github.com/agenthands/blobcas/pkg/catalog"
	"github.com/agenthands/blobcas/pkg/cidutil"
	"github.com/agenthands/blobcas/pkg/core"
	"github.com/agenthands/blobcas/pkg/manifest"
	"github.com/agenthands/blobcas/pkg/pack"
	"github.com/agenthands/blobcas/pkg/transform"
)

type gcCovFixture struct {
	cat       catalog.Catalog
	packs     pack.Manager
	manifests manifest.Codec
	cidHub    cidutil.Builder
	tr        transform.Transform
}

func newGCCovFixture(t testing.TB) *gcCovFixture {
	t.Helper()
	dir, err := os.MkdirTemp("", "blobcas-gc-cov-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	catDir := dir + "/catalog"
	packDir := dir + "/packs"

	cat, err := catalog.Open(catDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cat.Close() })

	pm, err := pack.NewManager(core.PackConfig{Dir: packDir, TargetPackBytes: 2048})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pm.Close() })

	return &gcCovFixture{
		cat:       cat,
		packs:     pm,
		manifests: manifest.NewCodec(core.LimitsConfig{}),
		cidHub:    cidutil.NewBuilder(),
		tr:        transform.NewNone(),
	}
}

func (f *gcCovFixture) storeChunkAndManifest(t testing.TB, ctx context.Context, data []byte, deadline time.Time) (core.CID, core.CID) {
	t.Helper()

	chunkCID, _ := f.cidHub.ChunkCID(data)
	encoded, _ := f.tr.Encode(data)
	pid, err := f.packs.PutBlock(ctx, chunkCID, encoded)
	if err != nil {
		t.Fatal(err)
	}
	f.cat.PutPackForCID(nil, chunkCID, pid)

	m := &manifest.ManifestV1{
		Version: 1,
		Length:  uint64(len(data)),
		Chunks:  []manifest.ChunkRef{{CID: chunkCID, Len: uint32(len(data))}},
	}
	mBytes, _ := f.manifests.Encode(m)
	mCID, _ := f.cidHub.ManifestCID(mBytes)
	mStored, _ := f.tr.Encode(mBytes)
	mPid, err := f.packs.PutBlock(ctx, mCID, mStored)
	if err != nil {
		t.Fatal(err)
	}
	f.cat.PutPackForCID(nil, mCID, mPid)
	f.cat.PutRootDeadline(nil, mCID, deadline)

	return chunkCID, mCID
}

func TestGC_StartNotEnabled(t *testing.T) {
	fix := newGCCovFixture(t)

	runner := NewRunner(core.GCConfig{
		Enabled: false, // disabled
	}, fix.cat, fix.packs, fix.manifests, fix.cidHub, fix.tr)

	ctx := context.Background()
	runner.Start(ctx) // should be a no-op

	// Stop should also be safe on a never-started runner
	runner.Stop()
}

func TestGC_StartAlreadyRunning(t *testing.T) {
	fix := newGCCovFixture(t)

	runner := NewRunner(core.GCConfig{
		Enabled:  true,
		RunEvery: 50 * time.Millisecond,
	}, fix.cat, fix.packs, fix.manifests, fix.cidHub, fix.tr)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runner.Start(ctx)
	// Second start should be a no-op
	runner.Start(ctx)

	runner.Stop()
}

func TestGC_DefaultRunEvery(t *testing.T) {
	fix := newGCCovFixture(t)

	// RunEvery == 0 should default to 24h
	runner := NewRunner(core.GCConfig{
		Enabled:  true,
		RunEvery: 0,
	}, fix.cat, fix.packs, fix.manifests, fix.cidHub, fix.tr)

	ctx := context.Background()
	res, err := runner.RunOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_ = res // just ensure it doesn't panic
}

func TestGC_CompactPack(t *testing.T) {
	ctx := context.Background()
	fix := newGCCovFixture(t)
	rng := testkit.RNG(42)

	// Create a pack with 10 blocks, only 2 will be live.
	// This creates <50% utilization which triggers compaction.
	var liveChunkCIDs []core.CID
	for i := 0; i < 10; i++ {
		data := testkit.RandomBytes(rng, 200)
		var deadline time.Time
		if i < 2 {
			deadline = time.Now().Add(24 * time.Hour) // live
		} else {
			deadline = time.Now().Add(-1 * time.Hour) // expired
		}
		chunkCID, _ := fix.storeChunkAndManifest(t, ctx, data, deadline)
		if i < 2 {
			liveChunkCIDs = append(liveChunkCIDs, chunkCID)
		}
	}

	fix.packs.SealActivePack(ctx)

	runner := NewRunner(core.GCConfig{
		Enabled: true,
	}, fix.cat, fix.packs, fix.manifests, fix.cidHub, fix.tr)

	res, err := runner.RunOnce(ctx)
	if err != nil {
		t.Fatalf("RunOnce failed: %v", err)
	}

	t.Logf("Compaction GC: Swept=%d Moved=%d", res.PacksSwept, res.BlocksMoved)

	// Verify live chunks are still accessible
	for i, c := range liveChunkCIDs {
		pid, ok, err := fix.cat.GetPackForCID(ctx, c)
		if err != nil {
			t.Errorf("live chunk %d lookup error: %v", i, err)
			continue
		}
		if !ok {
			t.Errorf("live chunk %d missing from catalog", i)
			continue
		}
		_, err = fix.packs.GetBlock(ctx, pid, c)
		if err != nil {
			t.Errorf("live chunk %d unreadable: %v", i, err)
		}
	}
}

func TestGC_MarkWithMissingPack(t *testing.T) {
	ctx := context.Background()
	fix := newGCCovFixture(t)
	rng := testkit.RNG(99)

	// Store a chunk and manifest, then register root pointing to manifest CID
	// but the manifest CID c2p entry points to a non-existent pack.
	data := testkit.RandomBytes(rng, 200)
	chunkCID, _ := fix.cidHub.ChunkCID(data)
	encoded, _ := fix.tr.Encode(data)
	pid, _ := fix.packs.PutBlock(ctx, chunkCID, encoded)
	fix.cat.PutPackForCID(nil, chunkCID, pid)

	m := &manifest.ManifestV1{
		Version: 1,
		Length:  uint64(len(data)),
		Chunks:  []manifest.ChunkRef{{CID: chunkCID, Len: uint32(len(data))}},
	}
	mBytes, _ := fix.manifests.Encode(m)
	mCID, _ := fix.cidHub.ManifestCID(mBytes)

	// Register root but DON'T store the manifest block
	fix.cat.PutRootDeadline(nil, mCID, time.Now().Add(24*time.Hour))

	fix.packs.SealActivePack(ctx)

	runner := NewRunner(core.GCConfig{
		Enabled: true,
	}, fix.cat, fix.packs, fix.manifests, fix.cidHub, fix.tr)

	// Should not crash despite missing manifest pack
	_, err := runner.RunOnce(ctx)
	// We accept either success or error, as long as it doesn't panic
	t.Logf("RunOnce with missing manifest pack: err=%v", err)
}

func TestGC_StopWithoutStart(t *testing.T) {
	fix := newGCCovFixture(t)

	runner := NewRunner(core.GCConfig{
		Enabled: true,
	}, fix.cat, fix.packs, fix.manifests, fix.cidHub, fix.tr)

	// Stop before Start should not panic
	runner.Stop()
}

func TestGC_ContextCancelDuringStart(t *testing.T) {
	fix := newGCCovFixture(t)

	runner := NewRunner(core.GCConfig{
		Enabled:  true,
		RunEvery: 10 * time.Millisecond,
	}, fix.cat, fix.packs, fix.manifests, fix.cidHub, fix.tr)

	ctx, cancel := context.WithCancel(context.Background())

	runner.Start(ctx)
	// Let it tick once
	time.Sleep(30 * time.Millisecond)
	cancel()
	// Wait for goroutine to notice
	time.Sleep(30 * time.Millisecond)
}
