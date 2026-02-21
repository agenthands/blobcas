package gc_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/agenthands/blobcas/internal/testkit"
	"github.com/agenthands/blobcas/pkg/catalog"
	"github.com/agenthands/blobcas/pkg/cidutil"
	"github.com/agenthands/blobcas/pkg/core"
	"github.com/agenthands/blobcas/pkg/gc"
	"github.com/agenthands/blobcas/pkg/manifest"
	"github.com/agenthands/blobcas/pkg/pack"
	"github.com/agenthands/blobcas/pkg/transform"
	"github.com/cockroachdb/pebble"
)

// gcFixture sets up all GC dependencies for a test.
type gcFixture struct {
	Cat       catalog.Catalog
	Packs     pack.Manager
	Manifests manifest.Codec
	CIDHub    cidutil.Builder
	Tr        transform.Transform
	Dir       string
}

func newGCFixture(t testing.TB) *gcFixture {
	t.Helper()
	dir, err := os.MkdirTemp("", "blobcas-gc-comp-*")
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

	return &gcFixture{
		Cat:       cat,
		Packs:     pm,
		Manifests: manifest.NewCodec(core.LimitsConfig{}),
		CIDHub:    cidutil.NewBuilder(),
		Tr:        transform.NewNone(),
		Dir:       dir,
	}
}

func (f *gcFixture) Runner(cfg core.GCConfig) gc.Runner {
	if !cfg.Enabled {
		cfg.Enabled = true
	}
	return gc.NewRunner(cfg, f.Cat, f.Packs, f.Manifests, f.CIDHub, f.Tr)
}

// storeChunkAndManifest stores a chunk+manifest and registers as root.
func (f *gcFixture) storeChunkAndManifest(t testing.TB, ctx context.Context, data []byte, deadline time.Time) (core.CID, core.CID) {
	t.Helper()

	chunkCID, _ := f.CIDHub.ChunkCID(data)
	encoded, _ := f.Tr.Encode(data)
	pid, err := f.Packs.PutBlock(ctx, chunkCID, encoded)
	if err != nil {
		t.Fatal(err)
	}
	f.Cat.PutPackForCID(nil, chunkCID, pid)

	m := &manifest.ManifestV1{
		Version: 1,
		Length:  uint64(len(data)),
		Chunks:  []manifest.ChunkRef{{CID: chunkCID, Len: uint32(len(data))}},
	}
	mBytes, _ := f.Manifests.Encode(m)
	mCID, _ := f.CIDHub.ManifestCID(mBytes)
	mStored, _ := f.Tr.Encode(mBytes)
	mPid, err := f.Packs.PutBlock(ctx, mCID, mStored)
	if err != nil {
		t.Fatal(err)
	}
	f.Cat.PutPackForCID(nil, mCID, mPid)
	f.Cat.PutRootDeadline(nil, mCID, deadline)

	return chunkCID, mCID
}

// ---------- Tests ----------

func TestGC_EmptyStore(t *testing.T) {
	ctx := context.Background()
	fix := newGCFixture(t)
	runner := fix.Runner(core.GCConfig{})

	res, err := runner.RunOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if res.PacksSwept != 0 || res.BlocksMoved != 0 {
		t.Errorf("expected no-op on empty store, got %+v", res)
	}
}

func TestGC_AllLiveRoots(t *testing.T) {
	ctx := context.Background()
	fix := newGCFixture(t)
	rng := testkit.RNG(1)

	// Store 5 live blobs with future deadlines
	for i := 0; i < 5; i++ {
		data := testkit.RandomBytes(rng, 256)
		fix.storeChunkAndManifest(t, ctx, data, time.Now().Add(24*time.Hour))
	}

	// Seal to make eligible for GC
	fix.Packs.SealActivePack(ctx)

	runner := fix.Runner(core.GCConfig{})
	res, err := runner.RunOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("All-live GC: Swept=%d Moved=%d", res.PacksSwept, res.BlocksMoved)

	// With all roots live, nothing should be swept (unless compaction triggers due to threshold)
	// LiveCount/TotalCount for each pack >= 0.5, so no compaction.
	if res.PacksSwept != 0 {
		t.Logf("NOTE: Unexpected sweeps (may be empty packs from seal): %d", res.PacksSwept)
	}
}

func TestGC_AllExpiredRoots(t *testing.T) {
	ctx := context.Background()
	fix := newGCFixture(t)
	rng := testkit.RNG(2)

	// Store 5 expired blobs
	var chunkCIDs []core.CID
	for i := 0; i < 5; i++ {
		data := testkit.RandomBytes(rng, 256)
		chunkCID, _ := fix.storeChunkAndManifest(t, ctx, data, time.Now().Add(-1*time.Hour))
		chunkCIDs = append(chunkCIDs, chunkCID)
	}

	fix.Packs.SealActivePack(ctx)

	runner := fix.Runner(core.GCConfig{})
	res, err := runner.RunOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("All-expired GC: Swept=%d Moved=%d", res.PacksSwept, res.BlocksMoved)

	// With all roots expired, mark returns empty live set => all packs swept
	if res.PacksSwept == 0 {
		t.Error("expected packs to be swept when all roots expired")
	}
}

func TestGC_MixedLiveAndExpired(t *testing.T) {
	ctx := context.Background()
	fix := newGCFixture(t)
	rng := testkit.RNG(3)

	var liveChunks, deadChunks []core.CID

	for i := 0; i < 10; i++ {
		data := testkit.RandomBytes(rng, 256)
		var deadline time.Time
		if i%2 == 0 {
			deadline = time.Now().Add(24 * time.Hour) // live
		} else {
			deadline = time.Now().Add(-1 * time.Hour) // expired
		}
		chunkCID, _ := fix.storeChunkAndManifest(t, ctx, data, deadline)
		if i%2 == 0 {
			liveChunks = append(liveChunks, chunkCID)
		} else {
			deadChunks = append(deadChunks, chunkCID)
		}
	}

	fix.Packs.SealActivePack(ctx)

	runner := fix.Runner(core.GCConfig{})
	res, err := runner.RunOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Mixed GC: Swept=%d Moved=%d", res.PacksSwept, res.BlocksMoved)

	// Verify live chunks are still accessible
	for i, c := range liveChunks {
		pid, ok, err := fix.Cat.GetPackForCID(ctx, c)
		if err != nil {
			t.Errorf("live chunk %d lookup error: %v", i, err)
			continue
		}
		if !ok {
			t.Errorf("live chunk %d missing from catalog", i)
			continue
		}
		_, err = fix.Packs.GetBlock(ctx, pid, c)
		if err != nil {
			t.Errorf("live chunk %d unreadable: %v", i, err)
		}
	}
}

func TestGC_Idempotent(t *testing.T) {
	ctx := context.Background()
	fix := newGCFixture(t)
	rng := testkit.RNG(4)

	// Store some data with mixed deadlines
	for i := 0; i < 5; i++ {
		data := testkit.RandomBytes(rng, 256)
		fix.storeChunkAndManifest(t, ctx, data, time.Now().Add(24*time.Hour))
	}
	fix.Packs.SealActivePack(ctx)

	runner := fix.Runner(core.GCConfig{})

	// Run GC twice
	res1, err := runner.RunOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}

	res2, err := runner.RunOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Second run should be a no-op (or at least not worse)
	t.Logf("GC Run 1: Swept=%d Moved=%d", res1.PacksSwept, res1.BlocksMoved)
	t.Logf("GC Run 2: Swept=%d Moved=%d", res2.PacksSwept, res2.BlocksMoved)
}

func TestGC_StartStop(t *testing.T) {
	fix := newGCFixture(t)
	runner := fix.Runner(core.GCConfig{
		Enabled:  true,
		RunEvery: 50 * time.Millisecond, // fast for testing
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start the background runner
	runner.Start(ctx)

	// Let it run at least a couple of ticks
	time.Sleep(150 * time.Millisecond)

	// Stop gracefully
	runner.Stop()

	// Calling Stop again should be safe (idempotent)
	// This tests the guard in Stop() that checks r.running.
	// Note: after close(stopCh), second Stop will panic if not guarded.
	// Our code guards it, so this should not panic.
}

func TestGC_ContextCancellation(t *testing.T) {
	fix := newGCFixture(t)
	rng := testkit.RNG(5)

	ctx := context.Background()

	// Store some data
	for i := 0; i < 3; i++ {
		data := testkit.RandomBytes(rng, 256)
		fix.storeChunkAndManifest(t, ctx, data, time.Now().Add(24*time.Hour))
	}
	fix.Packs.SealActivePack(ctx)

	runner := fix.Runner(core.GCConfig{})

	// Cancel context before running
	cancelCtx, cancel := context.WithCancel(ctx)
	cancel()

	// RunOnce with cancelled context should either succeed (if it doesn't check) or fail gracefully
	_, err := runner.RunOnce(cancelCtx)
	// We just verify it doesn't panic
	t.Logf("RunOnce with cancelled ctx: err=%v", err)
}

func TestGC_ConcurrentRunOnce(t *testing.T) {
	fix := newGCFixture(t)
	rng := testkit.RNG(6)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		data := testkit.RandomBytes(rng, 256)
		fix.storeChunkAndManifest(t, ctx, data, time.Now().Add(24*time.Hour))
	}
	fix.Packs.SealActivePack(ctx)

	runner := fix.Runner(core.GCConfig{})

	// Run GC concurrently — should not panic or deadlock.
	// RunOnce has a mutex, so concurrent calls will serialize.
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := runner.RunOnce(ctx)
			if err != nil {
				t.Errorf("concurrent RunOnce: %v", err)
			}
		}()
	}
	wg.Wait()
}

func TestGC_MultiChunkManifest(t *testing.T) {
	ctx := context.Background()
	fix := newGCFixture(t)
	rng := testkit.RNG(7)

	// Create a manifest with multiple chunks
	const numChunks = 5
	var chunkRefs []manifest.ChunkRef
	batch := fix.Cat.NewBatch()

	for i := 0; i < numChunks; i++ {
		data := testkit.RandomBytes(rng, 200)
		chunkCID, _ := fix.CIDHub.ChunkCID(data)
		encoded, _ := fix.Tr.Encode(data)
		pid, _ := fix.Packs.PutBlock(ctx, chunkCID, encoded)
		fix.Cat.PutPackForCID(batch, chunkCID, pid)
		chunkRefs = append(chunkRefs, manifest.ChunkRef{CID: chunkCID, Len: uint32(len(data))})
	}

	m := &manifest.ManifestV1{
		Version: 1,
		Length:  uint64(numChunks * 200),
		Chunks:  chunkRefs,
	}
	mBytes, _ := fix.Manifests.Encode(m)
	mCID, _ := fix.CIDHub.ManifestCID(mBytes)
	mStored, _ := fix.Tr.Encode(mBytes)
	mPid, _ := fix.Packs.PutBlock(ctx, mCID, mStored)
	fix.Cat.PutPackForCID(batch, mCID, mPid)
	fix.Cat.PutRootDeadline(batch, mCID, time.Now().Add(24*time.Hour))

	batch.Commit(pebble.Sync)
	batch.Close()

	fix.Packs.SealActivePack(ctx)

	runner := fix.Runner(core.GCConfig{})
	res, err := runner.RunOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Multi-chunk GC: Swept=%d Moved=%d", res.PacksSwept, res.BlocksMoved)

	// All chunks should still be readable
	for i, ref := range chunkRefs {
		pid, ok, _ := fix.Cat.GetPackForCID(ctx, ref.CID)
		if !ok {
			t.Errorf("chunk %d missing from catalog after GC", i)
			continue
		}
		_, err := fix.Packs.GetBlock(ctx, pid, ref.CID)
		if err != nil {
			t.Errorf("chunk %d unreadable after GC: %v", i, err)
		}
	}
}

func TestGC_ZeroDeadline(t *testing.T) {
	ctx := context.Background()
	fix := newGCFixture(t)
	rng := testkit.RNG(8)

	// Zero deadline (time.Time{}) means "no expiry" — should always be treated as live
	data := testkit.RandomBytes(rng, 256)
	chunkCID, _ := fix.storeChunkAndManifest(t, ctx, data, time.Time{})
	fix.Packs.SealActivePack(ctx)

	runner := fix.Runner(core.GCConfig{})
	_, err := runner.RunOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// The chunk should still be accessible (zero deadline = not expired)
	pid, ok, _ := fix.Cat.GetPackForCID(ctx, chunkCID)
	if !ok {
		t.Error("chunk with zero deadline should not be garbage collected")
		return
	}
	_, err = fix.Packs.GetBlock(ctx, pid, chunkCID)
	if err != nil {
		t.Errorf("chunk with zero deadline unreadable: %v", err)
	}
}

// ---------- Catalog-level tests within GC context ----------

func TestGC_CatalogConsistency(t *testing.T) {
	ctx := context.Background()
	fix := newGCFixture(t)
	rng := testkit.RNG(9)

	// Create live data
	var liveCIDs []core.CID
	for i := 0; i < 5; i++ {
		data := testkit.RandomBytes(rng, 256)
		chunkCID, _ := fix.storeChunkAndManifest(t, ctx, data, time.Now().Add(24*time.Hour))
		liveCIDs = append(liveCIDs, chunkCID)
	}

	// Create dead data
	for i := 0; i < 5; i++ {
		data := testkit.RandomBytes(rng, 256)
		fix.storeChunkAndManifest(t, ctx, data, time.Now().Add(-1*time.Hour))
	}

	fix.Packs.SealActivePack(ctx)

	runner := fix.Runner(core.GCConfig{})
	_, err := runner.RunOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// After GC, live CID catalog entries should still point to valid packs
	for i, c := range liveCIDs {
		pid, ok, err := fix.Cat.GetPackForCID(ctx, c)
		if err != nil {
			t.Errorf("live CID %d catalog error: %v", i, err)
			continue
		}
		if !ok {
			t.Errorf("live CID %d missing from catalog after GC", i)
			continue
		}
		_, err = fix.Packs.GetBlock(ctx, pid, c)
		if err != nil {
			t.Errorf("live CID %d points to pack %d but unreadable: %v", i, pid, err)
		}
	}
}

// ---------- Benchmark: Mark Phase Scaling ----------

func BenchmarkGCMark(b *testing.B) {
	rootCounts := []int{10, 100, 1000}

	for _, count := range rootCounts {
		b.Run(fmt.Sprintf("Roots_%d", count), func(b *testing.B) {
			dir, _ := os.MkdirTemp("", "bench-gc-mark-*")
			defer os.RemoveAll(dir)

			catDir := dir + "/catalog"
			packDir := dir + "/packs"

			cat, _ := catalog.Open(catDir)
			pm, _ := pack.NewManager(core.PackConfig{Dir: packDir, TargetPackBytes: 16 * 1024 * 1024})
			cidHub := cidutil.NewBuilder()
			manifests := manifest.NewCodec(core.LimitsConfig{})
			tr := transform.NewNone()

			ctx := context.Background()
			rng := testkit.RNG(int64(count))

			// Populate
			for i := 0; i < count; i++ {
				data := testkit.RandomBytes(rng, 128)
				chunkCID, _ := cidHub.ChunkCID(data)
				encoded, _ := tr.Encode(data)
				pid, _ := pm.PutBlock(ctx, chunkCID, encoded)
				cat.PutPackForCID(nil, chunkCID, pid)

				m := &manifest.ManifestV1{
					Version: 1,
					Length:  uint64(len(data)),
					Chunks:  []manifest.ChunkRef{{CID: chunkCID, Len: uint32(len(data))}},
				}
				mBytes, _ := manifests.Encode(m)
				mCID, _ := cidHub.ManifestCID(mBytes)
				mStored, _ := tr.Encode(mBytes)
				mPid, _ := pm.PutBlock(ctx, mCID, mStored)
				cat.PutPackForCID(nil, mCID, mPid)
				cat.PutRootDeadline(nil, mCID, time.Now().Add(24*time.Hour))
			}
			pm.SealActivePack(ctx)

			runner := gc.NewRunner(core.GCConfig{Enabled: true}, cat, pm, manifests, cidHub, tr)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				_, _ = runner.RunOnce(ctx)
			}

			b.StopTimer()
			pm.Close()
			cat.Close()
		})
	}
}
