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

// TestGC_CompactionWithLowUtilization exercises the compaction path with
// blocks that are live but at <50% utilization, forcing the compactPack code path.
func TestGC_CompactionWithLowUtilization(t *testing.T) {
	ctx := context.Background()
	dir, _ := os.MkdirTemp("", "gc-compact-*")
	defer os.RemoveAll(dir)

	cat, _ := catalog.Open(dir + "/catalog")
	defer cat.Close()

	pm, _ := pack.NewManager(core.PackConfig{Dir: dir + "/packs", TargetPackBytes: 64 * 1024})
	defer pm.Close()

	cidHub := cidutil.NewBuilder()
	manifests := manifest.NewCodec(core.LimitsConfig{})
	tr := transform.NewNone()
	rng := testkit.RNG(42)

	// Create 10 blocks in one pack. Only 2 will have live roots.
	var liveManifests []core.CID
	for i := 0; i < 10; i++ {
		data := testkit.RandomBytes(rng, 200)
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

		if i < 2 {
			// Live
			cat.PutRootDeadline(nil, mCID, time.Now().Add(24*time.Hour))
			liveManifests = append(liveManifests, mCID)
		} else {
			// Expired
			cat.PutRootDeadline(nil, mCID, time.Now().Add(-1*time.Hour))
		}
	}

	pm.SealActivePack(ctx)

	runner := NewRunner(core.GCConfig{Enabled: true}, cat, pm, manifests, cidHub, tr)

	res, err := runner.RunOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Compaction: Swept=%d Moved=%d", res.PacksSwept, res.BlocksMoved)

	// Verify live manifests are still readable
	for _, mCID := range liveManifests {
		pid, ok, _ := cat.GetPackForCID(ctx, mCID)
		if !ok {
			t.Errorf("live manifest missing after compaction")
			continue
		}
		_, err := pm.GetBlock(ctx, pid, mCID)
		if err != nil {
			t.Errorf("live manifest unreadable after compaction: %v", err)
		}
	}
}

// TestGC_EmptyPackSweep ensures that empty sealed packs get swept.
func TestGC_EmptyPackSweep(t *testing.T) {
	ctx := context.Background()
	dir, _ := os.MkdirTemp("", "gc-emptysweep-*")
	defer os.RemoveAll(dir)

	cat, _ := catalog.Open(dir + "/catalog")
	defer cat.Close()

	pm, _ := pack.NewManager(core.PackConfig{Dir: dir + "/packs", TargetPackBytes: 2048})
	defer pm.Close()

	cidHub := cidutil.NewBuilder()
	manifests := manifest.NewCodec(core.LimitsConfig{})
	tr := transform.NewNone()

	// Seal an empty pack
	pm.SealActivePack(ctx)

	sealedBefore := pm.ListSealedPacks()
	if len(sealedBefore) == 0 {
		t.Fatal("expected at least one sealed pack")
	}

	runner := NewRunner(core.GCConfig{Enabled: true}, cat, pm, manifests, cidHub, tr)

	res, err := runner.RunOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Empty sweep: Swept=%d", res.PacksSwept)
	// The empty pack should have been swept
	if res.PacksSwept == 0 {
		t.Error("expected empty pack to be swept")
	}
}
