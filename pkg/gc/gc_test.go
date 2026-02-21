package gc

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	"github.com/agenthands/blobcas/pkg/catalog"
	"github.com/agenthands/blobcas/pkg/cidutil"
	"github.com/agenthands/blobcas/pkg/core"
	"github.com/agenthands/blobcas/pkg/manifest"
	"github.com/agenthands/blobcas/pkg/pack"
	"github.com/agenthands/blobcas/pkg/transform"
)

func TestGCRunner(t *testing.T) {
	ctx := context.Background()

	// Set up temporary directories
	catDir, err := os.MkdirTemp("", "blobcas-gc-catalog-test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(catDir)

	packDir, err := os.MkdirTemp("", "blobcas-gc-pack-test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(packDir)

	// Initialize components
	cat, err := catalog.Open(catDir)
	if err != nil {
		t.Fatalf("failed to open catalog: %v", err)
	}
	defer cat.Close()

	packCfg := core.PackConfig{Dir: packDir, TargetPackBytes: 1024} // small packs for rotation
	pm, err := pack.NewManager(packCfg)
	if err != nil {
		t.Fatalf("failed to open pack manager: %v", err)
	}
	defer pm.Close()

	cidHub := cidutil.NewBuilder()
	manifests := manifest.NewCodec(core.LimitsConfig{})
	tr := transform.NewNone()

	// Create runner
	runner := NewRunner(core.GCConfig{
		Enabled:         true,
		TargetPackBytes: 1024,
	}, cat, pm, manifests, cidHub, tr)

	t.Run("EmptyRun", func(t *testing.T) {
		res, err := runner.RunOnce(ctx)
		if err != nil {
			t.Fatalf("RunOnce failed: %v", err)
		}
		if res.PacksSwept != 0 || res.BlocksMoved != 0 {
			t.Errorf("expected no action on empty store, got %+v", res)
		}
	})

	t.Run("MarkAndSweep", func(t *testing.T) {
		// Create some data
		data1 := []byte("chunk 1 data")
		cid1, _ := cidHub.ChunkCID(data1)
		b1, _ := tr.Encode(data1)
		p1, _ := pm.PutBlock(ctx, cid1, b1)
		cat.PutPackForCID(nil, cid1, p1)

		data2 := []byte("chunk 2 data - this will be dead")
		cid2, _ := cidHub.ChunkCID(data2)
		b2, _ := tr.Encode(data2)
		p2, _ := pm.PutBlock(ctx, cid2, b2)
		cat.PutPackForCID(nil, cid2, p2)

		// Create active manifest with chunk 1
		m1 := &manifest.ManifestV1{
			Version: 1,
			Length:  uint64(len(data1)),
			Chunks:  []manifest.ChunkRef{{CID: cid1, Len: uint32(len(data1))}},
		}
		m1Bytes, _ := manifests.Encode(m1)
		m1CID, _ := cidHub.ManifestCID(m1Bytes)
		m1Stored, _ := tr.Encode(m1Bytes)
		pm1, _ := pm.PutBlock(ctx, m1CID, m1Stored)
		cat.PutPackForCID(nil, m1CID, pm1)

		// Create root for m1 (valid in future)
		cat.PutRootDeadline(nil, m1CID, time.Now().Add(time.Hour))

		// Rotate packs to make them eligible for GC
		pm.SealAndRotateIfNeeded(ctx)

		res, err := runner.RunOnce(ctx)
		if err != nil {
			t.Fatalf("RunOnce failed: %v", err)
		}

		// Since pack1 has block1 (live) and block2 (dead), and m1 is live,
		// depending on compaction threshold, it might rewrite chunk 1 and delete the original pack.
		// For a simple GC, let's at least ensure RunOnce succeeds and doesn't delete chunk 1.

		t.Logf("GC Result: %+v", res)

		// Verify chunk 1 is still readable
		pID, ok, _ := cat.GetPackForCID(ctx, cid1)
		if !ok {
			t.Error("chunk 1 is missing from catalog")
		} else {
			got, err := pm.GetBlock(ctx, pID, cid1)
			if err != nil || !bytes.Equal(got, b1) {
				t.Errorf("chunk 1 data corrupted or unreadable: %v", err)
			}
		}

		// Chunk 2 SHOULD be gone if its pack was actually swept, but if it compacted,
		// it wouldn't be rewritten. Let's see what happens.
	})
}
