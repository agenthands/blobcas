package pack_test

import (
	"context"
	"os"
	"testing"

	"github.com/agenthands/blobcas/pkg/cidutil"
	"github.com/agenthands/blobcas/pkg/core"
	"github.com/agenthands/blobcas/pkg/pack"
)

func TestPackManager(t *testing.T) {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "blobcas-pack-test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	cfg := core.PackConfig{
		Dir:             dir,
		TargetPackBytes: 1024, // small for testing rotation
	}

	mgr, err := pack.NewManager(cfg)
	if err != nil {
		t.Fatalf("failed to create pack manager: %v", err)
	}
	defer mgr.Close()

	cidBuilder := cidutil.NewBuilder()

	t.Run("PutAndGetBlock", func(t *testing.T) {
		data := []byte("block data")
		cid, _ := cidBuilder.ChunkCID(data)

		packID, err := mgr.PutBlock(ctx, cid, data)
		if err != nil {
			t.Fatalf("PutBlock failed: %v", err)
		}

		gotData, err := mgr.GetBlock(ctx, packID, cid)
		if err != nil {
			t.Fatalf("GetBlock failed: %v", err)
		}
		if string(gotData) != string(data) {
			t.Errorf("expected %q, got %q", data, gotData)
		}
	})

	t.Run("Rotation", func(t *testing.T) {
		pack1 := mgr.CurrentPackID()

		// Write block larger than TargetPackBytes to trigger rotation
		data := make([]byte, 1025)
		cid, _ := cidBuilder.ChunkCID(data)

		_, err := mgr.PutBlock(ctx, cid, data)
		if err != nil {
			t.Fatalf("PutBlock failed: %v", err)
		}

		err = mgr.SealAndRotateIfNeeded(ctx)
		if err != nil {
			t.Fatalf("SealAndRotateIfNeeded failed: %v", err)
		}

		pack2 := mgr.CurrentPackID()
		if pack1 == pack2 {
			t.Error("expected pack ID to change after rotation")
		}

		// Verify old block still readable
		oldData := []byte("block data")
		cid1, _ := cidBuilder.ChunkCID(oldData)

		_, err = mgr.GetBlock(ctx, pack1, cid1)
		if err != nil {
			t.Errorf("could not read block from old pack: %v", err)
		}
	})

	t.Run("Discovery", func(t *testing.T) {
		// Close existing manager
		mgr.Close()

		// Reopen
		mgr2, err := pack.NewManager(cfg)
		if err != nil {
			t.Fatalf("failed to reopen manager: %v", err)
		}
		defer mgr2.Close()

		// Should have discovered packs.
		// currentID should be greater than 1 because we already rotated once.
		if mgr2.CurrentPackID() <= 1 {
			t.Errorf("expected currentID > 1, got %d", mgr2.CurrentPackID())
		}

		sealed := mgr2.ListSealedPacks()
		if len(sealed) == 0 {
			t.Error("expected to discover sealed packs")
		}

		// Verify we can still read the old blocks
		data := []byte("block data")
		cid, _ := cidBuilder.ChunkCID(data)
		_, err = mgr2.GetBlock(ctx, 1, cid)
		if err != nil {
			t.Errorf("could not read block after reopen: %v", err)
		}
	})
}
