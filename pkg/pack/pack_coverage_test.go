package pack_test

import (
	"context"
	"os"
	"testing"

	"github.com/agenthands/blobcas/internal/testkit"
	"github.com/agenthands/blobcas/pkg/cidutil"
	"github.com/agenthands/blobcas/pkg/core"
	"github.com/agenthands/blobcas/pkg/pack"
)

func TestPack_PutBlock_InvalidCID(t *testing.T) {
	ctx := context.Background()
	mgr, _ := newTestManager(t, 0)

	_, err := mgr.PutBlock(ctx, core.CID{Bytes: []byte("invalid-cid")}, []byte("data"))
	if err == nil {
		t.Error("expected error for invalid CID")
	}
}

func TestPack_SealAndRotate_EmptyPack(t *testing.T) {
	ctx := context.Background()
	mgr, _ := newTestManager(t, 0)

	// Seal empty pack
	err := mgr.SealActivePack(ctx)
	if err != nil {
		t.Fatalf("SealActivePack failed: %v", err)
	}

	// Verify rotation happened
	newID := mgr.CurrentPackID()
	if newID <= 1 {
		t.Error("expected pack ID to advance after seal")
	}
}

func TestPack_SealAndRotateIfNeeded_ExactThreshold(t *testing.T) {
	ctx := context.Background()
	cidBuilder := cidutil.NewBuilder()
	rng := testkit.RNG(111)

	// Very small threshold, should rotate after one large block
	mgr, _ := newTestManager(t, 128)

	data := testkit.RandomBytes(rng, 512)
	cid, _ := cidBuilder.ChunkCID(data)
	_, err := mgr.PutBlock(ctx, cid, data)
	if err != nil {
		t.Fatal(err)
	}

	idBefore := mgr.CurrentPackID()
	err = mgr.SealAndRotateIfNeeded(ctx)
	if err != nil {
		t.Fatal(err)
	}
	idAfter := mgr.CurrentPackID()

	if idBefore == idAfter {
		t.Error("expected rotation when pack exceeds threshold")
	}
}

func TestPack_GetBlock_FromActiveVsSealed(t *testing.T) {
	ctx := context.Background()
	cidBuilder := cidutil.NewBuilder()

	mgr, _ := newTestManager(t, 64*1024)

	// Put a block in the active pack
	data1 := []byte("active block data")
	cid1, _ := cidBuilder.ChunkCID(data1)
	pid1, err := mgr.PutBlock(ctx, cid1, data1)
	if err != nil {
		t.Fatal(err)
	}

	// Read from active
	got1, err := mgr.GetBlock(ctx, pid1, cid1)
	if err != nil {
		t.Fatalf("GetBlock from active failed: %v", err)
	}
	if string(got1) != string(data1) {
		t.Error("data mismatch from active pack")
	}

	// Seal
	mgr.SealActivePack(ctx)

	// Put a new block in the new active pack
	data2 := []byte("new active block data")
	cid2, _ := cidBuilder.ChunkCID(data2)
	pid2, _ := mgr.PutBlock(ctx, cid2, data2)

	// Read from sealed
	got1Again, err := mgr.GetBlock(ctx, pid1, cid1)
	if err != nil {
		t.Fatalf("GetBlock from sealed failed: %v", err)
	}
	if string(got1Again) != string(data1) {
		t.Error("data mismatch from sealed pack")
	}

	// Read from new active
	got2, err := mgr.GetBlock(ctx, pid2, cid2)
	if err != nil {
		t.Fatalf("GetBlock from new active failed: %v", err)
	}
	if string(got2) != string(data2) {
		t.Error("data mismatch from new active pack")
	}
}

func TestPack_Close_WithSealedPacks(t *testing.T) {
	ctx := context.Background()
	cidBuilder := cidutil.NewBuilder()
	rng := testkit.RNG(222)

	dir, err := os.MkdirTemp("", "blobcas-pack-close-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	mgr, err := pack.NewManager(core.PackConfig{Dir: dir, TargetPackBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}

	// Create and seal a few packs
	for i := 0; i < 3; i++ {
		data := testkit.RandomBytes(rng, 256)
		cid, _ := cidBuilder.ChunkCID(data)
		mgr.PutBlock(ctx, cid, data)
		mgr.SealActivePack(ctx)
	}

	// Close should cleanly close all sealed packs + finalize active
	err = mgr.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}
}

func TestPack_GetBlock_NonExistentBlock(t *testing.T) {
	ctx := context.Background()
	cidBuilder := cidutil.NewBuilder()

	mgr, _ := newTestManager(t, 0)

	// Valid CID but not stored
	data := []byte("not stored")
	cid, _ := cidBuilder.ChunkCID(data)

	_, err := mgr.GetBlock(ctx, mgr.CurrentPackID(), cid)
	if err == nil {
		t.Error("expected error for non-existent block in active pack")
	}
}
