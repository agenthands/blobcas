package catalog

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/agenthands/blobcas/pkg/core"
	"github.com/cockroachdb/pebble"
)

func TestCatalog(t *testing.T) {
	dir, err := os.MkdirTemp("", "blobcas-catalog-test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	cat, err := Open(dir)
	if err != nil {
		t.Fatalf("failed to open catalog: %v", err)
	}
	defer cat.Close()

	ctx := context.Background()

	t.Run("C2P", func(t *testing.T) {
		cid := core.CID{Bytes: []byte("chunk1")}
		packID := uint64(123)

		err := cat.PutPackForCID(nil, cid, packID)
		if err != nil {
			t.Fatalf("PutPackForCID failed: %v", err)
		}

		gotPack, ok, err := cat.GetPackForCID(ctx, cid)
		if err != nil {
			t.Fatalf("GetPackForCID failed: %v", err)
		}
		if !ok || gotPack != packID {
			t.Errorf("expected %d, got %d (ok=%v)", packID, gotPack, ok)
		}
	})

	t.Run("K2M", func(t *testing.T) {
		key := core.Key{Namespace: "ns", ID: "id1"}
		cid := core.CID{Bytes: []byte("manifest1")}

		err := cat.PutManifestForKey(nil, key, cid)
		if err != nil {
			t.Fatalf("PutManifestForKey failed: %v", err)
		}

		gotCID, ok, err := cat.GetManifestForKey(ctx, key)
		if err != nil {
			t.Fatalf("GetManifestForKey failed: %v", err)
		}
		if !ok || string(gotCID.Bytes) != string(cid.Bytes) {
			t.Errorf("expected %v, got %v (ok=%v)", cid, gotCID, ok)
		}
	})

	t.Run("Roots", func(t *testing.T) {
		cid := core.CID{Bytes: []byte("manifest1")}
		deadline := time.Now().Add(24 * time.Hour).Truncate(time.Second)

		err := cat.PutRootDeadline(nil, cid, deadline)
		if err != nil {
			t.Fatalf("PutRootDeadline failed: %v", err)
		}

		found := false
		err = cat.IterateRoots(ctx, func(gotCID core.CID, gotDeadline time.Time) error {
			if string(gotCID.Bytes) == string(cid.Bytes) {
				if gotDeadline.Equal(deadline) {
					found = true
				} else {
					t.Errorf("deadline mismatch: expected %v, got %v", deadline, gotDeadline)
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("IterateRoots failed: %v", err)
		}
		if !found {
			t.Error("root not found during iteration")
		}
	})

	t.Run("BatchAtomicity", func(t *testing.T) {
		cid1 := core.CID{Bytes: []byte("batch_chunk1")}
		cid2 := core.CID{Bytes: []byte("batch_chunk2")}
		packID := uint64(999)

		batch := cat.NewBatch()

		_ = cat.PutPackForCID(batch, cid1, packID)
		_ = cat.PutPackForCID(batch, cid2, packID)

		_, ok, _ := cat.GetPackForCID(ctx, cid1)
		if ok {
			t.Error("expected C1 not to be visible before commit")
		}

		batch.Close() // discarding without commit

		_, ok, _ = cat.GetPackForCID(ctx, cid1)
		if ok {
			t.Error("expected C1 not to be visible after discarded batch")
		}

		batch2 := cat.NewBatch()
		_ = cat.PutPackForCID(batch2, cid1, packID)
		_ = cat.PutPackForCID(batch2, cid2, packID)
		_ = batch2.Commit(pebble.Sync)
		batch2.Close()

		_, ok1, _ := cat.GetPackForCID(ctx, cid1)
		_, ok2, _ := cat.GetPackForCID(ctx, cid2)

		if !ok1 || !ok2 {
			t.Error("expected both items to be committed atomically")
		}
	})
}
