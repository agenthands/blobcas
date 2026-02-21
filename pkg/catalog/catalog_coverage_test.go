package catalog

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/agenthands/blobcas/pkg/core"
	"github.com/cockroachdb/pebble"
)

func TestCatalog_CoveragePaths(t *testing.T) {
	dir, err := os.MkdirTemp("", "blobcas-catalog-cov-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	cat, err := Open(dir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer cat.Close()

	ctx := context.Background()

	t.Run("GetPackForCID_NotFound", func(t *testing.T) {
		_, ok, err := cat.GetPackForCID(ctx, core.CID{Bytes: []byte("does-not-exist")})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if ok {
			t.Error("expected ok=false for non-existent CID")
		}
	})

	t.Run("GetPackForCID_CorruptValue", func(t *testing.T) {
		// Write a corrupt value (wrong length) directly via the db
		pc := cat.(*pebbleCatalog)
		key := append(PrefixC2P, []byte("corrupt-cid")...)
		_ = pc.db.Set(key, []byte{0x01, 0x02, 0x03}, pebble.Sync) // 3 bytes, not 8

		_, _, err := cat.GetPackForCID(ctx, core.CID{Bytes: []byte("corrupt-cid")})
		if err == nil {
			t.Error("expected error for corrupt pack ID value")
		}
	})

	t.Run("GetManifestForKey_NotFound", func(t *testing.T) {
		_, ok, err := cat.GetManifestForKey(ctx, core.Key{Namespace: "nope", ID: "nope"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if ok {
			t.Error("expected ok=false for non-existent key")
		}
	})

	t.Run("PutPackForCID_DirectWrite", func(t *testing.T) {
		cid := core.CID{Bytes: []byte("direct-write-cid")}
		err := cat.PutPackForCID(nil, cid, 42)
		if err != nil {
			t.Fatalf("direct PutPackForCID failed: %v", err)
		}

		pid, ok, err := cat.GetPackForCID(ctx, cid)
		if err != nil {
			t.Fatal(err)
		}
		if !ok || pid != 42 {
			t.Errorf("expected packID=42, got %d (ok=%v)", pid, ok)
		}
	})

	t.Run("PutManifestForKey_DirectWrite", func(t *testing.T) {
		key := core.Key{Namespace: "direct", ID: "write"}
		cid := core.CID{Bytes: []byte("direct-manifest-cid")}
		err := cat.PutManifestForKey(nil, key, cid)
		if err != nil {
			t.Fatalf("direct PutManifestForKey failed: %v", err)
		}

		got, ok, err := cat.GetManifestForKey(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		if !ok || string(got.Bytes) != string(cid.Bytes) {
			t.Error("direct write/read mismatch")
		}
	})

	t.Run("PutRootDeadline_DirectWrite", func(t *testing.T) {
		cid := core.CID{Bytes: []byte("direct-root-cid")}
		deadline := time.Now().Add(12 * time.Hour).Truncate(time.Second)
		err := cat.PutRootDeadline(nil, cid, deadline)
		if err != nil {
			t.Fatalf("direct PutRootDeadline failed: %v", err)
		}

		found := false
		cat.IterateRoots(ctx, func(m core.CID, d time.Time) error {
			if string(m.Bytes) == string(cid.Bytes) {
				found = true
				if !d.Equal(deadline) {
					t.Errorf("deadline mismatch: %v vs %v", d, deadline)
				}
			}
			return nil
		})
		if !found {
			t.Error("root not found after direct write")
		}
	})

	t.Run("PutPackForCID_BatchWrite", func(t *testing.T) {
		cid := core.CID{Bytes: []byte("batch-cid")}
		batch := cat.NewBatch()
		_ = cat.PutPackForCID(batch, cid, 99)
		_ = batch.Commit(pebble.Sync)
		batch.Close()

		pid, ok, _ := cat.GetPackForCID(ctx, cid)
		if !ok || pid != 99 {
			t.Errorf("expected 99, got %d", pid)
		}
	})

	t.Run("PutManifestForKey_BatchWrite", func(t *testing.T) {
		key := core.Key{Namespace: "batch", ID: "k2m"}
		cid := core.CID{Bytes: []byte("batch-manifest")}
		batch := cat.NewBatch()
		_ = cat.PutManifestForKey(batch, key, cid)
		_ = batch.Commit(pebble.Sync)
		batch.Close()

		got, ok, _ := cat.GetManifestForKey(ctx, key)
		if !ok || string(got.Bytes) != string(cid.Bytes) {
			t.Error("batch manifest write/read mismatch")
		}
	})

	t.Run("PutRootDeadline_BatchWrite", func(t *testing.T) {
		cid := core.CID{Bytes: []byte("batch-root")}
		deadline := time.Now().Add(6 * time.Hour).Truncate(time.Second)
		batch := cat.NewBatch()
		_ = cat.PutRootDeadline(batch, cid, deadline)
		_ = batch.Commit(pebble.Sync)
		batch.Close()

		found := false
		cat.IterateRoots(ctx, func(m core.CID, d time.Time) error {
			if string(m.Bytes) == string(cid.Bytes) {
				found = true
			}
			return nil
		})
		if !found {
			t.Error("batch root not found")
		}
	})

	t.Run("IterateRoots_SkipsBadValue", func(t *testing.T) {
		// Write a bad root entry (value too short)
		pc := cat.(*pebbleCatalog)
		key := append(PrefixRoots, []byte("bad-root-cid")...)
		_ = pc.db.Set(key, []byte{0x01, 0x02}, pebble.Sync) // 2 bytes, not 8

		// IterateRoots should skip bad entries without error
		err := cat.IterateRoots(ctx, func(m core.CID, d time.Time) error {
			return nil
		})
		if err != nil {
			t.Errorf("IterateRoots should not error on bad value, got: %v", err)
		}
	})
}

func TestCatalog_Open_BadDir(t *testing.T) {
	_, err := Open("/nonexistent/path/to/catalog")
	if err == nil {
		t.Error("expected error opening catalog in non-existent directory")
	}
}

func TestCatalog_IncrementByte_Overflow(t *testing.T) {
	result := incrementByte([]byte{0xFF, 0xFF})
	if result != nil {
		t.Errorf("expected nil for all-0xFF input, got %v", result)
	}

	result = incrementByte([]byte{0x01})
	if result == nil || result[0] != 0x02 {
		t.Errorf("expected [0x02], got %v", result)
	}

	result = incrementByte([]byte{0x01, 0xFF})
	if result == nil || result[0] != 0x02 || result[1] != 0x00 {
		t.Errorf("expected [0x02, 0x00], got %v", result)
	}
}

func TestCatalog_IterateRoots_CallbackError(t *testing.T) {
	dir, err := os.MkdirTemp("", "blobcas-catalog-iterr-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	cat, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer cat.Close()

	ctx := context.Background()

	// Add a root
	cid := core.CID{Bytes: []byte("iter-callback-root")}
	cat.PutRootDeadline(nil, cid, time.Now().Add(time.Hour))

	// Iterate with callback that returns error
	sentinel := errors.New("callback error")
	err = cat.IterateRoots(ctx, func(m core.CID, d time.Time) error {
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Errorf("expected sentinel error, got: %v", err)
	}
}
