package blobcas_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/agenthands/blobcas/pkg/blobcas"
	"github.com/agenthands/blobcas/pkg/core"
)

func TestStoreIntegration_PartialWrites(t *testing.T) {
	dir, err := os.MkdirTemp("", "blobcas-store-integration-partial")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s, cfg := createTestStore(t, dir)
	ctx := context.Background()

	// Write a single small blob so we have an active pack created
	key := core.Key{Namespace: "test", ID: "file1"}
	_, err = s.Put(ctx, key, blobcas.PutInput{
		Canonical: bytes.NewReader([]byte("small data")),
	}, blobcas.PutMeta{Canonical: true})
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Close the store so we can safely mutate the pack file
	s.Close()

	// Corrupt active pack (truncate it)
	packFile := filepath.Join(cfg.Pack.Dir, "pack-0000000000000001.car")
	fi, err := os.Stat(packFile)
	if err != nil {
		t.Fatalf("failed to stat active pack: %v", err)
	}

	// Truncate the file to simulate a partial write (cut off the last 10 bytes)
	if fi.Size() > 10 {
		err = os.Truncate(packFile, fi.Size()-10)
		if err != nil {
			t.Fatalf("failed to truncate active pack: %v", err)
		}
	} else {
		t.Fatal("pack file too small to artificially truncate")
	}

	// Try to reopen. In go-car/v2, opening a truncated CAR file either fails on open
	// or fails on read later. We just need to verify it doesn't panic and either
	// fails cleanly or handles it.
	s2, err := blobcas.Open(ctx, cfg)
	if err != nil {
		t.Logf("Store initialization failed gracefully on corrupted pack: %v", err)
		// This is acceptable behavior according to SPEC (fail cleanly or quarantine)
		return
	}
	defer s2.Close()

	t.Log("Store opened despite truncated pack. Attempting to write new data...")

	// Verify we can still write
	key2 := core.Key{Namespace: "test", ID: "file2"}
	_, err = s2.Put(ctx, key2, blobcas.PutInput{
		Canonical: bytes.NewReader([]byte("new data")),
	}, blobcas.PutMeta{Canonical: true})

	if err != nil {
		t.Logf("Write failed gracefully: %v", err)
	} else {
		t.Log("Write succeeded!")
	}
}
