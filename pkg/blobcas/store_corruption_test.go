package blobcas_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/agenthands/blobcas/internal/testkit"
	"github.com/agenthands/blobcas/pkg/blobcas"
	"github.com/agenthands/blobcas/pkg/core"
)

func TestStoreIntegration_CorruptionDetection(t *testing.T) {
	dir, err := os.MkdirTemp("", "blobcas-store-integration-corrupt")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s, cfg := createTestStore(t, dir)

	ctx := context.Background()
	r := testkit.RNG(1)
	content := testkit.RandomBytes(r, 128*1024)
	key := core.Key{Namespace: "test", ID: "corruptme"}

	ref, err := s.Put(ctx, key, blobcas.PutInput{
		Canonical: bytes.NewReader(content),
	}, blobcas.PutMeta{Canonical: true})

	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Close store to seal packs
	err = s.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Now intentionally corrupt a pack file.
	// Since we know the packs are in `cfg.Pack.Dir`, we can open them,
	// find the data, and flip a byte. A simpler approach is to read the pack file
	// into memory, flip a byte near the end (which will hit a block payload or manifest),
	// and write it back.

	packsDir := cfg.Pack.Dir
	entries, err := os.ReadDir(packsDir)
	if err != nil {
		t.Fatal(err)
	}

	corrupted := false
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".car" {
			packPath := filepath.Join(packsDir, entry.Name())
			b, err := os.ReadFile(packPath)
			if err != nil {
				t.Fatal(err)
			}

			// Corrupt a byte in the middle of the file.
			// We skip the CAR header (which is usually small).
			if len(b) > 4096 {
				b[len(b)/2] ^= 0xFF
				err = os.WriteFile(packPath, b, 0644)
				if err != nil {
					t.Fatal(err)
				}
				corrupted = true
				break
			}
		}
	}

	if !corrupted {
		t.Fatal("failed to find pack file to corrupt")
	}

	// Reopen the store
	s2, err := blobcas.Open(ctx, cfg)
	if err != nil {
		t.Fatalf("failed to reopen store: %v", err)
	}
	defer s2.Close()

	// Try to Get the blob. Our corruption should cause either CID mismatch,
	// zstd decode failure, or blockstore read failure. All should manifest as errors
	// during Get or io.ReadAll.

	rc, _, err := s2.Get(ctx, ref)
	if err != nil {
		// Valid, manifest itself might be corrupted and fail to read
		t.Logf("Get failed early: %v", err)
	} else {
		_, err = io.ReadAll(rc)
		rc.Close()
		if err == nil {
			t.Errorf("expected Get/Read to fail due to corruption")
		} else {
			t.Logf("Read failed as expected: %v", err)

			// If it's a CID mismatch, we might not always get exact `core.ErrCorrupt`
			// from inside the reader stream, but we should verify the API is robust.
			if !errors.Is(err, core.ErrCorrupt) && !errors.Is(err, core.ErrNotFound) {
				t.Logf("note: corruption surfaced as: %T / %v", err, err)
			}
		}
	}
}
