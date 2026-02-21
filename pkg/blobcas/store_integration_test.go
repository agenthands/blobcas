package blobcas_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/agenthands/blobcas/internal/testkit"
	"github.com/agenthands/blobcas/pkg/blobcas"
	"github.com/agenthands/blobcas/pkg/core"
)

// Helper to create a new store with a temporary directory
func createTestStore(t *testing.T, dir string) (blobcas.Store, core.Config) {
	cfg := core.Config{
		Dir: dir,
		Chunking: core.ChunkingConfig{
			Min: 64,
			Avg: 128,
			Max: 256,
		},
		Pack: core.PackConfig{
			Dir:             filepath.Join(dir, "packs"),
			TargetPackBytes: 10 * 1024, // small to trigger rotation
		},
		Catalog: core.CatalogConfig{
			Dir: filepath.Join(dir, "catalog"),
		},
		GC: core.GCConfig{
			DefaultRootTTL: 24 * time.Hour,
		},
		Limits: core.LimitsConfig{
			MaxChunksPerObject: 200000,
		},
	}

	s, err := blobcas.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	return s, cfg
}

func TestStoreIntegration_RoundtripSizes(t *testing.T) {
	dir, err := os.MkdirTemp("", "blobcas-store-integration")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s, _ := createTestStore(t, dir)
	defer s.Close()

	r := testkit.RNG(42)
	sizes := []int{0, 1, 4 * 1024, 1 * 1024 * 1024, 8 * 1024 * 1024} // up to 8MiB to keep tests fast, 64 is too slow for simple unit test

	for _, size := range sizes {
		t.Run(fmt.Sprintf("Size_%d", size), func(t *testing.T) {
			ctx := context.Background()

			var content []byte
			if size > 0 {
				content = testkit.RandomBytes(r, size)
			}

			key := core.Key{Namespace: "test", ID: fmt.Sprintf("obj_%d", size)}

			ref, err := s.Put(ctx, key, blobcas.PutInput{
				Canonical: bytes.NewReader(content),
			}, blobcas.PutMeta{
				Canonical: true,
			})
			if err != nil {
				t.Fatalf("Put failed: %v", err)
			}

			// Resolve
			resolvedRef, err := s.Resolve(ctx, key)
			if err != nil {
				t.Fatalf("Resolve failed: %v", err)
			}
			if resolvedRef.ManifestCID.Bytes == nil {
				t.Fatal("expected valid CID")
			}

			// Get
			rc, info, err := s.Get(ctx, ref)
			if err != nil {
				t.Fatalf("Get failed: %v", err)
			}
			defer rc.Close()

			if info.Length != uint64(len(content)) {
				t.Errorf("expected length %d, got %d", len(content), info.Length)
			}

			got, err := io.ReadAll(rc)
			if err != nil {
				t.Fatalf("io.ReadAll failed: %v", err)
			}
			if !bytes.Equal(got, content) {
				t.Errorf("content mismatch")
			}

			// Stat
			stat, err := s.Stat(ctx, ref)
			if err != nil {
				t.Fatalf("Stat failed: %v", err)
			}
			if stat.Length != uint64(len(content)) {
				t.Errorf("stat length %d != expected %d", stat.Length, len(content))
			}
		})
	}
}

func TestStoreIntegration_RetentionPrecedence(t *testing.T) {
	dir, err := os.MkdirTemp("", "blobcas-store-integration-retention")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s, _ := createTestStore(t, dir)
	defer s.Close()
	ctx := context.Background()

	t.Run("Conflicts", func(t *testing.T) {
		key := core.Key{Namespace: "test", ID: "conflict"}
		deadline := time.Now().Add(24 * time.Hour)
		ttl := 48 * time.Hour

		// In actual implementation if both are set we should reject, let's verify our code does this
		// Wait! Our store logic actually prefers Deadline over TTL in `computeDeadline` without an error.
		// The SPEC requirement says "Both set: ErrInvalidInput." We need to fix that first!
		// Let's assume we will fix the store code right after this test to return ErrInvalidInput.
		_, err := s.Put(ctx, key, blobcas.PutInput{
			Canonical: bytes.NewReader([]byte("test")),
		}, blobcas.PutMeta{
			Canonical:    true,
			RootDeadline: &deadline,
			RootTTL:      &ttl,
		})

		if err == nil || !errors.Is(err, core.ErrInvalidInput) {
			t.Errorf("expected ErrInvalidInput when both RootDeadline and RootTTL are set, got: %v", err)
		}
	})
}
