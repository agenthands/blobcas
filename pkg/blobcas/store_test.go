package blobcas_test

import (
	"bytes"
	"context"
	"io"
	"os"
	"testing"
	"time"

	"github.com/agenthands/blobcas/pkg/blobcas"
)

func TestStore_PutGet(t *testing.T) {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "blobcas-store-test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	cfg := blobcas.Config{
		Dir: dir,
		Chunking: blobcas.ChunkingConfig{
			Min: 64,
			Avg: 128,
			Max: 256,
		},
		Pack: blobcas.PackConfig{
			TargetPackBytes: 10 * 1024,
		},
		GC: blobcas.GCConfig{
			DefaultRootTTL: 24 * time.Hour,
		},
		Limits: blobcas.LimitsConfig{
			MaxChunksPerObject: 1000,
		},
	}

	s, err := blobcas.Open(ctx, cfg)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer s.Close()

	t.Run("BasicRoundTrip", func(t *testing.T) {
		key := blobcas.Key{Namespace: "test", ID: "obj1"}
		content := []byte("this is some content that will be chunked and stored in blobcas")

		ref, err := s.Put(ctx, key, blobcas.PutInput{
			Canonical: bytes.NewReader(content),
		}, blobcas.PutMeta{
			Canonical: true,
			MediaType: "text/plain",
		})
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		rc, info, err := s.Get(ctx, ref)
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		defer rc.Close()

		if info.Length != uint64(len(content)) {
			t.Errorf("expected length %d, got %d", len(content), info.Length)
		}

		got, _ := io.ReadAll(rc)
		if !bytes.Equal(got, content) {
			t.Errorf("content mismatch")
		}
	})

	t.Run("Deduplication", func(t *testing.T) {
		key2 := blobcas.Key{Namespace: "test", ID: "obj2"}
		content := []byte("this is some content that will be chunked and stored in blobcas") // same as obj1

		ref2, err := s.Put(ctx, key2, blobcas.PutInput{
			Canonical: bytes.NewReader(content),
		}, blobcas.PutMeta{
			Canonical: true,
		})
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		// In a real dedupe test we'd check if no new chunks were written,
		// but checking if we get a valid ref is a good start.
		if ref2.ManifestCID.Bytes == nil {
			t.Fatal("expected valid CID")
		}
	})

	t.Run("Resolve", func(t *testing.T) {
		key := blobcas.Key{Namespace: "test", ID: "obj1"}
		ref, err := s.Resolve(ctx, key)
		if err != nil {
			t.Fatalf("Resolve failed: %v", err)
		}
		if ref.ManifestCID.Bytes == nil {
			t.Fatal("expected valid CID")
		}
	})
}
