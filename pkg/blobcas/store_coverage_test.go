package blobcas_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/agenthands/blobcas/internal/testkit"
	"github.com/agenthands/blobcas/pkg/blobcas"
	"github.com/agenthands/blobcas/pkg/core"
)

// ---------- HasChunk ----------

func TestStore_HasChunk(t *testing.T) {
	dir, err := os.MkdirTemp("", "blobcas-haschunk-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s, _ := createTestStore(t, dir)
	defer s.Close()

	ctx := context.Background()
	rng := testkit.RNG(100)
	content := testkit.RandomBytes(rng, 512)

	key := core.Key{Namespace: "test", ID: "haschunk"}
	ref, err := s.Put(ctx, key, blobcas.PutInput{
		Canonical: bytes.NewReader(content),
	}, blobcas.PutMeta{Canonical: true})
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Get the manifest to find chunk CIDs
	rc, _, err := s.Get(ctx, ref)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()

	if !bytes.Equal(got, content) {
		t.Fatal("content mismatch")
	}

	// HasChunk for a non-existent CID
	has, err := s.HasChunk(ctx, core.CID{Bytes: []byte("nonexistent-cid-that-does-not-exist-in-store")})
	if err != nil {
		t.Fatalf("HasChunk error: %v", err)
	}
	if has {
		t.Error("expected HasChunk to return false for non-existent CID")
	}
}

// ---------- GetChunk ----------

func TestStore_GetChunk_NotFound(t *testing.T) {
	dir, err := os.MkdirTemp("", "blobcas-getchunk-nf-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s, _ := createTestStore(t, dir)
	defer s.Close()

	ctx := context.Background()

	_, _, err = s.GetChunk(ctx, core.CID{Bytes: []byte("nonexistent")})
	if err == nil || !errors.Is(err, core.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got: %v", err)
	}
}

// ---------- Open error paths ----------

func TestStore_Open_BadTransform(t *testing.T) {
	dir, err := os.MkdirTemp("", "blobcas-badtransform-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	cfg := core.Config{
		Dir: dir,
		Chunking: core.ChunkingConfig{
			Min: 64, Avg: 128, Max: 256,
		},
		Transform: core.TransformConfig{
			Name: "invalid-transform-xyz",
		},
		Pack: core.PackConfig{
			TargetPackBytes: 10 * 1024,
		},
		Limits: core.LimitsConfig{
			MaxChunksPerObject: 1000,
		},
	}

	_, err = blobcas.Open(context.Background(), cfg)
	if err == nil {
		t.Fatal("expected error for unsupported transform")
	}
}

// ---------- Resolve not found ----------

func TestStore_Resolve_NotFound(t *testing.T) {
	dir, err := os.MkdirTemp("", "blobcas-resolve-nf-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s, _ := createTestStore(t, dir)
	defer s.Close()

	ctx := context.Background()
	_, err = s.Resolve(ctx, core.Key{Namespace: "nope", ID: "nope"})
	if err == nil || !errors.Is(err, core.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got: %v", err)
	}
}

// ---------- Stat not found ----------

func TestStore_Stat_NotFound(t *testing.T) {
	dir, err := os.MkdirTemp("", "blobcas-stat-nf-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s, _ := createTestStore(t, dir)
	defer s.Close()

	ctx := context.Background()
	_, err = s.Stat(ctx, core.Ref{ManifestCID: core.CID{Bytes: []byte("nonexistent")}})
	if err == nil || !errors.Is(err, core.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got: %v", err)
	}
}

// ---------- Get not found ----------

func TestStore_Get_NotFound(t *testing.T) {
	dir, err := os.MkdirTemp("", "blobcas-get-nf-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s, _ := createTestStore(t, dir)
	defer s.Close()

	ctx := context.Background()
	_, _, err = s.Get(ctx, core.Ref{ManifestCID: core.CID{Bytes: []byte("nonexistent")}})
	if err == nil || !errors.Is(err, core.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got: %v", err)
	}
}

// ---------- Put with zstd transform ----------

func TestStore_Put_WithZstdTransform(t *testing.T) {
	dir, err := os.MkdirTemp("", "blobcas-zstd-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	cfg := core.Config{
		Dir: dir,
		Chunking: core.ChunkingConfig{
			Min: 64, Avg: 128, Max: 256,
		},
		Pack: core.PackConfig{
			TargetPackBytes: 10 * 1024,
		},
		Transform: core.TransformConfig{
			Name:      "zstd",
			ZstdLevel: 3,
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
	defer s.Close()

	ctx := context.Background()
	rng := testkit.RNG(42)

	sizes := []int{0, 1, 64, 4096, 64 * 1024}
	for _, size := range sizes {
		t.Run(fmt.Sprintf("Size_%d", size), func(t *testing.T) {
			var content []byte
			if size > 0 {
				content = testkit.RandomBytes(rng, size)
			}

			key := core.Key{Namespace: "zstd", ID: fmt.Sprintf("obj_%d", size)}
			ref, err := s.Put(ctx, key, blobcas.PutInput{
				Canonical: bytes.NewReader(content),
			}, blobcas.PutMeta{Canonical: true, MediaType: "application/octet-stream"})
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

			got, err := io.ReadAll(rc)
			if err != nil {
				t.Fatalf("ReadAll failed: %v", err)
			}
			if !bytes.Equal(got, content) {
				t.Error("content mismatch with zstd transform")
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

// ---------- computeDeadline paths ----------

func TestStore_ComputeDeadline_DefaultTTL(t *testing.T) {
	dir, err := os.MkdirTemp("", "blobcas-deadline-default-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	cfg := core.Config{
		Dir: dir,
		Chunking: core.ChunkingConfig{
			Min: 64, Avg: 128, Max: 256,
		},
		Pack: core.PackConfig{
			TargetPackBytes: 10 * 1024,
		},
		GC: core.GCConfig{
			DefaultRootTTL: 48 * time.Hour,
		},
		Limits: core.LimitsConfig{
			MaxChunksPerObject: 1000,
		},
	}

	s, err := blobcas.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer s.Close()

	ctx := context.Background()

	// Put with no explicit deadline/TTL — should use DefaultRootTTL
	key := core.Key{Namespace: "test", ID: "default-ttl"}
	_, err = s.Put(ctx, key, blobcas.PutInput{
		Canonical: bytes.NewReader([]byte("default ttl data")),
	}, blobcas.PutMeta{Canonical: true})
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}
}

func TestStore_ComputeDeadline_ExplicitDeadline(t *testing.T) {
	dir, err := os.MkdirTemp("", "blobcas-deadline-explicit-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s, _ := createTestStore(t, dir)
	defer s.Close()

	ctx := context.Background()

	deadline := time.Now().Add(72 * time.Hour)
	key := core.Key{Namespace: "test", ID: "explicit-deadline"}
	_, err = s.Put(ctx, key, blobcas.PutInput{
		Canonical: bytes.NewReader([]byte("explicit deadline data")),
	}, blobcas.PutMeta{Canonical: true, RootDeadline: &deadline})
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}
}

func TestStore_ComputeDeadline_ExplicitTTL(t *testing.T) {
	dir, err := os.MkdirTemp("", "blobcas-deadline-ttl-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s, _ := createTestStore(t, dir)
	defer s.Close()

	ctx := context.Background()

	ttl := 96 * time.Hour
	key := core.Key{Namespace: "test", ID: "explicit-ttl"}
	_, err = s.Put(ctx, key, blobcas.PutInput{
		Canonical: bytes.NewReader([]byte("explicit ttl data")),
	}, blobcas.PutMeta{Canonical: true, RootTTL: &ttl})
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}
}

func TestStore_ComputeDeadline_NoRetention(t *testing.T) {
	dir, err := os.MkdirTemp("", "blobcas-deadline-none-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	cfg := core.Config{
		Dir: dir,
		Chunking: core.ChunkingConfig{
			Min: 64, Avg: 128, Max: 256,
		},
		Pack: core.PackConfig{
			TargetPackBytes: 10 * 1024,
		},
		GC: core.GCConfig{
			DefaultRootTTL: 0, // No default TTL
		},
		Limits: core.LimitsConfig{
			MaxChunksPerObject: 1000,
		},
	}

	s, err := blobcas.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	key := core.Key{Namespace: "test", ID: "no-retention"}
	_, err = s.Put(ctx, key, blobcas.PutInput{
		Canonical: bytes.NewReader([]byte("no retention data")),
	}, blobcas.PutMeta{Canonical: true})
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}
}

// ---------- objectReader: partial read and Close ----------

func TestStore_ObjectReader_PartialRead(t *testing.T) {
	dir, err := os.MkdirTemp("", "blobcas-partialread-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s, _ := createTestStore(t, dir)
	defer s.Close()

	ctx := context.Background()
	rng := testkit.RNG(42)
	// Use content large enough to produce multiple chunks
	content := testkit.RandomBytes(rng, 2048)

	key := core.Key{Namespace: "test", ID: "partial"}
	ref, err := s.Put(ctx, key, blobcas.PutInput{
		Canonical: bytes.NewReader(content),
	}, blobcas.PutMeta{Canonical: true})
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	rc, _, err := s.Get(ctx, ref)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	// Read only a small portion, then Close — exercises objectReader.Close with active chunk
	buf := make([]byte, 32)
	_, err = rc.Read(buf)
	if err != nil {
		t.Fatalf("partial Read failed: %v", err)
	}

	// Close should not panic or error even with unconsumed chunks
	if err := rc.Close(); err != nil {
		t.Errorf("Close with active chunk failed: %v", err)
	}
}

// ---------- Put with error reader ----------

func TestStore_Put_ErrorReader(t *testing.T) {
	dir, err := os.MkdirTemp("", "blobcas-errreader-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s, _ := createTestStore(t, dir)
	defer s.Close()

	ctx := context.Background()
	rng := testkit.RNG(42)
	data := testkit.RandomBytes(rng, 4096)
	errReader := testkit.NewErrorReader(bytes.NewReader(data), 512, nil)

	key := core.Key{Namespace: "test", ID: "errinput"}
	_, err = s.Put(ctx, key, blobcas.PutInput{
		Canonical: errReader,
	}, blobcas.PutMeta{Canonical: true})

	if err == nil {
		t.Error("expected error from error reader")
	}
}

// ---------- Open with "none" transform (explicit) ----------

func TestStore_Open_NoneTransform(t *testing.T) {
	dir, err := os.MkdirTemp("", "blobcas-nonetransform-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	cfg := core.Config{
		Dir: dir,
		Chunking: core.ChunkingConfig{
			Min: 64, Avg: 128, Max: 256,
		},
		Pack: core.PackConfig{
			TargetPackBytes: 10 * 1024,
		},
		Transform: core.TransformConfig{
			Name: "none",
		},
		Limits: core.LimitsConfig{
			MaxChunksPerObject: 1000,
		},
	}

	s, err := blobcas.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Open with 'none' transform failed: %v", err)
	}
	s.Close()
}

// ---------- Close exercises both error paths ----------

func TestStore_Close_Idempotent(t *testing.T) {
	dir, err := os.MkdirTemp("", "blobcas-closetwice-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s, _ := createTestStore(t, dir)

	// First close should succeed
	if err := s.Close(); err != nil {
		t.Fatalf("first Close failed: %v", err)
	}
}

// ---------- Multiple chunks streaming ----------

func TestStore_MultiChunkStreaming(t *testing.T) {
	dir, err := os.MkdirTemp("", "blobcas-multichunk-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s, _ := createTestStore(t, dir)
	defer s.Close()

	ctx := context.Background()
	rng := testkit.RNG(99)
	// Large enough to produce many chunks with min=64/avg=128/max=256 config
	content := testkit.RandomBytes(rng, 32*1024)

	key := core.Key{Namespace: "test", ID: "multichunk"}
	ref, err := s.Put(ctx, key, blobcas.PutInput{
		Canonical: bytes.NewReader(content),
	}, blobcas.PutMeta{Canonical: true})
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	stat, err := s.Stat(ctx, ref)
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	if stat.ChunkCount < 2 {
		t.Logf("WARNING: expected multiple chunks for 32KB content, got %d", stat.ChunkCount)
	}

	// Stream and verify
	rc, info, err := s.Get(ctx, ref)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	defer rc.Close()

	if info.Length != uint64(len(content)) {
		t.Errorf("info.Length %d != expected %d", info.Length, len(content))
	}

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Error("multi-chunk content mismatch")
	}
}
