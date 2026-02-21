package blobcas_test

import (
	"bytes"
	"context"
	"io"
	"os"
	"testing"

	"github.com/agenthands/blobcas/internal/testkit"
	"github.com/agenthands/blobcas/pkg/blobcas"
	"github.com/agenthands/blobcas/pkg/core"
)

// ---------- Open error paths ----------

func TestStore_Open_BadCatalogDir(t *testing.T) {
	// Use a file path (not a directory) as catalog dir to trigger catalog.Open error
	tmpFile, err := os.CreateTemp("", "blobcas-fake-catalog-*")
	if err != nil {
		t.Fatal(err)
	}
	tmpFile.Write([]byte("not a catalog"))
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	cfg := core.Config{
		Dir: t.TempDir(),
		Chunking: core.ChunkingConfig{
			Min: 64, Avg: 128, Max: 256,
		},
		Catalog: core.CatalogConfig{
			Dir: tmpFile.Name(), // file, not dir
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
		t.Fatal("expected error for bad catalog directory")
	}
}

func TestStore_Open_BadPackDir(t *testing.T) {
	// Create a valid catalog dir first, then give an invalid pack dir
	catDir := t.TempDir()

	cfg := core.Config{
		Dir: t.TempDir(),
		Chunking: core.ChunkingConfig{
			Min: 64, Avg: 128, Max: 256,
		},
		Pack: core.PackConfig{
			Dir:             "/dev/null/impossible/path", // can't mkdir under /dev/null
			TargetPackBytes: 10 * 1024,
		},
		Catalog: core.CatalogConfig{
			Dir: catDir,
		},
		Limits: core.LimitsConfig{
			MaxChunksPerObject: 1000,
		},
	}

	_, err := blobcas.Open(context.Background(), cfg)
	if err == nil {
		t.Fatal("expected error for impossible pack directory")
	}
}

// ---------- GetChunk with valid CID ----------

func TestStore_GetChunk_Success(t *testing.T) {
	dir := t.TempDir()
	s, _ := createTestStore(t, dir)
	defer s.Close()

	ctx := context.Background()
	rng := testkit.RNG(42)
	content := testkit.RandomBytes(rng, 512)

	key := core.Key{Namespace: "test", ID: "getchunk-ok"}
	ref, err := s.Put(ctx, key, blobcas.PutInput{
		Canonical: bytes.NewReader(content),
	}, blobcas.PutMeta{Canonical: true})
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Get manifest to find chunk CIDs
	rc, _, err := s.Get(ctx, ref)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	gotAll, _ := io.ReadAll(rc)
	rc.Close()

	if !bytes.Equal(gotAll, content) {
		t.Fatal("content mismatch")
	}

	// Stat to get chunk count
	stat, err := s.Stat(ctx, ref)
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	t.Logf("ChunkCount: %d", stat.ChunkCount)
}

// ---------- Multiple Get/Stat on same ref ----------

func TestStore_GetAndStat_SameRef(t *testing.T) {
	dir := t.TempDir()
	s, _ := createTestStore(t, dir)
	defer s.Close()

	ctx := context.Background()
	content := []byte("test data for get and stat")

	key := core.Key{Namespace: "test", ID: "getstat"}
	ref, err := s.Put(ctx, key, blobcas.PutInput{
		Canonical: bytes.NewReader(content),
	}, blobcas.PutMeta{Canonical: true})
	if err != nil {
		t.Fatal(err)
	}

	// Get
	rc, info, err := s.Get(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if !bytes.Equal(got, content) {
		t.Error("Get content mismatch")
	}
	if info.Length != uint64(len(content)) {
		t.Errorf("length mismatch: %d vs %d", info.Length, len(content))
	}

	// Stat
	stat, err := s.Stat(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if stat.Length != uint64(len(content)) {
		t.Errorf("stat length mismatch: %d vs %d", stat.Length, len(content))
	}
	if !stat.Canonical {
		t.Error("expected canonical=true")
	}
}
