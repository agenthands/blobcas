package blobcas_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"os"

	"github.com/agenthands/blobcas/internal/testkit"
	"github.com/agenthands/blobcas/pkg/blobcas"
	"github.com/agenthands/blobcas/pkg/core"
	"github.com/agenthands/blobcas/pkg/pack"
)

func TestStoreIntegration_Deduplication(t *testing.T) {
	dir, err := os.MkdirTemp("", "blobcas-store-integration-dedupe")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s, cfg := createTestStore(t, dir)

	ctx := context.Background()
	r := testkit.RNG(1)

	// Generate a 4 MiB base blob
	base := testkit.RandomBytes(r, 4*1024*1024)

	// Create 5 variants with 10 mutations each
	variants := make([][]byte, 5)
	for i := range variants {
		variants[i] = testkit.MutateBytes(r, base, 10)
	}

	for i, v := range variants {
		key := core.Key{Namespace: "test", ID: fmt.Sprintf("variant_%d", i)}
		_, err := s.Put(ctx, key, blobcas.PutInput{
			Canonical: bytes.NewReader(v),
		}, blobcas.PutMeta{Canonical: true})
		if err != nil {
			t.Fatalf("Put variant %d failed: %v", i, err)
		}
	}

	// We assume FastCDC with 64-128-256 chunking config.
	// 4 MiB / 128 = ~32768 chunks per file.
	// We put 5 variants. Total raw chunks = ~163,840.
	// Since mutations are few, we expect unique chunks to be substantially lower.

	// Use testkit to count unique blocks in packs. Since store.go's Close() doesn't
	// expose the pack manager, we'll manually inspect via a separate pack manager open.
	s.Close() // Flush and seal

	pm, err := packManagerForDir(cfg.Pack.Dir)
	if err != nil {
		t.Fatal(err)
	}
	defer pm.Close()

	unique, err := testkit.CountUniqueBlocks(ctx, pm)
	if err != nil {
		t.Fatal(err)
	}

	// With extremely conservative estimates, even 5 completely different files would be exactly 163,840 unique blocks.
	// If deduplication works with FastCDC, it should be well under 40,000 unique blocks.
	t.Logf("Unique blocks across 5 mutated variants: %d", unique)
	if unique > 40000 {
		t.Errorf("deduplication failed or ineffective, expected < 40000 unique chunks, got %d", unique)
	}
}

func TestStoreIntegration_RepeatedKeys(t *testing.T) {
	dir, err := os.MkdirTemp("", "blobcas-store-integration-repeated")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s, cfg := createTestStore(t, dir)

	ctx := context.Background()
	content := []byte("repeated content")

	for i := 0; i < 100; i++ {
		key := core.Key{Namespace: "test", ID: "same-key"}
		_, err := s.Put(ctx, key, blobcas.PutInput{
			Canonical: bytes.NewReader(content),
		}, blobcas.PutMeta{Canonical: true})
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	s.Close() // Flush and seal before opening pack manager manually

	pm, err := packManagerForDir(cfg.Pack.Dir)
	if err != nil {
		t.Fatal(err)
	}
	defer pm.Close()

	unique, _ := testkit.CountUniqueBlocks(ctx, pm)

	// We expect 1 chunk block + 1 manifest block = 2 blocks total.
	if unique != 2 {
		t.Errorf("expected exactly 2 physical blocks written for repeated identical puts, got %d", unique)
	}
}

// helper to temporarily open pack manager for assertions
func packManagerForDir(dir string) (pack.Manager, error) {
	return pack.NewManager(core.PackConfig{Dir: dir})
}
