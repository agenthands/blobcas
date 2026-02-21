package blobcas_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/agenthands/blobcas/internal/testkit"
	"github.com/agenthands/blobcas/pkg/blobcas"
	"github.com/agenthands/blobcas/pkg/catalog"
	"github.com/agenthands/blobcas/pkg/core"
	"github.com/agenthands/blobcas/pkg/pack"
	"github.com/cockroachdb/pebble"
)

func BenchmarkReindex(b *testing.B) {
	dir, err := os.MkdirTemp("", "blobcas-bench-reindex")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// We'll prepare a repo with 10k physical chunks.
	s, cfg := createBenchStore(b, dir)
	ctx := context.Background()
	rng := testkit.RNG(42)

	// Insert ~1000 blobs of 256KB to generate lots of chunks
	for i := 0; i < 1000; i++ {
		key := core.Key{Namespace: "bench", ID: fmt.Sprintf("reblob_%d", i)}
		data := testkit.RandomBytes(rng, 256*1024)
		_, err := s.Put(ctx, key, blobcas.PutInput{
			Canonical: bytes.NewReader(data),
		}, blobcas.PutMeta{Canonical: true})
		if err != nil {
			b.Fatalf("setup Put %d failed: %v", i, err)
		}
	}

	s.Close() // flush and seal

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		// Wipe catalog for each run
		_ = os.RemoveAll(cfg.Catalog.Dir)
		_ = os.MkdirAll(cfg.Catalog.Dir, 0755)

		cat, _ := catalog.Open(cfg.Catalog.Dir)
		pm, _ := pack.NewManager(cfg.Pack)

		b.StartTimer()

		sealed := pm.ListSealedPacks()
		for _, pid := range sealed {
			batch := cat.NewBatch()
			_ = pm.IteratePackBlocks(ctx, pid, func(c core.CID) error {
				return cat.PutPackForCID(batch, c, pid)
			})
			_ = batch.Commit(pebble.Sync)
			batch.Close()
		}

		b.StopTimer()
		pm.Close()
		cat.Close()
	}
}
