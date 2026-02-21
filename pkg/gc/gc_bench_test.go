package gc_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/agenthands/blobcas/internal/testkit"
	"github.com/agenthands/blobcas/pkg/blobcas"
	"github.com/agenthands/blobcas/pkg/core"
	"github.com/agenthands/blobcas/pkg/gc"
)

func BenchmarkGCCompaction(b *testing.B) {
	dir, err := os.MkdirTemp("", "blobcas-bench-gc")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	cfg := core.Config{
		Dir: dir,
		Chunking: core.ChunkingConfig{
			Min: 64 * 1024,
			Avg: 128 * 1024,
			Max: 256 * 1024,
		},
		Pack: core.PackConfig{
			Dir:             dir + "/packs",
			TargetPackBytes: 4 * 1024 * 1024,
		},
		Catalog: core.CatalogConfig{
			Dir: dir + "/catalog",
		},
		GC: core.GCConfig{
			Enabled: true,
		},
	}

	s, err := blobcas.Open(context.Background(), cfg)
	if err != nil {
		b.Fatal(err)
	}

	ctx := context.Background()
	rng := testkit.RNG(1)

	// Create ~500 items, 50% live, 50% dead
	for i := 0; i < 500; i++ {
		data := testkit.RandomBytes(rng, 128*1024)
		key := core.Key{Namespace: "bench", ID: fmt.Sprintf("blob_%d", i)}

		var putMeta blobcas.PutMeta
		putMeta.Canonical = true

		if i%2 == 0 {
			ttl := 24 * time.Hour
			putMeta.RootTTL = &ttl
		} else {
			dead := time.Now().Add(-1 * time.Hour)
			putMeta.RootDeadline = &dead
		}

		_, _ = s.Put(ctx, key, blobcas.PutInput{
			Canonical: bytes.NewReader(data),
		}, putMeta)
	}

	s.Close()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Run GC one time on the repo.
		// Note: running GC multiple times without repopulating data means
		// subsequent runs find no dead data. That's a bit skewed for b.N > 1.
		// But it measures the cost of Mark across the whole catalog.
		openGCSuiteAndRun(b, cfg.Pack.Dir, cfg.Catalog.Dir, func(runner gc.Runner) {
			b.StartTimer()
			_, _ = runner.RunOnce(ctx)
			b.StopTimer()
		})
	}
}
