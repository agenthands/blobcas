package blobcas_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/agenthands/blobcas/internal/testkit"
	"github.com/agenthands/blobcas/pkg/blobcas"
	"github.com/agenthands/blobcas/pkg/core"
)

func BenchmarkStorePutGet(b *testing.B) {
	dir, err := os.MkdirTemp("", "blobcas-bench-store")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s, _ := createBenchStore(b, dir)
	defer s.Close()

	rng := testkit.RNG(42)
	sizes := []int{64 * 1024, 1 * 1024 * 1024, 16 * 1024 * 1024}
	dedupeRates := []int{0, 50, 90}

	for _, size := range sizes {
		// Generate base data
		baseData := testkit.RandomBytes(rng, size)

		for _, dedupe := range dedupeRates {
			b.Run(fmt.Sprintf("Size_%dMB_Dedupe_%d", size/(1024*1024), dedupe), func(b *testing.B) {
				b.ResetTimer()
				b.ReportAllocs()
				b.SetBytes(int64(size))

				ctx := context.Background()

				for i := 0; i < b.N; i++ {
					b.StopTimer() // pause timer for payload generation
					var payload []byte
					if rng.Intn(100) < dedupe {
						// highly duplicated, small mutations
						payload = testkit.MutateBytes(rng, baseData, 10)
					} else {
						// completely new data
						payload = testkit.RandomBytes(rng, size)
					}
					key := core.Key{Namespace: "bench", ID: fmt.Sprintf("req_%d", i)}
					b.StartTimer()

					ref, err := s.Put(ctx, key, blobcas.PutInput{
						Canonical: bytes.NewReader(payload),
					}, blobcas.PutMeta{Canonical: true})

					if err != nil {
						b.Fatalf("Put failed: %v", err)
					}

					// Verify ability to read it back
					rc, _, err := s.Get(ctx, ref)
					if err != nil {
						b.Fatalf("Get failed: %v", err)
					}

					// stream it out to measure combined throughput
					_, err = io.Copy(io.Discard, rc)
					rc.Close()
					if err != nil {
						b.Fatalf("Read copy failed: %v", err)
					}
				}
			})
		}
	}
}

// Ensure compatibility with bench test by mocking createBenchStore locally
func createBenchStore(t testing.TB, dir string) (blobcas.Store, core.Config) {
	cfg := core.Config{
		Dir: dir,
		Chunking: core.ChunkingConfig{
			Min: 64 * 1024,
			Avg: 256 * 1024,
			Max: 1024 * 1024,
		},
		Pack: core.PackConfig{
			Dir:             dir + "/packs",
			TargetPackBytes: 256 * 1024 * 1024,
		},
		Catalog: core.CatalogConfig{
			Dir: dir + "/catalog",
		},
		Transform: core.TransformConfig{
			Name: "none",
		},
		GC: core.GCConfig{
			Enabled: false,
		},
		Limits: core.LimitsConfig{
			MaxChunksPerObject: 100000,
		},
	}

	s, err := blobcas.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	return s, cfg
}
