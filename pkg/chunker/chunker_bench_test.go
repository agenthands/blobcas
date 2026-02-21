package chunker

import (
	"bytes"
	"context"
	"math/rand"
	"testing"

	"github.com/agenthands/blobcas/internal/testkit"
)

func BenchmarkChunker(b *testing.B) {
	cfg := Config{Min: 64 * 1024, Avg: 256 * 1024, Max: 1024 * 1024}
	c := NewChunker(cfg)

	datasets := []struct {
		name string
		gen  func(*rand.Rand, int) []byte
	}{
		{"Random", testkit.RandomBytes},
		{"Compressible", testkit.CompressibleBytes},
	}

	for _, ds := range datasets {
		b.Run(ds.name, func(b *testing.B) {
			rng := testkit.RNG(42)
			data := ds.gen(rng, 10*1024*1024) // 10 MiB payload

			b.ResetTimer()
			b.ReportAllocs()
			b.SetBytes(int64(len(data)))

			for i := 0; i < b.N; i++ {
				ctx := context.Background()
				chunks, _ := c.Split(ctx, bytes.NewReader(data))
				for range chunks {
					// drain
				}
			}
		})
	}
}
