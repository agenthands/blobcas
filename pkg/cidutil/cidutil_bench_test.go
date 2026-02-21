package cidutil

import (
	"fmt"
	"testing"

	"github.com/agenthands/blobcas/internal/testkit"
)

func BenchmarkCID(b *testing.B) {
	builder := NewBuilder()
	rng := testkit.RNG(1)

	sizes := []int{4 * 1024, 64 * 1024, 1024 * 1024, 8 * 1024 * 1024}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size_%d", size), func(b *testing.B) {
			data := testkit.RandomBytes(rng, size)

			b.ResetTimer()
			b.ReportAllocs()
			b.SetBytes(int64(size))

			for i := 0; i < b.N; i++ {
				_, _ = builder.ChunkCID(data)
			}
		})
	}
}
