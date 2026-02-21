package transform

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/agenthands/blobcas/internal/testkit"
)

func BenchmarkTransformZstd(b *testing.B) {
	tr := NewZstd(3)
	rng := testkit.RNG(42)

	sizes := []int{4 * 1024, 64 * 1024, 1024 * 1024}
	datasets := []struct {
		name string
		gen  func(*rand.Rand, int) []byte
	}{
		{"Random", testkit.RandomBytes},
		{"Compressible", testkit.CompressibleBytes},
	}

	for _, ds := range datasets {
		for _, size := range sizes {
			b.Run(fmt.Sprintf("%s_%d", ds.name, size), func(b *testing.B) {
				data := ds.gen(rng, size)

				b.ResetTimer()
				b.ReportAllocs()
				b.SetBytes(int64(size))

				for i := 0; i < b.N; i++ {
					encoded, _ := tr.Encode(data)
					_, _ = tr.Decode(encoded)
				}
			})
		}
	}
}
