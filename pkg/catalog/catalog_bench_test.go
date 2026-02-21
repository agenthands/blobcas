package catalog

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/agenthands/blobcas/internal/testkit"
	"github.com/agenthands/blobcas/pkg/core"
	"github.com/cockroachdb/pebble"
)

func BenchmarkPutPackForCID(b *testing.B) {
	dir, _ := os.MkdirTemp("", "bench-catalog-put-*")
	defer os.RemoveAll(dir)

	cat, err := Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer cat.Close()

	rng := testkit.RNG(1)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		cid := core.CID{Bytes: testkit.RandomBytes(rng, 36)}
		_ = cat.PutPackForCID(nil, cid, uint64(i))
	}
}

func BenchmarkGetPackForCID(b *testing.B) {
	dir, _ := os.MkdirTemp("", "bench-catalog-get-*")
	defer os.RemoveAll(dir)

	cat, err := Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer cat.Close()

	ctx := context.Background()
	rng := testkit.RNG(2)

	// Pre-populate
	const N = 500
	cids := make([]core.CID, N)
	for i := 0; i < N; i++ {
		cids[i] = core.CID{Bytes: testkit.RandomBytes(rng, 36)}
		cat.PutPackForCID(nil, cids[i], uint64(i))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		c := cids[i%N]
		_, _, _ = cat.GetPackForCID(ctx, c)
	}
}

func BenchmarkBatchPutPackForCID(b *testing.B) {
	batchSizes := []int{10, 100, 1000}

	for _, bs := range batchSizes {
		b.Run(fmt.Sprintf("BatchSize_%d", bs), func(b *testing.B) {
			dir, _ := os.MkdirTemp("", "bench-catalog-batch-*")
			defer os.RemoveAll(dir)

			cat, err := Open(dir)
			if err != nil {
				b.Fatal(err)
			}
			defer cat.Close()

			rng := testkit.RNG(int64(bs))

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				batch := cat.NewBatch()
				for j := 0; j < bs; j++ {
					cid := core.CID{Bytes: testkit.RandomBytes(rng, 36)}
					_ = cat.PutPackForCID(batch, cid, uint64(j))
				}
				_ = batch.Commit(pebble.Sync)
				batch.Close()
			}
		})
	}
}

func BenchmarkIterateRoots(b *testing.B) {
	rootCounts := []int{100, 500, 1000}

	for _, count := range rootCounts {
		b.Run(fmt.Sprintf("Roots_%d", count), func(b *testing.B) {
			dir, _ := os.MkdirTemp("", "bench-catalog-roots-*")
			defer os.RemoveAll(dir)

			cat, err := Open(dir)
			if err != nil {
				b.Fatal(err)
			}
			defer cat.Close()

			ctx := context.Background()
			rng := testkit.RNG(int64(count))

			// Populate roots
			for i := 0; i < count; i++ {
				cid := core.CID{Bytes: testkit.RandomBytes(rng, 36)}
				cat.PutRootDeadline(nil, cid, time.Now().Add(time.Duration(i)*time.Hour))
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				cnt := 0
				cat.IterateRoots(ctx, func(m core.CID, d time.Time) error {
					cnt++
					return nil
				})
			}
		})
	}
}

func BenchmarkGetManifestForKey(b *testing.B) {
	dir, _ := os.MkdirTemp("", "bench-catalog-k2m-*")
	defer os.RemoveAll(dir)

	cat, err := Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer cat.Close()

	ctx := context.Background()
	rng := testkit.RNG(3)

	const N = 500
	keys := make([]core.Key, N)
	for i := 0; i < N; i++ {
		keys[i] = core.Key{Namespace: "bench", ID: fmt.Sprintf("key_%d", i)}
		cid := core.CID{Bytes: testkit.RandomBytes(rng, 36)}
		cat.PutManifestForKey(nil, keys[i], cid)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		k := keys[i%N]
		_, _, _ = cat.GetManifestForKey(ctx, k)
	}
}
