package pack_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/agenthands/blobcas/internal/testkit"
	"github.com/agenthands/blobcas/pkg/cidutil"
	"github.com/agenthands/blobcas/pkg/core"
	"github.com/agenthands/blobcas/pkg/pack"
)

func BenchmarkPutBlock(b *testing.B) {
	sizes := []int{256, 4096, 64 * 1024, 256 * 1024}

	for _, sz := range sizes {
		b.Run(fmt.Sprintf("BlockSize_%d", sz), func(b *testing.B) {
			dir, _ := os.MkdirTemp("", "bench-put-*")
			defer os.RemoveAll(dir)

			cidBuilder := cidutil.NewBuilder()
			rng := testkit.RNG(int64(sz))
			ctx := context.Background()

			pm, err := pack.NewManager(core.PackConfig{
				Dir:             dir,
				TargetPackBytes: 256 * 1024 * 1024, // large so no rotation during bench
			})
			if err != nil {
				b.Fatal(err)
			}
			defer pm.Close()

			// Pre-generate unique blocks
			type block struct {
				cid  core.CID
				data []byte
			}
			blocks := make([]block, b.N)
			for i := range blocks {
				data := testkit.RandomBytes(rng, sz)
				cid, _ := cidBuilder.ChunkCID(data)
				blocks[i] = block{cid, data}
			}

			b.ResetTimer()
			b.SetBytes(int64(sz))
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				_, err := pm.PutBlock(ctx, blocks[i].cid, blocks[i].data)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkGetBlock(b *testing.B) {
	sizes := []int{256, 4096, 64 * 1024}

	for _, sz := range sizes {
		b.Run(fmt.Sprintf("BlockSize_%d", sz), func(b *testing.B) {
			dir, _ := os.MkdirTemp("", "bench-get-*")
			defer os.RemoveAll(dir)

			cidBuilder := cidutil.NewBuilder()
			rng := testkit.RNG(int64(sz + 1))
			ctx := context.Background()

			pm, err := pack.NewManager(core.PackConfig{
				Dir:             dir,
				TargetPackBytes: 256 * 1024 * 1024,
			})
			if err != nil {
				b.Fatal(err)
			}
			defer pm.Close()

			// Pre-populate
			const numBlocks = 100
			type stored struct {
				cid    core.CID
				packID uint64
			}
			entries := make([]stored, numBlocks)
			for i := 0; i < numBlocks; i++ {
				data := testkit.RandomBytes(rng, sz)
				cid, _ := cidBuilder.ChunkCID(data)
				pid, _ := pm.PutBlock(ctx, cid, data)
				entries[i] = stored{cid, pid}
			}

			b.ResetTimer()
			b.SetBytes(int64(sz))
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				e := entries[i%numBlocks]
				_, err := pm.GetBlock(ctx, e.packID, e.cid)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkIteratePackBlocks(b *testing.B) {
	blockCounts := []int{10, 100, 500}

	for _, count := range blockCounts {
		b.Run(fmt.Sprintf("Blocks_%d", count), func(b *testing.B) {
			dir, _ := os.MkdirTemp("", "bench-iterate-*")
			defer os.RemoveAll(dir)

			cidBuilder := cidutil.NewBuilder()
			rng := testkit.RNG(int64(count))
			ctx := context.Background()

			pm, err := pack.NewManager(core.PackConfig{
				Dir:             dir,
				TargetPackBytes: 256 * 1024 * 1024,
			})
			if err != nil {
				b.Fatal(err)
			}

			for i := 0; i < count; i++ {
				data := testkit.RandomBytes(rng, 1024)
				cid, _ := cidBuilder.ChunkCID(data)
				pm.PutBlock(ctx, cid, data)
			}

			packID := pm.CurrentPackID()
			pm.SealActivePack(ctx)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				pm.IteratePackBlocks(ctx, packID, func(c core.CID) error {
					return nil
				})
			}

			b.StopTimer()
			pm.Close()
		})
	}
}

func BenchmarkSealAndRotate(b *testing.B) {
	dir, _ := os.MkdirTemp("", "bench-seal-*")
	defer os.RemoveAll(dir)

	cidBuilder := cidutil.NewBuilder()
	rng := testkit.RNG(42)
	ctx := context.Background()

	pm, err := pack.NewManager(core.PackConfig{
		Dir:             dir,
		TargetPackBytes: 64 * 1024 * 1024,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer pm.Close()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Write some data then seal
		for j := 0; j < 5; j++ {
			data := testkit.RandomBytes(rng, 1024)
			cid, _ := cidBuilder.ChunkCID(data)
			pm.PutBlock(ctx, cid, data)
		}
		pm.SealActivePack(ctx)
	}
}

func BenchmarkDiscovery(b *testing.B) {
	dir, _ := os.MkdirTemp("", "bench-discovery-*")
	defer os.RemoveAll(dir)

	cidBuilder := cidutil.NewBuilder()
	rng := testkit.RNG(99)
	ctx := context.Background()

	// Create 20 sealed packs
	pm, _ := pack.NewManager(core.PackConfig{
		Dir:             dir,
		TargetPackBytes: 256 * 1024 * 1024,
	})
	for i := 0; i < 20; i++ {
		for j := 0; j < 10; j++ {
			data := testkit.RandomBytes(rng, 512)
			cid, _ := cidBuilder.ChunkCID(data)
			pm.PutBlock(ctx, cid, data)
		}
		pm.SealActivePack(ctx)
	}
	pm.Close()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		pm2, err := pack.NewManager(core.PackConfig{
			Dir:             dir,
			TargetPackBytes: 256 * 1024 * 1024,
		})
		if err != nil {
			b.Fatal(err)
		}
		pm2.Close()
	}
}
