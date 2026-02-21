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
	"github.com/agenthands/blobcas/pkg/catalog"
	"github.com/agenthands/blobcas/pkg/cidutil"
	"github.com/agenthands/blobcas/pkg/core"
	"github.com/agenthands/blobcas/pkg/gc"
	"github.com/agenthands/blobcas/pkg/manifest"
	"github.com/agenthands/blobcas/pkg/pack"
	"github.com/agenthands/blobcas/pkg/transform"
)

func TestGC_CompactionFractions(t *testing.T) {
	fractions := []int{10, 50, 90} // percent live

	for _, pct := range fractions {
		t.Run(fmt.Sprintf("%d_PercentLive", pct), func(t *testing.T) {
			dir, err := os.MkdirTemp("", fmt.Sprintf("blobcas-gc-compaction-%d", pct))
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(dir)

			cfg := core.Config{
				Dir: dir,
				Chunking: core.ChunkingConfig{
					Min: 64,
					Avg: 128,
					Max: 256,
				},
				Pack: core.PackConfig{
					Dir:             dir + "/packs",
					TargetPackBytes: 64 * 1024, // small to ensure many packs
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
				t.Fatal(err)
			}

			ctx := context.Background()
			rng := testkit.RNG(int64(pct))

			const numBlobs = 100
			var liveKeys []core.Key
			var deadKeys []core.Key

			for i := 0; i < numBlobs; i++ {
				data := testkit.RandomBytes(rng, 4096) // 4KB per blob
				key := core.Key{Namespace: "test", ID: fmt.Sprintf("blob_%d", i)}

				isLive := rng.Intn(100) < pct

				var putMeta blobcas.PutMeta
				putMeta.Canonical = true

				if isLive {
					ttl := 24 * time.Hour
					putMeta.RootTTL = &ttl
					liveKeys = append(liveKeys, key)
				} else {
					// Expired deadline
					dead := time.Now().Add(-1 * time.Hour)
					putMeta.RootDeadline = &dead
					deadKeys = append(deadKeys, key)
				}

				_, err := s.Put(ctx, key, blobcas.PutInput{
					Canonical: bytes.NewReader(data),
				}, putMeta)

				if err != nil {
					t.Fatalf("Put failed: %v", err)
				}
			}

			// We shouldn't use `blobcas.Store` directly to manually trigger GC in tests
			// because the `gc.Runner` is private inside `store` now unless we expose it.
			// However `blobcas-gc` uses `gc.NewRunner` directly opening components.
			// For testing here, we'll manually run GC via the `pkg/gc` package as done in `cmd/blobcas-gc`.
			// First, close store flushing everything.
			s.Close()

			// Open catalog and pack
			openGCSuiteAndRun(t, cfg.Pack.Dir, cfg.Catalog.Dir, func(runner gc.Runner) {
				res, err := runner.RunOnce(ctx)
				if err != nil {
					t.Fatalf("GC RunOnce failed: %v", err)
				}

				// Assertions
				t.Logf("GC Results for %d%%: Swept=%d Moved=%d", pct, res.PacksSwept, res.BlocksMoved)
			})

			// Re-open store and check accessibility of live vs dead items.
			s2, err := blobcas.Open(ctx, cfg)
			if err != nil {
				t.Fatalf("failed to open store 2: %v", err)
			}
			defer s2.Close()

			// Removing debug print

			for _, k := range liveKeys {
				ref, err := s2.Resolve(ctx, k)
				if err != nil {
					t.Errorf("expected live key %s to be resolved, got %v", k.ID, err)
					continue
				}
				rc, _, err := s2.Get(ctx, ref)
				if err != nil {
					t.Errorf("expected live ref to be readable, got %v (ref=%x, key=%s)", err, ref.ManifestCID.Bytes, k.ID)
					continue
				}
				rc.Close()
			}

			// Check dead keys
			// Our GC doesn't delete `k2m` entries automatically (roots mapping goes away),
			// but let's see if the underlying blobs were removed.
			deadUnreachable := 0
			for _, k := range deadKeys {
				ref, err := s2.Resolve(ctx, k)
				if err != nil {
					deadUnreachable++
					continue
				}
				rc, _, err := s2.Get(ctx, ref)
				if err != nil {
					deadUnreachable++
					continue
				}
				rc.Close()
			}
			t.Logf("Dead blobs unreachable: %d/%d", deadUnreachable, len(deadKeys))
		})
	}
}

// helper to instantiate GC runner for a closed repo dir
func openGCSuiteAndRun(t testing.TB, packDir, catDir string, fn func(gc.Runner)) {
	cat, err := catalog.Open(catDir)
	if err != nil {
		t.Fatal(err)
	}

	pm, err := pack.NewManager(core.PackConfig{Dir: packDir})
	if err != nil {
		cat.Close()
		t.Fatal(err)
	}

	runner := gc.NewRunner(
		core.GCConfig{Enabled: true, TargetPackBytes: 64 * 1024},
		cat,
		pm,
		manifest.NewCodec(core.LimitsConfig{}),
		cidutil.NewBuilder(),
		transform.NewNone(),
	)

	fn(runner)

	if err := pm.Close(); err != nil {
		t.Errorf("pm close error: %v", err)
	}
	if err := cat.Close(); err != nil {
		t.Errorf("cat close error: %v", err)
	}
}
