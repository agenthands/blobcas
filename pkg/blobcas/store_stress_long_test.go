//go:build stress
// +build stress

package blobcas_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/agenthands/blobcas/internal/testkit"
	"github.com/agenthands/blobcas/pkg/blobcas"
	"github.com/agenthands/blobcas/pkg/core"
)

func TestStoreIntegration_LongRunning_Stress(t *testing.T) {
	dir, err := os.MkdirTemp("", "blobcas-store-long-stress")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s, _ := createTestStore(t, dir)
	defer s.Close()

	// Run for 60 seconds
	duration := 60 * time.Second
	if val := os.Getenv("STRESS_DURATION"); val != "" {
		if d, err := time.ParseDuration(val); err == nil {
			duration = d
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	fmt.Printf("Starting long-running stress test for %v...\n", duration)

	var wg sync.WaitGroup
	var refsMu sync.Mutex
	var refs []blobcas.Ref

	// 5 continuous writers
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			rng := testkit.RNG(int64(id))
			counter := 0

			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				size := 512*1024 + rng.Intn(2*1024*1024) // 512KB to 2.5MB
				data := testkit.RandomBytes(rng, size)

				key := core.Key{Namespace: "stress", ID: fmt.Sprintf("w%d_b%d", id, counter)}

				// Short TTL sometimes to trigger GC potential
				var putMeta blobcas.PutMeta
				putMeta.Canonical = true
				if rng.Intn(10) == 0 {
					ttl := 1 * time.Second
					putMeta.RootTTL = &ttl
				}

				ref, err := s.Put(ctx, key, blobcas.PutInput{
					Canonical: bytes.NewReader(data),
				}, putMeta)

				if err != nil {
					t.Errorf("stress writer error: %v", err)
					return
				}

				refsMu.Lock()
				refs = append(refs, ref)
				refsMu.Unlock()
				counter++
				time.Sleep(10 * time.Millisecond) // Don't overwhelm IO completely
			}
		}(i)
	}

	// 10 continuous readers
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			rng := testkit.RNG(int64(id + 100))

			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				refsMu.Lock()
				n := len(refs)
				var target blobcas.Ref
				if n > 0 {
					target = refs[rng.Intn(n)]
				}
				refsMu.Unlock()

				if n == 0 {
					time.Sleep(50 * time.Millisecond)
					continue
				}

				rc, _, err := s.Get(ctx, target)
				if err != nil {
					if err == core.ErrNotFound {
						// Might have been GC'd
						continue
					}
					t.Errorf("stress reader error: %v", err)
					return
				}

				// stream to discard
				_, err = io.Copy(io.Discard, rc)
				rc.Close()
				if err != nil {
					t.Errorf("stress read copy error: %v", err)
				}

				time.Sleep(5 * time.Millisecond)
			}
		}(i)
	}

	wg.Wait()
	fmt.Println("Long-running stress test completed successfully.")
}
