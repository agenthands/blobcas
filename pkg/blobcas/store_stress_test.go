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

func TestStoreIntegration_ConcurrentPutGet_Race(t *testing.T) {
	dir, err := os.MkdirTemp("", "blobcas-store-integration-race")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s, _ := createTestStore(t, dir)
	defer s.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const numWriters = 10
	const numReaders = 20
	const blobsPerWriter = 20

	var wg sync.WaitGroup
	var refsMu sync.Mutex
	var storedRefs []blobcas.Ref

	// Writers
	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			rng := testkit.RNG(int64(writerID))

			for b := 0; b < blobsPerWriter; b++ {
				select {
				case <-ctx.Done():
					return
				default:
				}

				// Sometimes generate duplicate data to test dedupe race conditions
				size := 1024 + rng.Intn(64*1024)
				var content []byte
				if rng.Intn(5) == 0 {
					// 20% chance of identical static blob
					content = bytes.Repeat([]byte("duplicate"), size/9)
				} else {
					content = testkit.RandomBytes(rng, size)
				}

				key := core.Key{Namespace: "race", ID: fmt.Sprintf("w%d_b%d", writerID, b)}
				ref, err := s.Put(ctx, key, blobcas.PutInput{
					Canonical: bytes.NewReader(content),
				}, blobcas.PutMeta{Canonical: true})

				if err != nil {
					t.Errorf("writer %d failed on blob %d: %v", writerID, b, err)
					return
				}

				refsMu.Lock()
				storedRefs = append(storedRefs, ref)
				refsMu.Unlock()
			}
		}(w)
	}

	// Wait a tiny bit for some writes to land
	time.Sleep(10 * time.Millisecond)

	// Readers
	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			rng := testkit.RNG(int64(readerID + 100))

			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				refsMu.Lock()
				n := len(storedRefs)
				var targetRef blobcas.Ref
				if n > 0 {
					targetRef = storedRefs[rng.Intn(n)]
				}
				refsMu.Unlock()

				if n == 0 {
					time.Sleep(1 * time.Millisecond)
					continue
				}

				// Try to get it
				rc, info, err := s.Get(ctx, targetRef)
				if err != nil {
					t.Errorf("reader %d failed to get ref %v: %v", readerID, targetRef, err)
					return
				}

				// Read output
				_, err = io.ReadAll(rc)
				rc.Close()
				if err != nil {
					t.Errorf("reader %d failed to read stream: %v", readerID, err)
					return
				}

				// Try Stat
				if _, err := s.Stat(ctx, targetRef); err != nil {
					t.Errorf("reader %d failed to stat: %v", readerID, err)
				}

				_ = info
			}
		}(r)
	}

	wg.Wait()
}
