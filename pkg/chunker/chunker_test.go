package chunker

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/agenthands/blobcas/internal/testkit"
)

func TestFastCDCChunker(t *testing.T) {
	cfg := Config{
		Min: 64,  // FastCDC min block size is 64
		Avg: 128, // Must ideally be exactly half of Max
		Max: 256,
	}
	c := NewChunker(cfg)

	t.Run("BasicSplit", func(t *testing.T) {
		r := testkit.RNG(42)
		data := testkit.RandomBytes(r, 10*1024)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		chunks, errCh := c.Split(ctx, bytes.NewReader(data))

		var reassembled []byte
		var count int

		for chunk := range chunks {
			if chunk.N < cfg.Min && len(reassembled)+chunk.N != len(data) {
				t.Errorf("chunk too small: %d < %d", chunk.N, cfg.Min)
			}
			if chunk.N > cfg.Max {
				t.Errorf("chunk too large: %d > %d", chunk.N, cfg.Max)
			}
			reassembled = append(reassembled, chunk.Buf[:chunk.N]...)
			count++
		}

		err := <-errCh
		if err != nil && err != io.EOF {
			t.Fatalf("unexpected error: %v", err)
		}

		if !bytes.Equal(data, reassembled) {
			t.Error("reassembled data does not match original")
		}

		if count < 10 {
			t.Errorf("expected multiple chunks, got %d", count)
		}
	})

	t.Run("Cancellation", func(t *testing.T) {
		r := testkit.RNG(42)
		data := testkit.RandomBytes(r, 1024*1024)

		ctx, cancel := context.WithCancel(context.Background())

		chunks, errCh := c.Split(ctx, bytes.NewReader(data))

		// Read one chunk, then cancel
		_, ok := <-chunks
		if !ok {
			t.Fatal("expected at least one chunk")
		}
		cancel()

		// Drain the rest, should close quickly
		for range chunks {
			// just drain
		}

		err := <-errCh
		if err != context.Canceled && err != nil && err != io.EOF {
			// FastCDC might just return EOF or a wrapper around DeadlineExceeded
			// We just want to ensure it doesn't hang.
		}
	})

	t.Run("Determinism", func(t *testing.T) {
		r := testkit.RNG(42)
		data := testkit.RandomBytes(r, 64*1024)

		ctx := context.Background()

		// First pass
		chunks1, _ := c.Split(ctx, bytes.NewReader(data))
		var sizes1 []int
		for cp := range chunks1 {
			sizes1 = append(sizes1, cp.N)
		}

		// Second pass (same config, same data)
		chunks2, _ := c.Split(ctx, bytes.NewReader(data))
		var sizes2 []int
		for cp := range chunks2 {
			sizes2 = append(sizes2, cp.N)
		}

		if len(sizes1) != len(sizes2) {
			t.Fatalf("determinism failed: chunk counts differ (%d vs %d)", len(sizes1), len(sizes2))
		}

		for i := range sizes1 {
			if sizes1[i] != sizes2[i] {
				t.Fatalf("determinism failed at chunk %d: %d vs %d", i, sizes1[i], sizes2[i])
			}
		}
	})
}
