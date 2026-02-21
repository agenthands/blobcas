package chunker

import (
	"bytes"
	"context"
	"testing"

	"github.com/agenthands/blobcas/internal/testkit"
)

func TestChunker_ReturnBuffer(t *testing.T) {
	cfg := Config{Min: 64, Avg: 128, Max: 256}
	c := NewChunker(cfg)

	rng := testkit.RNG(42)
	data := testkit.RandomBytes(rng, 2048)

	ctx := context.Background()
	chunks, errCh := c.Split(ctx, bytes.NewReader(data))

	var count int
	for chunk := range chunks {
		// Return each buffer to the pool
		c.ReturnBuffer(chunk.Buf)
		count++
	}

	if err, ok := <-errCh; ok && err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if count == 0 {
		t.Error("expected at least one chunk")
	}
}

func TestChunker_EmptyInput(t *testing.T) {
	cfg := Config{Min: 64, Avg: 128, Max: 256}
	c := NewChunker(cfg)

	ctx := context.Background()
	chunks, errCh := c.Split(ctx, bytes.NewReader(nil))

	count := 0
	for range chunks {
		count++
	}

	if err, ok := <-errCh; ok && err != nil {
		t.Fatalf("unexpected error on empty input: %v", err)
	}

	if count != 0 {
		t.Errorf("expected 0 chunks for empty input, got %d", count)
	}
}

func TestChunker_SingleByteInput(t *testing.T) {
	cfg := Config{Min: 64, Avg: 128, Max: 256}
	c := NewChunker(cfg)

	ctx := context.Background()
	chunks, errCh := c.Split(ctx, bytes.NewReader([]byte{0x42}))

	var total int
	for chunk := range chunks {
		total += chunk.N
		c.ReturnBuffer(chunk.Buf)
	}

	if err, ok := <-errCh; ok && err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if total != 1 {
		t.Errorf("expected total bytes = 1, got %d", total)
	}
}
