package chunker

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"
)

func TestChunker_ContextCancelDuringSend(t *testing.T) {
	cfg := Config{Min: 64, Avg: 128, Max: 256}
	c := NewChunker(cfg)

	data := make([]byte, 64*1024)
	for i := range data {
		data[i] = byte(i % 251)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	chunks, errCh := c.Split(ctx, &slowReader{data: data, delay: 10 * time.Millisecond})

	// Read first chunk, then cancel context to block sender
	<-chunks
	cancel()

	// Drain remaining
	for range chunks {
	}

	err := <-errCh
	if err != context.Canceled {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

func TestChunker_ErrorFromReader(t *testing.T) {
	cfg := Config{Min: 64, Avg: 128, Max: 256}
	c := NewChunker(cfg)

	expected := errors.New("mock read error")
	ctx := context.Background()
	chunks, errCh := c.Split(ctx, &failingReader{err: expected})

	for range chunks {
	}

	err := <-errCh
	if err != expected {
		t.Errorf("expected %v, got %v", expected, err)
	}
}

func TestChunker_ConfigError(t *testing.T) {
	c := NewChunker(Config{Min: 100, Max: 10}) // invalid config
	ctx := context.Background()
	_, errCh := c.Split(ctx, &failingReader{err: io.EOF})
	err := <-errCh
	if err == nil {
		t.Errorf("expected fastcdc config error, got nil")
	}
}

func TestChunker_ContextCancelBeforeNext(t *testing.T) {
	c := NewChunker(Config{Min: 64, Avg: 128, Max: 256})
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately
	_, errCh := c.Split(ctx, &slowReader{data: []byte("test"), delay: time.Second})

	err := <-errCh
	if err != context.Canceled {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

// slowReader introduces a small delay per read to allow cancellation to race.
type slowReader struct {
	data  []byte
	pos   int
	delay time.Duration
}

func (r *slowReader) Read(p []byte) (n int, err error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	time.Sleep(r.delay)
	n = copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

// failingReader always returns an error.
type failingReader struct {
	err error
}

func (r *failingReader) Read(p []byte) (n int, err error) {
	return 0, r.err
}
