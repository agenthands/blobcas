package chunker

import (
	"context"
	"io"
	"sync"

	"github.com/jotfs/fastcdc-go"
)

// Chunk represents a single chunk of data.
type Chunk struct {
	Buf []byte // owned by chunker; returned to pool by consumer
	N   int
}

// Config defines the chunking parameters.
type Config struct {
	Min int
	Avg int
	Max int
}

// Chunker defines the interface for splitting an io.Reader into chunks.
type Chunker interface {
	Split(ctx context.Context, r io.Reader) (<-chan Chunk, <-chan error)
	// ReturnBuffer returns a chunk buffer to the internal pool for reuse.
	ReturnBuffer(buf []byte)
}

type fastCDCChunker struct {
	cfg  Config
	pool sync.Pool
}

// NewChunker returns a new Chunker implementation.
func NewChunker(cfg Config) Chunker {
	return &fastCDCChunker{
		cfg: cfg,
		pool: sync.Pool{
			New: func() interface{} {
				return make([]byte, cfg.Max)
			},
		},
	}
}

func (c *fastCDCChunker) Split(ctx context.Context, r io.Reader) (<-chan Chunk, <-chan error) {
	chunks := make(chan Chunk, 1) // small buffer for the channel
	errs := make(chan error, 1)

	go func() {
		defer close(chunks)
		defer close(errs)

		cdc, err := fastcdc.NewChunker(r, fastcdc.Options{
			MinSize:     c.cfg.Min,
			AverageSize: c.cfg.Avg,
			MaxSize:     c.cfg.Max,
		})
		if err != nil {
			errs <- err
			return
		}

		for {
			select {
			case <-ctx.Done():
				errs <- ctx.Err()
				return
			default:
				chunk, err := cdc.Next()
				if err != nil {
					if err != io.EOF {
						errs <- err
					}
					return
				}

				// Copy data to a pooled buffer
				buf := c.pool.Get().([]byte)
				n := copy(buf, chunk.Data)

				select {
				case <-ctx.Done():
					errs <- ctx.Err()
					return
				case chunks <- Chunk{Buf: buf, N: n}:
				}
			}
		}
	}()

	return chunks, errs
}

// ReturnBuffer returns a buffer to the pool.
func (c *fastCDCChunker) ReturnBuffer(buf []byte) {
	c.pool.Put(buf)
}
