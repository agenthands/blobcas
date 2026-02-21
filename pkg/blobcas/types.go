package blobcas

import (
	"context"
	"io"
	"time"

	"github.com/agenthands/blobcas/pkg/core"
)

// Alias core types to maintain public API compatibility as per SPEC.MD
type CID = core.CID
type Ref = core.Ref
type Key = core.Key

// PutInput contains the readers for the content to be stored.
type PutInput struct {
	// Canonical is required. If caller does not canonicalize, set PutMeta.Canonical=false.
	Canonical io.Reader

	// RawWire is optional. If provided, BlobCAS stores it as a separate object and links it from the canonical manifest.
	RawWire io.Reader
}

// PutMeta contains metadata and retention settings for a Put operation.
type PutMeta struct {
	Canonical           bool              // indicates whether Canonical reader is canonicalized bytes
	MediaType           string            // optional
	OrigContentEncoding string            // optional; for metadata only
	Tags                map[string]string // optional, bounded

	// Retention override:
	// - If RootDeadline != nil: use exactly that deadline.
	// - Else if RootTTL != nil: deadline = now + *RootTTL.
	// - Else if cfg.GC.DefaultRootTTL > 0: deadline = now + cfg.GC.DefaultRootTTL.
	// - Else: do not create a root entry.
	RootDeadline *time.Time
	RootTTL      *time.Duration
}

// GetInfo provides metadata about a retrieved object.
type GetInfo struct {
	Length    uint64
	MediaType string
	Canonical bool
	HasRaw    bool
}

// Stat provides summary information about a stored object.
type Stat struct {
	Length     uint64
	ChunkCount uint32
	Canonical  bool
}

// Store is the primary interface for BlobCAS.
type Store interface {
	Put(ctx context.Context, key Key, in PutInput, meta PutMeta) (Ref, error)
	Resolve(ctx context.Context, key Key) (Ref, error)
	Get(ctx context.Context, ref Ref) (io.ReadCloser, GetInfo, error)

	HasChunk(ctx context.Context, cid CID) (bool, error)
	GetChunk(ctx context.Context, cid CID) (io.ReadCloser, uint32 /*plainLen*/, error)

	Stat(ctx context.Context, ref Ref) (Stat, error)
	Close() error
}
