package blobcas

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"time"

	"github.com/agenthands/blobcas/pkg/catalog"
	"github.com/agenthands/blobcas/pkg/chunker"
	"github.com/agenthands/blobcas/pkg/cidutil"
	"github.com/agenthands/blobcas/pkg/core"
	"github.com/agenthands/blobcas/pkg/manifest"
	"github.com/agenthands/blobcas/pkg/pack"
	"github.com/agenthands/blobcas/pkg/transform"
)

type store struct {
	cfg Config

	chunker   chunker.Chunker
	cidHub    cidutil.Builder
	manifests manifest.Codec
	packs     pack.Manager
	catalog   catalog.Catalog
	transform transform.Transform

	putMu sync.Mutex // Single-writer invariant (v1)
}

// Open initializes and opens a BlobCAS store.
func Open(ctx context.Context, cfg Config) (Store, error) {
	// Defaults for directory layout
	if cfg.Pack.Dir == "" {
		cfg.Pack.Dir = filepath.Join(cfg.Dir, "packs")
	}
	if cfg.Catalog.Dir == "" {
		cfg.Catalog.Dir = filepath.Join(cfg.Dir, "catalog")
	}

	cat, err := catalog.Open(cfg.Catalog.Dir)
	if err != nil {
		return nil, fmt.Errorf("failed to open catalog: %w", err)
	}

	pm, err := pack.NewManager(cfg.Pack)
	if err != nil {
		cat.Close()
		return nil, fmt.Errorf("failed to open pack manager: %w", err)
	}

	var tr transform.Transform
	switch cfg.Transform.Name {
	case "zstd":
		tr = transform.NewZstd(cfg.Transform.ZstdLevel)
	case "none", "":
		tr = transform.NewNone()
	default:
		cat.Close()
		pm.Close()
		return nil, fmt.Errorf("unsupported transform: %s", cfg.Transform.Name)
	}

	s := &store{
		cfg:       cfg,
		chunker:   chunker.NewChunker(chunker.Config{Min: cfg.Chunking.Min, Avg: cfg.Chunking.Avg, Max: cfg.Chunking.Max}),
		cidHub:    cidutil.NewBuilder(),
		manifests: manifest.NewCodec(cfg.Limits),
		packs:     pm,
		catalog:   cat,
		transform: tr,
	}

	return s, nil
}

func (s *store) Close() error {
	err1 := s.catalog.Close()
	err2 := s.packs.Close()
	if err1 != nil {
		return err1
	}
	return err2
}

func (s *store) Put(ctx context.Context, key Key, in PutInput, meta PutMeta) (Ref, error) {
	if meta.RootDeadline != nil && meta.RootTTL != nil {
		return Ref{}, core.ErrInvalidInput
	}

	s.putMu.Lock()
	defer s.putMu.Unlock()

	// 1. Chunk canonical stream
	chunks, errs := s.chunker.Split(ctx, in.Canonical)

	var chunkRefs []manifest.ChunkRef
	var totalLen uint64

	batch := s.catalog.NewBatch()
	defer batch.Close()

	// Consume all chunks first. This avoids the race inherent in a select{}
	// over both channels: when both close simultaneously, Go's select picks
	// randomly, potentially skipping the last chunk on the chunks channel.
	for c := range chunks {
		if ctx.Err() != nil {
			s.chunker.ReturnBuffer(c.Buf)
			return Ref{}, ctx.Err()
		}

		cid, err := s.cidHub.ChunkCID(c.Buf[:c.N])
		if err != nil {
			s.chunker.ReturnBuffer(c.Buf)
			return Ref{}, err
		}

		// Check dedupe
		_, exists, err := s.catalog.GetPackForCID(ctx, cid)
		if err != nil {
			s.chunker.ReturnBuffer(c.Buf)
			return Ref{}, err
		}

		if !exists {
			// Store new chunk
			stored, err := s.transform.Encode(c.Buf[:c.N])
			if err != nil {
				s.chunker.ReturnBuffer(c.Buf)
				return Ref{}, err
			}

			packID, err := s.packs.PutBlock(ctx, cid, stored)
			if err != nil {
				s.chunker.ReturnBuffer(c.Buf)
				return Ref{}, err
			}

			if err := s.catalog.PutPackForCID(batch, cid, packID); err != nil {
				s.chunker.ReturnBuffer(c.Buf)
				return Ref{}, err
			}
		}

		chunkRefs = append(chunkRefs, manifest.ChunkRef{
			CID: cid,
			Len: uint32(c.N),
		})
		totalLen += uint64(c.N)

		// Return buffer to pool after all uses of c.Buf are done
		s.chunker.ReturnBuffer(c.Buf)
	}

	// Check for chunker errors after all chunks have been consumed
	if err, ok := <-errs; ok && err != nil {
		return Ref{}, err
	}

	m := &manifest.ManifestV1{
		Version:   1,
		Canonical: meta.Canonical,
		MediaType: meta.MediaType,
		Length:    totalLen,
		Chunks:    chunkRefs,
		Tags:      meta.Tags,
	}

	mBytes, err := s.manifests.Encode(m)
	if err != nil {
		return Ref{}, err
	}

	mCID, err := s.cidHub.ManifestCID(mBytes)
	if err != nil {
		return Ref{}, err
	}

	// Store manifest block
	mStored, err := s.transform.Encode(mBytes)
	if err != nil {
		return Ref{}, err
	}

	mPackID, err := s.packs.PutBlock(ctx, mCID, mStored)
	if err != nil {
		return Ref{}, err
	}

	if err := s.catalog.PutPackForCID(batch, mCID, mPackID); err != nil {
		return Ref{}, err
	}

	// Catalog updates
	if err := s.catalog.PutManifestForKey(batch, key, mCID); err != nil {
		return Ref{}, err
	}

	// Retention
	deadline := s.computeDeadline(meta)
	if !deadline.IsZero() {
		if err := s.catalog.PutRootDeadline(batch, mCID, deadline); err != nil {
			return Ref{}, err
		}
	}

	if err := batch.Commit(nil); err != nil {
		return Ref{}, err
	}

	// Rotate packs if needed
	_ = s.packs.SealAndRotateIfNeeded(ctx)

	return Ref{ManifestCID: mCID}, nil
}

func (s *store) computeDeadline(meta PutMeta) time.Time {
	if meta.RootDeadline != nil {
		return *meta.RootDeadline
	}
	if meta.RootTTL != nil {
		return time.Now().Add(*meta.RootTTL)
	}
	if s.cfg.GC.DefaultRootTTL > 0 {
		return time.Now().Add(s.cfg.GC.DefaultRootTTL)
	}
	return time.Time{}
}

func (s *store) Resolve(ctx context.Context, key Key) (Ref, error) {
	cid, ok, err := s.catalog.GetManifestForKey(ctx, key)
	if err != nil {
		return Ref{}, err
	}
	if !ok {
		return Ref{}, ErrNotFound
	}
	return Ref{ManifestCID: cid}, nil
}

func (s *store) Get(ctx context.Context, ref Ref) (io.ReadCloser, GetInfo, error) {
	// 1. Load manifest
	packID, ok, err := s.catalog.GetPackForCID(ctx, ref.ManifestCID)
	if err != nil {
		return nil, GetInfo{}, err
	}
	if !ok {
		return nil, GetInfo{}, ErrNotFound
	}

	mStored, err := s.packs.GetBlock(ctx, packID, ref.ManifestCID)
	if err != nil {
		return nil, GetInfo{}, err
	}

	mBytes, err := s.transform.Decode(mStored)
	if err != nil {
		return nil, GetInfo{}, err
	}

	if err := s.cidHub.Verify(ref.ManifestCID, mBytes); err != nil {
		return nil, GetInfo{}, err
	}

	m, err := s.manifests.Decode(mBytes)
	if err != nil {
		return nil, GetInfo{}, err
	}

	// 2. Return a reader that fetches chunks lazily
	r := &objectReader{
		ctx:    ctx,
		s:      s,
		m:      m,
		chunks: m.Chunks,
	}

	return r, GetInfo{
		Length:    m.Length,
		MediaType: m.MediaType,
		Canonical: m.Canonical,
		HasRaw:    m.RawRef != nil,
	}, nil
}

func (s *store) HasChunk(ctx context.Context, cid CID) (bool, error) {
	_, ok, err := s.catalog.GetPackForCID(ctx, cid)
	return ok, err
}

func (s *store) GetChunk(ctx context.Context, cid CID) (io.ReadCloser, uint32, error) {
	packID, ok, err := s.catalog.GetPackForCID(ctx, cid)
	if err != nil {
		return nil, 0, err
	}
	if !ok {
		return nil, 0, ErrNotFound
	}

	stored, err := s.packs.GetBlock(ctx, packID, cid)
	if err != nil {
		return nil, 0, err
	}

	plain, err := s.transform.Decode(stored)
	if err != nil {
		return nil, 0, err
	}

	if err := s.cidHub.Verify(cid, plain); err != nil {
		return nil, 0, err
	}

	return io.NopCloser(bytes.NewReader(plain)), uint32(len(plain)), nil
}

func (s *store) Stat(ctx context.Context, ref Ref) (Stat, error) {
	// Need to load manifest to get stats
	packID, ok, err := s.catalog.GetPackForCID(ctx, ref.ManifestCID)
	if err != nil {
		return Stat{}, err
	}
	if !ok {
		return Stat{}, ErrNotFound
	}

	stored, err := s.packs.GetBlock(ctx, packID, ref.ManifestCID)
	if err != nil {
		return Stat{}, err
	}

	mBytes, err := s.transform.Decode(stored)
	if err != nil {
		return Stat{}, err
	}

	m, err := s.manifests.Decode(mBytes)
	if err != nil {
		return Stat{}, err
	}

	return Stat{
		Length:     m.Length,
		ChunkCount: uint32(len(m.Chunks)),
		Canonical:  m.Canonical,
	}, nil
}

type objectReader struct {
	ctx    context.Context
	s      *store
	m      *manifest.ManifestV1
	chunks []manifest.ChunkRef

	currentChunk io.ReadCloser
	chunkIdx     int
}

func (r *objectReader) Read(p []byte) (n int, err error) {
	for {
		if r.currentChunk == nil {
			if r.chunkIdx >= len(r.chunks) {
				return 0, io.EOF
			}

			cRef := r.chunks[r.chunkIdx]
			rc, _, err := r.s.GetChunk(r.ctx, cRef.CID)
			if err != nil {
				return 0, err
			}
			r.currentChunk = rc
		}

		n, err = r.currentChunk.Read(p)
		if err == io.EOF {
			r.currentChunk.Close()
			r.currentChunk = nil
			r.chunkIdx++
			if n > 0 {
				return n, nil
			}
			continue
		}
		return n, err
	}
}

func (r *objectReader) Close() error {
	if r.currentChunk != nil {
		return r.currentChunk.Close()
	}
	return nil
}
