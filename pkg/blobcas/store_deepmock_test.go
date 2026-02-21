package blobcas_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/agenthands/blobcas/pkg/blobcas"
	"github.com/agenthands/blobcas/pkg/catalog"
	"github.com/agenthands/blobcas/pkg/chunker"
	"github.com/agenthands/blobcas/pkg/cidutil"
	"github.com/agenthands/blobcas/pkg/core"
	"github.com/agenthands/blobcas/pkg/manifest"
	"github.com/agenthands/blobcas/pkg/pack"
	"github.com/agenthands/blobcas/pkg/transform"
	"github.com/cockroachdb/pebble"
)

// ---------- Mock Pack Manager ----------

type mockPackManager struct {
	putBlockErr error
	getBlockErr error
	sealErr     error
	real        pack.Manager
}

func (m *mockPackManager) PutBlock(ctx context.Context, c core.CID, stored []byte) (uint64, error) {
	if m.putBlockErr != nil {
		return 0, m.putBlockErr
	}
	return m.real.PutBlock(ctx, c, stored)
}
func (m *mockPackManager) GetBlock(ctx context.Context, packID uint64, c core.CID) ([]byte, error) {
	if m.getBlockErr != nil {
		return nil, m.getBlockErr
	}
	return m.real.GetBlock(ctx, packID, c)
}
func (m *mockPackManager) SealAndRotateIfNeeded(ctx context.Context) error {
	return m.real.SealAndRotateIfNeeded(ctx)
}
func (m *mockPackManager) CurrentPackID() uint64 { return m.real.CurrentPackID() }
func (m *mockPackManager) IteratePackBlocks(ctx context.Context, packID uint64, fn func(core.CID) error) error {
	return m.real.IteratePackBlocks(ctx, packID, fn)
}
func (m *mockPackManager) ListSealedPacks() []uint64      { return m.real.ListSealedPacks() }
func (m *mockPackManager) RemovePack(packID uint64) error { return m.real.RemovePack(packID) }
func (m *mockPackManager) SealActivePack(ctx context.Context) error {
	return m.real.SealActivePack(ctx)
}
func (m *mockPackManager) Close() error { return m.real.Close() }

// ---------- Mock CID Builder ----------

type mockCIDBuilder struct {
	chunkCIDErr error
	manifestErr error
	verifyErr   error
	callCount   int
	failOnCall  int // fail on the Nth call
	real        cidutil.Builder
}

func (m *mockCIDBuilder) ChunkCID(data []byte) (core.CID, error) {
	m.callCount++
	if m.chunkCIDErr != nil {
		return core.CID{}, m.chunkCIDErr
	}
	return m.real.ChunkCID(data)
}

func (m *mockCIDBuilder) ManifestCID(data []byte) (core.CID, error) {
	if m.manifestErr != nil {
		return core.CID{}, m.manifestErr
	}
	return m.real.ManifestCID(data)
}

func (m *mockCIDBuilder) Verify(cid core.CID, data []byte) error {
	if m.verifyErr != nil {
		return m.verifyErr
	}
	return m.real.Verify(cid, data)
}

// ---------- Mock Manifest Codec ----------

type mockManifestCodec struct {
	decodeErr error
	real      manifest.Codec
}

func (m *mockManifestCodec) Encode(man *manifest.ManifestV1) ([]byte, error) {
	return m.real.Encode(man)
}

func (m *mockManifestCodec) Decode(data []byte) (*manifest.ManifestV1, error) {
	if m.decodeErr != nil {
		return nil, m.decodeErr
	}
	return m.real.Decode(data)
}

// ---------- Mock Pebble Batch (for commit errors) ----------

type errOnCommitCatalog struct {
	real      catalog.Catalog
	commitErr error
}

func (c *errOnCommitCatalog) GetPackForCID(ctx context.Context, cid core.CID) (uint64, bool, error) {
	return c.real.GetPackForCID(ctx, cid)
}
func (c *errOnCommitCatalog) PutPackForCID(batch *pebble.Batch, cid core.CID, packID uint64) error {
	return c.real.PutPackForCID(batch, cid, packID)
}
func (c *errOnCommitCatalog) GetManifestForKey(ctx context.Context, key core.Key) (core.CID, bool, error) {
	return c.real.GetManifestForKey(ctx, key)
}
func (c *errOnCommitCatalog) PutManifestForKey(batch *pebble.Batch, key core.Key, m core.CID) error {
	return c.real.PutManifestForKey(batch, key, m)
}
func (c *errOnCommitCatalog) PutRootDeadline(batch *pebble.Batch, m core.CID, d time.Time) error {
	return c.real.PutRootDeadline(batch, m, d)
}
func (c *errOnCommitCatalog) IterateRoots(ctx context.Context, fn func(core.CID, time.Time) error) error {
	return c.real.IterateRoots(ctx, fn)
}
func (c *errOnCommitCatalog) NewBatch() *pebble.Batch { return c.real.NewBatch() }
func (c *errOnCommitCatalog) Close() error            { return c.real.Close() }

// ---------- Helper ----------

func buildStore(t testing.TB, cat catalog.Catalog, pm pack.Manager, cid cidutil.Builder, man manifest.Codec, tr transform.Transform) blobcas.Store {
	return blobcas.NewStoreForTest(
		core.Config{
			Chunking: core.ChunkingConfig{Min: 64, Avg: 128, Max: 256},
			GC:       core.GCConfig{DefaultRootTTL: 24 * time.Hour},
			Limits:   core.LimitsConfig{MaxChunksPerObject: 200000},
		},
		chunker.NewChunker(chunker.Config{Min: 64, Avg: 128, Max: 256}),
		cid, man, pm, cat, tr,
	)
}

// ---------- Tests ----------

func TestDeepMock_Put_ChunkCIDError(t *testing.T) {
	cat, _ := catalog.Open(t.TempDir())
	pm, _ := pack.NewManager(core.PackConfig{Dir: t.TempDir(), TargetPackBytes: 64 * 1024})

	mc := &mockCIDBuilder{chunkCIDErr: fmt.Errorf("%w: cid fault", errMock), real: cidutil.NewBuilder()}
	s := buildStore(t, cat, pm, mc, manifest.NewCodec(core.LimitsConfig{MaxChunksPerObject: 200000}), transform.NewNone())
	defer s.Close()

	_, err := s.Put(context.Background(), core.Key{Namespace: "t", ID: "1"}, blobcas.PutInput{
		Canonical: bytes.NewReader([]byte("content")),
	}, blobcas.PutMeta{Canonical: true})

	if !errors.Is(err, errMock) {
		t.Errorf("expected CID error, got: %v", err)
	}
}

func TestDeepMock_Put_ManifestCIDError(t *testing.T) {
	cat, _ := catalog.Open(t.TempDir())
	pm, _ := pack.NewManager(core.PackConfig{Dir: t.TempDir(), TargetPackBytes: 64 * 1024})

	mc := &mockCIDBuilder{manifestErr: fmt.Errorf("%w: manifest CID fault", errMock), real: cidutil.NewBuilder()}
	s := buildStore(t, cat, pm, mc, manifest.NewCodec(core.LimitsConfig{MaxChunksPerObject: 200000}), transform.NewNone())
	defer s.Close()

	_, err := s.Put(context.Background(), core.Key{Namespace: "t", ID: "1"}, blobcas.PutInput{
		Canonical: bytes.NewReader([]byte("content")),
	}, blobcas.PutMeta{Canonical: true})

	if !errors.Is(err, errMock) {
		t.Errorf("expected manifest CID error, got: %v", err)
	}
}

func TestDeepMock_Put_PutBlockError(t *testing.T) {
	cat, _ := catalog.Open(t.TempDir())
	realPM, _ := pack.NewManager(core.PackConfig{Dir: t.TempDir(), TargetPackBytes: 64 * 1024})

	mpm := &mockPackManager{putBlockErr: fmt.Errorf("%w: putblock fault", errMock), real: realPM}
	s := buildStore(t, cat, mpm, cidutil.NewBuilder(), manifest.NewCodec(core.LimitsConfig{MaxChunksPerObject: 200000}), transform.NewNone())
	defer s.Close()

	_, err := s.Put(context.Background(), core.Key{Namespace: "t", ID: "1"}, blobcas.PutInput{
		Canonical: bytes.NewReader([]byte("content")),
	}, blobcas.PutMeta{Canonical: true})

	if !errors.Is(err, errMock) {
		t.Errorf("expected PutBlock error, got: %v", err)
	}
}

func TestDeepMock_Put_ManifestEncodeError(t *testing.T) {
	// Use faulty transform that fails ONLY on the manifest encode (after chunks work)
	cat, _ := catalog.Open(t.TempDir())
	pm, _ := pack.NewManager(core.PackConfig{Dir: t.TempDir(), TargetPackBytes: 64 * 1024})

	// Transform that fails on second encode (first = chunk, second = manifest)
	ft := &countingFaultyTransform{
		failOnEncodeN: 2,
		encodeErr:     fmt.Errorf("%w: manifest encode fault", errMock),
		inner:         transform.NewNone(),
	}

	s := buildStore(t, cat, pm, cidutil.NewBuilder(), manifest.NewCodec(core.LimitsConfig{MaxChunksPerObject: 200000}), ft)
	defer s.Close()

	_, err := s.Put(context.Background(), core.Key{Namespace: "t", ID: "1"}, blobcas.PutInput{
		Canonical: bytes.NewReader([]byte("content")),
	}, blobcas.PutMeta{Canonical: true})

	if !errors.Is(err, errMock) {
		t.Errorf("expected manifest encode error, got: %v", err)
	}
}

func TestDeepMock_Get_GetBlockError(t *testing.T) {
	dir := t.TempDir()
	catDir := dir + "/catalog"
	packDir := dir + "/packs"

	cat, _ := catalog.Open(catDir)
	realPM, _ := pack.NewManager(core.PackConfig{Dir: packDir, TargetPackBytes: 64 * 1024})

	// First, store data with real deps
	s := buildStore(t, cat, realPM, cidutil.NewBuilder(), manifest.NewCodec(core.LimitsConfig{MaxChunksPerObject: 200000}), transform.NewNone())

	ref, err := s.Put(context.Background(), core.Key{Namespace: "t", ID: "1"}, blobcas.PutInput{
		Canonical: bytes.NewReader([]byte("content for get block error")),
	}, blobcas.PutMeta{Canonical: true})
	if err != nil {
		t.Fatal(err)
	}
	s.Close()

	// Reopen with mock pack that fails on GetBlock
	cat2, _ := catalog.Open(catDir)
	realPM2, _ := pack.NewManager(core.PackConfig{Dir: packDir, TargetPackBytes: 64 * 1024})
	mpm := &mockPackManager{getBlockErr: fmt.Errorf("%w: getblock fault", errMock), real: realPM2}

	s2 := buildStore(t, cat2, mpm, cidutil.NewBuilder(), manifest.NewCodec(core.LimitsConfig{MaxChunksPerObject: 200000}), transform.NewNone())
	defer s2.Close()

	_, _, err = s2.Get(context.Background(), ref)
	if !errors.Is(err, errMock) {
		t.Errorf("expected GetBlock error in Get, got: %v", err)
	}

	// Also test Stat with GetBlock error
	_, err = s2.Stat(context.Background(), ref)
	if !errors.Is(err, errMock) {
		t.Errorf("expected GetBlock error in Stat, got: %v", err)
	}
}

func TestDeepMock_Get_VerifyError(t *testing.T) {
	dir := t.TempDir()
	catDir := dir + "/catalog"
	packDir := dir + "/packs"

	cat, _ := catalog.Open(catDir)
	pm, _ := pack.NewManager(core.PackConfig{Dir: packDir, TargetPackBytes: 64 * 1024})

	s := buildStore(t, cat, pm, cidutil.NewBuilder(), manifest.NewCodec(core.LimitsConfig{MaxChunksPerObject: 200000}), transform.NewNone())

	ref, _ := s.Put(context.Background(), core.Key{Namespace: "t", ID: "1"}, blobcas.PutInput{
		Canonical: bytes.NewReader([]byte("content for verify")),
	}, blobcas.PutMeta{Canonical: true})
	s.Close()

	// Reopen with verify error
	cat2, _ := catalog.Open(catDir)
	pm2, _ := pack.NewManager(core.PackConfig{Dir: packDir, TargetPackBytes: 64 * 1024})

	mc := &mockCIDBuilder{verifyErr: fmt.Errorf("%w: verify fault", errMock), real: cidutil.NewBuilder()}
	s2 := buildStore(t, cat2, pm2, mc, manifest.NewCodec(core.LimitsConfig{MaxChunksPerObject: 200000}), transform.NewNone())
	defer s2.Close()

	_, _, err := s2.Get(context.Background(), ref)
	if !errors.Is(err, errMock) {
		t.Errorf("expected verify error, got: %v", err)
	}
}

func TestDeepMock_Get_ManifestDecodeError(t *testing.T) {
	dir := t.TempDir()
	catDir := dir + "/catalog"
	packDir := dir + "/packs"

	cat, _ := catalog.Open(catDir)
	pm, _ := pack.NewManager(core.PackConfig{Dir: packDir, TargetPackBytes: 64 * 1024})

	s := buildStore(t, cat, pm, cidutil.NewBuilder(), manifest.NewCodec(core.LimitsConfig{MaxChunksPerObject: 200000}), transform.NewNone())

	ref, _ := s.Put(context.Background(), core.Key{Namespace: "t", ID: "1"}, blobcas.PutInput{
		Canonical: bytes.NewReader([]byte("content for manifest decode")),
	}, blobcas.PutMeta{Canonical: true})
	s.Close()

	cat2, _ := catalog.Open(catDir)
	pm2, _ := pack.NewManager(core.PackConfig{Dir: packDir, TargetPackBytes: 64 * 1024})

	mmc := &mockManifestCodec{decodeErr: fmt.Errorf("%w: decode fault", errMock), real: manifest.NewCodec(core.LimitsConfig{MaxChunksPerObject: 200000})}
	s2 := buildStore(t, cat2, pm2, cidutil.NewBuilder(), mmc, transform.NewNone())
	defer s2.Close()

	_, _, err := s2.Get(context.Background(), ref)
	if !errors.Is(err, errMock) {
		t.Errorf("expected manifest decode error in Get, got: %v", err)
	}

	_, err = s2.Stat(context.Background(), ref)
	if !errors.Is(err, errMock) {
		t.Errorf("expected manifest decode error in Stat, got: %v", err)
	}
}

func TestDeepMock_GetChunk_GetBlockError(t *testing.T) {
	dir := t.TempDir()
	catDir := dir + "/catalog"
	packDir := dir + "/packs"

	cat, _ := catalog.Open(catDir)
	pm, _ := pack.NewManager(core.PackConfig{Dir: packDir, TargetPackBytes: 64 * 1024})

	s := buildStore(t, cat, pm, cidutil.NewBuilder(), manifest.NewCodec(core.LimitsConfig{MaxChunksPerObject: 200000}), transform.NewNone())

	ref, _ := s.Put(context.Background(), core.Key{Namespace: "t", ID: "1"}, blobcas.PutInput{
		Canonical: bytes.NewReader([]byte("content for getblock chunk")),
	}, blobcas.PutMeta{Canonical: true})
	s.Close()

	cat2, _ := catalog.Open(catDir)
	realPM2, _ := pack.NewManager(core.PackConfig{Dir: packDir, TargetPackBytes: 64 * 1024})
	mpm := &mockPackManager{getBlockErr: fmt.Errorf("%w: getblock fault", errMock), real: realPM2}

	s2 := buildStore(t, cat2, mpm, cidutil.NewBuilder(), manifest.NewCodec(core.LimitsConfig{MaxChunksPerObject: 200000}), transform.NewNone())
	defer s2.Close()

	_, _, err := s2.GetChunk(context.Background(), ref.ManifestCID)
	if !errors.Is(err, errMock) {
		t.Errorf("expected GetBlock error in GetChunk, got: %v", err)
	}
}

// countingFaultyTransform fails on the Nth Encode call.
type countingFaultyTransform struct {
	failOnEncodeN int
	encodeErr     error
	encodeCount   int
	inner         transform.Transform
}

func (t *countingFaultyTransform) Name() string { return "counting-faulty" }

func (t *countingFaultyTransform) Encode(plain []byte) ([]byte, error) {
	t.encodeCount++
	if t.encodeCount == t.failOnEncodeN {
		return nil, t.encodeErr
	}
	return t.inner.Encode(plain)
}

func (t *countingFaultyTransform) Decode(stored []byte) ([]byte, error) {
	return t.inner.Decode(stored)
}

// countingMockPM fails on the Nth PutBlock call.
type countingMockPM struct {
	putCount   int
	failOnPutN int
	putErr     error
	real       pack.Manager
}

func (m *countingMockPM) PutBlock(ctx context.Context, c core.CID, stored []byte) (uint64, error) {
	m.putCount++
	if m.putCount == m.failOnPutN {
		return 0, m.putErr
	}
	return m.real.PutBlock(ctx, c, stored)
}
func (m *countingMockPM) GetBlock(ctx context.Context, packID uint64, c core.CID) ([]byte, error) {
	return m.real.GetBlock(ctx, packID, c)
}
func (m *countingMockPM) SealAndRotateIfNeeded(ctx context.Context) error {
	return m.real.SealAndRotateIfNeeded(ctx)
}
func (m *countingMockPM) CurrentPackID() uint64 { return m.real.CurrentPackID() }
func (m *countingMockPM) IteratePackBlocks(ctx context.Context, packID uint64, fn func(core.CID) error) error {
	return m.real.IteratePackBlocks(ctx, packID, fn)
}
func (m *countingMockPM) ListSealedPacks() []uint64      { return m.real.ListSealedPacks() }
func (m *countingMockPM) RemovePack(packID uint64) error { return m.real.RemovePack(packID) }
func (m *countingMockPM) SealActivePack(ctx context.Context) error {
	return m.real.SealActivePack(ctx)
}
func (m *countingMockPM) Close() error { return m.real.Close() }

// countingMockCatalog fails on Nth PutPackForCID call.
type countingMockCatalog struct {
	putPackCount   int
	failOnPutPackN int
	putPackErr     error
	real           catalog.Catalog
}

func (c *countingMockCatalog) GetPackForCID(ctx context.Context, cid core.CID) (uint64, bool, error) {
	return c.real.GetPackForCID(ctx, cid)
}
func (c *countingMockCatalog) PutPackForCID(batch *pebble.Batch, cid core.CID, packID uint64) error {
	c.putPackCount++
	if c.putPackCount == c.failOnPutPackN {
		return c.putPackErr
	}
	return c.real.PutPackForCID(batch, cid, packID)
}
func (c *countingMockCatalog) GetManifestForKey(ctx context.Context, key core.Key) (core.CID, bool, error) {
	return c.real.GetManifestForKey(ctx, key)
}
func (c *countingMockCatalog) PutManifestForKey(batch *pebble.Batch, key core.Key, m core.CID) error {
	return c.real.PutManifestForKey(batch, key, m)
}
func (c *countingMockCatalog) PutRootDeadline(batch *pebble.Batch, m core.CID, d time.Time) error {
	return c.real.PutRootDeadline(batch, m, d)
}
func (c *countingMockCatalog) IterateRoots(ctx context.Context, fn func(core.CID, time.Time) error) error {
	return c.real.IterateRoots(ctx, fn)
}
func (c *countingMockCatalog) NewBatch() *pebble.Batch { return c.real.NewBatch() }
func (c *countingMockCatalog) Close() error            { return c.real.Close() }

func TestDeepMock_Put_ManifestPutBlockError(t *testing.T) {
	cat, _ := catalog.Open(t.TempDir())
	realPM, _ := pack.NewManager(core.PackConfig{Dir: t.TempDir(), TargetPackBytes: 64 * 1024})

	mpm := &countingMockPM{
		failOnPutN: 2,
		putErr:     fmt.Errorf("%w: manifest putblock", errMock),
		real:       realPM,
	}
	s := buildStore(t, cat, mpm, cidutil.NewBuilder(), manifest.NewCodec(core.LimitsConfig{MaxChunksPerObject: 200000}), transform.NewNone())
	defer s.Close()

	_, err := s.Put(context.Background(), core.Key{Namespace: "t", ID: "1"}, blobcas.PutInput{
		Canonical: bytes.NewReader([]byte("content")),
	}, blobcas.PutMeta{Canonical: true})

	if !errors.Is(err, errMock) {
		t.Errorf("expected manifest PutBlock error, got: %v", err)
	}
}

func TestDeepMock_Put_ManifestPutPackForCIDError(t *testing.T) {
	realCat, _ := catalog.Open(t.TempDir())

	mc := &countingMockCatalog{
		failOnPutPackN: 2,
		putPackErr:     fmt.Errorf("%w: manifest putpack", errMock),
		real:           realCat,
	}

	pm, _ := pack.NewManager(core.PackConfig{Dir: t.TempDir(), TargetPackBytes: 64 * 1024})
	s := buildStore(t, mc, pm, cidutil.NewBuilder(), manifest.NewCodec(core.LimitsConfig{MaxChunksPerObject: 200000}), transform.NewNone())
	defer s.Close()

	_, err := s.Put(context.Background(), core.Key{Namespace: "t", ID: "1"}, blobcas.PutInput{
		Canonical: bytes.NewReader([]byte("content")),
	}, blobcas.PutMeta{Canonical: true})

	if !errors.Is(err, errMock) {
		t.Errorf("expected manifest PutPackForCID error, got: %v", err)
	}
}
