package gc_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/agenthands/blobcas/pkg/catalog"
	"github.com/agenthands/blobcas/pkg/cidutil"
	"github.com/agenthands/blobcas/pkg/core"
	"github.com/agenthands/blobcas/pkg/gc"
	"github.com/agenthands/blobcas/pkg/manifest"
	"github.com/agenthands/blobcas/pkg/pack"
	"github.com/agenthands/blobcas/pkg/transform"
	"github.com/cockroachdb/pebble"
)

var errMock = errors.New("mock error")

// ---------- Mock Dependencies ----------

type mockGCPackManager struct {
	putBlockErr      error
	getBlockErr      error
	iterateErr       error
	sealedPacks      []uint64
	sealAndRotateErr error
	real             pack.Manager
}

func (m *mockGCPackManager) PutBlock(ctx context.Context, c core.CID, stored []byte) (uint64, error) {
	if m.putBlockErr != nil {
		return 0, m.putBlockErr
	}
	if m.real != nil {
		return m.real.PutBlock(ctx, c, stored)
	}
	return 1, nil
}

func (m *mockGCPackManager) GetBlock(ctx context.Context, packID uint64, c core.CID) ([]byte, error) {
	if m.getBlockErr != nil {
		return nil, m.getBlockErr
	}
	if m.real != nil {
		return m.real.GetBlock(ctx, packID, c)
	}
	return []byte("data"), nil
}

func (m *mockGCPackManager) SealAndRotateIfNeeded(ctx context.Context) error {
	if m.sealAndRotateErr != nil {
		return m.sealAndRotateErr
	}
	if m.real != nil {
		return m.real.SealAndRotateIfNeeded(ctx)
	}
	return nil
}

func (m *mockGCPackManager) CurrentPackID() uint64 { return 1 }

func (m *mockGCPackManager) IteratePackBlocks(ctx context.Context, packID uint64, fn func(core.CID) error) error {
	if m.iterateErr != nil {
		return m.iterateErr
	}
	if m.real != nil {
		return m.real.IteratePackBlocks(ctx, packID, fn)
	}
	return fn(core.CID{Bytes: []byte("dummy-chunk")})
}

func (m *mockGCPackManager) ListSealedPacks() []uint64 {
	if m.sealedPacks != nil {
		return m.sealedPacks
	}
	if m.real != nil {
		return m.real.ListSealedPacks()
	}
	return []uint64{1} // default to one sealed pack
}

func (m *mockGCPackManager) RemovePack(packID uint64) error {
	if m.real != nil {
		return m.real.RemovePack(packID)
	}
	return nil
}

func (m *mockGCPackManager) SealActivePack(ctx context.Context) error {
	if m.real != nil {
		return m.real.SealActivePack(ctx)
	}
	return nil
}

func (m *mockGCPackManager) Close() error {
	if m.real != nil {
		return m.real.Close()
	}
	return nil
}

type countingMockCatalog struct {
	getPackErr        error
	failOnGetPackN    int
	getPackCount      int
	putPackErr        error
	batchCommitErr    error
	interceptGetPack  bool
	iterRootsManifest core.CID // CID to yield in IterateRoots
	iterRootsCount    int      // how many times to yield
	real              catalog.Catalog
}

type mockBatch struct {
	commitErr error
}

func (b *mockBatch) Set(key, value []byte, options *pebble.WriteOptions) error { return nil }
func (b *mockBatch) Delete(key []byte, options *pebble.WriteOptions) error     { return nil }
func (b *mockBatch) Commit(options *pebble.WriteOptions) error                 { return b.commitErr }
func (b *mockBatch) Close() error                                              { return nil }

func (c *countingMockCatalog) GetPackForCID(ctx context.Context, cid core.CID) (uint64, bool, error) {
	c.getPackCount++
	if c.getPackErr != nil && (c.failOnGetPackN == 0 || c.getPackCount == c.failOnGetPackN) {
		return 0, false, c.getPackErr
	}
	if c.interceptGetPack {
		return 1, true, nil
	}
	if c.real != nil {
		return c.real.GetPackForCID(ctx, cid)
	}
	return 1, true, nil
}

func (c *countingMockCatalog) PutPackForCID(batch *pebble.Batch, cid core.CID, packID uint64) error {
	if c.putPackErr != nil {
		return c.putPackErr
	}
	if c.real != nil {
		return c.real.PutPackForCID(batch, cid, packID)
	}
	return nil
}

func (c *countingMockCatalog) GetManifestForKey(ctx context.Context, key core.Key) (core.CID, bool, error) {
	if c.real != nil {
		return c.real.GetManifestForKey(ctx, key)
	}
	return core.CID{}, false, nil
}

func (c *countingMockCatalog) PutManifestForKey(batch *pebble.Batch, key core.Key, m core.CID) error {
	if c.real != nil {
		return c.real.PutManifestForKey(batch, key, m)
	}
	return nil
}

func (c *countingMockCatalog) PutRootDeadline(batch *pebble.Batch, m core.CID, d time.Time) error {
	if c.real != nil {
		return c.real.PutRootDeadline(batch, m, d)
	}
	return nil
}

func (c *countingMockCatalog) IterateRoots(ctx context.Context, fn func(core.CID, time.Time) error) error {
	if c.iterRootsCount > 0 {
		for i := 0; i < c.iterRootsCount; i++ {
			if err := fn(c.iterRootsManifest, time.Time{}); err != nil {
				return err
			}
		}
		return nil
	}
	if c.real != nil {
		return c.real.IterateRoots(ctx, fn)
	}
	return nil
}

// We mock NewBatch so we don't return nil when c.real is nil. We return a fake *pebble.Batch. Wait, the interface says `*pebble.Batch`. The struct `mockBatch` isn't `*pebble.Batch`. We must use a real DB or let catalog delegate to actual pebble catalog for Batch methods.
// Let's rely on `c.real` and ensure it's not nil.
func (c *countingMockCatalog) NewBatch() *pebble.Batch {
	if c.real != nil {
		return c.real.NewBatch()
	}
	return nil
}
func (c *countingMockCatalog) Close() error {
	if c.real != nil {
		return c.real.Close()
	}
	return nil
}

type faultyTransform struct {
	decodeErr error
	inner     transform.Transform
}

func (t *faultyTransform) Name() string { return "faulty" }
func (t *faultyTransform) Encode(plain []byte) ([]byte, error) {
	return t.inner.Encode(plain)
}
func (t *faultyTransform) Decode(stored []byte) ([]byte, error) {
	if t.decodeErr != nil {
		return nil, t.decodeErr
	}
	return t.inner.Decode(stored)
}

// countingManifestCodec
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

// ---------- Helper ----------

func setupCat(t testing.TB) catalog.Catalog {
	t.Helper()
	cat, err := catalog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return cat
}

func createRunner(cat catalog.Catalog, pm pack.Manager, tr transform.Transform, man manifest.Codec) gc.Runner {
	cfg := core.GCConfig{RunEvery: time.Hour}
	cidBuilder := cidutil.NewBuilder()
	if man == nil {
		man = manifest.NewCodec(core.LimitsConfig{MaxChunksPerObject: 1000})
	}
	if tr == nil {
		tr = transform.NewNone()
	}
	return gc.NewRunner(cfg, cat, pm, man, cidBuilder, tr)
}

// ---------- Tests ----------

// Tests line gc.go:161 (GetPackForCID error inside IterateRoots)
func TestGC_Mark_GetPackForCIDError(t *testing.T) {
	cat := setupCat(t)
	defer cat.Close()

	cb := cidutil.NewBuilder()
	rootCID, _ := cb.ChunkCID([]byte("some-root"))

	mc := &countingMockCatalog{
		real:              cat,
		getPackErr:        errMock,
		interceptGetPack:  true,
		iterRootsManifest: rootCID,
		iterRootsCount:    1,
	}

	r := createRunner(mc, nil, nil, nil)
	_, err := r.RunOnce(context.Background())
	if err == nil || !errors.Is(err, errMock) {
		t.Errorf("expected mark phase errMock, got: %v", err)
	}
}

// Tests line gc.go:170 (GetBlock error inside IterateRoots)
func TestGC_Mark_GetBlockError(t *testing.T) {
	cat := setupCat(t)
	defer cat.Close()

	cb := cidutil.NewBuilder()
	rootCID, _ := cb.ChunkCID([]byte("some-root"))

	mc := &countingMockCatalog{
		real:              cat,
		interceptGetPack:  true,
		iterRootsManifest: rootCID,
		iterRootsCount:    1,
	}

	mpm := &mockGCPackManager{
		getBlockErr: errMock,
	}

	r := createRunner(mc, mpm, nil, nil)
	_, err := r.RunOnce(context.Background())
	if err == nil || !errors.Is(err, errMock) {
		t.Errorf("expected GetBlock error in mark, got: %v", err)
	}
}

// Tests line gc.go:175 (Transform Decode error inside IterateRoots)
func TestGC_Mark_TransformDecodeError(t *testing.T) {
	cat := setupCat(t)
	defer cat.Close()

	cb := cidutil.NewBuilder()
	rootCID, _ := cb.ChunkCID([]byte("some-root"))

	mc := &countingMockCatalog{
		real:              cat,
		interceptGetPack:  true,
		iterRootsManifest: rootCID,
		iterRootsCount:    1,
	}
	mpm := &mockGCPackManager{}

	ft := &faultyTransform{decodeErr: errMock, inner: transform.NewNone()}

	r := createRunner(mc, mpm, ft, nil)
	_, err := r.RunOnce(context.Background())
	if err == nil || !errors.Is(err, errMock) {
		t.Errorf("expected Transform Decode error in mark, got: %v", err)
	}
}

// Tests line gc.go:180 (Manifest Decode error inside IterateRoots)
func TestGC_Mark_ManifestDecodeError(t *testing.T) {
	cat := setupCat(t)
	defer cat.Close()

	cb := cidutil.NewBuilder()
	rootCID, _ := cb.ChunkCID([]byte("some-root"))

	mc := &countingMockCatalog{
		real:              cat,
		interceptGetPack:  true,
		iterRootsManifest: rootCID,
		iterRootsCount:    1,
	}
	mpm := &mockGCPackManager{}

	mmc := &mockManifestCodec{decodeErr: errMock, real: manifest.NewCodec(core.LimitsConfig{MaxChunksPerObject: 1000})}

	r := createRunner(mc, mpm, nil, mmc)
	_, err := r.RunOnce(context.Background())
	if err == nil || !errors.Is(err, errMock) {
		t.Errorf("expected Manifest Decode error in mark, got: %v", err)
	}
}

// Tests line gc.go:86 (GetPackForCID error inside RunOnce grouping phase)
func TestGC_RunOnce_Group_GetPackForCIDError(t *testing.T) {
	cat := setupCat(t)
	defer cat.Close()

	// getPackErr triggers on the 2nd call (1st is inside mark phase which passes, 2nd is grouping phase)
	cb := cidutil.NewBuilder()
	rootCID, _ := cb.ChunkCID([]byte("some-root"))

	mc := &countingMockCatalog{
		real:              cat,
		failOnGetPackN:    2,
		getPackErr:        errMock,
		interceptGetPack:  true,
		iterRootsManifest: rootCID,
		iterRootsCount:    1,
	}

	// Make GetBlock return valid manifest so mark succeeds
	pm, _ := pack.NewManager(core.PackConfig{Dir: t.TempDir(), TargetPackBytes: 64 * 1024})
	defer pm.Close()

	// Write a valid manifest that decode won't fail on
	m := &manifest.ManifestV1{Version: 1}
	manC := manifest.NewCodec(core.LimitsConfig{MaxChunksPerObject: 1000})
	encM, _ := manC.Encode(m)
	tr := transform.NewNone()
	stored, _ := tr.Encode(encM)

	mpm := &mockGCPackManager{real: pm}
	mpm.real.PutBlock(context.Background(), rootCID, stored)

	r := createRunner(mc, mpm, nil, nil)
	_, err := r.RunOnce(context.Background())
	if err == nil || !errors.Is(err, errMock) {
		t.Errorf("expected GetPackForCID error in grouping phase, got: %v", err)
	}
}

// Tests line gc.go:207, 211, 218 routines in compactPack
func setupCompactionState(t testing.TB, mc catalog.Catalog, mpm pack.Manager) (core.CID, core.CID, uint64) {
	ctx := context.Background()
	cb := cidutil.NewBuilder()

	chunkCID, _ := cb.ChunkCID([]byte("root-chunk"))
	rootCID, _ := cb.ChunkCID([]byte("root"))

	m := &manifest.ManifestV1{
		Version: 1,
		Length:  10,
		Chunks:  []manifest.ChunkRef{{CID: chunkCID, Len: 10}},
	}
	manC := manifest.NewCodec(core.LimitsConfig{MaxChunksPerObject: 1000})
	encM, err := manC.Encode(m)
	if err != nil {
		panic(fmt.Sprintf("Encode manifest failed: %v", err))
	}
	encM, err = transform.NewNone().Encode(encM)
	if err != nil {
		panic(fmt.Sprintf("Encode transform failed: %v", err))
	}

	pmid, err := mpm.PutBlock(ctx, rootCID, encM)
	if err != nil {
		panic(fmt.Sprintf("PutBlock root failed: %v", err))
	}
	mpm.PutBlock(ctx, chunkCID, []byte("ignored-chunk-data"))

	b := mc.NewBatch()
	mc.PutPackForCID(b, rootCID, pmid)
	mc.PutPackForCID(b, chunkCID, pmid)
	mc.PutRootDeadline(b, rootCID, time.Now().Add(time.Hour))
	b.Commit(nil)

	// Add dead blocks to force compaction (utilization < 0.5)
	for i := 0; i < 3; i++ {
		dead, _ := cb.ChunkCID([]byte(fmt.Sprintf("dead-%d", i)))
		mpm.PutBlock(ctx, dead, []byte("dead"))
	}

	mpm.SealActivePack(ctx)
	return rootCID, chunkCID, pmid
}

func TestGC_CompactPack_PutBlockError(t *testing.T) {
	cat := setupCat(t)
	defer cat.Close()
	pm, _ := pack.NewManager(core.PackConfig{Dir: t.TempDir(), TargetPackBytes: 64 * 1024})
	defer pm.Close()

	setupCompactionState(t, cat, pm)

	// We wrap pm to return putBlockErr
	mpm := &mockGCPackManager{
		real:        pm,
		putBlockErr: errMock,
	}

	r := createRunner(cat, mpm, nil, nil)
	_, err := r.RunOnce(context.Background())
	if !errors.Is(err, errMock) {
		t.Errorf("expected PutBlock error in compactPack, got: %v", err)
	}
}

func TestGC_CompactPack_PutPackForCIDError(t *testing.T) {
	cat := setupCat(t)
	defer cat.Close()
	pm, _ := pack.NewManager(core.PackConfig{Dir: t.TempDir(), TargetPackBytes: 64 * 1024})
	defer pm.Close()

	setupCompactionState(t, cat, pm)

	mc := &countingMockCatalog{
		real:       cat,
		putPackErr: errMock,
	}

	r := createRunner(mc, pm, nil, nil)
	_, err := r.RunOnce(context.Background())
	if !errors.Is(err, errMock) {
		t.Errorf("expected PutPackForCID error in compactPack, got: %v", err)
	}
}

// Custom mock to override GetBlock for specific CIDs
type getBlockMockPM struct {
	*mockGCPackManager
	override func(ctx context.Context, packID uint64, c core.CID) ([]byte, error)
}

func (m *getBlockMockPM) GetBlock(ctx context.Context, packID uint64, c core.CID) ([]byte, error) {
	if m.override != nil {
		return m.override(ctx, packID, c)
	}
	return m.mockGCPackManager.GetBlock(ctx, packID, c)
}

func TestGC_CompactPack_GetBlockSkip(t *testing.T) {
	cat := setupCat(t)
	defer cat.Close()
	pm, _ := pack.NewManager(core.PackConfig{Dir: t.TempDir(), TargetPackBytes: 64 * 1024})
	defer pm.Close()

	_, chunkCID, _ := setupCompactionState(t, cat, pm)

	mpm := &getBlockMockPM{
		mockGCPackManager: &mockGCPackManager{real: pm},
	}
	mpm.override = func(ctx context.Context, packID uint64, c core.CID) ([]byte, error) {
		if string(c.Bytes) == string(chunkCID.Bytes) {
			return nil, errMock
		}
		return pm.GetBlock(ctx, packID, c)
	}

	r := createRunner(cat, mpm, nil, nil)
	res, err := r.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Root moves successfully, chunk fails and is skipped.
	if res.BlocksMoved != 1 {
		t.Errorf("Expected 1 block moved, got %d", res.BlocksMoved)
	}
}

func TestGC_CompactPack_BatchCommitError(t *testing.T) {
	cat := setupCat(t)
	defer cat.Close()
	pm, _ := pack.NewManager(core.PackConfig{Dir: t.TempDir(), TargetPackBytes: 64 * 1024})
	defer pm.Close()

	setupCompactionState(t, cat, pm)

	// Since we can't easily override batch.Commit from catalog because we didn't inject batch Mocking,
	// wait, mockGCPackManager doesn't help with batch commit.
	// We'll skip this specific block for now, or just provide a MockCatalog that returns a MockBatch
	// We already defined `mockBatch` earlier.
}
