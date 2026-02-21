package blobcas_test

import (
	"bytes"
	"context"
	"errors"
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

var errMock = errors.New("mock error")

// ---------- Mock Catalog ----------

type mockCatalog struct {
	getPackErr     error
	putPackErr     error
	getManifestErr error
	putManifestErr error
	putRootErr     error
	closeErr       error
	real           catalog.Catalog
}

func (m *mockCatalog) GetPackForCID(ctx context.Context, cid core.CID) (uint64, bool, error) {
	if m.getPackErr != nil {
		return 0, false, m.getPackErr
	}
	return m.real.GetPackForCID(ctx, cid)
}

func (m *mockCatalog) PutPackForCID(batch *pebble.Batch, cid core.CID, packID uint64) error {
	if m.putPackErr != nil {
		return m.putPackErr
	}
	return m.real.PutPackForCID(batch, cid, packID)
}

func (m *mockCatalog) GetManifestForKey(ctx context.Context, key core.Key) (core.CID, bool, error) {
	if m.getManifestErr != nil {
		return core.CID{}, false, m.getManifestErr
	}
	return m.real.GetManifestForKey(ctx, key)
}

func (m *mockCatalog) PutManifestForKey(batch *pebble.Batch, key core.Key, mani core.CID) error {
	if m.putManifestErr != nil {
		return m.putManifestErr
	}
	return m.real.PutManifestForKey(batch, key, mani)
}

func (m *mockCatalog) PutRootDeadline(batch *pebble.Batch, mani core.CID, deadline time.Time) error {
	if m.putRootErr != nil {
		return m.putRootErr
	}
	return m.real.PutRootDeadline(batch, mani, deadline)
}

func (m *mockCatalog) IterateRoots(ctx context.Context, fn func(core.CID, time.Time) error) error {
	return m.real.IterateRoots(ctx, fn)
}

func (m *mockCatalog) NewBatch() *pebble.Batch { return m.real.NewBatch() }
func (m *mockCatalog) Close() error {
	if m.closeErr != nil {
		return m.closeErr
	}
	return m.real.Close()
}

// ---------- Helpers ----------

func openRealCatalog(t testing.TB) catalog.Catalog {
	t.Helper()
	cat, err := catalog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return cat
}

func makeMockStore(t testing.TB, cat catalog.Catalog, tr transform.Transform) blobcas.Store {
	t.Helper()
	pm, err := pack.NewManager(core.PackConfig{Dir: t.TempDir(), TargetPackBytes: 64 * 1024})
	if err != nil {
		t.Fatal(err)
	}
	return blobcas.NewStoreForTest(
		core.Config{
			Chunking: core.ChunkingConfig{Min: 64, Avg: 128, Max: 256},
			GC:       core.GCConfig{DefaultRootTTL: 24 * time.Hour},
			Limits:   core.LimitsConfig{MaxChunksPerObject: 200000},
		},
		chunker.NewChunker(chunker.Config{Min: 64, Avg: 128, Max: 256}),
		cidutil.NewBuilder(),
		manifest.NewCodec(core.LimitsConfig{MaxChunksPerObject: 200000}),
		pm, cat, tr,
	)
}

// ---------- Tests ----------

func TestMock_Put_CatalogGetPackError(t *testing.T) {
	realCat := openRealCatalog(t)
	mc := &mockCatalog{real: realCat, getPackErr: errMock}
	s := makeMockStore(t, mc, transform.NewNone())
	defer s.Close()

	_, err := s.Put(context.Background(), core.Key{Namespace: "t", ID: "1"}, blobcas.PutInput{
		Canonical: bytes.NewReader([]byte("content")),
	}, blobcas.PutMeta{Canonical: true})

	if !errors.Is(err, errMock) {
		t.Errorf("expected mock error, got: %v", err)
	}
}

func TestMock_Get_CatalogGetPackError(t *testing.T) {
	mc := &mockCatalog{real: openRealCatalog(t), getPackErr: errMock}
	s := makeMockStore(t, mc, transform.NewNone())
	defer s.Close()

	_, _, err := s.Get(context.Background(), core.Ref{ManifestCID: core.CID{Bytes: []byte("x")}})
	if !errors.Is(err, errMock) {
		t.Errorf("expected mock error, got: %v", err)
	}
}

func TestMock_Stat_CatalogGetPackError(t *testing.T) {
	mc := &mockCatalog{real: openRealCatalog(t), getPackErr: errMock}
	s := makeMockStore(t, mc, transform.NewNone())
	defer s.Close()

	_, err := s.Stat(context.Background(), core.Ref{ManifestCID: core.CID{Bytes: []byte("x")}})
	if !errors.Is(err, errMock) {
		t.Errorf("expected mock error, got: %v", err)
	}
}

func TestMock_GetChunk_CatalogGetPackError(t *testing.T) {
	mc := &mockCatalog{real: openRealCatalog(t), getPackErr: errMock}
	s := makeMockStore(t, mc, transform.NewNone())
	defer s.Close()

	_, _, err := s.GetChunk(context.Background(), core.CID{Bytes: []byte("x")})
	if !errors.Is(err, errMock) {
		t.Errorf("expected mock error, got: %v", err)
	}
}

func TestMock_Resolve_CatalogGetManifestError(t *testing.T) {
	mc := &mockCatalog{real: openRealCatalog(t), getManifestErr: errMock}
	s := makeMockStore(t, mc, transform.NewNone())
	defer s.Close()

	_, err := s.Resolve(context.Background(), core.Key{Namespace: "t", ID: "1"})
	if !errors.Is(err, errMock) {
		t.Errorf("expected mock error, got: %v", err)
	}
}

func TestMock_Put_PutManifestError(t *testing.T) {
	mc := &mockCatalog{real: openRealCatalog(t), putManifestErr: errMock}
	s := makeMockStore(t, mc, transform.NewNone())
	defer s.Close()

	_, err := s.Put(context.Background(), core.Key{Namespace: "t", ID: "1"}, blobcas.PutInput{
		Canonical: bytes.NewReader([]byte("content for manifest err")),
	}, blobcas.PutMeta{Canonical: true})

	if !errors.Is(err, errMock) {
		t.Errorf("expected mock error from PutManifestForKey, got: %v", err)
	}
}

func TestMock_Put_PutRootDeadlineError(t *testing.T) {
	mc := &mockCatalog{real: openRealCatalog(t), putRootErr: errMock}
	s := makeMockStore(t, mc, transform.NewNone())
	defer s.Close()

	_, err := s.Put(context.Background(), core.Key{Namespace: "t", ID: "1"}, blobcas.PutInput{
		Canonical: bytes.NewReader([]byte("content for root err")),
	}, blobcas.PutMeta{Canonical: true})

	if !errors.Is(err, errMock) {
		t.Errorf("expected mock error from PutRootDeadline, got: %v", err)
	}
}

func TestMock_Put_PutPackForCIDError(t *testing.T) {
	mc := &mockCatalog{real: openRealCatalog(t), putPackErr: errMock}
	s := makeMockStore(t, mc, transform.NewNone())
	defer s.Close()

	_, err := s.Put(context.Background(), core.Key{Namespace: "t", ID: "1"}, blobcas.PutInput{
		Canonical: bytes.NewReader([]byte("content for putpack err")),
	}, blobcas.PutMeta{Canonical: true})

	if !errors.Is(err, errMock) {
		t.Errorf("expected mock error from PutPackForCID, got: %v", err)
	}
}

func TestMock_CloseError(t *testing.T) {
	mc := &mockCatalog{real: openRealCatalog(t), closeErr: errMock}
	s := makeMockStore(t, mc, transform.NewNone())

	err := s.Close()
	if !errors.Is(err, errMock) {
		t.Errorf("expected mock error from Close, got: %v", err)
	}
}
