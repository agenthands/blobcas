package blobcas_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
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
)

var errFault = errors.New("injected fault")

// ---------- Faulty Transform ----------

type faultyTransform struct {
	encodeErr error
	decodeErr error
	inner     transform.Transform
}

func (t *faultyTransform) Name() string { return "faulty" }

func (t *faultyTransform) Encode(plain []byte) ([]byte, error) {
	if t.encodeErr != nil {
		return nil, t.encodeErr
	}
	return t.inner.Encode(plain)
}

func (t *faultyTransform) Decode(stored []byte) ([]byte, error) {
	if t.decodeErr != nil {
		return nil, t.decodeErr
	}
	return t.inner.Decode(stored)
}

func makeFaultStore(catDir, packDir string, tr transform.Transform) blobcas.Store {
	cat, _ := catalog.Open(catDir)
	pm, _ := pack.NewManager(core.PackConfig{Dir: packDir, TargetPackBytes: 64 * 1024})

	return blobcas.NewStoreForTest(
		core.Config{
			Chunking: core.ChunkingConfig{Min: 64, Avg: 128, Max: 256},
			GC:       core.GCConfig{DefaultRootTTL: 24 * time.Hour},
			Limits:   core.LimitsConfig{MaxChunksPerObject: 200000},
		},
		chunker.NewChunker(chunker.Config{Min: 64, Avg: 128, Max: 256}),
		cidutil.NewBuilder(),
		manifest.NewCodec(core.LimitsConfig{MaxChunksPerObject: 200000}),
		pm,
		cat,
		tr,
	)
}

// ---------- Tests ----------

func TestFault_Put_TransformEncodeError(t *testing.T) {
	dir := t.TempDir()
	ft := &faultyTransform{
		encodeErr: fmt.Errorf("%w: encode fault", errFault),
		inner:     transform.NewNone(),
	}
	s := makeFaultStore(dir+"/catalog", dir+"/packs", ft)
	defer s.Close()

	ctx := context.Background()
	key := core.Key{Namespace: "test", ID: "encode-err"}
	_, err := s.Put(ctx, key, blobcas.PutInput{
		Canonical: bytes.NewReader([]byte("some content that will be chunked")),
	}, blobcas.PutMeta{Canonical: true})

	if err == nil {
		t.Fatal("expected error from faulty encode")
	}
	if !errors.Is(err, errFault) {
		t.Errorf("expected fault error, got: %v", err)
	}
}

func TestFault_Get_TransformDecodeError(t *testing.T) {
	dir := t.TempDir()
	catDir := dir + "/catalog"
	packDir := dir + "/packs"

	// Store with good transform
	s1 := makeFaultStore(catDir, packDir, transform.NewNone())

	ctx := context.Background()
	key := core.Key{Namespace: "test", ID: "decode-err"}
	ref, err := s1.Put(ctx, key, blobcas.PutInput{
		Canonical: bytes.NewReader([]byte("content for decode test")),
	}, blobcas.PutMeta{Canonical: true})
	if err != nil {
		t.Fatal(err)
	}
	s1.Close() // release locks

	// Reopen with faulty decode
	ft := &faultyTransform{
		decodeErr: fmt.Errorf("%w: decode fault", errFault),
		inner:     transform.NewNone(),
	}
	s2 := makeFaultStore(catDir, packDir, ft)
	defer s2.Close()

	_, _, err = s2.Get(ctx, ref)
	if err == nil {
		t.Fatal("expected error from faulty decode in Get")
	}
	t.Logf("Got expected Get error: %v", err)
}

func TestFault_Stat_TransformDecodeError(t *testing.T) {
	dir := t.TempDir()
	catDir := dir + "/catalog"
	packDir := dir + "/packs"

	s1 := makeFaultStore(catDir, packDir, transform.NewNone())
	ctx := context.Background()
	key := core.Key{Namespace: "test", ID: "stat-err"}
	ref, err := s1.Put(ctx, key, blobcas.PutInput{
		Canonical: bytes.NewReader([]byte("stat content")),
	}, blobcas.PutMeta{Canonical: true})
	if err != nil {
		t.Fatal(err)
	}
	s1.Close()

	ft := &faultyTransform{
		decodeErr: fmt.Errorf("%w: decode fault", errFault),
		inner:     transform.NewNone(),
	}
	s2 := makeFaultStore(catDir, packDir, ft)
	defer s2.Close()

	_, err = s2.Stat(ctx, ref)
	if err == nil {
		t.Fatal("expected error from faulty decode in Stat")
	}
	t.Logf("Got expected Stat error: %v", err)
}

func TestFault_GetChunk_TransformDecodeError(t *testing.T) {
	dir := t.TempDir()
	catDir := dir + "/catalog"
	packDir := dir + "/packs"

	s1 := makeFaultStore(catDir, packDir, transform.NewNone())
	ctx := context.Background()

	key := core.Key{Namespace: "test", ID: "getchunk-decode"}
	ref, err := s1.Put(ctx, key, blobcas.PutInput{
		Canonical: bytes.NewReader([]byte("getchunk content for decode error")),
	}, blobcas.PutMeta{Canonical: true})
	if err != nil {
		t.Fatal(err)
	}
	s1.Close()

	ft := &faultyTransform{
		decodeErr: fmt.Errorf("%w: decode fault", errFault),
		inner:     transform.NewNone(),
	}
	s2 := makeFaultStore(catDir, packDir, ft)
	defer s2.Close()

	// Try GetChunk with the manifest CID — it'll find it, get block, but decode fails
	_, _, err = s2.GetChunk(ctx, ref.ManifestCID)
	if err == nil {
		t.Fatal("expected error from faulty decode in GetChunk")
	}
	t.Logf("Got expected GetChunk error: %v", err)
}

func TestFault_Put_ManifestEncodeFault(t *testing.T) {
	dir := t.TempDir()
	catDir := dir + "/catalog"
	packDir := dir + "/packs"

	cat, _ := catalog.Open(catDir)
	pm, _ := pack.NewManager(core.PackConfig{Dir: packDir, TargetPackBytes: 64 * 1024})

	s := blobcas.NewStoreForTest(
		core.Config{
			Chunking: core.ChunkingConfig{Min: 64, Avg: 128, Max: 256},
			GC:       core.GCConfig{DefaultRootTTL: 24 * time.Hour},
			Limits:   core.LimitsConfig{MaxChunksPerObject: 1}, // only 1 chunk
		},
		chunker.NewChunker(chunker.Config{Min: 64, Avg: 128, Max: 256}),
		cidutil.NewBuilder(),
		manifest.NewCodec(core.LimitsConfig{MaxChunksPerObject: 1}),
		pm,
		cat,
		transform.NewNone(),
	)
	defer s.Close()

	ctx := context.Background()
	key := core.Key{Namespace: "test", ID: "too-many-chunks"}
	_, err := s.Put(ctx, key, blobcas.PutInput{
		Canonical: bytes.NewReader(bytes.Repeat([]byte("x"), 1024)),
	}, blobcas.PutMeta{Canonical: true})

	if err == nil {
		t.Fatal("expected error for too many chunks in manifest")
	}
	t.Logf("Got expected error: %v", err)
}

func TestFault_ObjectReader_Close_NoActiveChunk(t *testing.T) {
	dir := t.TempDir()
	s, _ := createTestStore(t, dir)
	defer s.Close()

	ctx := context.Background()
	key := core.Key{Namespace: "test", ID: "reader-close"}
	ref, err := s.Put(ctx, key, blobcas.PutInput{
		Canonical: bytes.NewReader([]byte("reader close test data is here")),
	}, blobcas.PutMeta{Canonical: true})
	if err != nil {
		t.Fatal(err)
	}

	rc, _, err := s.Get(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}

	// Close without reading
	err = rc.Close()
	if err != nil {
		t.Errorf("Close without read should succeed: %v", err)
	}
}

func TestFault_ObjectReader_FullRead_SmallBuffer(t *testing.T) {
	dir := t.TempDir()
	s, _ := createTestStore(t, dir)
	defer s.Close()

	ctx := context.Background()
	content := bytes.Repeat([]byte("full read data "), 200)
	key := core.Key{Namespace: "test", ID: "reader-full"}
	ref, err := s.Put(ctx, key, blobcas.PutInput{
		Canonical: bytes.NewReader(content),
	}, blobcas.PutMeta{Canonical: true})
	if err != nil {
		t.Fatal(err)
	}

	rc, _, err := s.Get(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}

	var out []byte
	buf := make([]byte, 17)
	for {
		n, err := rc.Read(buf)
		out = append(out, buf[:n]...)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Read failed: %v", err)
		}
	}
	rc.Close()

	if !bytes.Equal(out, content) {
		t.Error("full read content mismatch")
	}
}

func TestFault_Resolve_Success(t *testing.T) {
	dir := t.TempDir()
	s, _ := createTestStore(t, dir)
	defer s.Close()

	ctx := context.Background()

	key := core.Key{Namespace: "test", ID: "resolve-ok"}
	ref, err := s.Put(ctx, key, blobcas.PutInput{
		Canonical: bytes.NewReader([]byte("resolve content")),
	}, blobcas.PutMeta{Canonical: true})
	if err != nil {
		t.Fatal(err)
	}

	got, err := s.Resolve(ctx, key)
	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}
	if string(got.ManifestCID.Bytes) != string(ref.ManifestCID.Bytes) {
		t.Error("Resolve returned wrong ref")
	}
}

func TestFault_HasChunk_ExistingChunk(t *testing.T) {
	dir := t.TempDir()
	s := makeFaultStore(dir+"/catalog", dir+"/packs", transform.NewNone())
	defer s.Close()

	ctx := context.Background()
	key := core.Key{Namespace: "test", ID: "haschunk-exists"}
	ref, err := s.Put(ctx, key, blobcas.PutInput{
		Canonical: bytes.NewReader([]byte("haschunk content")),
	}, blobcas.PutMeta{Canonical: true})
	if err != nil {
		t.Fatal(err)
	}

	has, err := s.HasChunk(ctx, ref.ManifestCID)
	if err != nil {
		t.Fatal(err)
	}
	if !has {
		t.Error("expected HasChunk to return true for manifest CID")
	}
}

func TestFault_BothDeadlineAndTTL(t *testing.T) {
	dir := t.TempDir()
	s, _ := createTestStore(t, dir)
	defer s.Close()

	ctx := context.Background()
	deadline := time.Now().Add(time.Hour)
	ttl := time.Hour

	key := core.Key{Namespace: "test", ID: "both"}
	_, err := s.Put(ctx, key, blobcas.PutInput{
		Canonical: bytes.NewReader([]byte("content")),
	}, blobcas.PutMeta{
		Canonical:    true,
		RootDeadline: &deadline,
		RootTTL:      &ttl,
	})
	if err == nil {
		t.Fatal("expected error when both deadline and TTL are set")
	}
	if !errors.Is(err, core.ErrInvalidInput) {
		t.Errorf("expected ErrInvalidInput, got: %v", err)
	}
}

func TestFault_Put_BatchCommit(t *testing.T) {
	dir := t.TempDir()
	s := makeFaultStore(dir+"/catalog", dir+"/packs", transform.NewNone())
	defer s.Close()

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		key := core.Key{Namespace: "test", ID: fmt.Sprintf("batch-%d", i)}
		_, err := s.Put(ctx, key, blobcas.PutInput{
			Canonical: bytes.NewReader([]byte(fmt.Sprintf("batch content %d", i))),
		}, blobcas.PutMeta{Canonical: true})
		if err != nil {
			t.Fatalf("Put %d failed: %v", i, err)
		}
	}
}

func TestFault_EmptyContent(t *testing.T) {
	dir := t.TempDir()
	s, _ := createTestStore(t, dir)
	defer s.Close()

	ctx := context.Background()
	key := core.Key{Namespace: "test", ID: "empty"}
	ref, err := s.Put(ctx, key, blobcas.PutInput{
		Canonical: bytes.NewReader(nil),
	}, blobcas.PutMeta{Canonical: true})
	if err != nil {
		t.Fatal(err)
	}

	rc, info, err := s.Get(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()

	if info.Length != 0 {
		t.Errorf("expected 0 length, got %d", info.Length)
	}
	if len(got) != 0 {
		t.Errorf("expected empty content, got %d bytes", len(got))
	}
}
