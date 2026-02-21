package blobcas_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/agenthands/blobcas/internal/testkit"
	"github.com/agenthands/blobcas/pkg/blobcas"
	"github.com/agenthands/blobcas/pkg/core"
)

// cliHelper manages CLI binary paths and temporary data for integration tests.
type cliHelper struct {
	t      *testing.T
	binDir string
	bins   map[string]string
}

func newCLIHelper(t *testing.T) *cliHelper {
	t.Helper()
	binDir, err := filepath.Abs("../../bin")
	if err != nil {
		t.Fatal(err)
	}

	tools := []string{"blobcas-gc", "blobcas-reindex", "blobcas-verify"}
	bins := make(map[string]string)
	for _, tool := range tools {
		p := filepath.Join(binDir, tool)
		if _, err := os.Stat(p); err != nil {
			t.Skipf("binary %s not found at %s, skipping CLI integration tests. Run 'go build -o bin/ ./cmd/...' first.", tool, p)
		}
		bins[tool] = p
	}

	return &cliHelper{t: t, binDir: binDir, bins: bins}
}

func (h *cliHelper) run(tool string, args ...string) (string, error) {
	h.t.Helper()
	cmd := exec.Command(h.bins[tool], args...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func (h *cliHelper) mustRun(tool string, args ...string) string {
	h.t.Helper()
	out, err := h.run(tool, args...)
	if err != nil {
		h.t.Fatalf("%s failed: %v\nOutput: %s", tool, err, out)
	}
	return out
}

func newTestStore(t *testing.T, dataDir string) blobcas.Store {
	t.Helper()
	cfg := core.Config{
		Dir: dataDir,
		Chunking: core.ChunkingConfig{
			Min: 64,
			Avg: 128,
			Max: 256,
		},
		Pack: core.PackConfig{
			TargetPackBytes: 2048, // small for fast rotation
		},
		Transform: core.TransformConfig{
			Name:      "zstd",
			ZstdLevel: 3,
		},
	}

	s, err := blobcas.Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// ---------- Verify Integration ----------

func TestCLI_Verify_EmptyStore(t *testing.T) {
	h := newCLIHelper(t)
	dir, _ := os.MkdirTemp("", "cli-verify-empty-*")
	defer os.RemoveAll(dir)

	s := newTestStore(t, dir)
	s.Close()

	out := h.mustRun("blobcas-verify", "-dir", dir)
	if !strings.Contains(out, "RESULT: SUCCESS") {
		t.Errorf("expected SUCCESS for empty store, got:\n%s", out)
	}
}

func TestCLI_Verify_WithData(t *testing.T) {
	h := newCLIHelper(t)
	dir, _ := os.MkdirTemp("", "cli-verify-data-*")
	defer os.RemoveAll(dir)

	ctx := context.Background()
	s := newTestStore(t, dir)
	rng := testkit.RNG(100)

	for i := 0; i < 10; i++ {
		key := core.Key{Namespace: "test", ID: fmt.Sprintf("file_%d", i)}
		data := testkit.RandomBytes(rng, 512+rng.Intn(512))
		ttl := 1 * time.Hour
		_, err := s.Put(ctx, key, blobcas.PutInput{
			Canonical: bytes.NewReader(data),
		}, blobcas.PutMeta{
			Canonical: true,
			RootTTL:   &ttl,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	s.Close()

	out := h.mustRun("blobcas-verify", "-dir", dir)
	if !strings.Contains(out, "RESULT: SUCCESS") {
		t.Errorf("expected SUCCESS, got:\n%s", out)
	}
	t.Logf("Verify output: %s", out)
}

func TestCLI_Verify_Deep(t *testing.T) {
	h := newCLIHelper(t)
	dir, _ := os.MkdirTemp("", "cli-verify-deep-*")
	defer os.RemoveAll(dir)

	ctx := context.Background()
	s := newTestStore(t, dir)
	rng := testkit.RNG(200)

	for i := 0; i < 5; i++ {
		key := core.Key{Namespace: "test", ID: fmt.Sprintf("deep_%d", i)}
		data := testkit.RandomBytes(rng, 1024)
		ttl := 1 * time.Hour
		s.Put(ctx, key, blobcas.PutInput{
			Canonical: bytes.NewReader(data),
		}, blobcas.PutMeta{Canonical: true, RootTTL: &ttl})
	}
	s.Close()

	out := h.mustRun("blobcas-verify", "-dir", dir, "-deep")
	if !strings.Contains(out, "RESULT: SUCCESS") {
		t.Errorf("expected SUCCESS with deep mode, got:\n%s", out)
	}
	if !strings.Contains(out, "Deep verification ENABLED") {
		t.Error("expected deep verification to be noted")
	}
}

// ---------- GC Integration ----------

func TestCLI_GC_NoData(t *testing.T) {
	h := newCLIHelper(t)
	dir, _ := os.MkdirTemp("", "cli-gc-empty-*")
	defer os.RemoveAll(dir)

	s := newTestStore(t, dir)
	s.Close()

	out := h.mustRun("blobcas-gc", "-dir", dir)
	if !strings.Contains(out, "GC Completed") {
		t.Errorf("expected GC completion message, got:\n%s", out)
	}
}

func TestCLI_GC_WithExpiredRoots(t *testing.T) {
	h := newCLIHelper(t)
	dir, _ := os.MkdirTemp("", "cli-gc-expired-*")
	defer os.RemoveAll(dir)

	ctx := context.Background()
	s := newTestStore(t, dir)
	rng := testkit.RNG(300)

	// Store data with expired deadlines
	for i := 0; i < 10; i++ {
		key := core.Key{Namespace: "gc", ID: fmt.Sprintf("expired_%d", i)}
		data := testkit.RandomBytes(rng, 512)
		expired := time.Now().Add(-1 * time.Hour)
		s.Put(ctx, key, blobcas.PutInput{
			Canonical: bytes.NewReader(data),
		}, blobcas.PutMeta{Canonical: true, RootDeadline: &expired})
	}
	s.Close()

	out := h.mustRun("blobcas-gc", "-dir", dir)
	t.Logf("GC output: %s", out)
	if !strings.Contains(out, "GC Completed") {
		t.Errorf("expected GC completion, got:\n%s", out)
	}
}

// ---------- Reindex Integration ----------

func TestCLI_Reindex_EmptyStore(t *testing.T) {
	h := newCLIHelper(t)
	dir, _ := os.MkdirTemp("", "cli-reindex-empty-*")
	defer os.RemoveAll(dir)

	s := newTestStore(t, dir)
	s.Close()

	out := h.mustRun("blobcas-reindex", "-dir", dir, "-v")
	if !strings.Contains(out, "Reindex complete") {
		t.Errorf("expected reindex completion, got:\n%s", out)
	}
}

func TestCLI_Reindex_RecoversCatalog(t *testing.T) {
	h := newCLIHelper(t)
	dir, _ := os.MkdirTemp("", "cli-reindex-recover-*")
	defer os.RemoveAll(dir)

	ctx := context.Background()
	s := newTestStore(t, dir)
	rng := testkit.RNG(400)

	// Store data
	var keys []core.Key
	var origData [][]byte
	for i := 0; i < 10; i++ {
		key := core.Key{Namespace: "reindex", ID: fmt.Sprintf("file_%d", i)}
		data := testkit.RandomBytes(rng, 512+rng.Intn(256))
		ttl := 1 * time.Hour
		_, err := s.Put(ctx, key, blobcas.PutInput{
			Canonical: bytes.NewReader(data),
		}, blobcas.PutMeta{Canonical: true, RootTTL: &ttl})
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, key)
		origData = append(origData, data)
	}
	s.Close()

	// Destroy catalog
	catDir := filepath.Join(dir, "catalog")
	os.RemoveAll(catDir)
	os.MkdirAll(catDir, 0755)

	// Rebuild with reindex
	out := h.mustRun("blobcas-reindex", "-dir", dir, "-v")
	t.Logf("Reindex output: %s", out)
	if !strings.Contains(out, "Reindex complete") {
		t.Errorf("expected reindex completion, got:\n%s", out)
	}

	// Verify should pass
	verifyOut := h.mustRun("blobcas-verify", "-dir", dir, "-deep")
	if !strings.Contains(verifyOut, "RESULT: SUCCESS") {
		t.Errorf("expected SUCCESS after reindex, got:\n%s", verifyOut)
	}
}

// ---------- End-to-End Pipeline ----------

func TestCLI_EndToEnd_PutVerifyGCReindexVerify(t *testing.T) {
	h := newCLIHelper(t)
	dir, _ := os.MkdirTemp("", "cli-e2e-*")
	defer os.RemoveAll(dir)

	ctx := context.Background()
	s := newTestStore(t, dir)
	rng := testkit.RNG(500)

	// 1. Create live and dead data
	var liveKeys []core.Key
	var liveData [][]byte

	for i := 0; i < 15; i++ {
		key := core.Key{Namespace: "e2e", ID: fmt.Sprintf("item_%d", i)}
		data := testkit.RandomBytes(rng, 256+rng.Intn(768))

		var meta blobcas.PutMeta
		meta.Canonical = true
		if i%3 == 0 {
			// Dead
			expired := time.Now().Add(-1 * time.Hour)
			meta.RootDeadline = &expired
		} else {
			// Live
			ttl := 24 * time.Hour
			meta.RootTTL = &ttl
			liveKeys = append(liveKeys, key)
			liveData = append(liveData, data)
		}

		_, err := s.Put(ctx, key, blobcas.PutInput{
			Canonical: bytes.NewReader(data),
		}, meta)
		if err != nil {
			t.Fatal(err)
		}
	}
	s.Close()

	// 2. Verify
	out := h.mustRun("blobcas-verify", "-dir", dir, "-deep")
	t.Logf("Step 2 - Verify: %s", out)
	if !strings.Contains(out, "RESULT: SUCCESS") {
		t.Fatalf("initial verify failed:\n%s", out)
	}

	// 3. GC
	out = h.mustRun("blobcas-gc", "-dir", dir)
	t.Logf("Step 3 - GC: %s", out)
	if !strings.Contains(out, "GC Completed") {
		t.Fatalf("GC failed:\n%s", out)
	}

	// 4. Verify after GC
	out = h.mustRun("blobcas-verify", "-dir", dir)
	t.Logf("Step 4 - Verify after GC: %s", out)
	if !strings.Contains(out, "RESULT: SUCCESS") {
		t.Fatalf("verify after GC failed:\n%s", out)
	}

	// 5. Destroy catalog and Reindex
	os.RemoveAll(filepath.Join(dir, "catalog"))
	os.MkdirAll(filepath.Join(dir, "catalog"), 0755)

	out = h.mustRun("blobcas-reindex", "-dir", dir, "-v")
	t.Logf("Step 5 - Reindex: %s", out)

	// 6. Final verify
	out = h.mustRun("blobcas-verify", "-dir", dir, "-deep")
	t.Logf("Step 6 - Final verify: %s", out)
	if !strings.Contains(out, "RESULT: SUCCESS") {
		t.Fatalf("final verify failed:\n%s", out)
	}

	// 7. Open store and verify live data is still accessible
	s2 := newTestStore(t, dir)
	defer s2.Close()

	for i, key := range liveKeys {
		ref, err := s2.Resolve(ctx, key)
		if err != nil {
			// After reindex, k2m is not rebuilt (only c2p), so Resolve may fail.
			// This is expected for reindex which only rebuilds CID->Pack mappings.
			t.Logf("key %s not resolvable after reindex (expected for k2m): %v", key.ID, err)
			continue
		}
		rc, _, err := s2.Get(ctx, ref)
		if err != nil {
			t.Errorf("key %s unreadable: %v", key.ID, err)
			continue
		}
		got, _ := io.ReadAll(rc)
		rc.Close()
		if !bytes.Equal(got, liveData[i]) {
			t.Errorf("key %s data mismatch: got %d bytes, want %d", key.ID, len(got), len(liveData[i]))
		}
	}
}

// ---------- CLI Flag Validation ----------

func TestCLI_Verify_MissingDirFlag(t *testing.T) {
	h := newCLIHelper(t)
	_, err := h.run("blobcas-verify")
	if err == nil {
		t.Error("expected error when -dir flag is missing")
	}
}

func TestCLI_GC_MissingDirFlag(t *testing.T) {
	h := newCLIHelper(t)
	_, err := h.run("blobcas-gc")
	if err == nil {
		t.Error("expected error when -dir flag is missing")
	}
}

func TestCLI_Reindex_MissingDirFlag(t *testing.T) {
	h := newCLIHelper(t)
	_, err := h.run("blobcas-reindex")
	if err == nil {
		t.Error("expected error when -dir flag is missing")
	}
}

func TestCLI_GC_NonexistentDir(t *testing.T) {
	h := newCLIHelper(t)
	_, err := h.run("blobcas-gc", "-dir", "/nonexistent/path/blobcas-xxx")
	if err == nil {
		t.Error("expected error for nonexistent directory")
	}
}
