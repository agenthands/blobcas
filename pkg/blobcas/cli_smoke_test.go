package blobcas_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/agenthands/blobcas/pkg/blobcas"
	"github.com/agenthands/blobcas/pkg/core"
)

func TestCLITools_Smoke(t *testing.T) {
	// 1. Build binaries
	binDir, err := filepath.Abs("../../bin")
	if err != nil {
		t.Fatal(err)
	}

	// Ensure binaries exist (built in previous step, but let's be safe)
	tools := []string{"blobcas-gc", "blobcas-reindex", "blobcas-verify"}
	for _, tool := range tools {
		if _, err := os.Stat(filepath.Join(binDir, tool)); err != nil {
			t.Skipf("binaries not found in %s, skipping CLI smoke test. Run 'go build' first.", binDir)
		}
	}

	dataDir, err := os.MkdirTemp("", "blobcas-cli-smoke")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dataDir)

	ctx := context.Background()
	cfg := core.Config{
		Dir: dataDir,
		Chunking: core.ChunkingConfig{
			Min: 64,
			Avg: 128,
			Max: 256,
		},
		Pack: core.PackConfig{
			TargetPackBytes: 1024, // small
		},
		Transform: core.TransformConfig{
			Name:      "zstd",
			ZstdLevel: 3,
		},
	}

	s, err := blobcas.Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}

	// 2. Put some data to create packs
	for i := 0; i < 20; i++ {
		key := core.Key{Namespace: "cli", ID: fmt.Sprintf("file_%d", i)}
		_, err := s.Put(ctx, key, blobcas.PutInput{
			Canonical: bytes.NewReader([]byte(fmt.Sprintf("some unique content %d %d", i, time.Now().UnixNano()))),
		}, blobcas.PutMeta{
			Canonical: true,
			RootTTL:   func() *time.Duration { d := 1 * time.Hour; return &d }(),
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	s.Close()

	// 3. Run Verify
	cmdVerify := exec.Command(filepath.Join(binDir, "blobcas-verify"), "-dir", dataDir, "-deep")
	output, err := cmdVerify.CombinedOutput()
	if err != nil {
		t.Fatalf("verify failed: %v\nOutput: %s", err, string(output))
	}
	t.Logf("Verify output: %s", string(output))

	// 4. Run GC
	cmdGC := exec.Command(filepath.Join(binDir, "blobcas-gc"), "-dir", dataDir)
	output, err = cmdGC.CombinedOutput()
	if err != nil {
		t.Fatalf("gc failed: %v\nOutput: %s", err, string(output))
	}
	t.Logf("GC output: %s", string(output))

	// 5. Corrupt catalog and Reindex
	catDir := filepath.Join(dataDir, "catalog")
	os.RemoveAll(catDir)
	os.MkdirAll(catDir, 0755)

	cmdReindex := exec.Command(filepath.Join(binDir, "blobcas-reindex"), "-dir", dataDir, "-v")
	output, err = cmdReindex.CombinedOutput()
	if err != nil {
		t.Fatalf("reindex failed: %v\nOutput: %s", err, string(output))
	}
	t.Logf("Reindex output: %s", string(output))

	// 6. Verify again after reindex
	cmdVerifyFinal := exec.Command(filepath.Join(binDir, "blobcas-verify"), "-dir", dataDir, "-deep")
	output, err = cmdVerifyFinal.CombinedOutput()
	if err != nil {
		t.Fatalf("final verify failed: %v\nOutput: %s", err, string(output))
	}
	t.Logf("Final verify output: %s", string(output))
}
