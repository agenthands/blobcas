package pack

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/agenthands/blobcas/pkg/core"
)

func TestManager_ConfigError(t *testing.T) {
	_, err := NewManager(core.PackConfig{Dir: ""})
	if err == nil {
		t.Error("expected error for empty dir")
	}
}

func TestManager_MkdirError(t *testing.T) {
	// Create a read-only parent directory
	parent := t.TempDir()
	os.Chmod(parent, 0500) // no write permission
	defer os.Chmod(parent, 0700)

	dir := filepath.Join(parent, "packdir")
	_, err := NewManager(core.PackConfig{Dir: dir})
	if err == nil {
		t.Error("expected MkdirAll error")
	}
}

func TestManager_DiscoverReadDirError(t *testing.T) {
	dir := t.TempDir()
	os.Chmod(dir, 0000) // no read or execute permission
	defer os.Chmod(dir, 0700)

	_, err := NewManager(core.PackConfig{Dir: dir})
	if err == nil {
		t.Error("expected ReadDir error")
	}
}

func TestManager_OpenActiveError(t *testing.T) {
	dir := t.TempDir()
	os.Chmod(dir, 0500) // read and execute, but no write
	defer os.Chmod(dir, 0700)

	_, err := NewManager(core.PackConfig{Dir: dir})
	if err == nil {
		t.Error("expected OpenReadWrite error")
	}
}

func TestManager_SealAndRotateosStatError(t *testing.T) {
	dir := t.TempDir()
	pm, err := NewManager(core.PackConfig{Dir: dir, TargetPackBytes: 64 * 1024})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	// Make dir impenetrable to stat
	os.Chmod(dir, 0000)
	defer os.Chmod(dir, 0700)

	err = pm.SealAndRotateIfNeeded(ctx)
	if err == nil {
		t.Error("expected stat error")
	}
}

func TestManager_IteratePackOpenError(t *testing.T) {
	dir := t.TempDir()
	pm, err := NewManager(core.PackConfig{Dir: dir, TargetPackBytes: 64 * 1024})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	// Add a block to trigger sealing
	cBytes := []byte{0x01, 0x55, 0x00, 0x04, 'd', 'a', 't', 'a'}

	_, err = pm.PutBlock(ctx, core.CID{Bytes: cBytes}, []byte("data"))
	if err != nil {
		t.Fatal(err)
	}
	pm.SealActivePack(ctx)

	sealed := pm.ListSealedPacks()
	if len(sealed) == 0 {
		t.Fatal("expected sealed packs")
	}
	id := sealed[0]

	// Make dir impenetrable to open
	os.Chmod(dir, 0000)
	defer os.Chmod(dir, 0700)

	err = pm.IteratePackBlocks(ctx, id, func(c core.CID) error { return nil })
	if err == nil {
		t.Error("expected os.Open error")
	}
}
