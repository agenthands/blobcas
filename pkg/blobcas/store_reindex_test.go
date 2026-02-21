package blobcas_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/agenthands/blobcas/pkg/blobcas"
	"github.com/agenthands/blobcas/pkg/catalog"
	"github.com/agenthands/blobcas/pkg/core"
	"github.com/agenthands/blobcas/pkg/pack"
	"github.com/cockroachdb/pebble"
)

func TestStoreIntegration_Reindex(t *testing.T) {
	dir, err := os.MkdirTemp("", "blobcas-store-integration-reindex")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s, cfg := createTestStore(t, dir)
	ctx := context.Background()

	// Put some blobs
	keys := []core.Key{
		{Namespace: "test", ID: "file1"},
		{Namespace: "test", ID: "file2"},
	}

	refs := make([]blobcas.Ref, 0)

	for _, k := range keys {
		ref, err := s.Put(ctx, k, blobcas.PutInput{
			Canonical: bytes.NewReader([]byte("data for " + k.ID)),
		}, blobcas.PutMeta{Canonical: true})
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
		refs = append(refs, ref)
	}

	// Close store
	s.Close()

	// Wipe out the catalog directory
	err = os.RemoveAll(cfg.Catalog.Dir)
	if err != nil {
		t.Fatalf("failed to delete catalog dir: %v", err)
	}

	// Create new empty catalog dir
	err = os.MkdirAll(cfg.Catalog.Dir, 0755)
	if err != nil {
		t.Fatalf("failed to recreate catalog dir: %v", err)
	}

	// Run the reindex simulation (similar to cmd/blobcas-reindex)
	cat, err := catalog.Open(cfg.Catalog.Dir)
	if err != nil {
		t.Fatalf("failed to open new catalog: %v", err)
	}

	pm, err := pack.NewManager(cfg.Pack)
	if err != nil {
		t.Fatalf("failed to open packs: %v", err)
	}

	var reindexedCIDs []string

	sealed := pm.ListSealedPacks()
	for _, pid := range sealed {
		batch := cat.NewBatch()
		pm.IteratePackBlocks(ctx, pid, func(c core.CID) error {
			reindexedCIDs = append(reindexedCIDs, fmt.Sprintf("%x", c.Bytes))
			return cat.PutPackForCID(batch, c, pid)
		})
		batch.Commit(pebble.Sync)
	}

	pm.Close()
	cat.Close()

	// Reopen the store
	s2, err := blobcas.Open(ctx, cfg)
	if err != nil {
		t.Fatalf("failed to reopen store: %v", err)
	}
	defer s2.Close()

	// Verify we can read by Ref (since c2p is rebuilt)
	for i, ref := range refs {
		rc, _, err := s2.Get(ctx, ref)
		if err != nil {
			t.Errorf("failed to Get ref %d after reindex: %v", i, err)
			continue
		}
		rc.Close()
	}
}
