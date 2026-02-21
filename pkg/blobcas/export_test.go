package blobcas

import (
	"github.com/agenthands/blobcas/pkg/catalog"
	"github.com/agenthands/blobcas/pkg/chunker"
	"github.com/agenthands/blobcas/pkg/cidutil"
	"github.com/agenthands/blobcas/pkg/manifest"
	"github.com/agenthands/blobcas/pkg/pack"
	"github.com/agenthands/blobcas/pkg/transform"
)

// NewStoreForTest constructs a Store with injected dependencies. Test-only.
func NewStoreForTest(
	cfg Config,
	ch chunker.Chunker,
	cid cidutil.Builder,
	man manifest.Codec,
	pm pack.Manager,
	cat catalog.Catalog,
	tr transform.Transform,
) Store {
	return &store{
		cfg:       cfg,
		chunker:   ch,
		cidHub:    cid,
		manifests: man,
		packs:     pm,
		catalog:   cat,
		transform: tr,
	}
}
