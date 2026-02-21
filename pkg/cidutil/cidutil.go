package cidutil

import (
	"bytes"
	"fmt"

	"github.com/agenthands/blobcas/pkg/core"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
)

// Builder defines the interface for creating and verifying CIDs.
type Builder interface {
	ChunkCID(plain []byte) (core.CID, error)
	ManifestCID(dagCbor []byte) (core.CID, error)
	Verify(c core.CID, plain []byte) error
}

type builder struct{}

// NewBuilder returns a new CID builder implementation.
func NewBuilder() Builder {
	return &builder{}
}

func (b *builder) ChunkCID(plain []byte) (core.CID, error) {
	return b.buildCID(cid.Raw, plain)
}

func (b *builder) ManifestCID(dagCbor []byte) (core.CID, error) {
	return b.buildCID(cid.DagCBOR, dagCbor)
}

func (b *builder) buildCID(codec uint64, data []byte) (core.CID, error) {
	hash, err := multihash.Sum(data, multihash.SHA2_256, -1)
	if err != nil {
		return core.CID{}, fmt.Errorf("failed to compute multihash: %w", err)
	}

	c := cid.NewCidV1(codec, hash)
	return core.CID{Bytes: c.Bytes()}, nil
}

func (b *builder) Verify(c core.CID, plain []byte) error {
	id, err := cid.Cast(c.Bytes)
	if err != nil {
		return fmt.Errorf("%w: invalid CID bytes: %v", core.ErrCorrupt, err)
	}

	prefix := id.Prefix()
	hash, err := multihash.Sum(plain, prefix.MhType, prefix.MhLength)
	if err != nil {
		return fmt.Errorf("failed to compute multihash for verification: %w", err)
	}

	if !bytes.Equal(id.Hash(), hash) {
		return fmt.Errorf("%w: CID mismatch", core.ErrCorrupt)
	}

	return nil
}
