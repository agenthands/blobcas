package testkit

import (
	"context"

	"github.com/agenthands/blobcas/pkg/core"
	"github.com/agenthands/blobcas/pkg/pack"
)

// CountUniqueBlocks returns the number of unique CIDs stored across all sealed packs.
func CountUniqueBlocks(ctx context.Context, pm pack.Manager) (int, error) {
	unique := make(map[string]struct{})
	sealed := pm.ListSealedPacks()

	for _, pid := range sealed {
		err := pm.IteratePackBlocks(ctx, pid, func(c core.CID) error {
			unique[string(c.Bytes)] = struct{}{}
			return nil
		})
		if err != nil {
			return 0, err
		}
	}

	return len(unique), nil
}

// CorruptPackBlock takes a raw pack block payload and flips a byte to simulate corruption.
// This assumes the payload is at least 1 byte long.
func CorruptPackBlock(payload []byte) []byte {
	out := make([]byte, len(payload))
	copy(out, payload)
	if len(out) > 0 {
		out[0] ^= 0xFF // Flip bits in the first byte
	}
	return out
}
