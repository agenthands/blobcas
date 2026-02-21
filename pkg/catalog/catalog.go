package catalog

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/agenthands/blobcas/pkg/core"
	"github.com/cockroachdb/pebble"
)

var (
	PrefixC2P      = []byte("c2p:")
	PrefixK2M      = []byte("k2m:")
	PrefixRoots    = []byte("rt:")
	PrefixPackMeta = []byte("pm:")
)

// Catalog defines the interface for the embedded KV store.
type Catalog interface {
	GetPackForCID(ctx context.Context, cid core.CID) (uint64, bool, error)
	PutPackForCID(batch *pebble.Batch, cid core.CID, packID uint64) error

	GetManifestForKey(ctx context.Context, key core.Key) (core.CID, bool, error)
	PutManifestForKey(batch *pebble.Batch, key core.Key, manifest core.CID) error

	PutRootDeadline(batch *pebble.Batch, manifest core.CID, deadline time.Time) error
	IterateRoots(ctx context.Context, fn func(manifest core.CID, deadline time.Time) error) error

	NewBatch() *pebble.Batch
	Close() error
}

type pebbleCatalog struct {
	db *pebble.DB
}

// Open opens a Pebble-based catalog in the specified directory.
func Open(dir string) (Catalog, error) {
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		return nil, fmt.Errorf("failed to open pebble db: %w", err)
	}
	return &pebbleCatalog{db: db}, nil
}

func (c *pebbleCatalog) Close() error {
	return c.db.Close()
}

func (c *pebbleCatalog) NewBatch() *pebble.Batch {
	return c.db.NewBatch()
}

func (c *pebbleCatalog) GetPackForCID(ctx context.Context, cid core.CID) (uint64, bool, error) {
	key := append(PrefixC2P, cid.Bytes...)
	val, closer, err := c.db.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound {
			return 0, false, nil
		}
		return 0, false, err
	}
	defer closer.Close()

	if len(val) != 8 {
		return 0, false, fmt.Errorf("%w: invalid pack ID length", core.ErrCorrupt)
	}
	return binary.BigEndian.Uint64(val), true, nil
}

func (c *pebbleCatalog) PutPackForCID(batch *pebble.Batch, cid core.CID, packID uint64) error {
	key := append(PrefixC2P, cid.Bytes...)
	val := make([]byte, 8)
	binary.BigEndian.PutUint64(val, packID)

	if batch != nil {
		return batch.Set(key, val, nil)
	}
	return c.db.Set(key, val, pebble.Sync)
}

func (c *pebbleCatalog) GetManifestForKey(ctx context.Context, key core.Key) (core.CID, bool, error) {
	k := encodeKey(key)
	val, closer, err := c.db.Get(k)
	if err != nil {
		if err == pebble.ErrNotFound {
			return core.CID{}, false, nil
		}
		return core.CID{}, false, err
	}
	defer closer.Close()

	res := make([]byte, len(val))
	copy(res, val)
	return core.CID{Bytes: res}, true, nil
}

func (c *pebbleCatalog) PutManifestForKey(batch *pebble.Batch, key core.Key, manifest core.CID) error {
	k := encodeKey(key)
	if batch != nil {
		return batch.Set(k, manifest.Bytes, nil)
	}
	return c.db.Set(k, manifest.Bytes, pebble.Sync)
}

func (c *pebbleCatalog) PutRootDeadline(batch *pebble.Batch, manifest core.CID, deadline time.Time) error {
	key := append(PrefixRoots, manifest.Bytes...)
	val := make([]byte, 8)
	binary.BigEndian.PutUint64(val, uint64(deadline.Unix()))

	if batch != nil {
		return batch.Set(key, val, nil)
	}
	return c.db.Set(key, val, pebble.Sync)
}

func (c *pebbleCatalog) IterateRoots(ctx context.Context, fn func(manifest core.CID, deadline time.Time) error) error {
	iter, _ := c.db.NewIter(&pebble.IterOptions{
		LowerBound: PrefixRoots,
		UpperBound: incrementByte(PrefixRoots),
	})
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		mCID := iter.Key()[len(PrefixRoots):]
		cidCopy := make([]byte, len(mCID))
		copy(cidCopy, mCID)

		val := iter.Value()
		if len(val) != 8 {
			continue // or return error
		}
		ts := int64(binary.BigEndian.Uint64(val))
		deadline := time.Unix(ts, 0)

		if err := fn(core.CID{Bytes: cidCopy}, deadline); err != nil {
			return err
		}
	}
	return nil
}

func encodeKey(k core.Key) []byte {
	// Simple encoding: k2m:<namespace>:<id>
	// Note: In production we should handle delimiters properly if they can appear in namespace/id
	return []byte(fmt.Sprintf("%s%s:%s", PrefixK2M, k.Namespace, k.ID))
}

func incrementByte(b []byte) []byte {
	res := make([]byte, len(b))
	copy(res, b)
	for i := len(res) - 1; i >= 0; i-- {
		res[i]++
		if res[i] != 0 {
			return res
		}
	}
	return nil
}
