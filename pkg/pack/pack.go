package pack

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/agenthands/blobcas/pkg/core"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/blockstore"
)

// Manager defines the interface for managing CARv2 pack files.
type Manager interface {
	PutBlock(ctx context.Context, c core.CID, stored []byte) (uint64, error)
	GetBlock(ctx context.Context, packID uint64, c core.CID) ([]byte, error)
	SealAndRotateIfNeeded(ctx context.Context) error
	CurrentPackID() uint64
	IteratePackBlocks(ctx context.Context, packID uint64, fn func(c core.CID) error) error

	// GC Support
	ListSealedPacks() []uint64
	RemovePack(packID uint64) error
	SealActivePack(ctx context.Context) error

	Close() error
}

type packManager struct {
	cfg core.PackConfig

	mu sync.RWMutex

	currentID uint64
	active    *blockstore.ReadWrite

	// sealed packs: packID -> blockstore
	sealed map[uint64]*blockstore.ReadOnly
}

// NewManager creates a new pack manager.
func NewManager(cfg core.PackConfig) (Manager, error) {
	if cfg.Dir == "" {
		return nil, fmt.Errorf("pack directory not specified")
	}

	if err := os.MkdirAll(cfg.Dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create pack directory: %w", err)
	}

	m := &packManager{
		cfg:    cfg,
		sealed: make(map[uint64]*blockstore.ReadOnly),
	}

	if err := m.discoverPacks(); err != nil {
		return nil, err
	}

	return m, nil
}

func (m *packManager) discoverPacks() error {
	entries, err := os.ReadDir(m.cfg.Dir)
	if err != nil {
		return err
	}

	var packIDs []uint64
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), "pack-") || !strings.HasSuffix(entry.Name(), ".car") {
			continue
		}

		idStr := strings.TrimPrefix(entry.Name(), "pack-")
		idStr = strings.TrimSuffix(idStr, ".car")
		id, err := strconv.ParseUint(idStr, 16, 64)
		if err != nil {
			continue
		}
		packIDs = append(packIDs, id)
	}

	sort.Slice(packIDs, func(i, j int) bool { return packIDs[i] < packIDs[j] })

	// All discovered packs are considered sealed for now.
	for _, id := range packIDs {
		path := m.packPath(id)
		bs, err := blockstore.OpenReadOnly(path)
		if err != nil {
			return fmt.Errorf("failed to open sealed pack %d: %w", id, err)
		}
		m.sealed[id] = bs
		m.currentID = id
	}

	// Always start with a fresh pack to avoid complex resumption of CARv2 writing for now.
	m.currentID++
	return m.openActive(m.currentID)
}

func (m *packManager) openActive(id uint64) error {
	path := m.packPath(id)

	// Create a new CARv2 RW blockstore
	bs, err := blockstore.OpenReadWrite(path, []cid.Cid{})
	if err != nil {
		return fmt.Errorf("failed to create active pack %d: %w", id, err)
	}

	m.active = bs
	return nil
}

func (m *packManager) packPath(id uint64) string {
	return filepath.Join(m.cfg.Dir, fmt.Sprintf("pack-%016x.car", id))
}

func (m *packManager) CurrentPackID() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.currentID
}

func (m *packManager) PutBlock(ctx context.Context, c core.CID, stored []byte) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	id, err := cid.Cast(c.Bytes)
	if err != nil {
		return 0, fmt.Errorf("invalid CID: %w", err)
	}

	// Check if already in active pack (basic dedupe)
	has, err := m.active.Has(ctx, id)
	if err != nil {
		return 0, err
	}
	if has {
		return m.currentID, nil
	}

	blk, err := blocks.NewBlockWithCid(stored, id)
	if err != nil {
		return 0, err
	}

	if err := m.active.Put(ctx, blk); err != nil {
		return 0, err
	}

	return m.currentID, nil
}

func (m *packManager) GetBlock(ctx context.Context, packID uint64, c core.CID) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	id, err := cid.Cast(c.Bytes)
	if err != nil {
		return nil, fmt.Errorf("invalid CID: %w", err)
	}

	var bs interface {
		Get(context.Context, cid.Cid) (blocks.Block, error)
	}

	if packID == m.currentID {
		bs = m.active
	} else {
		var ok bool
		var rbs *blockstore.ReadOnly
		rbs, ok = m.sealed[packID]
		if !ok {
			return nil, fmt.Errorf("%w: pack %d not found", core.ErrNotFound, packID)
		}
		bs = rbs
	}

	blk, err := bs.Get(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", core.ErrNotFound, err)
	}

	return blk.RawData(), nil
}

func (m *packManager) SealAndRotateIfNeeded(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check active pack size
	fi, err := os.Stat(m.packPath(m.currentID))
	if err != nil {
		return err
	}

	if uint64(fi.Size()) < m.cfg.TargetPackBytes {
		return nil
	}

	// Seal current pack
	if err := m.active.Finalize(); err != nil {
		return fmt.Errorf("failed to finalize active pack: %w", err)
	}

	// Open it as sealed
	bs, err := blockstore.OpenReadOnly(m.packPath(m.currentID))
	if err != nil {
		return fmt.Errorf("failed to open sealed pack: %w", err)
	}
	m.sealed[m.currentID] = bs

	// Rotate
	m.currentID++
	return m.openActive(m.currentID)
}

func (m *packManager) SealActivePack(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Seal current pack regardless of size
	if err := m.active.Finalize(); err != nil {
		return fmt.Errorf("failed to finalize active pack: %w", err)
	}

	// Open it as sealed
	bs, err := blockstore.OpenReadOnly(m.packPath(m.currentID))
	if err != nil {
		return fmt.Errorf("failed to open sealed pack: %w", err)
	}
	m.sealed[m.currentID] = bs

	// Rotate
	m.currentID++
	return m.openActive(m.currentID)
}

func (m *packManager) ListSealedPacks() []uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	res := make([]uint64, 0, len(m.sealed))
	for id := range m.sealed {
		res = append(res, id)
	}
	sort.Slice(res, func(i, j int) bool { return res[i] < res[j] })
	return res
}

// IteratePackBlocks reads the actual CAR blocks linearly instead of parsing the
// go-car/v2 index because the index constructor currently forces all CID results to use generic raw codecs, losing original manifest codecs like DagCBOR.
func (m *packManager) IteratePackBlocks(ctx context.Context, packID uint64, fn func(c core.CID) error) error {
	m.mu.RLock()
	_, ok := m.sealed[packID]
	m.mu.RUnlock()

	if !ok {
		return fmt.Errorf("%w: pack %d is not sealed or doesn't exist", core.ErrNotFound, packID)
	}

	f, err := os.Open(m.packPath(packID))
	if err != nil {
		return fmt.Errorf("failed to open pack %d: %w", packID, err)
	}
	defer f.Close()

	br, err := carv2.NewBlockReader(f, carv2.WithTrustedCAR(true))
	if err != nil {
		return fmt.Errorf("failed to create block reader for pack %d: %w", packID, err)
	}

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		blk, err := br.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("failed to read block from pack %d: %w", packID, err)
		}

		if err := fn(core.CID{Bytes: blk.Cid().Bytes()}); err != nil {
			return err
		}
	}
}

func (m *packManager) RemovePack(packID uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	bs, ok := m.sealed[packID]
	if ok {
		delete(m.sealed, packID)
		bs.Close()
	} else if packID == m.currentID {
		return fmt.Errorf("cannot remove active pack")
	}

	return os.Remove(m.packPath(packID))
}

func (m *packManager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var errs []string
	if m.active != nil {
		// Finalize if possible, ignore error if already finalized/closed
		_ = m.active.Finalize()
	}

	for id, bs := range m.sealed {
		if err := bs.Close(); err != nil {
			errs = append(errs, fmt.Sprintf("pack %d: %v", id, err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing pack manager: %s", strings.Join(errs, "; "))
	}
	return nil
}
