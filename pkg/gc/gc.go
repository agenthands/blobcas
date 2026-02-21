package gc

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/agenthands/blobcas/pkg/catalog"
	"github.com/agenthands/blobcas/pkg/cidutil"
	"github.com/agenthands/blobcas/pkg/core"
	"github.com/agenthands/blobcas/pkg/manifest"
	"github.com/agenthands/blobcas/pkg/pack"
	"github.com/agenthands/blobcas/pkg/transform"
	"github.com/cockroachdb/pebble"
)

// Result contains statistics from a GC run.
type Result struct {
	PacksSwept     int
	BlocksMoved    int
	BytesReclaimed uint64
}

// Runner defines the GC and compaction interface.
type Runner interface {
	RunOnce(ctx context.Context) (Result, error)
	Start(ctx context.Context)
	Stop()
}

type runner struct {
	cfg       core.GCConfig
	cat       catalog.Catalog
	packs     pack.Manager
	manifests manifest.Codec
	cidHub    cidutil.Builder
	tr        transform.Transform

	mu      sync.Mutex
	running bool
	stopCh  chan struct{}
}

// NewRunner creates a new GC runner.
func NewRunner(
	cfg core.GCConfig,
	cat catalog.Catalog,
	packs pack.Manager,
	manifests manifest.Codec,
	cidHub cidutil.Builder,
	tr transform.Transform,
) Runner {
	if cfg.RunEvery == 0 {
		cfg.RunEvery = 24 * time.Hour
	}
	// default threshold if 0 could be 50%
	return &runner{
		cfg:       cfg,
		cat:       cat,
		packs:     packs,
		manifests: manifests,
		cidHub:    cidHub,
		tr:        tr,
		stopCh:    make(chan struct{}),
	}
}

func (r *runner) RunOnce(ctx context.Context) (Result, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	var res Result

	// 1. Mark phase: find all live CIDs
	liveBlocks, err := r.mark(ctx)
	if err != nil {
		return res, fmt.Errorf("mark phase failed: %w", err)
	}

	// 2. Group live blocks by auth pack
	liveByPack := make(map[uint64][]core.CID)
	for cStr := range liveBlocks {
		c := core.CID{Bytes: []byte(cStr)}
		pid, ok, err := r.cat.GetPackForCID(ctx, c)
		if err != nil {
			return res, err
		}
		if ok {
			liveByPack[pid] = append(liveByPack[pid], c)
		}
	}

	sealedPacks := r.packs.ListSealedPacks()
	var toCompact []uint64
	var toSweep []uint64

	for _, pid := range sealedPacks {
		liveCIDs := liveByPack[pid]
		liveCount := len(liveCIDs)

		totalCount := 0
		_ = r.packs.IteratePackBlocks(ctx, pid, func(c core.CID) error {
			totalCount++
			return nil
		})

		if totalCount == 0 {
			toSweep = append(toSweep, pid)
		} else if liveCount == 0 {
			toSweep = append(toSweep, pid)
		} else {
			utilization := float64(liveCount) / float64(totalCount)
			if utilization < 0.5 {
				toCompact = append(toCompact, pid)
			}
		}
	}

	// 3. Compact phase
	for _, pid := range toCompact {
		moved, err := r.compactPack(ctx, pid, liveByPack[pid])
		if err != nil {
			return res, fmt.Errorf("compaction failed for pack %d: %w", pid, err)
		}
		res.BlocksMoved += moved
		toSweep = append(toSweep, pid)
	}

	// 4. Flush the new pack to disk so Reopen can find it
	if res.BlocksMoved > 0 {
		_ = r.packs.SealActivePack(ctx)
	}

	// 5. Sweep phase
	for _, pid := range toSweep {
		if err := r.packs.RemovePack(pid); err == nil {
			res.PacksSwept++
			// Approximation, real bytes reclaimed would require stat'ing the file
			// before deleting it.
		}
	}

	return res, nil
}

func (r *runner) mark(ctx context.Context) (map[string]struct{}, error) {
	live := make(map[string]struct{})

	err := r.cat.IterateRoots(ctx, func(mCID core.CID, deadline time.Time) error {
		if !deadline.IsZero() && time.Now().After(deadline) {
			// Expired!
			return nil // skip marking as live
		}

		// It's live!
		live[string(mCID.Bytes)] = struct{}{}

		// Read manifest to find chunks
		pid, ok, err := r.cat.GetPackForCID(ctx, mCID)
		if err != nil {
			return err
		}
		if !ok {
			// Should be in catalog if it's a root
			return nil
		}

		mStored, err := r.packs.GetBlock(ctx, pid, mCID)
		if err != nil {
			return err
		}

		mBytes, err := r.tr.Decode(mStored)
		if err != nil {
			return err
		}

		m, err := r.manifests.Decode(mBytes)
		if err != nil {
			return err
		}

		for _, chunk := range m.Chunks {
			live[string(chunk.CID.Bytes)] = struct{}{}
		}

		return nil
	})

	return live, err
}

func (r *runner) compactPack(ctx context.Context, packID uint64, toMove []core.CID) (int, error) {
	var moved int
	batch := r.cat.NewBatch()
	defer batch.Close()

	for _, c := range toMove {
		stored, err := r.packs.GetBlock(ctx, packID, c)
		if err != nil {
			fmt.Printf("compactPack %d: GetBlock for %x FAILED: %v\n", packID, c.Bytes, err)
			continue // skip
		}

		newPackID, err := r.packs.PutBlock(ctx, c, stored)
		if err != nil {
			return moved, err
		}

		if err := r.cat.PutPackForCID(batch, c, newPackID); err != nil {
			return moved, err
		}
		moved++
		fmt.Printf("compactPack %d: Moved CID %x to pack %d\n", packID, c.Bytes, newPackID)
	}

	if err := batch.Commit(pebble.Sync); err != nil {
		return moved, err
	}

	// We MUST flush/seal the active pack if we moved blocks so that the subsequent Open
	// or other processes can see the newly written blocks cleanly.
	if moved > 0 {
		_ = r.packs.SealAndRotateIfNeeded(ctx)
	}

	return moved, nil
}

func (r *runner) Start(ctx context.Context) {
	r.mu.Lock()
	if r.running || !r.cfg.Enabled {
		r.mu.Unlock()
		return
	}
	r.running = true
	r.mu.Unlock()

	go func() {
		ticker := time.NewTicker(r.cfg.RunEvery)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-r.stopCh:
				return
			case <-ticker.C:
				_, _ = r.RunOnce(ctx)
			}
		}
	}()
}

func (r *runner) Stop() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.running {
		r.running = false
		close(r.stopCh)
	}
}
