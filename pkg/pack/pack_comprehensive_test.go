package pack_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"
	"testing"

	"github.com/agenthands/blobcas/internal/testkit"
	"github.com/agenthands/blobcas/pkg/cidutil"
	"github.com/agenthands/blobcas/pkg/core"
	"github.com/agenthands/blobcas/pkg/pack"
)

// newTestManager creates a pack manager suitable for unit tests using small packs.
func newTestManager(t testing.TB, targetPack uint64) (pack.Manager, string) {
	t.Helper()
	dir, err := os.MkdirTemp("", "blobcas-pack-comp-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	if targetPack == 0 {
		targetPack = 4096
	}

	mgr, err := pack.NewManager(core.PackConfig{
		Dir:             dir,
		TargetPackBytes: targetPack,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { mgr.Close() })
	return mgr, dir
}

// ---------- IteratePackBlocks ----------

func TestIteratePackBlocks_RoundTrip(t *testing.T) {
	ctx := context.Background()
	mgr, _ := newTestManager(t, 0)
	cidBuilder := cidutil.NewBuilder()
	rng := testkit.RNG(100)

	// Put N unique blocks, seal, then iterate and verify all CIDs are returned.
	const n = 10
	expectedCIDs := make(map[string]struct{})

	for i := 0; i < n; i++ {
		data := testkit.RandomBytes(rng, 256)
		cid, _ := cidBuilder.ChunkCID(data)
		_, err := mgr.PutBlock(ctx, cid, data)
		if err != nil {
			t.Fatalf("PutBlock %d: %v", i, err)
		}
		expectedCIDs[string(cid.Bytes)] = struct{}{}
	}

	packID := mgr.CurrentPackID()
	err := mgr.SealActivePack(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Iterate and collect CIDs
	gotCIDs := make(map[string]struct{})
	err = mgr.IteratePackBlocks(ctx, packID, func(c core.CID) error {
		gotCIDs[string(c.Bytes)] = struct{}{}
		return nil
	})
	if err != nil {
		t.Fatalf("IteratePackBlocks: %v", err)
	}

	if len(gotCIDs) != len(expectedCIDs) {
		t.Errorf("expected %d CIDs, got %d", len(expectedCIDs), len(gotCIDs))
	}
	for k := range expectedCIDs {
		if _, ok := gotCIDs[k]; !ok {
			t.Errorf("missing CID from iteration: %x", k[:8])
		}
	}
}

func TestIteratePackBlocks_UnsealedPack(t *testing.T) {
	ctx := context.Background()
	mgr, _ := newTestManager(t, 0)

	// Iterating an unsealed (active) pack should return ErrNotFound
	currentID := mgr.CurrentPackID()
	err := mgr.IteratePackBlocks(ctx, currentID, func(c core.CID) error {
		return nil
	})
	if err == nil || !errors.Is(err, core.ErrNotFound) {
		t.Errorf("expected ErrNotFound for unsealed pack, got: %v", err)
	}
}

func TestIteratePackBlocks_NonexistentPack(t *testing.T) {
	ctx := context.Background()
	mgr, _ := newTestManager(t, 0)

	err := mgr.IteratePackBlocks(ctx, 9999, func(c core.CID) error {
		return nil
	})
	if err == nil || !errors.Is(err, core.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got: %v", err)
	}
}

func TestIteratePackBlocks_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	mgr, _ := newTestManager(t, 0)
	cidBuilder := cidutil.NewBuilder()
	rng := testkit.RNG(200)

	// Put some blocks and seal
	for i := 0; i < 5; i++ {
		data := testkit.RandomBytes(rng, 128)
		cid, _ := cidBuilder.ChunkCID(data)
		mgr.PutBlock(ctx, cid, data)
	}
	packID := mgr.CurrentPackID()
	mgr.SealActivePack(ctx)

	// Cancel context before iterating
	cancel()

	err := mgr.IteratePackBlocks(ctx, packID, func(c core.CID) error {
		return nil
	})
	if err == nil {
		t.Error("expected error from cancelled context")
	}
}

func TestIteratePackBlocks_CallbackError(t *testing.T) {
	ctx := context.Background()
	mgr, _ := newTestManager(t, 0)
	cidBuilder := cidutil.NewBuilder()
	rng := testkit.RNG(300)

	data := testkit.RandomBytes(rng, 128)
	cid, _ := cidBuilder.ChunkCID(data)
	mgr.PutBlock(ctx, cid, data)
	packID := mgr.CurrentPackID()
	mgr.SealActivePack(ctx)

	sentinel := errors.New("callback error")
	err := mgr.IteratePackBlocks(ctx, packID, func(c core.CID) error {
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Errorf("expected sentinel error, got: %v", err)
	}
}

// ---------- SealActivePack ----------

func TestSealActivePack(t *testing.T) {
	ctx := context.Background()
	mgr, _ := newTestManager(t, 0)
	cidBuilder := cidutil.NewBuilder()

	id1 := mgr.CurrentPackID()
	data := []byte("seal test data")
	cid, _ := cidBuilder.ChunkCID(data)
	mgr.PutBlock(ctx, cid, data)

	err := mgr.SealActivePack(ctx)
	if err != nil {
		t.Fatal(err)
	}

	id2 := mgr.CurrentPackID()
	if id1 == id2 {
		t.Error("expected pack ID to change after seal")
	}

	// The old ID should now be in ListSealedPacks
	sealed := mgr.ListSealedPacks()
	found := false
	for _, s := range sealed {
		if s == id1 {
			found = true
		}
	}
	if !found {
		t.Errorf("sealed list %v does not contain old pack %d", sealed, id1)
	}

	// Old data should still be readable
	got, err := mgr.GetBlock(ctx, id1, cid)
	if err != nil {
		t.Fatalf("GetBlock from sealed pack: %v", err)
	}
	if string(got) != string(data) {
		t.Errorf("data mismatch: got %q", got)
	}
}

// ---------- Multiple Seal Cycles ----------

func TestMultipleSealCycles(t *testing.T) {
	ctx := context.Background()
	mgr, _ := newTestManager(t, 0)
	cidBuilder := cidutil.NewBuilder()
	rng := testkit.RNG(400)

	const cycles = 5
	const blocksPerCycle = 3

	type storedBlock struct {
		packID uint64
		cid    core.CID
		data   []byte
	}
	var stored []storedBlock

	for c := 0; c < cycles; c++ {
		for b := 0; b < blocksPerCycle; b++ {
			data := testkit.RandomBytes(rng, 200)
			cid, _ := cidBuilder.ChunkCID(data)
			pid, err := mgr.PutBlock(ctx, cid, data)
			if err != nil {
				t.Fatalf("cycle %d block %d: %v", c, b, err)
			}
			stored = append(stored, storedBlock{pid, cid, data})
		}
		if err := mgr.SealActivePack(ctx); err != nil {
			t.Fatalf("seal cycle %d: %v", c, err)
		}
	}

	// All sealed packs should be listed
	sealed := mgr.ListSealedPacks()
	if len(sealed) < cycles {
		t.Errorf("expected >= %d sealed packs, got %d", cycles, len(sealed))
	}

	// Verify sorted ordering
	sorted := sort.SliceIsSorted(sealed, func(i, j int) bool { return sealed[i] < sealed[j] })
	if !sorted {
		t.Error("ListSealedPacks should return sorted IDs")
	}

	// Verify all stored blocks readable
	for i, sb := range stored {
		got, err := mgr.GetBlock(ctx, sb.packID, sb.cid)
		if err != nil {
			t.Errorf("block %d unreadable: %v", i, err)
			continue
		}
		if string(got) != string(sb.data) {
			t.Errorf("block %d data mismatch", i)
		}
	}
}

// ---------- RemovePack ----------

func TestRemovePack_SealedPack(t *testing.T) {
	ctx := context.Background()
	mgr, _ := newTestManager(t, 0)
	cidBuilder := cidutil.NewBuilder()

	data := []byte("remove test")
	cid, _ := cidBuilder.ChunkCID(data)
	mgr.PutBlock(ctx, cid, data)
	packID := mgr.CurrentPackID()
	mgr.SealActivePack(ctx)

	// Remove the sealed pack
	err := mgr.RemovePack(packID)
	if err != nil {
		t.Fatal(err)
	}

	// Should no longer appear in sealed list
	for _, s := range mgr.ListSealedPacks() {
		if s == packID {
			t.Error("removed pack still in sealed list")
		}
	}

	// GetBlock should fail
	_, err = mgr.GetBlock(ctx, packID, cid)
	if err == nil {
		t.Error("expected error getting block from removed pack")
	}
}

func TestRemovePack_ActivePack(t *testing.T) {
	mgr, _ := newTestManager(t, 0)
	currentID := mgr.CurrentPackID()

	err := mgr.RemovePack(currentID)
	if err == nil {
		t.Error("expected error when removing active pack")
	}
}

func TestRemovePack_NonexistentPack(t *testing.T) {
	mgr, _ := newTestManager(t, 0)
	err := mgr.RemovePack(9999)
	// Should return an error (file not found)
	if err == nil {
		t.Error("expected error when removing nonexistent pack")
	}
}

// ---------- GetBlock Error Paths ----------

func TestGetBlock_InvalidCID(t *testing.T) {
	ctx := context.Background()
	mgr, _ := newTestManager(t, 0)

	// Invalid CID bytes
	_, err := mgr.GetBlock(ctx, mgr.CurrentPackID(), core.CID{Bytes: []byte("not-a-cid")})
	if err == nil {
		t.Error("expected error for invalid CID")
	}
}

func TestGetBlock_MissingPack(t *testing.T) {
	ctx := context.Background()
	mgr, _ := newTestManager(t, 0)
	cidBuilder := cidutil.NewBuilder()

	data := []byte("some data")
	cid, _ := cidBuilder.ChunkCID(data)

	_, err := mgr.GetBlock(ctx, 9999, cid)
	if err == nil {
		t.Error("expected error for missing pack")
	}
}

// ---------- Concurrent Access ----------

func TestConcurrentPutAndGet(t *testing.T) {
	ctx := context.Background()
	mgr, _ := newTestManager(t, 64*1024)
	cidBuilder := cidutil.NewBuilder()

	const numWriters = 4
	const blocksPerWriter = 20

	type result struct {
		cid    core.CID
		data   []byte
		packID uint64
	}

	results := make([][]result, numWriters)
	var wg sync.WaitGroup

	// Concurrent writers
	for w := 0; w < numWriters; w++ {
		w := w
		wg.Add(1)
		localRng := testkit.RNG(int64(w + 600))
		go func() {
			defer wg.Done()
			for i := 0; i < blocksPerWriter; i++ {
				data := testkit.RandomBytes(localRng, 256+localRng.Intn(256))
				cid, _ := cidBuilder.ChunkCID(data)
				pid, err := mgr.PutBlock(ctx, cid, data)
				if err != nil {
					t.Errorf("writer %d block %d: %v", w, i, err)
					return
				}
				results[w] = append(results[w], result{cid, data, pid})
			}
		}()
	}
	wg.Wait()

	// Verify all blocks readable
	for w, wr := range results {
		for i, r := range wr {
			got, err := mgr.GetBlock(ctx, r.packID, r.cid)
			if err != nil {
				t.Errorf("reader w=%d i=%d: %v", w, i, err)
				continue
			}
			if string(got) != string(r.data) {
				t.Errorf("reader w=%d i=%d: data mismatch", w, i)
			}
		}
	}
}

func TestConcurrentSealAndRead(t *testing.T) {
	ctx := context.Background()
	mgr, _ := newTestManager(t, 2048)
	cidBuilder := cidutil.NewBuilder()

	// Prepopulate
	var cids []core.CID
	var pids []uint64
	rng := testkit.RNG(700)

	for i := 0; i < 5; i++ {
		data := testkit.RandomBytes(rng, 512)
		cid, _ := cidBuilder.ChunkCID(data)
		pid, _ := mgr.PutBlock(ctx, cid, data)
		cids = append(cids, cid)
		pids = append(pids, pid)
	}

	var wg sync.WaitGroup

	// Seal in background
	wg.Add(1)
	go func() {
		defer wg.Done()
		mgr.SealActivePack(ctx)
	}()

	// Concurrent read
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i, c := range cids {
			_, _ = mgr.GetBlock(ctx, pids[i], c)
		}
	}()

	wg.Wait()
}

// ---------- SealAndRotateIfNeeded ----------

func TestSealAndRotateIfNeeded_BelowThreshold(t *testing.T) {
	ctx := context.Background()
	mgr, _ := newTestManager(t, 1024*1024) // 1MB threshold
	cidBuilder := cidutil.NewBuilder()

	// Put a small block
	data := []byte("small")
	cid, _ := cidBuilder.ChunkCID(data)
	mgr.PutBlock(ctx, cid, data)

	packBefore := mgr.CurrentPackID()
	err := mgr.SealAndRotateIfNeeded(ctx)
	if err != nil {
		t.Fatal(err)
	}
	packAfter := mgr.CurrentPackID()

	if packBefore != packAfter {
		t.Error("should not have rotated below threshold")
	}
}

func TestSealAndRotateIfNeeded_AboveThreshold(t *testing.T) {
	ctx := context.Background()
	mgr, _ := newTestManager(t, 256) // very small threshold
	cidBuilder := cidutil.NewBuilder()
	rng := testkit.RNG(800)

	data := testkit.RandomBytes(rng, 512)
	cid, _ := cidBuilder.ChunkCID(data)
	mgr.PutBlock(ctx, cid, data)

	packBefore := mgr.CurrentPackID()
	err := mgr.SealAndRotateIfNeeded(ctx)
	if err != nil {
		t.Fatal(err)
	}
	packAfter := mgr.CurrentPackID()

	if packBefore == packAfter {
		t.Error("should have rotated above threshold")
	}
}

// ---------- PutBlock Deduplication ----------

func TestPutBlock_Dedupe(t *testing.T) {
	ctx := context.Background()
	mgr, _ := newTestManager(t, 0)
	cidBuilder := cidutil.NewBuilder()

	data := []byte("duplicate data")
	cid, _ := cidBuilder.ChunkCID(data)

	pid1, err := mgr.PutBlock(ctx, cid, data)
	if err != nil {
		t.Fatal(err)
	}

	pid2, err := mgr.PutBlock(ctx, cid, data)
	if err != nil {
		t.Fatal(err)
	}

	if pid1 != pid2 {
		t.Error("expected same packID for deduplicated block")
	}
}

// ---------- Discovery After Reopen ----------

func TestDiscovery_MultiplePacks(t *testing.T) {
	dir, err := os.MkdirTemp("", "blobcas-pack-discovery-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	ctx := context.Background()
	cidBuilder := cidutil.NewBuilder()
	rng := testkit.RNG(900)

	cfg := core.PackConfig{Dir: dir, TargetPackBytes: 4096}

	// Phase 1: create and seal multiple packs
	mgr1, err := pack.NewManager(cfg)
	if err != nil {
		t.Fatal(err)
	}

	type stored struct {
		packID uint64
		cid    core.CID
		data   []byte
	}
	var blocks []stored

	for cycle := 0; cycle < 3; cycle++ {
		for i := 0; i < 5; i++ {
			data := testkit.RandomBytes(rng, 200+rng.Intn(200))
			cid, _ := cidBuilder.ChunkCID(data)
			pid, err := mgr1.PutBlock(ctx, cid, data)
			if err != nil {
				t.Fatal(err)
			}
			blocks = append(blocks, stored{pid, cid, data})
		}
		mgr1.SealActivePack(ctx)
	}
	sealedBefore := mgr1.ListSealedPacks()
	mgr1.Close()

	// Phase 2: reopen and verify discovery
	mgr2, err := pack.NewManager(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr2.Close()

	sealedAfter := mgr2.ListSealedPacks()

	if len(sealedAfter) < len(sealedBefore) {
		t.Errorf("discovered fewer sealed packs after reopen: %d vs %d", len(sealedAfter), len(sealedBefore))
	}

	// Verify all blocks readable
	for i, sb := range blocks {
		got, err := mgr2.GetBlock(ctx, sb.packID, sb.cid)
		if err != nil {
			t.Errorf("block %d unreadable after reopen: %v", i, err)
			continue
		}
		if string(got) != string(sb.data) {
			t.Errorf("block %d data mismatch after reopen", i)
		}
	}
}

// ---------- Empty Pack ----------

func TestEmptyPack_Operations(t *testing.T) {
	ctx := context.Background()
	mgr, _ := newTestManager(t, 0)

	// Seal an empty pack
	emptyID := mgr.CurrentPackID()
	err := mgr.SealActivePack(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Iterate should return 0 blocks
	count := 0
	err = mgr.IteratePackBlocks(ctx, emptyID, func(c core.CID) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("iterate empty pack: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 blocks in empty pack, got %d", count)
	}
}

// ---------- Large Block Count ----------

func TestLargeBlockCount(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large block test in short mode")
	}

	ctx := context.Background()
	mgr, _ := newTestManager(t, 16*1024*1024) // 16MB packs
	cidBuilder := cidutil.NewBuilder()
	rng := testkit.RNG(1000)

	const N = 500
	type stored struct {
		cid    core.CID
		data   []byte
		packID uint64
	}
	blocks := make([]stored, N)

	for i := 0; i < N; i++ {
		data := testkit.RandomBytes(rng, 1024) // 1KB per block
		cid, _ := cidBuilder.ChunkCID(data)
		pid, err := mgr.PutBlock(ctx, cid, data)
		if err != nil {
			t.Fatalf("PutBlock %d: %v", i, err)
		}
		blocks[i] = stored{cid, data, pid}
	}

	// Seal and iterate
	packID := mgr.CurrentPackID()
	mgr.SealActivePack(ctx)

	count := 0
	mgr.IteratePackBlocks(ctx, packID, func(c core.CID) error {
		count++
		return nil
	})

	if count != N {
		t.Errorf("expected %d blocks, iterated %d", N, count)
	}

	// Random access verification
	for i := 0; i < 50; i++ {
		idx := rng.Intn(N)
		sb := blocks[idx]
		got, err := mgr.GetBlock(ctx, sb.packID, sb.cid)
		if err != nil {
			t.Errorf("random access block %d: %v", idx, err)
			continue
		}
		if string(got) != string(sb.data) {
			t.Errorf("random access block %d: data mismatch", idx)
		}
	}
}

// ---------- Manager with Empty Directory ----------

func TestNewManager_EmptyDir(t *testing.T) {
	_, err := pack.NewManager(core.PackConfig{Dir: ""})
	if err == nil {
		t.Error("expected error for empty directory")
	}
}

func TestNewManager_CreatesDir(t *testing.T) {
	dir, _ := os.MkdirTemp("", "blobcas-pack-mkdir-*")
	os.RemoveAll(dir) // remove so NewManager creates it

	target := fmt.Sprintf("%s/sub/packs", dir)
	mgr, err := pack.NewManager(core.PackConfig{Dir: target, TargetPackBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	mgr.Close()

	if _, err := os.Stat(target); err != nil {
		t.Errorf("expected directory to be created: %v", err)
	}
	os.RemoveAll(dir)
}
