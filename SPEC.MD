# BlobCAS v1 — Embeddable Content‑Addressable Storage for Blobs (FastCDC + CARv2)

**Status:** Final implementation specification (v1.0)  
**Language/Runtime:** Go (>= 1.22 recommended)  
**Primary use-case:** High‑volume HTTP request/response body storage for a MITM proxy / DAST pipeline with heavy near‑duplicate data.

---

## Table of contents

1. [Scope](#scope)  
2. [Normative language](#normative-language)  
3. [Goals and non-goals](#goals-and-non-goals)  
4. [Concepts and data model](#concepts-and-data-model)  
5. [High-level architecture](#high-level-architecture)  
6. [On-disk formats](#on-disk-formats)  
7. [Public API](#public-api)  
8. [Configuration](#configuration)  
9. [Modules and detailed specifications](#modules-and-detailed-specifications)  
10. [Algorithms and data flows](#algorithms-and-data-flows)  
11. [Durability and crash recovery](#durability-and-crash-recovery)  
12. [GC and compaction](#gc-and-compaction)  
13. [Security](#security)  
14. [Performance and concurrency](#performance-and-concurrency)  
15. [Repository layout](#repository-layout)  
16. [Testing strategy (TDD)](#testing-strategy-tdd)  
17. [Operational tooling](#operational-tooling)  
18. [Revision notes](#revision-notes)  
19. [References](#references)

---

## Scope

BlobCAS is an **embeddable** (library) **content‑addressable storage** system that stores **chunks** and **manifests** as **blocks** inside **CARv2 pack files**, and maintains a small **catalog** (manifest table + CID-to-pack map) for fast lookup.

BlobCAS is designed to be used by another system (e.g., your MITM proxy) that stores “message metadata” elsewhere (headers, URL, timestamps, IDs), while BlobCAS stores the heavy body bytes.

---

## Normative language

The key words **MUST**, **MUST NOT**, **SHOULD**, **SHOULD NOT**, and **MAY** are to be interpreted as described in RFC 2119.

---

## Goals and non-goals

### Goals
1. **Streaming ingestion**: store arbitrarily large bodies without buffering full bodies in memory.
2. **High deduplication** for near-duplicate web traffic using **FastCDC** content-defined chunking.
3. **Pack-based storage** to avoid filesystem inode explosion: chunks/manifests are stored in **CARv2** files.
4. **Random access reads**: retrieve bodies by key/ref with bounded latency; avoid scanning packs on every read.
5. **Catalog-as-derivative**: catalog can be rebuilt from packs (pack scan + index rebuild).
6. **Strict correctness**: reconstructed bytes MUST match original plaintext bytes used to compute chunk CIDs.
7. **Secure by default**: input validation, bounds, corruption detection, safe handling of attacker-controlled traffic.
8. **Configurable retention and pack sizing** through config (no hard-coded defaults that cannot be changed).

### Non-goals (v1)
1. Distributed replication, consensus, or remote protocol.
2. Multi-process concurrent writers to the same repo directory.
3. Strong “dedupe under encryption without equality leakage” (requires additional designs like server-aided MLE; see §Security).

---

## Concepts and data model

### Canonical bytes vs raw wire bytes
BlobCAS supports storing:
- **Canonical** bytes: usually HTTP body after decoding `Content-Encoding` (gzip/br/deflate). This maximizes dedupe.
- **Raw wire** bytes (optional): exact bytes observed on the wire (for exact replay/forensics).

Canonical and raw wire are stored as separate objects; the canonical object MAY reference the raw object via `raw_ref` in the manifest.

**Important:** BlobCAS does not parse HTTP by itself. The caller controls canonicalization and provides:
- canonical reader (required)
- optional raw wire reader

### Chunking
BlobCAS uses **FastCDC** to split canonical and raw streams into content-defined chunks for high dedupe under small edits and boundary shifts.

### Content addressing
Each chunk is addressed by a **CID** (Content Identifier). BlobCAS uses **CIDv1** (CIDv0 is deprecated compatibility only).

- **Chunk CID** = CIDv1 with multicodec `raw` and multihash `sha2-256(plaintextChunkBytes)`.
- **Manifest CID** = CIDv1 with multicodec `dag-cbor` and multihash `sha2-256(canonicalDagCborBytes)`.

### Packs
Chunks and manifests are stored as blocks inside **CARv2** pack files. CARv2 supports an optional index; an **index offset of 0 indicates “no index present”** and readers MUST NOT attempt to read an index when it is absent.

### Catalog
Catalog provides:
1. `CID -> PackID` mapping (`c2p`) because CARv2 index is per-pack and does not locate which file contains a CID.
2. `Key -> ManifestCID` mapping (`k2m`) serving as the “manifest table” for external systems.
3. `roots -> deadline` for retention/GC.

Catalog is a **derivative view** and MUST be rebuildable by scanning packs.

---

## High-level architecture

### Components
- **Chunker**: streams bytes, produces content-defined chunks (FastCDC).
- **CID builder**: computes chunk and manifest CIDs and verifies integrity.
- **Transform**: optional storage-layer compression and/or encryption.
- **Pack manager**: writes/reads CARv2 packs; handles rotation and sealing.
- **Catalog**: embedded KV mapping (`c2p`, `k2m`, `roots`, `packmeta`).
- **Store orchestrator**: public API implementation; coordinates the pipeline.
- **GC/Compactor**: background mark + compact + sweep.

### Write path overview
1. Caller provides `PutInput{Canonical: r1, RawWire: r2?}` plus metadata.
2. Chunk canonical stream with FastCDC.
3. For each chunk:
   - compute CID over plaintext chunk bytes
   - if already present in catalog, skip storing
   - else apply transform and write block to active pack
   - update catalog `c2p`
4. Build and write manifest block (and optionally raw wire manifest block).
5. Write catalog `k2m` key mapping and `roots` deadline.

### Read path overview
1. Resolve key → manifest CID via catalog.
2. Load manifest block.
3. For each chunk CID referenced:
   - locate pack via catalog `c2p`
   - read block from pack
   - decode transform → plaintext
   - verify plaintext hashes to CID
4. Stream plaintext to caller.

---

## On-disk formats

### Directory layout
Repo directory (`cfg.Dir`) MUST contain:

```
<dir>/
  packs/          # CARv2 pack files
  catalog/        # KV store files (Pebble default)
  meta/           # pack registry, locks, health markers
  quarantine/     # corrupted/invalid packs moved here (optional)
```

### Pack file naming
Pack files MUST be deterministic and sortable:
`pack-<packID:016x>.car`

PackID is a monotonically increasing uint64 allocated by the pack manager.

### CARv2 pack compliance
CAR requires roots. BlobCAS writes a **PackRoot** block (dag-cbor) and uses it as the single CAR root for each pack.

#### PackRootV1 (dag-cbor)
Fields:
- `version:uint16 = 1`
- `pack_id:uint64`
- `created_at_unix_nano:uint64`
- `chunking_profile:string`
- `transform:string`
- `notes:map[string]string` (optional; bounded)

PackRoot is for compliance and diagnostics; reads do not depend on roots.

### Transform envelope (stored bytes)
Transform applies to chunks and manifests independently.

Stored block bytes are either plaintext (transform `none`) or an envelope:

```
MAGIC[4] = "BCAS"
VER[1]   = 1
FLAGS[1] = bit0:compressed, bit1:encrypted
ALG[1]   = 1=zstd, 2=aesgcm, 3=chacha20poly1305 (reserved)
KIDLEN[1] + KID[...]          (optional if encrypted)
NONCELEN[1] + NONCE[...]      (required if encrypted)
PAYLOAD[...]                  (compressed bytes or ciphertext)
```

**CID is always computed over plaintext**, so reads MUST decode transform and verify CID match.

### Manifest format (dag-cbor canonical)
`ManifestV1` (strict; unknown fields rejected):

- `version:uint16 = 1`
- `canonical:bool`
- `media_type:string` (optional; bounded)
- `orig_content_encoding:string` (optional; bounded)
- `length:uint64`
- `chunks:[ChunkRef]`
- `raw_ref:?Ref` (optional; links to raw-wire object manifest)
- `whole_sha256:bytes(32)` (optional)
- `tags:map[string]string` (optional; bounded)

`ChunkRef`:
- `cid:bytes` (binary CID)
- `len:uint32`

Validation MUST enforce:
- `sum(chunks.len) == length`
- `len(chunks) == 0` iff `length == 0`
- bounds: max chunks per object, max tag count/size, max media_type length

---

## Public API

Package: `pkg/blobcas`

### Types

```go
package blobcas

type CID struct { Bytes []byte } // binary CID bytes

type Ref struct { ManifestCID CID }

type Key struct {
    Namespace string
    ID        string
}

type PutInput struct {
    // Canonical is required. If caller does not canonicalize, set PutMeta.Canonical=false.
    Canonical io.Reader

    // RawWire is optional. If provided, BlobCAS stores it as a separate object and links it from the canonical manifest.
    RawWire io.Reader
}

type PutMeta struct {
    Canonical           bool              // indicates whether Canonical reader is canonicalized bytes
    MediaType           string            // optional
    OrigContentEncoding string            // optional; for metadata only
    Tags                map[string]string // optional, bounded

    // Retention override (see precedence rules below):
    // - If RootDeadline != nil: use exactly that deadline.
    // - Else if RootTTL != nil: deadline = now + *RootTTL.
    // - Else if cfg.GC.DefaultRootTTL > 0: deadline = now + cfg.GC.DefaultRootTTL.
    // - Else: do not create a root entry.
    RootDeadline *time.Time
    RootTTL      *time.Duration
}

type GetInfo struct {
    Length    uint64
    MediaType string
    Canonical bool
    HasRaw    bool
}

type Stat struct {
    Length     uint64
    ChunkCount uint32
    Canonical  bool
}
```

### Interface

```go
type Store interface {
    Put(ctx context.Context, key Key, in PutInput, meta PutMeta) (Ref, error)
    Resolve(ctx context.Context, key Key) (Ref, error)
    Get(ctx context.Context, ref Ref) (io.ReadCloser, GetInfo, error)

    HasChunk(ctx context.Context, cid CID) (bool, error)
    GetChunk(ctx context.Context, cid CID) (io.ReadCloser, uint32 /*plainLen*/, error)

    Stat(ctx context.Context, ref Ref) (Stat, error)
    Close() error
}
```

### Errors (stable contract)

```go
var (
    ErrNotFound     = errors.New("blobcas: not found")
    ErrInvalidInput = errors.New("blobcas: invalid input")
    ErrCorrupt      = errors.New("blobcas: corrupt data")
    ErrTooLarge     = errors.New("blobcas: too large")
    ErrClosed       = errors.New("blobcas: store closed")
)
```

Rules:
- `Get` MUST return `ErrNotFound` if manifest missing OR any referenced chunk missing.
- `Get` MUST return `ErrCorrupt` on CID mismatch, transform decode failure, or manifest validation failure.
- All errors SHOULD wrap underlying errors with `%w`.

---

## Configuration

Package: `pkg/blobcas`

```go
type Config struct {
    Dir string // repo root

    Chunking ChunkingConfig
    Pack     PackConfig
    Catalog  CatalogConfig
    Limits   LimitsConfig
    Transform TransformConfig
    GC       GCConfig
}

type ChunkingConfig struct {
    // DefaultProfile chooses min/avg/max. Profiles are purely convenience.
    // "web64k": min=16KiB avg=64KiB max=1MiB
    // "large1m": min=256KiB avg=1MiB max=4MiB
    DefaultProfile string

    // Explicit overrides (if non-zero) override profile values.
    Min int
    Avg int
    Max int

    // Normalization parameter passed to FastCDC implementation (if supported).
    Normalization int
}

type PackConfig struct {
    Dir string // default: <Config.Dir>/packs

    // TargetPackBytes controls rotation threshold. MUST be configurable.
    TargetPackBytes uint64

    // MaxPackBytes is a hard ceiling; if 0 => 2*TargetPackBytes.
    MaxPackBytes uint64

    // RequireIndexOnSeal: sealed packs MUST contain a CARv2 index.
    RequireIndexOnSeal bool

    // Durability knobs
    SealFsync       bool   // fsync on seal
    FsyncEveryBytes uint64 // periodic fsync; 0 disables
}

type CatalogConfig struct {
    Dir string // default: <Config.Dir>/catalog
    Backend string // "pebble" (default), "badger" (optional)
}

type TransformConfig struct {
    Name string // "none" (default), "zstd", "zstd+aead" (optional)
    ZstdLevel int

    // Optional encryption keys
    KeyID string
    KeyBytes []byte
}

type LimitsConfig struct {
    // Limits apply to plaintext streams as they are read (not stored size).
    MaxCanonicalBytes uint64 // 0 => unlimited (not recommended)
    MaxRawWireBytes   uint64 // 0 => unlimited

    // Manifest safety
    MaxChunksPerObject uint32
    MaxTags            int
    MaxTagKeyLen       int
    MaxTagValLen       int
    MaxMediaTypeLen    int
}

type GCConfig struct {
    Enabled bool

    // DefaultRootTTL MUST be configurable; applied when PutMeta does not override.
    // If 0, roots are not created by default.
    DefaultRootTTL time.Duration

    RunEvery time.Duration // background GC interval; 0 disables periodic scheduling
    MaxPackAge time.Duration
    TargetPackBytes uint64 // target size for compacted packs (may differ from Pack.TargetPackBytes)
    MaxIOPS int // optional throttle; 0 => best-effort
}
```

### Retention precedence rules (normative)
When `Store.Put()` stores a canonical object manifest:
1. If `meta.RootDeadline != nil`, it MUST be used exactly as the deadline.
2. Else if `meta.RootTTL != nil`, the deadline MUST be computed as `now + *meta.RootTTL`.
3. Else if `cfg.GC.DefaultRootTTL > 0`, deadline MUST be `now + cfg.GC.DefaultRootTTL`.
4. Else, Store MUST NOT create a `roots|manifestCID` entry.

Validation:
- `RootDeadline` and `RootTTL` MUST NOT both be set; else `ErrInvalidInput`.
- Deadlines in the past and negative TTL MUST return `ErrInvalidInput` (v1 default).

---

## Modules and detailed specifications

### `pkg/chunker`
Responsibilities:
- Streaming FastCDC chunking with bounds.

Key types:
```go
type Chunk struct {
    Buf []byte // owned by chunker; returned to pool by consumer
    N   int
}
type Chunker interface {
    Split(ctx context.Context, r io.Reader) (<-chan Chunk, <-chan error)
}
```

Implementation requirements:
- MUST enforce min/avg/max bounds.
- MUST respect context cancellation.
- MUST use bounded buffering and `sync.Pool`.

### `pkg/cidutil`
Responsibilities:
- Create and verify CIDv1 for chunks and manifests.
- CIDv1 SHOULD be used (CIDv0 deprecated compatibility only).

Key functions:
```go
type Builder interface {
    ChunkCID(plain []byte) (blobcas.CID, error)
    ManifestCID(dagCbor []byte) (blobcas.CID, error)
    Verify(cid blobcas.CID, plain []byte) error
}
```

### `pkg/manifest`
Responsibilities:
- Define `ManifestV1`, strict validation, canonical dag-cbor encoding/decoding.

Interfaces:
```go
type Codec interface {
    Encode(m *ManifestV1) ([]byte, error)
    Decode(b []byte) (*ManifestV1, error)
}
```

### `pkg/transform`
Responsibilities:
- Encode/decode block payloads (zstd, optional AEAD).
- MUST be streaming-friendly at chunk granularity (per-chunk).

Interfaces:
```go
type Transform interface {
    Name() string
    Encode(plain []byte) ([]byte, error)
    Decode(stored []byte) ([]byte, error)
}
```

### `pkg/pack`
Responsibilities:
- Manage CARv2 read-write blockstore for active pack and read-only readers for sealed packs.
- Pack rotation and sealing.
- Ensure sealed packs have index if configured; CARv2 index offset 0 implies no index.

Interfaces:
```go
type PackID uint64

type Manager interface {
    PutBlock(ctx context.Context, cid blobcas.CID, stored []byte) (PackID, error)
    GetBlock(ctx context.Context, packID PackID, cid blobcas.CID) ([]byte, error)
    SealAndRotateIfNeeded(ctx context.Context) error
    CurrentPackID() PackID
    Close() error
}
```

Implementation requirements:
- Single-writer semantics for pack writes (enforced by store orchestrator).
- `SealAndRotateIfNeeded` MUST:
  - finalize index (if `RequireIndexOnSeal`)
  - fsync if `SealFsync`
  - persist `packmeta` to catalog

### `pkg/catalog`
Responsibilities:
- Implement `c2p`, `k2m`, `roots`, `packmeta` keyspaces.
- Provide atomic batches for updates.

Interface:
```go
type Catalog interface {
    GetPackForCID(ctx context.Context, cid blobcas.CID) (pack.PackID, bool, error)
    PutPackForCID(ctx context.Context, cid blobcas.CID, packID pack.PackID) error

    GetManifestForKey(ctx context.Context, key blobcas.Key) (blobcas.CID, bool, error)
    PutManifestForKey(ctx context.Context, key blobcas.Key, manifest blobcas.CID) error

    PutRootDeadline(ctx context.Context, manifest blobcas.CID, deadline time.Time) error
    IterateRoots(ctx context.Context, fn func(manifest blobcas.CID, deadline time.Time) error) error

    PutPackMeta(ctx context.Context, meta PackMetaV1) error
    Close() error
}
```

### `pkg/blobcas` (store orchestrator)
Responsibilities:
- Provide public API.
- Coordinate chunking, dedupe checks, pack writes, catalog updates.
- Enforce single-writer for writes to avoid duplicate first-writes.

Internal model:
- One writer goroutine servicing `Put` requests.
- Multiple concurrent readers.
- Pack reader caching MUST cache handles, NOT heap-cached `[]byte` payloads (avoid GC pressure).

### `pkg/gc`
Responsibilities:
- Background compaction and retention enforcement.
- Mark live manifests by `roots` deadlines; compact packs by copying live blocks to new packs; delete dead packs.

Interfaces:
```go
type Runner interface {
    RunOnce(ctx context.Context) (Report, error)
}
```

---

## Algorithms and data flows

### Put (canonical + optional raw)
Input: `key`, `PutInput{Canonical, RawWire?}`, `meta`

Steps (canonical):
1. Validate `key` (length, charset), validate `meta` bounds, validate retention fields.
2. Stream `in.Canonical`:
   - split into chunks via chunker
   - for each chunk:
     - compute `chunkCID`
     - check `catalog.c2p`
     - if missing:
       - `stored = transform.Encode(plain)`
       - `packID = pack.PutBlock(chunkCID, stored)`
       - `catalog.PutPackForCID(chunkCID, packID)`
3. Create `ManifestV1` with chunk refs and metadata.
4. Encode manifest to canonical dag-cbor bytes.
5. Compute `manifestCID`.
6. Store manifest as a block (dedupe identical manifests naturally):
   - check `c2p`, write if missing, update `c2p`
7. If `in.RawWire != nil`:
   - store raw object under an internal key (implementation detail) OR store raw manifest independently
   - set `raw_ref` in canonical manifest to raw manifest CID
8. Apply retention precedence rules and write `roots|manifestCID` if applicable.
9. Write `k2m|key -> manifestCID`.

Atomicity:
- Catalog updates MUST be done in a Pebble batch per Put:
  - new `c2p`
  - manifest `c2p`
  - `roots` (if any)
  - `k2m` mapping

### Resolve
- `Resolve(key)` reads `k2m` and returns `Ref{ManifestCID}` or `ErrNotFound`.

### Get
Input: `Ref{ManifestCID}`
1. Find pack for manifest via `c2p`; load manifest block from pack.
2. Decode and validate manifest.
3. For each chunk ref:
   - locate pack via `c2p`
   - read stored bytes
   - decode transform → plaintext
   - verify plaintext hashes to chunk CID
   - stream plaintext to caller
4. Return `GetInfo` with length/media_type/canonical and `HasRaw` (raw_ref present).

### Reindex / rebuild catalog
Tooling MUST support:
- scanning all packs and rebuilding `c2p` and `packmeta`.
- optionally verifying CID integrity during rebuild (slow mode).

The CAR format contains no internal indexing; indexing is external by design, and CARv2 adds an optional index; however, pack selection still requires `c2p` catalog.

---

## Durability and crash recovery

### Durability knobs
- Packs are append-only.
- `Pack.FsyncEveryBytes` MAY be enabled for periodic durability.
- `Pack.SealFsync` SHOULD default to true for safety on rotation/seal.

### Crash scenarios
- Crash after chunks written but before manifest/k2m:
  - orphan chunks remain in packs; catalog may or may not reference them
  - they MUST be reclaimable by GC compaction (no root references).
- Crash with catalog corruption/loss:
  - MUST be recoverable using `blobcas-reindex` scanning packs to rebuild `c2p`.
  - `k2m` and `roots` may be lost if not stored elsewhere; caller SHOULD treat them as higher-level DB data and be able to re-add roots/mappings.

---

## GC and compaction

### Model
- Retention is based on `roots|manifestCID -> deadline`.
- GC is **mark + compact + sweep**:
  1. Mark live manifests whose deadline > now.
  2. Load each manifest and mark its chunk CIDs.
  3. Compact: copy live blocks from old packs into new packs.
  4. Update `c2p` to new packIDs.
  5. Delete old pack when it contains no live blocks.

### Safety requirements
- GC MUST be cancellable via context.
- GC MUST throttle I/O if configured.
- GC MUST be idempotent (safe to re-run after crash).

---

## Security

### Input validation
- Reject keys with path separators or control characters.
- Enforce bounds in `LimitsConfig`.
- Strictly validate manifest decode and chunk count/length sums.
- Caller SHOULD enforce decompression limits during canonicalization to prevent bombs.

### Corruption detection
- Always verify CID match after transform decode.
- Any mismatch MUST return `ErrCorrupt`.

### CARv2 index safety
- Loading CARv2 indexes from **untrusted sources** is dangerous; indexes MUST be regenerated locally when ingesting external CAR files.
- BlobCAS does not ingest external CAR files in v1; only reads packs it produced.
- The `blobcas-reindex` tool MUST be able to rebuild indexes locally when needed.

### Encryption modes (optional)
BlobCAS supports (configurable) transforms:
1. `none`
2. `zstd`
3. `zstd+aead` (AEAD per block) — CID remains plaintext CID; this leaks equality/existence of plaintext by CID membership (typical for CAS).

If you require encrypted dedupe with resistance to offline confirmation attacks, you need a stronger design (server-aided key derivation / MLE variants). That is out-of-scope for v1.

---

## Performance and concurrency

### Concurrency model (v1)
- **Single writer goroutine** for `Put` to prevent duplicate first-writes and to serialize rotation/seal.
- Multiple concurrent readers.
- Chunking/hashing MAY be parallelized inside Put (implementation detail) but must preserve stream order within each object’s manifest.

### Memory and GC pressure
- Never cache payload blocks as `[]byte` across calls.
- Cache pack reader handles and rely on OS page cache.
- For direct mmap reads, use `golang.org/x/exp/mmap.ReaderAt` patterns (safe ReaderAt abstraction).

### Throughput
- Avoid fsync per chunk; use periodic fsync and fsync on seal.
- Catalog writes use Pebble batches per Put.

---

## Repository layout

```
blobcas/
  cmd/
    blobcas-reindex/
    blobcas-verify/
    blobcas-gc/
  pkg/
    blobcas/
    chunker/
    cidutil/
    manifest/
    transform/
    pack/
    catalog/
    gc/
  internal/
    testkit/
  SPEC.md
  README.md
  go.mod
```

---

## Testing strategy (TDD)

**Policy:** No commit without:
- all tests passing
- **100% coverage of new code**
- cross-validation tests passing (where applicable)

### Unit tests
- `chunker`: determinism, bounds, cancellation.
- `cidutil`: stable CID, mismatch detection.
- `manifest`: strict decode/encode, bounds, sum length validation.
- `transform`: roundtrip, tamper detection (if AEAD).
- `catalog`: atomic batch behavior, iteration, corruption handling.

### Integration tests
1. `TestPutGet_RoundTrip` for sizes: 0B, 1B, 4KiB, 1MiB, 100MiB.
2. `TestDedup_NearDuplicate`: store A then A’ with inserts; verify large chunk reuse.
3. `TestConcurrentPut_SameContent`: many goroutines put same content; ensure only one physical write of each chunk CID.
4. `TestCrash_OrphanChunks`: crash simulation mid-put; reindex; ensure Get works for completed puts.
5. `TestCorruption`: flip bytes in pack; ensure `ErrCorrupt` on Get.

### GC tests
- mark correctness (deadline honored)
- compact moves live blocks and updates `c2p`
- sweep deletes only fully-dead packs

---

## Operational tooling

### `blobcas-verify`
- verifies catalog `c2p` consistency with packs
- spot-checks CID integrity by sampling blocks

### `blobcas-reindex`
- rebuilds `c2p` and pack metadata by scanning packs
- optional full integrity verification (slow mode)

### `blobcas-gc`
- runs one GC cycle; prints moved blocks, reclaimed packs, bytes reclaimed

---

## Revision notes

- **Retention default is now fully configurable** via `cfg.GC.DefaultRootTTL` with explicit per-Put overrides (`PutMeta.RootDeadline` / `PutMeta.RootTTL`) and strict precedence.
- **Pack target size is now fully configurable** via `cfg.Pack.TargetPackBytes` (and `MaxPackBytes` ceiling) with explicit seal/index durability knobs.
- Clarified CARv2 semantics: index offset `0` means “no index present”.
- Catalog is explicitly derivative and rebuildable; reindex tooling is required.

---

## References

- CARv2 specification (index offset 0 means no index present): https://ipld.io/specs/transport/car/carv2/  
- CARv1 specification (no internal indexing; external indexing is required): https://ipld.io/specs/transport/car/carv1/  
- `go-car` features: RW blockstore for CARv2, appending, resumption, random access: https://github.com/ipld/go-car  
- `go-car/v2` package docs: https://pkg.go.dev/github.com/ipld/go-car/v2  
- FastCDC paper (USENIX ATC 2016): https://www.usenix.org/system/files/conference/atc16/atc16-paper-xia.pdf  
- fastcdc-go implementation: https://github.com/jotfs/fastcdc-go  
- go-cid package (CIDv1 recommended; CIDv0 deprecated): https://pkg.go.dev/github.com/ipfs/go-cid  
- zstd (klauspost) Go package docs: https://pkg.go.dev/github.com/klauspost/compress/zstd  
- x/exp/mmap ReaderAt: https://pkg.go.dev/golang.org/x/exp/mmap  
- go-car advisory (untrusted CARv2 index parsing risks): https://github.com/advisories/GHSA-9x4h-8wgm-8xfg  
- Pack+index design inspiration (restic): https://restic.readthedocs.io/en/stable/design.html  
