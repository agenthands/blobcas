# BlobCAS

**BlobCAS** is an **embeddable content-addressable storage (CAS)** engine for storing huge volumes of blobs with heavy near-duplication.

It combines:
- **FastCDC content-defined chunking** (high dedupe under small edits/shifts)
- **CARv2 packfiles** for append-only, seek-friendly block storage
- A small **embedded catalog** (CID → pack + key → manifest) that is **rebuildable by scanning packs**

---

## Why this design

- **Near-duplicates are normal for many cases (JSON templates, repeated JS/CSS, API responses with small diffs).
- **Content-defined chunking** keeps chunk boundaries stable even when bytes are inserted/removed.
- **CARv2** provides a standard container for content-addressed blocks and optional indexes.
- **Catalog is a derivative view** (fast lookups), but can be rebuilt from pack files.

**Specs:** See [`SPEC.md`](./SPEC.md) for the normative storage format and semantics.

---

## Features

- ✅ **Streaming Put/Get** (no full-body buffering)
- ✅ **Canonical-body storage** for best dedupe (decoded `Content-Encoding`), with optional **raw-wire** storage for exact replay
- ✅ **Storage-layer compression** (zstd) without changing content addresses
- ✅ **Append-only packs** with configurable rotation and sealing
- ✅ **Reindex tool** to rebuild catalog from packs
- ✅ **Background GC/compaction** (mark + compact + sweep) with configurable default retention TTL

---

## Data model (one paragraph)

Each body is stored as a **Manifest** that references an ordered list of **FastCDC chunks**.

- Chunk **CID** is computed over **plaintext chunk bytes** (canonical bytes by default).
- Stored block bytes may be **compressed/encrypted** at rest; CID remains the plaintext hash.
- Manifests and chunks are stored as blocks inside **CARv2 pack files**.

---

## Quickstart

### Install

```bash
go get ./...
```

### Minimal example

```go
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"your/module/path/pkg/blobcas"
)

func main() {
	ctx := context.Background()

	cfg := blobcas.Config{
		Dir: "./repo",
		Pack: blobcas.PackConfig{
			TargetPackBytes:    512 << 20, // configurable
			MaxPackBytes:       0,         // 0 => 2*TargetPackBytes
			RequireIndexOnSeal: true,
			SealFsync:          true,
			FsyncEveryBytes:    64 << 20, // optional periodic fsync
		},
		GC: blobcas.GCConfig{
			Enabled:        true,
			DefaultRootTTL: 30 * 24 * time.Hour, // configurable default retention
		},
		Transform: blobcas.TransformConfig{
			Name:      "zstd",
			ZstdLevel: 3,
		},
	}

	s, err := blobcas.Open(ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer s.Close()

	key := blobcas.Key{Namespace: "http", ID: "req-000001"}

	canonical := bytes.NewReader([]byte(`{"hello":"world"}`)) // decoded bytes recommended

	ref, err := s.Put(ctx, key, blobcas.PutInput{Canonical: canonical}, blobcas.PutMeta{
		Canonical: true,
		MediaType: "application/json",
		// Optional override:
		// RootTTL: ptr(7 * 24 * time.Hour),
	})
	if err != nil {
		log.Fatal(err)
	}

	rc, info, err := s.Get(ctx, ref)
	if err != nil {
		log.Fatal(err)
	}
	defer rc.Close()

	out, _ := io.ReadAll(rc)
	fmt.Printf("len=%d canonical=%v body=%s\n", info.Length, info.Canonical, out)
}
```

> For raw-wire support (exact replay), provide `PutInput.RawWire` and the canonical manifest will include a `raw_ref`.

---

## Configuration highlights

### Default retention TTL (configurable)

`cfg.GC.DefaultRootTTL` is applied when **no per-Put retention override** is provided.

Per-Put precedence (normative; see SPEC.md):
1. `PutMeta.RootDeadline` (exact)
2. `PutMeta.RootTTL` (now + ttl)
3. `cfg.GC.DefaultRootTTL` (now + default)
4. else: no root entry

### Pack sizing (configurable)

`cfg.Pack.TargetPackBytes` controls rotation size. `cfg.Pack.MaxPackBytes` is a hard ceiling (defaults to `2 * TargetPackBytes`).

---

## Tools

All tools are optional binaries under `cmd/`:

- `blobcas-reindex` — rebuild `CID → pack` mappings by scanning packs (recreate catalog)
- `blobcas-verify` — verify catalog consistency and sample integrity checks
- `blobcas-gc` — run one GC/compaction cycle and report reclaimed space

---

## Security notes

BlobCAS is designed for **attacker-controlled inputs** (proxy traffic):
- Strict bounds on manifest decode and chunk counts
- Optional limits for canonical and raw-wire sizes
- CID verification after transform decode (detects corruption/tamper)
- Avoid loading CAR indexes from untrusted external sources; reindex locally

See `SPEC.md` for the full security model and validation rules.

---

## Status / roadmap

**v1** focuses on correctness + performance for an embedded single-process store:
- [ ] add structured metrics hooks (Prometheus-friendly interface)
- [ ] richer compaction heuristics (utilization-based selection)
- [ ] optional WAL-like commit markers for “stronger” k2m durability

---

## Contributing

- Follow `AGENT.MD` for workflow expectations (TDD, coverage, commands).
- Changes that impact on-disk format MUST update `SPEC.md` and bump format version fields.

---

## License

Apache-2.0.

