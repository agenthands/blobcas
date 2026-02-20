# AGENT.MD — Working agreements for coding agents and contributors

This file is **repository guidance** for humans and AI coding agents.

Many tools look for `AGENTS.md` in the repo root. If your agent expects that filename, copy this file:
```bash
cp AGENT.MD AGENTS.md
```

---

## Project overview

**BlobCAS** is an embeddable CAS engine optimized for **high-volume HTTP bodies**:
- FastCDC chunking
- CARv2 packfiles
- Embedded catalog (CID→pack, key→manifest)
- Catalog is rebuildable from pack scan
- Background GC/compaction with configurable default retention TTL

**Normative spec:** `SPEC.MD` is the source of truth for on-disk formats and semantics.

---

## Golden rules (do not violate)

1. **Do not change on-disk formats casually.**
   - Any change to manifest schema, block envelope, keyspaces, or pack layout MUST:
     - update `SPEC.md`
     - bump the relevant `version` field(s)
     - include migration / compatibility notes

2. **Always stream.**
   - Never read an entire object into memory unless the object is explicitly bounded and small.
   - The store must be safe for very large bodies and many concurrent reads.

3. **Integrity is mandatory.**
   - Reads MUST verify: `CID(plaintext) == expected CID` after transform decode.
   - Return `ErrCorrupt` on mismatch.

4. **Catalog is a derivative view.**
   - Do not introduce hard dependencies where catalog loss makes the repo unrecoverable.
   - `blobcas-reindex` MUST remain able to rebuild `c2p` by scanning packs.

5. **Single-writer invariant (v1).**
   - Do not add multiple concurrent writers to the same repo without a locking protocol and extensive tests.

---

## Default workflow (TDD)

Follow this exact loop:

1. **Write the test first** (new `_test.go` exists and fails)
2. **Run the test** (confirm RED)
3. **Write minimal implementation** (just enough for GREEN)
4. **Run the test** (confirm GREEN)
5. **Refactor**
6. **Run all tests**
7. **Commit** (tests + implementation together)

### Never commit without
- ✅ all tests passing
- ✅ **100% coverage of new code**
- ✅ cross-validation passing (when applicable)

---

## Commands (canonical)

Run from repo root:

```bash
go test ./...
go test -race ./...
go test ./... -coverprofile=coverage.out
go tool cover -func=coverage.out
```

Formatting:

```bash
gofmt -w .
```

If a linter is introduced later, document it here and pin versions.

---

## Dependency policy

- Prefer Go stdlib.
- Adding a new dependency requires:
  - justification in PR description / commit message
  - security/maintenance check (actively maintained, permissive license)
  - tests proving value

Core dependencies we expect:
- CARv2 tooling: `github.com/ipld/go-car/v2`
- Chunking: `github.com/jotfs/fastcdc-go`
- Catalog: `github.com/cockroachdb/pebble`
- Compression: `github.com/klauspost/compress/zstd`

Do not replace these without a strong reason and benchmarks.

---

## Performance expectations

- No `[]byte` block cache on heap. Cache **reader handles**, rely on OS page cache.
- Use `sync.Pool` for chunk buffers.
- Avoid excessive allocations in hot paths (Put/Get).
- Keep pack rotation and sealing cheap; no per-block fsync.

Any performance change MUST include a microbenchmark or a before/after profile note.

---

## Security expectations

Treat all inputs as hostile:
- enforce `LimitsConfig` everywhere it matters
- strict manifest decoding (reject unknown fields)
- bounds on tag sizes, chunk counts, media_type length
- never trust external CAR index data; reindex locally if you ever ingest external packs

---

## Where to change what

- `pkg/blobcas/` — public API, config, Open(), errors
- `pkg/chunker/` — FastCDC split logic and profiles
- `pkg/cidutil/` — CID building + verification
- `pkg/manifest/` — Manifest schema + dag-cbor codec + validation
- `pkg/transform/` — zstd / encryption envelopes
- `pkg/pack/` — CARv2 pack manager (rotation/seal/read)
- `pkg/catalog/` — Pebble keyspaces and batch writes
- `pkg/gc/` — mark + compact + sweep

Tools:
- `cmd/blobcas-reindex/`
- `cmd/blobcas-verify/`
- `cmd/blobcas-gc/`

---

## Change checklist

Before you finish a task:
- [ ] Tests added/updated and pass
- [ ] New code fully covered
- [ ] `SPEC.md` updated if semantics changed
- [ ] Benchmarks added if performance-sensitive
- [ ] `README.md` updated if user-facing behavior changed
