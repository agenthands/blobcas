# BlobCAS Testing & Benchmarking Guide

BlobCAS includes a comprehensive suite of tests and benchmarks organized around unit, integration, fuzzing, and stress testing. The test suite strictly enforces the system's design constraints, such as streaming I/O, absence of heap caching, and zero import cycles.

## Running Tests

### Standard Unit Tests
Run the core unit test suite mapping directly to individual packages:
```bash
go test -v ./...
```

### Integration Tests
Integration tests target the orchestrator package (`pkg/blobcas`) to verify end-to-end capabilities, such as near-duplicate deduplication, reindexing, garbage collection, and fault tolerance:
```bash
go test -v ./pkg/blobcas ./pkg/gc
```

### Stress and Race Tests
Stress tests evaluate the `blobcas.Store` under heavy, concurrent operations. Since these tests use randomized delays and loop continuously, they are meant to catch infrequent race conditions:
```bash
go test -v -race -run TestStoreIntegration_ConcurrentPutGet_Race ./pkg/blobcas
```

For the long-running endurance test (defaults to 60s, configurable via `STRESS_DURATION`):
```bash
go test -v -tags stress -run TestStoreIntegration_LongRunning_Stress ./pkg/blobcas
```

### Fuzz Testing
BlobCAS utilizes standard Go fuzzing to ensure decoders and parsers do not panic on malicious input. Fuzz tests simulate corrupted manifests, invalid chunk hashes, and malformed transform envelopes:
```bash
go test -fuzz=FuzzManifestDecode ./pkg/manifest
go test -fuzz=FuzzTransformDecode ./pkg/transform
go test -fuzz=FuzzCIDVerify ./pkg/cidutil
```

---

## Benchmarking Suite

The project includes specialized benchmarks to measure raw throughput, allocation profiles, and E2E system metrics. To run all benchmarks:
```bash
go test -bench . -benchmem ./...
```

### Subsystem Benchmarks
1. **Chunker**: Validates FastCDC splitting speed on random vs highly compressible bytes.
2. **CID Hashing**: Profiles SHA-256 + CIDv1 generation throughput.
3. **Pack Append**: Measures raw sequential blockstore write speeds.
4. **Transform**: Highlights compression throughput overhead.

### E2E Benchmarks
1. **`BenchmarkStorePutGet`**: Measures the entire `Put` and `Get` pipeline across varying deduplication rates (0%, 50%, 90%) and blob sizes (64KB to 16MB).
2. **`BenchmarkReindex`**: Profiles the raw speed of the `blobcas-reindex` logic (scanning physical packs to rebuild Pebble KV states).
3. **`BenchmarkGCCompaction`**: Profiles `blobcas-gc` speed scanning and isolating live bytes from dead bytes.

---

## `internal/testkit` Package

Due to the complex nature of deterministic testing on streaming chunkers and deduplication engines, BlobCAS uses an internal `testkit` to provide deterministic randomized inputs.

**Features included:**
- Deterministic RNG `testkit.RNG(seed)`
- Compressible `testkit.CompressibleBytes()` vs Random `testkit.RandomBytes()` stream generators
- `io.Reader` fault injection (`ErrorReader`, `PauseReader`, `BlockingReader`) to simulate I/O delays or partial network writes during `Put`
- Pack and catalog introspection tools (like `CountUniqueBlocks` to verify deduplication effectiveness without breaking capsulation).
