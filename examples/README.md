# BlobCAS Examples

This directory contains executable code examples that demonstrate how to use the `BlobCAS` engine, a content-addressable storage solution optimized for heavy near-duplication payloads.

## Getting Started

You can run any of these examples directly using `go run`. They are designed to be self-contained and automatically clean up after themselves by using temporary storage directories.

### 1. Basic Usage (`01_basic_usage`)

Demonstrates the absolute minimum required configuration to initialize a store, Put a JSON payload, and Retrieve it using the generated Manifest CID.

```bash
go run examples/01_basic_usage/main.go
```

### 2. Deduplication (`02_deduplication`)

Demonstrates how BlobCAS uses FastCDC (content-defined chunking) to deduplicate similarly structured data. It saves two JSON payloads with only a tiny modification between them, showing how efficiently the chunking algorithm isolates changes to avoid storing redundant bytes.

```bash
go run examples/02_deduplication/main.go
```

### 3. Garbage Collection (`03_garbage_collection`)

Demonstrates how to configure default and per-item Time-to-Live (TTL) retention policies. Shows what happens when an item's deadline expires, making it eligible for BlobCAS compaction and sweep events.

```bash
go run examples/03_garbage_collection/main.go
```
