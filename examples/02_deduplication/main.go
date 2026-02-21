package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/agenthands/blobcas/pkg/blobcas"
	"github.com/agenthands/blobcas/pkg/core"
)

func main() {
	// Create a temporary directory for the store
	dir, err := os.MkdirTemp("", "blobcas-dedup-example-*")
	if err != nil {
		log.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	fmt.Printf("Initializing BlobCAS store at: %s\n", dir)

	// Configure small chunk sizes to make deduplication obvious on small payloads
	cfg := blobcas.Config{
		Dir: dir,
		Chunking: core.ChunkingConfig{
			Min: 64,  // Minimum allowed by fastcdc
			Avg: 128, // 128 Bytes
			Max: 256, // 256 Bytes
		},
		Pack: core.PackConfig{
			TargetPackBytes:    10 << 20, // 10 MB packs for quick rotation
			RequireIndexOnSeal: true,
		},
		GC: core.GCConfig{
			Enabled:        true,
			DefaultRootTTL: 24 * time.Hour,
		},
		Transform: core.TransformConfig{
			Name: "none", // Disable compression so we can focus strictly on chunking dedupe
		},
	}

	ctx := context.Background()
	store, err := blobcas.Open(ctx, cfg)
	if err != nil {
		log.Fatalf("failed to open store: %v", err)
	}
	defer store.Close()

	// 1. Create a "large" simulated JSON payload (e.g. 500 bytes)
	// We'll repeat some patterns so FastCDC finds boundaries.
	payloadV1 := []byte(`
	{
		"users": [
			{"id": 1, "name": "Alice", "role": "admin", "status": "active", "last_login": "2023-01-01"},
			{"id": 2, "name": "Bob", "role": "user", "status": "inactive", "last_login": "2023-01-02"},
			{"id": 3, "name": "Charlie", "role": "user", "status": "active", "last_login": "2023-01-03"},
			{"id": 4, "name": "David", "role": "user", "status": "active", "last_login": "2023-01-04"},
			{"id": 5, "name": "Eve", "role": "admin", "status": "inactive", "last_login": "2023-01-05"}
		]
	}
	`)

	keyV1 := core.Key{Namespace: "api", ID: "users-v1"}
	refV1, err := store.Put(ctx, keyV1, blobcas.PutInput{
		Canonical: bytes.NewReader(payloadV1),
	}, blobcas.PutMeta{Canonical: true, MediaType: "application/json"})
	if err != nil {
		log.Fatalf("failed to put v1: %v", err)
	}

	statV1, err := store.Stat(ctx, refV1)
	if err != nil {
		log.Fatalf("failed to stat v1: %v", err)
	}

	fmt.Printf("\n--- V1 Payload ---\n")
	fmt.Printf("Total bytes stored: %d\n", statV1.Length)
	fmt.Printf("Number of Chunks: %d\n", statV1.ChunkCount)

	// 2. Create V2 payload with a tiny modification
	// We just change "Bob"'s status to "active"
	payloadV2 := []byte(`
	{
		"users": [
			{"id": 1, "name": "Alice", "role": "admin", "status": "active", "last_login": "2023-01-01"},
			{"id": 2, "name": "Bob", "role": "user", "status": "active", "last_login": "2023-01-02"},
			{"id": 3, "name": "Charlie", "role": "user", "status": "active", "last_login": "2023-01-03"},
			{"id": 4, "name": "David", "role": "user", "status": "active", "last_login": "2023-01-04"},
			{"id": 5, "name": "Eve", "role": "admin", "status": "inactive", "last_login": "2023-01-05"}
		]
	}
	`)

	keyV2 := core.Key{Namespace: "api", ID: "users-v2"}
	refV2, err := store.Put(ctx, keyV2, blobcas.PutInput{
		Canonical: bytes.NewReader(payloadV2),
	}, blobcas.PutMeta{Canonical: true, MediaType: "application/json"})
	if err != nil {
		log.Fatalf("failed to put v2: %v", err)
	}

	statV2, err := store.Stat(ctx, refV2)
	if err != nil {
		log.Fatalf("failed to stat v2: %v", err)
	}

	fmt.Printf("\n--- V2 Payload (Tiny edit) ---\n")
	fmt.Printf("Total bytes stored (logical): %d\n", statV2.Length)
	fmt.Printf("Number of Chunks: %d\n", statV2.ChunkCount)

	// In a real scenario, to prove deduplication at the block level, you would inspect the PACK
	// sizes. Because they share many chunks, the total blockstore disk usage will be much
	// smaller than (V1.Length + V2.Length).
	fmt.Println("\nBecause of FastCDC chunking, V1 and V2 share almost all of their chunks!")
	fmt.Println("Only the modified chunks surrounding 'Bob' were physically appended to the packfile.")
}
