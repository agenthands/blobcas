package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/agenthands/blobcas/pkg/blobcas"
	"github.com/agenthands/blobcas/pkg/core"
)

func main() {
	// 1. Configuration
	// We'll create a temporary directory for the store for this example.
	// In a real application, you would use a persistent path.
	dir, err := os.MkdirTemp("", "blobcas-basic-example-*")
	if err != nil {
		log.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir) // Clean up after the example runs

	fmt.Printf("Initializing BlobCAS store at: %s\n", dir)

	cfg := blobcas.Config{
		Dir: dir,
		Chunking: core.ChunkingConfig{
			Min: 64,   // 64 Bytes min
			Avg: 1024, // ~1 KB targeting
			Max: 4096, // 4 KB max
		},
		Pack: core.PackConfig{
			TargetPackBytes:    512 << 20, // 512 MB
			RequireIndexOnSeal: true,
		},
		GC: core.GCConfig{
			Enabled:        true,
			DefaultRootTTL: 30 * 24 * time.Hour, // Keep data for 30 days by default
		},
		Transform: core.TransformConfig{
			Name:      "zstd", // Compress blocks using Zstandard
			ZstdLevel: 3,      // Default fast compression level
		},
	}

	// 2. Open the Store
	ctx := context.Background()
	store, err := blobcas.Open(ctx, cfg)
	if err != nil {
		log.Fatalf("failed to open store: %v", err)
	}
	defer store.Close()
	fmt.Println("Store opened successfully.")

	// 3. Put a Blob
	// Let's define a unique key for our content.
	key := core.Key{
		Namespace: "examples",
		ID:        "hello-world-001",
	}

	content := []byte(`{"message": "Hello, BlobCAS World!"}`)
	fmt.Printf("\nPutting content: %s\n", content)

	// Since we are storing text/json, we provide it as the canonical reader.
	// We also optionally provide metadata like the MediaType.
	ref, err := store.Put(ctx, key, blobcas.PutInput{
		Canonical: bytes.NewReader(content),
	}, blobcas.PutMeta{
		Canonical: true,
		MediaType: "application/json",
	})
	if err != nil {
		log.Fatalf("failed to put content: %v", err)
	}

	fmt.Printf("Content stored successfully! Manifest CID: %s\n", ref.ManifestCID)

	// 4. Resolve the Key (Optional, to get the CID if you only have the Key)
	resolvedRef, err := store.Resolve(ctx, key)
	if err != nil {
		log.Fatalf("failed to resolve key: %v", err)
	}
	fmt.Printf("Resolved Key to Manifest CID: %s\n", resolvedRef.ManifestCID)

	// 5. Get the Blob
	rc, info, err := store.Get(ctx, resolvedRef)
	if err != nil {
		log.Fatalf("failed to get content: %v", err)
	}
	defer rc.Close()

	// Read the content back
	retrievedData, err := io.ReadAll(rc)
	if err != nil {
		log.Fatalf("failed to read retrieved content: %v", err)
	}

	fmt.Printf("\n--- Retrieved Information ---\n")
	fmt.Printf("Length: %d bytes\n", info.Length)
	fmt.Printf("Media Type: %s\n", info.MediaType)
	fmt.Printf("Content: %s\n", retrievedData)
}
