package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/agenthands/blobcas/pkg/blobcas"
	"github.com/agenthands/blobcas/pkg/core"
)

func main() {
	ctx := context.Background()

	cfg := blobcas.Config{
		Dir: "./repo",
		Chunking: core.ChunkingConfig{
			Min: 64,
			Avg: 128,
			Max: 256,
		},
		Pack: core.PackConfig{
			TargetPackBytes:    512 << 20, // configurable
			MaxPackBytes:       0,         // 0 => 2*TargetPackBytes
			RequireIndexOnSeal: true,
			SealFsync:          true,
			FsyncEveryBytes:    64 << 20, // optional periodic fsync
		},
		GC: core.GCConfig{
			Enabled:        true,
			DefaultRootTTL: 30 * 24 * time.Hour, // configurable default retention
		},
		Transform: core.TransformConfig{
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
