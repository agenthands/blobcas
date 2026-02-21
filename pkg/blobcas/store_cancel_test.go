package blobcas_test

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/agenthands/blobcas/internal/testkit"
	"github.com/agenthands/blobcas/pkg/blobcas"
	"github.com/agenthands/blobcas/pkg/core"
)

func TestStoreIntegration_ContextCancellation(t *testing.T) {
	dir, err := os.MkdirTemp("", "blobcas-store-integration-cancel")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s, _ := createTestStore(t, dir)
	defer s.Close()

	ctx, cancel := context.WithCancel(context.Background())

	r := testkit.RNG(1)
	base := testkit.RandomBytes(r, 1024*1024)

	// Will block halfway through reading
	pr, unpause := testkit.NewPauseReader(bytes.NewReader(base))

	key := core.Key{Namespace: "test", ID: "cancel"}

	errCh := make(chan error, 1)
	go func() {
		_, err := s.Put(ctx, key, blobcas.PutInput{
			Canonical: pr,
		}, blobcas.PutMeta{Canonical: true})
		errCh <- err
	}()

	// Give it a moment to chunk some bytes
	time.Sleep(50 * time.Millisecond)
	cancel()
	unpause()

	err = <-errCh
	if err == nil {
		t.Fatal("expected error from cancelled Put")
	}

	if err != context.Canceled && !strings.Contains(err.Error(), "canceled") {
		t.Errorf("expected context canceled error, got: %v", err)
	}

	// Verify repo remains openable
	key2 := core.Key{Namespace: "test", ID: "ok"}
	_, err = s.Put(context.Background(), key2, blobcas.PutInput{
		Canonical: bytes.NewReader(base),
	}, blobcas.PutMeta{Canonical: true})

	if err != nil {
		t.Errorf("repo should accept new puts after a cancellation, got: %v", err)
	}
}
