package cidutil

import (
	"bytes"
	"testing"

	"github.com/agenthands/blobcas/internal/testkit"
	"github.com/agenthands/blobcas/pkg/core"
)

func TestCIDBuilder(t *testing.T) {
	builder := NewBuilder()

	t.Run("ChunkCID", func(t *testing.T) {
		data := []byte("hello world")
		cid, err := builder.ChunkCID(data)
		if err != nil {
			t.Fatalf("ChunkCID failed: %v", err)
		}

		if len(cid.Bytes) == 0 {
			t.Fatal("expected non-empty CID bytes")
		}

		err = builder.Verify(cid, data)
		if err != nil {
			t.Errorf("Verify failed for correct data: %v", err)
		}

		err = builder.Verify(cid, []byte("wrong data"))
		if err == nil {
			t.Error("Verify should have failed for wrong data")
		}
	})

	t.Run("ManifestCID", func(t *testing.T) {
		data := []byte{0xa1, 0x61, 0x61, 0x01} // minimal dag-cbor: {"a": 1}
		cid, err := builder.ManifestCID(data)
		if err != nil {
			t.Fatalf("ManifestCID failed: %v", err)
		}

		if len(cid.Bytes) == 0 {
			t.Fatal("expected non-empty CID bytes")
		}

		err = builder.Verify(cid, data)
		if err != nil {
			t.Errorf("Verify failed for correct data: %v", err)
		}
	})

	t.Run("Determinism", func(t *testing.T) {
		data := []byte("deterministic content")
		cid1, _ := builder.ChunkCID(data)
		cid2, _ := builder.ChunkCID(data)

		if !bytes.Equal(cid1.Bytes, cid2.Bytes) {
			t.Error("CIDs for same content should be identical")
		}
	})

	t.Run("BitFlipCorruption", func(t *testing.T) {
		data := []byte("original payload")
		cid, _ := builder.ChunkCID(data)

		// flip one bit in payload
		corruptedData := append([]byte(nil), data...)
		corruptedData[3] ^= 0x01

		err := builder.Verify(cid, corruptedData)
		if err == nil {
			t.Error("expected Verify to fail with bit-flipped payload")
		}
	})

	t.Run("MalformedCIDBytes", func(t *testing.T) {
		data := []byte("payload")

		err := builder.Verify(core.CID{Bytes: []byte{0x00, 0x01}}, data)
		if err == nil {
			t.Error("expected Verify to fail on truncated/malformed CID bytes")
		}

		err = builder.Verify(core.CID{Bytes: nil}, data)
		if err == nil {
			t.Error("expected Verify to fail on nil CID bytes")
		}
	})

	t.Run("LargePayload", func(t *testing.T) {
		r := testkit.RNG(42)
		data := testkit.RandomBytes(r, 4*1024*1024) // 4 MiB
		cid, err := builder.ChunkCID(data)
		if err != nil {
			t.Fatal(err)
		}
		if err := builder.Verify(cid, data); err != nil {
			t.Fatal(err)
		}
	})
}
