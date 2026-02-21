package manifest

import (
	"testing"

	"github.com/agenthands/blobcas/pkg/core"
	"github.com/fxamacker/cbor/v2"
)

func TestManifestCodec(t *testing.T) {
	codec := NewCodec(core.LimitsConfig{
		MaxChunksPerObject: 1000,
		MaxTags:            10,
		MaxTagKeyLen:       64,
		MaxTagValLen:       256,
		MaxMediaTypeLen:    128,
	})

	t.Run("RoundTrip", func(t *testing.T) {
		m := &ManifestV1{
			Version:   1,
			Canonical: true,
			MediaType: "application/json",
			Length:    1234,
			Chunks: []ChunkRef{
				{CID: core.CID{Bytes: []byte("cid1")}, Len: 1000},
				{CID: core.CID{Bytes: []byte("cid2")}, Len: 234},
			},
			Tags: map[string]string{"foo": "bar"},
		}

		encoded, err := codec.Encode(m)
		if err != nil {
			t.Fatalf("Encode failed: %v", err)
		}

		decoded, err := codec.Decode(encoded)
		if err != nil {
			t.Fatalf("Decode failed: %v", err)
		}

		if decoded.Version != m.Version || decoded.Length != m.Length || decoded.MediaType != m.MediaType {
			t.Errorf("Decoded manifest doesn't match original")
		}

		if len(decoded.Chunks) != len(m.Chunks) {
			t.Errorf("Expected %d chunks, got %d", len(m.Chunks), len(decoded.Chunks))
		}
	})

	t.Run("Validation_LengthMismatch", func(t *testing.T) {
		m := &ManifestV1{
			Version: 1,
			Length:  1000,
			Chunks: []ChunkRef{
				{CID: core.CID{Bytes: []byte("cid1")}, Len: 500},
			},
		}
		_, err := codec.Encode(m)
		if err == nil {
			t.Error("expected error for length mismatch")
		}
	})

	t.Run("Validation_EmptyManifest", func(t *testing.T) {
		m := &ManifestV1{
			Version: 1,
			Length:  0,
			Chunks:  []ChunkRef{},
		}
		_, err := codec.Encode(m)
		if err != nil {
			t.Errorf("expected empty manifest to encode successfully, got: %v", err)
		}

		mWithChunks := &ManifestV1{
			Version: 1,
			Length:  0,
			Chunks: []ChunkRef{
				{CID: core.CID{Bytes: []byte("cid1")}, Len: 100},
			},
		}
		_, err = codec.Encode(mWithChunks)
		if err == nil {
			t.Error("expected error for empty length but chunks present")
		}
	})

	t.Run("Validation_Limits", func(t *testing.T) {
		m := &ManifestV1{
			Version: 1,
			Length:  10,
			Chunks:  make([]ChunkRef, 1001), // exceeds MaxChunksPerObject
		}
		for i := range m.Chunks {
			m.Chunks[i] = ChunkRef{CID: core.CID{Bytes: []byte("a")}, Len: 0}
			m.Length += 0 // sum is 0
		}
		_, err := codec.Encode(m)
		if err == nil {
			t.Error("expected error for exceeding MaxChunksPerObject")
		}
	})

	t.Run("DecodeStrict", func(t *testing.T) {
		// Test random garbage
		garbage := []byte{0xff, 0xff, 0xff, 0x00}
		_, err := codec.Decode(garbage)
		if err == nil {
			t.Error("expected Decode on garbage to fail")
		}

		// Test unsupported version
		mBadVersion := &ManifestV1{
			Version: 99,
		}

		// Use standard cbor to bypass the Codec strictly enforcing version during Encode
		em, _ := cbor.EncOptions{Sort: cbor.SortCanonical}.EncMode()
		b, _ := em.Marshal(mBadVersion)

		_, err = codec.Decode(b)
		if err == nil {
			t.Error("expected Decode to fail on unsupported version")
		}
	})
}
