package manifest

import (
	"strings"
	"testing"

	"github.com/agenthands/blobcas/pkg/core"
)

func TestManifestValidation_AllBranches(t *testing.T) {
	codec := NewCodec(core.LimitsConfig{
		MaxChunksPerObject: 100,
		MaxTags:            2,
		MaxTagKeyLen:       5,
		MaxTagValLen:       10,
		MaxMediaTypeLen:    20,
	})

	t.Run("UnsupportedVersion", func(t *testing.T) {
		m := &ManifestV1{Version: 2}
		_, err := codec.Encode(m)
		if err == nil {
			t.Error("expected error for unsupported version")
		}
	})

	t.Run("TooManyTags", func(t *testing.T) {
		m := &ManifestV1{
			Version: 1,
			Length:  100,
			Chunks: []ChunkRef{
				{CID: core.CID{Bytes: []byte("c1")}, Len: 100},
			},
			Tags: map[string]string{
				"a": "1",
				"b": "2",
				"c": "3",
			},
		}
		_, err := codec.Encode(m)
		if err == nil {
			t.Error("expected error for too many tags")
		}
	})

	t.Run("TagKeyTooLong", func(t *testing.T) {
		m := &ManifestV1{
			Version: 1,
			Length:  100,
			Chunks: []ChunkRef{
				{CID: core.CID{Bytes: []byte("c1")}, Len: 100},
			},
			Tags: map[string]string{
				"toolong": "ok",
			},
		}
		_, err := codec.Encode(m)
		if err == nil {
			t.Error("expected error for tag key too long")
		}
	})

	t.Run("TagValueTooLong", func(t *testing.T) {
		m := &ManifestV1{
			Version: 1,
			Length:  100,
			Chunks: []ChunkRef{
				{CID: core.CID{Bytes: []byte("c1")}, Len: 100},
			},
			Tags: map[string]string{
				"ok": "this-value-is-way-too-long",
			},
		}
		_, err := codec.Encode(m)
		if err == nil {
			t.Error("expected error for tag value too long")
		}
	})

	t.Run("MediaTypeTooLong", func(t *testing.T) {
		m := &ManifestV1{
			Version:   1,
			Length:    100,
			MediaType: strings.Repeat("x", 25),
			Chunks: []ChunkRef{
				{CID: core.CID{Bytes: []byte("c1")}, Len: 100},
			},
		}
		_, err := codec.Encode(m)
		if err == nil {
			t.Error("expected error for media type too long")
		}
	})

	t.Run("EmptyCIDInChunk", func(t *testing.T) {
		m := &ManifestV1{
			Version: 1,
			Length:  100,
			Chunks: []ChunkRef{
				{CID: core.CID{Bytes: nil}, Len: 100},
			},
		}
		_, err := codec.Encode(m)
		if err == nil {
			t.Error("expected error for empty CID in chunk")
		}
	})

	t.Run("LengthPositiveButNoChunks", func(t *testing.T) {
		m := &ManifestV1{
			Version: 1,
			Length:  100,
			Chunks:  []ChunkRef{},
		}
		_, err := codec.Encode(m)
		if err == nil {
			t.Error("expected error for length > 0 but no chunks")
		}
	})

	t.Run("LengthZeroWithChunks", func(t *testing.T) {
		m := &ManifestV1{
			Version: 1,
			Length:  0,
			Chunks: []ChunkRef{
				{CID: core.CID{Bytes: []byte("c1")}, Len: 0},
			},
		}
		_, err := codec.Encode(m)
		if err == nil {
			t.Error("expected error for length=0 but chunks present")
		}
	})

	t.Run("ValidEmptyManifest", func(t *testing.T) {
		m := &ManifestV1{
			Version: 1,
			Length:  0,
			Chunks:  nil,
		}
		_, err := codec.Encode(m)
		if err != nil {
			t.Errorf("expected empty manifest to encode, got: %v", err)
		}
	})

	t.Run("ValidWithTags", func(t *testing.T) {
		m := &ManifestV1{
			Version: 1,
			Length:  100,
			Chunks: []ChunkRef{
				{CID: core.CID{Bytes: []byte("c1")}, Len: 100},
			},
			Tags: map[string]string{
				"a": "1",
			},
		}
		encoded, err := codec.Encode(m)
		if err != nil {
			t.Fatalf("Encode failed: %v", err)
		}

		decoded, err := codec.Decode(encoded)
		if err != nil {
			t.Fatalf("Decode failed: %v", err)
		}
		if decoded.Tags["a"] != "1" {
			t.Error("tag round-trip failed")
		}
	})

	// Test Decode with limits-violating content (bypassing Encode validation)
	t.Run("DecodeWithBadVersion", func(t *testing.T) {
		// Already tested in the original test file, included for completeness
		_, err := codec.Decode([]byte{0xFF})
		if err == nil {
			t.Error("expected Decode to fail on garbage")
		}
	})
}
