package transform

import (
	"bytes"
	"testing"

	"github.com/agenthands/blobcas/internal/testkit"
)

func TestTransformNone(t *testing.T) {
	tr := NewNone()

	if tr.Name() != "none" {
		t.Errorf("expected none, got %s", tr.Name())
	}

	data := []byte("hello world")
	encoded, err := tr.Encode(data)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	if !bytes.Equal(encoded, data) {
		t.Error("none transform should not change data")
	}

	decoded, err := tr.Decode(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if !bytes.Equal(decoded, data) {
		t.Error("none transform should not change data")
	}
}

func TestTransformZstd(t *testing.T) {
	tr := NewZstd(3)

	if tr.Name() != "zstd" {
		t.Errorf("expected zstd, got %s", tr.Name())
	}

	t.Run("Roundtrip", func(t *testing.T) {
		// Use highly compressible bytes to actually test compression
		r := testkit.RNG(1)
		data := testkit.CompressibleBytes(r, 1024*1024)

		encoded, err := tr.Encode(data)
		if err != nil {
			t.Fatalf("Encode failed: %v", err)
		}

		if len(encoded) >= len(data) {
			t.Errorf("expected zstd to compress data, %d >= %d", len(encoded), len(data))
		}

		decoded, err := tr.Decode(encoded)
		if err != nil {
			t.Fatalf("Decode failed: %v", err)
		}

		if !bytes.Equal(decoded, data) {
			t.Error("zstd transform corrupted data on roundtrip")
		}
	})

	t.Run("SmallBlock", func(t *testing.T) {
		data := []byte("tiny")

		encoded, err := tr.Encode(data)
		if err != nil {
			t.Fatalf("Encode failed: %v", err)
		}

		decoded, err := tr.Decode(encoded)
		if err != nil {
			t.Fatalf("Decode failed: %v", err)
		}

		if !bytes.Equal(decoded, data) {
			t.Error("zstd transform corrupted small data")
		}
	})

	t.Run("Corruption", func(t *testing.T) {
		data := []byte("hello world this is a test payload")
		encoded, _ := tr.Encode(data)

		// Truncated envelope
		_, err := tr.Decode(encoded[:6])
		if err == nil {
			t.Error("expected error for truncated envelope")
		}

		// Invalid magic
		corruptMagic := append([]byte(nil), encoded...)
		corruptMagic[0] = 'X'
		_, err = tr.Decode(corruptMagic)
		if err == nil {
			t.Error("expected error for invalid magic")
		}

		// Invalid version
		corruptVersion := append([]byte(nil), encoded...)
		corruptVersion[4] = 99
		_, err = tr.Decode(corruptVersion)
		if err == nil {
			t.Error("expected error for invalid version")
		}

		// Invalid alg
		corruptAlg := append([]byte(nil), encoded...)
		corruptAlg[6] = 99
		_, err = tr.Decode(corruptAlg)
		if err == nil {
			t.Error("expected error for invalid alg")
		}

		// Corrupt payload
		corruptPayload := append([]byte(nil), encoded...)
		corruptPayload[len(corruptPayload)-1] ^= 0x01
		_, err = tr.Decode(corruptPayload)
		if err == nil {
			t.Error("expected Decode to fail on corrupt zstd payload")
		}
	})
}
