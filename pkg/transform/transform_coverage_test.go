package transform

import (
	"testing"
)

func TestTransformZstd_Name(t *testing.T) {
	tr := NewZstd(1)
	if tr.Name() != "zstd" {
		t.Errorf("expected 'zstd', got %q", tr.Name())
	}
}

func TestTransformNone_Name(t *testing.T) {
	tr := NewNone()
	if tr.Name() != "none" {
		t.Errorf("expected 'none', got %q", tr.Name())
	}
}

func TestTransformZstd_DecodeNonCompressedFlag(t *testing.T) {
	// Create an envelope with compressed flag = 0 (not compressed)
	// Magic + Version + Flags(0) + Alg(0) + payload
	envelope := []byte{'B', 'C', 'A', 'S', Version, 0x00, 0x00}
	envelope = append(envelope, []byte("raw payload")...)

	tr := NewZstd(3)
	decoded, err := tr.Decode(envelope)
	if err != nil {
		t.Fatalf("Decode uncompressed failed: %v", err)
	}

	if string(decoded) != "raw payload" {
		t.Errorf("expected 'raw payload', got %q", decoded)
	}
}

func TestTransformZstd_DecodeTooSmall(t *testing.T) {
	tr := NewZstd(3)
	_, err := tr.Decode([]byte{1, 2, 3})
	if err == nil {
		t.Error("expected error for block too small")
	}
}

func TestTransformZstd_DecodeEmptyPayload(t *testing.T) {
	tr := NewZstd(3)

	// Encode empty data
	encoded, err := tr.Encode(nil)
	if err != nil {
		t.Fatalf("Encode nil failed: %v", err)
	}

	decoded, err := tr.Decode(encoded)
	if err != nil {
		t.Fatalf("Decode empty failed: %v", err)
	}

	if len(decoded) != 0 {
		t.Errorf("expected empty output, got %d bytes", len(decoded))
	}
}

func TestTransformZstd_EncodeDecode_MultipleLevels(t *testing.T) {
	levels := []int{1, 3}
	data := []byte("test data for multiple compression levels -- repeating content repeating content repeating content repeating content")

	for _, level := range levels {
		tr := NewZstd(level)
		encoded, err := tr.Encode(data)
		if err != nil {
			t.Fatalf("level %d: Encode failed: %v", level, err)
		}

		decoded, err := tr.Decode(encoded)
		if err != nil {
			t.Fatalf("level %d: Decode failed: %v", level, err)
		}

		if string(decoded) != string(data) {
			t.Errorf("level %d: data mismatch", level)
		}
	}
}
