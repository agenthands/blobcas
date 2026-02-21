package transform

import (
	"testing"
)

func FuzzTransformDecode(f *testing.F) {
	tr := NewZstd(3)

	// Seed corpora
	f.Add([]byte{})
	f.Add([]byte("garbage input"))

	validPlaintext := []byte("highly compressible compressible data")
	validEncoded, _ := tr.Encode(validPlaintext)
	f.Add(validEncoded)

	// Subtly corrupted envelopes
	trunc := append([]byte(nil), validEncoded...)
	if len(trunc) > 5 {
		trunc = trunc[:len(trunc)-5]
	}
	f.Add(trunc)

	f.Fuzz(func(t *testing.T, data []byte) {
		// Should not panic on any byte slice
		_, _ = tr.Decode(data)
	})
}
