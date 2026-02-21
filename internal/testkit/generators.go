package testkit

import (
	"math/rand"
	"time"
)

// RNG provides a deterministic random number generator.
// If seed is 0, it uses the current time.
func RNG(seed int64) *rand.Rand {
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	return rand.New(rand.NewSource(seed))
}

// RandomBytes generates a slice of random bytes of the given length.
func RandomBytes(r *rand.Rand, length int) []byte {
	b := make([]byte, length)
	// Read is deprecated in newer Go, but for testkit math/rand.Read or loop is fine.
	for i := range b {
		b[i] = byte(r.Intn(256))
	}
	return b
}

// CompressibleBytes generates a slice of highly compressible bytes of the given length.
func CompressibleBytes(r *rand.Rand, length int) []byte {
	b := make([]byte, length)
	pattern := []byte("highly compressible repeating pattern ")
	pLen := len(pattern)
	for i := 0; i < length; i++ {
		b[i] = pattern[i%pLen]
	}

	// Sprinkle a tiny bit of randomness to avoid being 100% uniform if desired
	for i := 0; i < length/1024; i++ {
		b[r.Intn(length)] = byte(r.Intn(256))
	}

	return b
}

// MutateBytes takes a base slice and returns a new slice with random insertions, deletions, or modifications.
// This is useful for creating near-duplicate sequences to test content-defined chunking.
func MutateBytes(r *rand.Rand, base []byte, mutations int) []byte {
	out := make([]byte, len(base))
	copy(out, base)

	for i := 0; i < mutations; i++ {
		op := r.Intn(3)
		offset := r.Intn(len(out))

		switch op {
		case 0: // Insert
			val := byte(r.Intn(256))
			out = append(out[:offset], append([]byte{val}, out[offset:]...)...)
		case 1: // Delete
			if len(out) > 0 {
				out = append(out[:offset], out[offset+1:]...)
			}
		case 2: // Modify
			if len(out) > 0 {
				out[offset] = byte(r.Intn(256))
			}
		}
	}
	return out
}
