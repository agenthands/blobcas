package transform

import (
	"fmt"

	"github.com/agenthands/blobcas/pkg/core"
	"github.com/klauspost/compress/zstd"
)

const (
	Magic   = "BCAS"
	Version = 1
)

const (
	FlagCompressed = 1 << 0
	FlagEncrypted  = 1 << 1
)

const (
	AlgZstd = 1
)

// Transform defines the interface for encoding/decoding block payloads.
type Transform interface {
	Name() string
	Encode(plain []byte) ([]byte, error)
	Decode(stored []byte) ([]byte, error)
}

// None transform doesn't apply any transformation.
type noneTransform struct{}

func NewNone() Transform {
	return &noneTransform{}
}

func (t *noneTransform) Name() string                         { return "none" }
func (t *noneTransform) Encode(plain []byte) ([]byte, error)  { return plain, nil }
func (t *noneTransform) Decode(stored []byte) ([]byte, error) { return stored, nil }

// Zstd transform applies zstd compression.
type zstdTransform struct {
	encoder *zstd.Encoder
	decoder *zstd.Decoder
}

func NewZstd(level int) Transform {
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.EncoderLevel(level)))
	if err != nil {
		panic(fmt.Sprintf("failed to create zstd writer: %v", err))
	}
	dec, err := zstd.NewReader(nil)
	if err != nil {
		panic(fmt.Sprintf("failed to create zstd reader: %v", err))
	}
	return &zstdTransform{
		encoder: enc,
		decoder: dec,
	}
}

func (t *zstdTransform) Name() string { return "zstd" }

func (t *zstdTransform) Encode(plain []byte) ([]byte, error) {
	compressed := t.encoder.EncodeAll(plain, nil)

	// Wrap in envelope: direct append avoids bytes.Buffer heap allocation
	envelope := make([]byte, 0, 7+len(compressed))
	envelope = append(envelope, Magic...)
	envelope = append(envelope, Version, FlagCompressed, AlgZstd)
	envelope = append(envelope, compressed...)

	return envelope, nil
}

func (t *zstdTransform) Decode(stored []byte) ([]byte, error) {
	if len(stored) < 7 {
		return nil, fmt.Errorf("%w: block too small for envelope", core.ErrCorrupt)
	}

	if string(stored[:4]) != Magic {
		return nil, fmt.Errorf("%w: invalid magic", core.ErrCorrupt)
	}

	if stored[4] != Version {
		return nil, fmt.Errorf("%w: unsupported version %d", core.ErrCorrupt, stored[4])
	}

	flags := stored[5]
	alg := stored[6]
	payload := stored[7:]

	if flags&FlagCompressed != 0 {
		if alg != AlgZstd {
			return nil, fmt.Errorf("%w: unsupported compression algorithm %d", core.ErrCorrupt, alg)
		}
		return t.decoder.DecodeAll(payload, nil)
	}

	return payload, nil
}
