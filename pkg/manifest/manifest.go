package manifest

import (
	"fmt"

	"github.com/agenthands/blobcas/pkg/core"
	"github.com/fxamacker/cbor/v2"
)

// ChunkRef references a chunk by its CID and its plaintext length.
type ChunkRef struct {
	CID core.CID `cbor:"cid"`
	Len uint32   `cbor:"len"`
}

// ManifestV1 represents the on-disk format for an object manifest.
type ManifestV1 struct {
	Version             uint16            `cbor:"version"`
	Canonical           bool              `cbor:"canonical"`
	MediaType           string            `cbor:"media_type,omitempty"`
	OrigContentEncoding string            `cbor:"orig_content_encoding,omitempty"`
	Length              uint64            `cbor:"length"`
	Chunks              []ChunkRef        `cbor:"chunks"`
	RawRef              *core.Ref         `cbor:"raw_ref,omitempty"`
	WholeSha256         []byte            `cbor:"whole_sha256,omitempty"`
	Tags                map[string]string `cbor:"tags,omitempty"`
}

// Codec defines the interface for manifest encoding/decoding and validation.
type Codec interface {
	Encode(m *ManifestV1) ([]byte, error)
	Decode(b []byte) (*ManifestV1, error)
}

type codec struct {
	limits  core.LimitsConfig
	encMode cbor.EncMode
}

// NewCodec returns a new Codec implementation.
func NewCodec(limits core.LimitsConfig) Codec {
	// Use canonical CBOR encoding (Core Deterministic Encoding Requirements)
	em, _ := cbor.CanonicalEncOptions().EncMode()
	return &codec{
		limits:  limits,
		encMode: em,
	}
}

func (c *codec) Encode(m *ManifestV1) ([]byte, error) {
	if err := c.validate(m); err != nil {
		return nil, fmt.Errorf("%w: %v", core.ErrInvalidInput, err)
	}

	return c.encMode.Marshal(m)
}

func (c *codec) Decode(b []byte) (*ManifestV1, error) {
	var m ManifestV1
	if err := cbor.Unmarshal(b, &m); err != nil {
		return nil, fmt.Errorf("%w: failed to unmarshal manifest: %v", core.ErrCorrupt, err)
	}

	if err := c.validate(&m); err != nil {
		return nil, fmt.Errorf("%w: %v", core.ErrCorrupt, err)
	}

	return &m, nil
}

func (c *codec) validate(m *ManifestV1) error {
	if m.Version != 1 {
		return fmt.Errorf("unsupported manifest version %d", m.Version)
	}

	// Validate chunk count and cumulative length
	if uint32(len(m.Chunks)) > c.limits.MaxChunksPerObject && c.limits.MaxChunksPerObject > 0 {
		return fmt.Errorf("too many chunks: %d > %d", len(m.Chunks), c.limits.MaxChunksPerObject)
	}

	var sumLength uint64
	for i, chunk := range m.Chunks {
		if len(chunk.CID.Bytes) == 0 {
			return fmt.Errorf("chunk %d has empty CID", i)
		}
		sumLength += uint64(chunk.Len)
	}

	if sumLength != m.Length {
		return fmt.Errorf("length mismatch: manifest says %d, chunks sum to %d", m.Length, sumLength)
	}

	if m.Length == 0 && len(m.Chunks) > 0 {
		return fmt.Errorf("length is 0 but chunks are present")
	}

	// Validate tags
	if len(m.Tags) > c.limits.MaxTags && c.limits.MaxTags > 0 {
		return fmt.Errorf("too many tags: %d > %d", len(m.Tags), c.limits.MaxTags)
	}

	for k, v := range m.Tags {
		if len(k) > c.limits.MaxTagKeyLen && c.limits.MaxTagKeyLen > 0 {
			return fmt.Errorf("tag key too long: %d > %d", len(k), c.limits.MaxTagKeyLen)
		}
		if len(v) > c.limits.MaxTagValLen && c.limits.MaxTagValLen > 0 {
			return fmt.Errorf("tag value too long: %d > %d", len(v), c.limits.MaxTagValLen)
		}
	}

	// Validate media type
	if len(m.MediaType) > c.limits.MaxMediaTypeLen && c.limits.MaxMediaTypeLen > 0 {
		return fmt.Errorf("media type too long: %d > %d", len(m.MediaType), c.limits.MaxMediaTypeLen)
	}

	return nil
}
