package manifest

import (
	"testing"

	"github.com/agenthands/blobcas/pkg/core"
)

func FuzzManifestDecode(f *testing.F) {
	// Add some seed corpora (valid and invalid)
	codec := NewCodec(core.LimitsConfig{
		MaxChunksPerObject: 1000,
		MaxTags:            10,
		MaxTagKeyLen:       64,
		MaxTagValLen:       256,
		MaxMediaTypeLen:    128,
	})

	m := &ManifestV1{
		Version:   1,
		Canonical: true,
		MediaType: "text/plain",
		Length:    10,
		Chunks: []ChunkRef{
			{CID: core.CID{Bytes: []byte("chunkcid12345")}, Len: 10},
		},
	}
	encoded, _ := codec.Encode(m)
	f.Add(encoded)
	f.Add([]byte("garbage input"))
	f.Add([]byte{})
	f.Add([]byte{0xa1, 0x61, 0x76, 0x01}) // roughly {"v": 1}

	f.Fuzz(func(t *testing.T, data []byte) {
		// The goal of fuzzing here is primarily to parse untrusted input
		// without panicking.
		_, _ = codec.Decode(data)
	})
}
