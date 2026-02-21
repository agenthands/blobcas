package cidutil

import (
	"testing"

	"github.com/agenthands/blobcas/pkg/core"
)

func FuzzCIDVerify(f *testing.F) {
	builder := NewBuilder()

	// Add seed corpora
	validData := []byte("hello world payload")
	validCID, _ := builder.ChunkCID(validData)

	f.Add(validCID.Bytes, validData)

	// Corrupted CID
	corruptCID := append([]byte(nil), validCID.Bytes...)
	corruptCID[3] ^= 0x01
	f.Add(corruptCID, validData)

	// Corrupted Payload
	corruptData := append([]byte(nil), validData...)
	corruptData[0] ^= 0xFF
	f.Add(validCID.Bytes, corruptData)

	// Garbage CID
	f.Add([]byte("not a cid at all"), validData)
	f.Add([]byte{}, validData)

	f.Fuzz(func(t *testing.T, cidBytes []byte, payload []byte) {
		// Verify should not panic regardless of input
		_ = builder.Verify(core.CID{Bytes: cidBytes}, payload)
	})
}
