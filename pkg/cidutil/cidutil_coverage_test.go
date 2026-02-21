package cidutil

import (
	"strings"
	"testing"

	"github.com/agenthands/blobcas/pkg/core"
)

func TestVerify_UnsupportedMultihash(t *testing.T) {
	b := NewBuilder()

	// Create a dummy CID with an unsupported multihash code (e.g., 0xffff)
	// We'll construct the raw bytes manually to bypass go-cid builder checks

	// CIDv1 = 0x01, Codec = DagCBOR (0x71), HashType = 0xffff, HashLen = 4, Hash = dummy

	// Build raw CID bytes:
	// Multibase (not used here, core.CID holds raw bytes)
	// Version: 1
	// Codec: 0x55 (Raw)
	// Multihash format:
	// - Type: 0xffff (encoded as varint: 0xff, 0xff, 0x03)
	// - Len: 4
	// - Digest: "abcd"

	// 0x01 (version), 0x55 (raw), 0xff, 0xff, 0x03 (0xffff), 0x04 (len), 'a', 'b', 'c', 'd'
	rawCidBytes := []byte{0x01, 0x55, 0xff, 0xff, 0x03, 0x04, 'a', 'b', 'c', 'd'}
	c := core.CID{Bytes: rawCidBytes}

	err := b.Verify(c, []byte("test"))
	if err == nil {
		t.Fatal("expected error for unsupported multihash, got nil")
	}

	if !strings.Contains(err.Error(), "failed to compute multihash for verification") {
		t.Errorf("unexpected error message: %v", err)
	}
}
