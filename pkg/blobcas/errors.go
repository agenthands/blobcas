package blobcas

import (
	"github.com/agenthands/blobcas/pkg/core"
)

var (
	ErrNotFound     = core.ErrNotFound
	ErrInvalidInput = core.ErrInvalidInput
	ErrCorrupt      = core.ErrCorrupt
	ErrTooLarge     = core.ErrTooLarge
	ErrClosed       = core.ErrClosed
)
