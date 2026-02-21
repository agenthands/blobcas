package core

import (
	"errors"
)

var (
	ErrNotFound     = errors.New("blobcas: not found")
	ErrInvalidInput = errors.New("blobcas: invalid input")
	ErrCorrupt      = errors.New("blobcas: corrupt data")
	ErrTooLarge     = errors.New("blobcas: too large")
	ErrClosed       = errors.New("blobcas: store closed")
)
