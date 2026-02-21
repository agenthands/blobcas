package testkit

import (
	"errors"
	"io"
)

var ErrInjectedFault = errors.New("injected fault")

// ErrorReader wraps an io.Reader and returns an error after returning N bytes.
type ErrorReader struct {
	r     io.Reader
	limit int64
	read  int64
	err   error
}

// NewErrorReader returns a reader that will inject the given error after reading 'limit' bytes.
// If err is nil, ErrInjectedFault is used.
func NewErrorReader(r io.Reader, limit int64, err error) *ErrorReader {
	if err == nil {
		err = ErrInjectedFault
	}
	return &ErrorReader{
		r:     r,
		limit: limit,
		err:   err,
	}
}

func (e *ErrorReader) Read(p []byte) (n int, err error) {
	if e.read >= e.limit {
		return 0, e.err
	}

	space := e.limit - e.read
	if int64(len(p)) > space {
		p = p[:space]
	}

	n, err = e.r.Read(p)
	e.read += int64(n)

	if err != nil {
		return n, err
	}

	if e.read >= e.limit {
		return n, e.err
	}

	return n, nil
}

// PauseReader wraps an io.Reader and blocks the Read call until the Unpause channel is closed.
type PauseReader struct {
	r       io.Reader
	unpause chan struct{}
}

// NewPauseReader returns a reader that stalls.
func NewPauseReader(r io.Reader) (*PauseReader, func()) {
	ch := make(chan struct{})
	return &PauseReader{
		r:       r,
		unpause: ch,
	}, func() { close(ch) }
}

func (p *PauseReader) Read(b []byte) (n int, err error) {
	<-p.unpause
	return p.r.Read(b)
}

// BlockingReader stalls exactly once on the first read.
type BlockingReader struct {
	r        io.Reader
	BlockCh  chan struct{}
	ResumeCh chan struct{}
	blocked  bool
}

func NewBlockingReader(r io.Reader) *BlockingReader {
	return &BlockingReader{
		r:        r,
		BlockCh:  make(chan struct{}),
		ResumeCh: make(chan struct{}),
	}
}

func (b *BlockingReader) Read(p []byte) (n int, err error) {
	if !b.blocked {
		b.blocked = true
		close(b.BlockCh)
		<-b.ResumeCh
	}
	return b.r.Read(p)
}
