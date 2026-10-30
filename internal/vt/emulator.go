package vt

import (
	"errors"
	"fmt"
	"io"

	xvt "github.com/charmbracelet/x/vt"
)

// Size limits. Each side has the protocol's pane bounds; the cell limit
// bounds memory.
const (
	MaxCols  = 10_000
	MaxRows  = 10_000
	MaxCells = 250_000
)

var (
	// ErrInvalidSize reports a terminal size outside the package bounds.
	ErrInvalidSize = errors.New("vt: invalid size")
	// ErrInvalidPattern reports an invalid input-line expression.
	ErrInvalidPattern = errors.New("vt: invalid input pattern")
	// ErrClosed reports an operation on a closed emulator.
	ErrClosed = errors.New("vt: emulator closed")
)

// Emulator reconstructs one terminal screen from an output stream. It is
// not safe for concurrent use: its owner serialises the calls.
type Emulator struct {
	term             *xvt.Emulator
	pending          []byte
	state            parseState
	csi              csiTracker
	leftRightMargins bool
	syncOpen         bool
	replyPipe        io.Closer
	replyDone        chan struct{}
	closed           bool
}

// New returns an emulator of cols x rows within the package's memory bounds.
func New(cols, rows int) (*Emulator, error) {
	if err := validSize(cols, rows); err != nil {
		return nil, err
	}
	term := xvt.NewEmulator(cols, rows)
	term.SetScrollbackSize(1)
	e := &Emulator{
		term:      term,
		replyDone: make(chan struct{}),
	}
	if closer, ok := term.InputPipe().(io.Closer); ok {
		e.replyPipe = closer
	}
	go e.discardReplies()
	return e, nil
}

func validSize(cols, rows int) error {
	if cols < 1 || rows < 1 || cols > MaxCols || rows > MaxRows || cols > MaxCells/rows {
		return fmt.Errorf("%w: %dx%d", ErrInvalidSize, cols, rows)
	}
	return nil
}

func (e *Emulator) discardReplies() {
	defer close(e.replyDone)
	buf := make([]byte, 4096)
	for {
		if _, err := e.term.Read(buf); err != nil {
			return
		}
	}
}

// Write feeds output bytes. It always accepts all bytes unless Close ran.
func (e *Emulator) Write(p []byte) (int, error) {
	if e.closed {
		return 0, ErrClosed
	}
	if len(p) == 0 {
		return 0, nil
	}
	data := make([]byte, 0, len(e.pending)+len(p))
	data = append(data, e.pending...)
	data = append(data, p...)
	data = normaliseUTF8(data, e.state, true)
	e.pending = e.pending[:0]

	end, state := holdStart(data, e.state)
	e.feed(data[:end], e.state)
	e.state = state
	e.pending = append(e.pending, data[end:]...)
	return len(p), nil
}

// Flush feeds the bytes Write is holding back.
func (e *Emulator) Flush() error {
	if e.closed {
		return ErrClosed
	}
	if len(e.pending) == 0 {
		return nil
	}
	pending := normaliseUTF8(e.pending, e.state, false)
	state := e.state
	for _, b := range pending {
		state, _, _ = advance(state, b)
	}
	e.feed(pending, e.state)
	e.state = state
	e.pending = e.pending[:0]
	return nil
}

// Resize changes the size after flushing the stream boundary.
func (e *Emulator) Resize(cols, rows int) error {
	if e.closed {
		return ErrClosed
	}
	if err := validSize(cols, rows); err != nil {
		return err
	}
	if err := e.Flush(); err != nil {
		return err
	}
	e.term.Resize(cols, rows)
	return nil
}

// Close releases the emulator. It is idempotent and waits for its reply
// drain before closing x/vt, which avoids x/vt v0.1.0's close/read race.
func (e *Emulator) Close() error {
	if e.closed {
		return nil
	}
	e.closed = true
	if e.replyPipe != nil {
		_ = e.replyPipe.Close()
	}
	<-e.replyDone
	_ = e.term.Close()
	return nil
}

// writeTerm contains x/vt parser failures: arbitrary terminal bytes must not
// panic the caller. A malformed sequence may be ignored after a parser panic.
func (e *Emulator) writeTerm(p []byte) {
	if len(p) == 0 {
		return
	}
	defer func() { _ = recover() }()
	_, _ = e.term.Write(p)
}
