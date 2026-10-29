package proxy

import (
	"context"
	"errors"
	"io"
	"os"
	"strconv"
	"syscall"
)

// Proxy sentinels.
var (
	// ErrNoCommand means Options.Command is empty.
	ErrNoCommand = errors.New("no command to run")
	// ErrNotTerminal means Options.In or Options.Out is not a terminal.
	ErrNotTerminal = errors.New("not a terminal")
)

var (
	// errCanceled ends a wait when the session is torn down.
	errCanceled = errors.New("proxy: canceled")
	// errNothingPending reports a descriptor that select marked ready but
	// has no data right now.
	errNothingPending = errors.New("proxy: nothing pending")
	// errFDTooLarge reports a descriptor unix.Select cannot watch.
	errFDTooLarge = errors.New("proxy: file descriptor too large for select")
)

// DaemonNotice is the line Run writes to Options.Notice, once, before the
// harness starts, when the daemon socket accepts no connection.
const DaemonNotice = "gifford: the daemon is not running; this session runs without Gifford"

// Options configures one Run.
type Options struct {
	// Command is the harness and its arguments; Command[0] is looked up
	// like os/exec does.
	Command []string
	// In is the outer terminal input, put in raw mode; it must be a terminal.
	In *os.File
	// Out is the outer terminal output, normally the same terminal; it must
	// be a terminal.
	Out *os.File
	// Notice receives DaemonNotice; nil discards it.
	Notice io.Writer
	// DaemonSocket is the Unix socket probed once before the start;
	// "" counts as not running.
	DaemonSocket string
	// Signals carries the handled signals; nil makes Run subscribe to them
	// for its lifetime.
	Signals <-chan os.Signal
}

// Exit says how the harness ended.
type Exit struct {
	// Code is the exit status 0-255; -1 when a signal killed the harness.
	Code int
	// Signal is the signal that killed the harness; 0 when it exited.
	Signal syscall.Signal
}

// Status returns what a shell would report: Code, or 128 plus the signal
// number when a signal killed the harness.
func (e Exit) Status() int {
	if e.Signal != 0 {
		return 128 + int(e.Signal)
	}
	return e.Code
}

// String describes the exit: "harness exited with status 7" or
// "harness killed by signal 15 (terminated), status 143".
func (e Exit) String() string {
	if e.Signal != 0 {
		return "harness killed by signal " + strconv.Itoa(int(e.Signal)) +
			" (" + e.Signal.String() + "), status " + strconv.Itoa(e.Status())
	}
	return "harness exited with status " + strconv.Itoa(e.Code)
}

// Run starts opts.Command on a child PTY and copies bytes untouched between
// it and the outer terminal until the harness exits. Without PTY support it
// returns an error wrapping errors.ErrUnsupported.
func Run(ctx context.Context, opts Options) (Exit, error) {
	return run(ctx, opts)
}
