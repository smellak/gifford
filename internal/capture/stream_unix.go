//go:build unix

package capture

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// adoptStream takes ownership of In: it duplicates the descriptor with
// CLOEXEC inside the syscall connection, closes the original at once,
// sets the duplicate non-blocking, and wraps it exactly once. The
// duplicate is the pipe's only holder: closing it releases the pipe.
func adoptStream(in *os.File) (*os.File, error) {
	conn, err := in.SyscallConn()
	if err != nil {
		return nil, fmt.Errorf("%w: pane stream: %w", ErrUnsafePath, err)
	}
	dup := -1
	ctlErr := conn.Control(func(fd uintptr) {
		var err error
		dup, err = unix.FcntlInt(fd, unix.F_DUPFD_CLOEXEC, 0)
		if err != nil {
			return
		}
		if err := unix.SetNonblock(dup, true); err != nil {
			_ = unix.Close(dup)
			dup = -1
		}
	})
	_ = in.Close()
	if ctlErr != nil {
		return nil, fmt.Errorf("%w: pane stream: %w", ErrUnsafePath, ctlErr)
	}
	if dup < 0 {
		return nil, fmt.Errorf("%w: pane stream dup", ErrUnsafePath)
	}
	return os.NewFile(uintptr(dup), "pane-stream"), nil
}

// pollReadable waits up to timeout for data or hangup on a
// non-blocking descriptor.
func pollReadable(fd uintptr, timeoutMs int) (bool, error) {
	//nolint:gosec // G115: a process descriptor always fits int32.
	fds := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN | unix.POLLHUP | unix.POLLERR}}
	n, err := unix.Poll(fds, timeoutMs)
	if err != nil {
		return false, fmt.Errorf("pane poll: %w", err)
	}
	return n > 0, nil
}

// pollInterrupted reports an interrupted poll that deserves a retry.
func pollInterrupted(err error) bool {
	return errors.Is(err, unix.EINTR)
}

// readReady reads one ready chunk: EAGAIN means retry, n == 0 means the
// writer closed the pipe.
func readReady(fd int, buf []byte) (int, bool, error) {
	n, err := unix.Read(fd, buf)
	if err != nil {
		if errors.Is(err, unix.EAGAIN) {
			return 0, true, nil
		}
		return 0, false, fmt.Errorf("pane read: %w", err)
	}
	return n, false, nil
}
