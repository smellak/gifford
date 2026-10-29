//go:build unix

package session

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// errInputCanceled reports a wait interrupted by the end of the session.
var errInputCanceled = errors.New("session: input wait canceled")

// inputWaiter waits for the blocking outer terminal to be readable or for
// the session to end. The outer terminal is never made non-blocking (its
// file description is shared with the parent shell) and takes no read
// deadline, so a cancel pipe is the only way to leave a pending read.
type inputWaiter struct {
	r, w *os.File // os.Pipe: close-on-exec
	pfd  int
	inFD int
}

func newInputWaiter(in *os.File) (*inputWaiter, error) {
	r, w, err := os.Pipe()
	if err != nil {
		return nil, fmt.Errorf("session: cancel pipe: %w", err)
	}
	pfd, inFD := int(r.Fd()), int(in.Fd())
	if pfd >= unix.FD_SETSIZE || inFD >= unix.FD_SETSIZE {
		_ = r.Close()
		_ = w.Close()
		return nil, errors.New("session: descriptor too large for select")
	}
	return &inputWaiter{r: r, w: w, pfd: pfd, inFD: inFD}, nil
}

// wait blocks until the outer terminal is readable or cancel was called;
// the cancel wins.
func (iw *inputWaiter) wait() error {
	for {
		var rfds unix.FdSet
		rfds.Set(iw.pfd)
		rfds.Set(iw.inFD)
		_, err := unix.Select(max(iw.pfd, iw.inFD)+1, &rfds, nil, nil, nil)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			return fmt.Errorf("session: select: %w", err)
		}
		if rfds.IsSet(iw.pfd) {
			return errInputCanceled
		}
		return nil
	}
}

// cancel interrupts the current and every later wait. The pipe is never
// drained, so it is idempotent.
func (iw *inputWaiter) cancel() { _, _ = iw.w.Write([]byte{0}) }

// close releases the pipe, only after the pump returned.
func (iw *inputWaiter) close() error { return errors.Join(iw.r.Close(), iw.w.Close()) }
