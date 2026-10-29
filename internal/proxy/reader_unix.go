//go:build unix

package proxy

import (
	"errors"
	"fmt"
	"io"
	"os"

	"golang.org/x/sys/unix"
)

// canceler interrupts every wait made through it: cancel writes one byte
// to a pipe that is never drained, so each later select returns at once.
type canceler struct {
	r, w *os.File // os.Pipe: close-on-exec, so the harness never inherits it
	fd   int      // r's descriptor
}

// newCanceler returns a canceler watching a fresh pipe.
func newCanceler() (*canceler, error) {
	r, w, err := os.Pipe()
	if err != nil {
		return nil, fmt.Errorf("canceler pipe: %w", err)
	}
	if r.Fd() >= unix.FD_SETSIZE || w.Fd() >= unix.FD_SETSIZE {
		_ = r.Close()
		_ = w.Close()
		return nil, errFDTooLarge
	}
	return &canceler{r: r, w: w, fd: int(r.Fd())}, nil
}

// cancel interrupts every wait made through c. It is idempotent: the pipe
// is never drained, so further cancels only queue one more byte.
func (c *canceler) cancel() {
	_, _ = c.w.Write([]byte{0})
}

// close closes both pipe ends; only after every wait returned.
func (c *canceler) close() error {
	return errors.Join(c.r.Close(), c.w.Close())
}

// wait blocks until fd is readable (writable if write) or the cancel pipe
// is; the cancel pipe wins: errCanceled.
func (c *canceler) wait(fd int, write bool) error {
	for {
		var rfds, wfds unix.FdSet
		rfds.Set(c.fd)
		if write {
			wfds.Set(fd)
		} else {
			rfds.Set(fd)
		}
		nfds := c.fd
		if fd > nfds {
			nfds = fd
		}
		_, err := unix.Select(nfds+1, &rfds, &wfds, nil, nil)
		if err == unix.EINTR {
			continue
		}
		if err != nil {
			return fmt.Errorf("select: %w", err)
		}
		if rfds.IsSet(c.fd) {
			return errCanceled
		}
		return nil
	}
}

// fdReader reads the blocking outer terminal.
type fdReader struct {
	file *os.File
	fd   int
	c    *canceler
}

// reader watches f through c; the descriptor must fit unix.Select.
func (c *canceler) reader(f *os.File) (*fdReader, error) {
	fd := int(f.Fd())
	if fd >= unix.FD_SETSIZE {
		return nil, errFDTooLarge
	}
	return &fdReader{file: f, fd: fd, c: c}, nil
}

// Read waits for input or a cancel, then does one blocking read.
func (r *fdReader) Read(p []byte) (int, error) {
	if err := r.c.wait(r.fd, false); err != nil {
		return 0, err
	}
	n, err := r.file.Read(p)
	if err != nil {
		return n, fmt.Errorf("read outer: %w", err)
	}
	return n, nil
}

// childEnd is ptmx's descriptor, non-blocking.
type childEnd struct {
	fd int
	c  *canceler
}

// child takes ptmx's descriptor, the last ptmx.Fd(), and turns it
// non-blocking; the descriptor must fit unix.Select.
func (c *canceler) child(ptmx *os.File) (*childEnd, error) {
	fd := int(ptmx.Fd())
	if fd >= unix.FD_SETSIZE {
		return nil, errFDTooLarge
	}
	if err := unix.SetNonblock(fd, true); err != nil {
		return nil, fmt.Errorf("child nonblock: %w", err)
	}
	return &childEnd{fd: fd, c: c}, nil
}

// Read waits for output or a cancel, then reads until data or an error.
func (p *childEnd) Read(b []byte) (int, error) {
	for {
		if err := p.c.wait(p.fd, false); err != nil {
			return 0, err
		}
		n, err := p.readNow(b)
		if errors.Is(err, errNothingPending) {
			continue
		}
		return n, err
	}
}

// readNow does one unix.Read; EAGAIN means nothing is pending right now.
func (p *childEnd) readNow(b []byte) (int, error) {
	for {
		n, err := unix.Read(p.fd, b)
		if err == unix.EINTR {
			continue
		}
		if err == unix.EAGAIN {
			return 0, errNothingPending
		}
		if err != nil {
			return n, fmt.Errorf("read: %w", err)
		}
		if n == 0 {
			return 0, io.EOF
		}
		return n, nil
	}
}

// setSize resizes the child PTY through the proxy's own descriptor.
func (p *childEnd) setSize(rows, cols, x, y uint16) error {
	ws := &unix.Winsize{Row: rows, Col: cols, Xpixel: x, Ypixel: y}
	if err := unix.IoctlSetWinsize(p.fd, unix.TIOCSWINSZ, ws); err != nil {
		return fmt.Errorf("set size: %w", err)
	}
	return nil
}
