//go:build unix

package proxy

import (
	"fmt"

	"golang.org/x/sys/unix"
)

// writeChild writes all of b to the child PTY; on EAGAIN, or a zero-byte
// write, it waits for the descriptor to turn writable or the cancel pipe,
// and a cancel drops the rest of b. Only writer.go writes to the child PTY.
func writeChild(child *childEnd, b []byte) error {
	for len(b) > 0 {
		n, err := unix.Write(child.fd, b)
		switch {
		case err == unix.EINTR:
			continue
		case err == unix.EAGAIN || (err == nil && n == 0):
			if werr := child.c.wait(child.fd, true); werr != nil {
				return werr
			}
		case err != nil:
			return fmt.Errorf("write: %w", err)
		default:
			b = b[n:]
		}
	}
	return nil
}

// pumpInput copies In to the child PTY until the harness hangs up or the
// session stops. It writes all it read before looking at the read error,
// and it is the only writer of the child PTY, through writeChild.
func pumpInput(inR *fdReader, child *childEnd) error {
	buf := make([]byte, 4096)
	for {
		n, rerr := inR.Read(buf)
		if n > 0 {
			if werr := writeChild(child, buf[:n]); werr != nil {
				if endOfStream(werr) {
					return errChildClosed
				}
				return werr
			}
		}
		if rerr != nil {
			if endOfStream(rerr) {
				return errOuterClosed
			}
			return rerr
		}
	}
}
