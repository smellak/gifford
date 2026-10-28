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
