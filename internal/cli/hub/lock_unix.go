//go:build unix

package hub

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// tryLockFile takes a non-blocking exclusive flock on f.
func tryLockFile(f *os.File) error {
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return fmt.Errorf("exclusive runtime lock: %w", err)
	}
	return nil
}

// unlockFile releases the flock on f.
func unlockFile(f *os.File) error {
	if err := unix.Flock(int(f.Fd()), unix.LOCK_UN); err != nil {
		return fmt.Errorf("release runtime lock: %w", err)
	}
	return nil
}
