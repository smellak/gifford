//go:build unix

package runtimelock

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func tryLockFile(file *os.File) error {
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return fmt.Errorf("exclusive runtime lock: %w", err)
	}
	return nil
}

func unlockFile(file *os.File) error {
	if err := unix.Flock(int(file.Fd()), unix.LOCK_UN); err != nil {
		return fmt.Errorf("release runtime lock: %w", err)
	}
	return nil
}
