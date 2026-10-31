//go:build windows

package runtimelock

import (
	"fmt"
	"os"

	"golang.org/x/sys/windows"
)

func tryLockFile(file *os.File) error {
	overlap := windows.Overlapped{}
	if err := windows.LockFileEx(windows.Handle(file.Fd()),
		windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY,
		0, 1, 0, &overlap); err != nil {
		return fmt.Errorf("exclusive runtime lock: %w", err)
	}
	return nil
}

func unlockFile(file *os.File) error {
	overlap := windows.Overlapped{}
	if err := windows.UnlockFileEx(windows.Handle(file.Fd()), 0, 1, 0, &overlap); err != nil {
		return fmt.Errorf("release runtime lock: %w", err)
	}
	return nil
}
