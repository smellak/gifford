//go:build windows

package hub

import (
	"fmt"
	"os"

	"golang.org/x/sys/windows"
)

// tryLockFile takes a non-blocking exclusive lock on the whole file.
func tryLockFile(f *os.File) error {
	overlap := windows.Overlapped{}
	if err := windows.LockFileEx(windows.Handle(f.Fd()),
		windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY,
		0, 1, 0, &overlap); err != nil {
		return fmt.Errorf("exclusive runtime lock: %w", err)
	}
	return nil
}

// unlockFile releases the lock on the whole file.
func unlockFile(f *os.File) error {
	overlap := windows.Overlapped{}
	if err := windows.UnlockFileEx(windows.Handle(f.Fd()), 0, 1, 0, &overlap); err != nil {
		return fmt.Errorf("release runtime lock: %w", err)
	}
	return nil
}
