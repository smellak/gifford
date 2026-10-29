//go:build darwin

package capture

import (
	"errors"
	"fmt"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

// renameNoReplace moves old onto new inside dir, failing when new
// exists. EEXIST keeps the existing file; anything without the
// primitive fails the check.
func renameNoReplace(dir *os.File, old, name string) error {
	err := unix.RenameatxNp(int(dir.Fd()), old, int(dir.Fd()), name, unix.RENAME_EXCL)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, syscall.EEXIST):
		return errNameExists
	case errors.Is(err, syscall.EINVAL) || errors.Is(err, syscall.ENOSYS) || errors.Is(err, syscall.ENOTSUP):
		return errRenameUnsupported
	default:
		return fmt.Errorf("rename: %w", err)
	}
}
