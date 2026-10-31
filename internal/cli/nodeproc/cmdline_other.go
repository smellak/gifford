//go:build unix && !linux

package nodeproc

import (
	"errors"
	"syscall"
)

// runs reports whether process pid is alive. Without /proc, its command
// line is not checked: the PID file gifford up wrote names the process.
func runs(pid int, _ []string) (bool, error) {
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM), nil
}
