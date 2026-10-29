//go:build linux

package hub

import (
	"fmt"
	"syscall"
)

// peerCredPID returns the pid the kernel reports for the far end of a
// Unix socket via SO_PEERCRED.
func peerCredPID(raw syscall.RawConn) (int, error) {
	var ucred *syscall.Ucred
	var controlErr error
	if err := raw.Control(func(fd uintptr) {
		ucred, controlErr = syscall.GetsockoptUcred(int(fd), syscall.SOL_SOCKET, syscall.SO_PEERCRED)
	}); err != nil {
		return 0, fmt.Errorf("peer credentials control: %w", err)
	}
	if controlErr != nil {
		return 0, fmt.Errorf("peer credentials socket option: %w", controlErr)
	}
	return int(ucred.Pid), nil
}
