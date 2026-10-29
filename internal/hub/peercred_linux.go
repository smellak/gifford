//go:build linux

package hub

import (
	"fmt"
	"syscall"
)

// peerCred returns the identity the kernel reports for the far end of a
// Unix socket via SO_PEERCRED.
func peerCred(raw syscall.RawConn) (peerIdentity, error) {
	var ucred *syscall.Ucred
	var controlErr error
	if err := raw.Control(func(fd uintptr) {
		ucred, controlErr = syscall.GetsockoptUcred(int(fd), syscall.SOL_SOCKET, syscall.SO_PEERCRED)
	}); err != nil {
		return peerIdentity{}, fmt.Errorf("peer credentials control: %w", err)
	}
	if controlErr != nil {
		return peerIdentity{}, fmt.Errorf("peer credentials socket option: %w", controlErr)
	}
	return peerIdentity{PID: int(ucred.Pid), UID: int64(ucred.Uid), GID: int64(ucred.Gid)}, nil
}
