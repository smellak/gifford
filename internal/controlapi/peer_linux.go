//go:build linux

package controlapi

import (
	"fmt"
	"net"

	"golang.org/x/sys/unix"
)

// peerCredentials authenticates a Linux Unix-socket peer with SO_PEERCRED.
func peerCredentials(conn net.Conn) (Peer, error) {
	unixConn, ok := conn.(*net.UnixConn)
	if !ok {
		return Peer{}, fmt.Errorf("control connection is not unix")
	}
	raw, err := unixConn.SyscallConn()
	if err != nil {
		return Peer{}, fmt.Errorf("access unix control connection: %w", err)
	}
	var cred *unix.Ucred
	var controlErr error
	if err := raw.Control(func(fd uintptr) {
		cred, controlErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
	}); err != nil {
		return Peer{}, fmt.Errorf("read unix control credentials: %w", err)
	}
	if controlErr != nil {
		return Peer{}, fmt.Errorf("get unix control credentials: %w", controlErr)
	}
	return Peer{PID: int(cred.Pid), UID: cred.Uid, GID: cred.Gid, Verified: true}, nil
}
