//go:build linux

package main

import (
	"fmt"
	"net"
	"syscall"
)

// e2eUnixPeerPID returns the PID authenticated by SO_PEERCRED for conn.
func e2eUnixPeerPID(conn net.Conn) (int, error) {
	unixConn, ok := conn.(*net.UnixConn)
	if !ok {
		return 0, fmt.Errorf("artifact interruption peer is not a unix connection")
	}
	raw, err := unixConn.SyscallConn()
	if err != nil {
		return 0, fmt.Errorf("access artifact interruption peer: %w", err)
	}
	var (
		credential    *syscall.Ucred
		credentialErr error
	)
	if err := raw.Control(func(fd uintptr) {
		credential, credentialErr = syscall.GetsockoptUcred(
			int(fd), syscall.SOL_SOCKET, syscall.SO_PEERCRED,
		)
	}); err != nil {
		return 0, fmt.Errorf("read artifact interruption peer: %w", err)
	}
	if credentialErr != nil {
		return 0, fmt.Errorf("get artifact interruption peer: %w", credentialErr)
	}
	if credential == nil || credential.Pid <= 0 {
		return 0, fmt.Errorf("artifact interruption peer has no pid")
	}
	return int(credential.Pid), nil
}
