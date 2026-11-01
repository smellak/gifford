//go:build linux

package nativeevents

import (
	"errors"
	"fmt"
	"net"

	"golang.org/x/sys/unix"
)

func readPeerCredentials(conn net.Conn) (peerCredentials, error) {
	unixConn, ok := conn.(*net.UnixConn)
	if !ok {
		return peerCredentials{}, errors.New("nativeevents: connection is not unix")
	}
	raw, err := unixConn.SyscallConn()
	if err != nil {
		return peerCredentials{}, fmt.Errorf("nativeevents: access connection: %w", err)
	}
	var credentials *unix.Ucred
	var credentialErr error
	if err := raw.Control(func(fd uintptr) {
		credentials, credentialErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
	}); err != nil {
		return peerCredentials{}, fmt.Errorf("nativeevents: read peer credentials: %w", err)
	}
	if credentialErr != nil {
		return peerCredentials{}, fmt.Errorf("nativeevents: get peer credentials: %w", credentialErr)
	}
	return peerCredentials{uid: int64(credentials.Uid), pid: int64(credentials.Pid)}, nil
}
