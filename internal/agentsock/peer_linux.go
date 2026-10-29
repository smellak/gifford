//go:build linux

package agentsock

import (
	"errors"
	"fmt"
	"net"
	"os"

	"golang.org/x/sys/unix"
)

// errForeignPeer means the peer runs as another user.
var errForeignPeer = errors.New("agentsock: peer is another user")

// samePeer admits a peer of this user only, read with SO_PEERCRED.
func samePeer(conn net.Conn) error {
	unixConn, ok := conn.(*net.UnixConn)
	if !ok {
		return fmt.Errorf("agentsock: connection is not unix")
	}
	raw, err := unixConn.SyscallConn()
	if err != nil {
		return fmt.Errorf("agentsock: access connection: %w", err)
	}
	var cred *unix.Ucred
	var credErr error
	if err := raw.Control(func(fd uintptr) {
		cred, credErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
	}); err != nil {
		return fmt.Errorf("agentsock: read peer credentials: %w", err)
	}
	if credErr != nil {
		return fmt.Errorf("agentsock: get peer credentials: %w", credErr)
	}
	if int64(cred.Uid) != int64(os.Getuid()) {
		return errForeignPeer
	}
	return nil
}
