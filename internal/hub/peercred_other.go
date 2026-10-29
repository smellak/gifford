//go:build !linux

package hub

import (
	"errors"
	"fmt"
	"syscall"
)

// errPeerCredUnsupported means the platform reports no peer credentials,
// so no connection binds to an agent there.
var errPeerCredUnsupported = errors.New("peer credentials unsupported")

// peerCred fails closed where the kernel reports no peer credentials:
// without SO_PEERCRED no connection can bind to an agent.
func peerCred(_ syscall.RawConn) (peerIdentity, error) {
	return peerIdentity{}, fmt.Errorf("peer credentials: %w", errPeerCredUnsupported)
}
