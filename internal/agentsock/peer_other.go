//go:build unix && !linux

package agentsock

import (
	"errors"
	"net"
)

// errPeerUnverified means this platform cannot read the peer's user.
var errPeerUnverified = errors.New("agentsock: peer credentials not verifiable here")

// samePeer cannot authenticate a peer outside Linux, so it refuses every
// connection: the socket fails closed.
func samePeer(net.Conn) error { return errPeerUnverified }
