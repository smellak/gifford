//go:build !linux

package controlapi

import "net"

// peerCredentials cannot authenticate a portable Unix-socket peer. The
// server still accepts reads, while Handle rejects every mutation.
func peerCredentials(net.Conn) (Peer, error) { return Peer{}, nil }
