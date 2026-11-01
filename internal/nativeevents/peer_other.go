//go:build unix && !linux

package nativeevents

import (
	"errors"
	"net"
)

func readPeerCredentials(net.Conn) (peerCredentials, error) {
	return peerCredentials{}, errors.New("nativeevents: peer credentials unavailable")
}
