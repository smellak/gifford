//go:build unix && !linux

package nativeevents

import (
	"errors"
	"net"
)

func readPeerCredentials(net.Conn) (peerCredentials, error) {
	return peerCredentials{}, errors.New("nativeevents: peer credentials unavailable")
}

func readProcessStartTime(int64) (int64, error) {
	return 0, errors.New("nativeevents: process start time unavailable")
}
