//go:build !linux

package main

import (
	"fmt"
	"net"
)

func e2eUnixPeerPID(net.Conn) (int, error) {
	return 0, fmt.Errorf("artifact interruption peer credentials require Linux")
}
