//go:build !linux

package hub

import (
	"errors"
	"net"
)

// peerPidfd fails closed: other platforms have no SO_PEERPIDFD.
func peerPidfd(*net.UnixConn) (int, error) { return -1, errors.ErrUnsupported }

func pidfdAlive(int) bool { return false }

func closePidfd(int) {}

func checkNodeSocketDir(string) error { return errors.ErrUnsupported }
