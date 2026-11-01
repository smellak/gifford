//go:build linux

package nativeevents

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"

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

func readProcessStartTime(pid int64) (int64, error) {
	if pid <= 0 {
		return 0, errors.New("nativeevents: invalid peer pid")
	}
	raw, err := os.ReadFile("/proc/" + strconv.FormatInt(pid, 10) + "/stat")
	if err != nil {
		return 0, fmt.Errorf("nativeevents: read peer start time: %w", err)
	}
	closing := strings.LastIndexByte(string(raw), ')')
	if closing < 0 {
		return 0, errors.New("nativeevents: parse peer start time")
	}
	fields := strings.Fields(string(raw[closing+1:]))
	if len(fields) < 20 {
		return 0, errors.New("nativeevents: parse peer start time")
	}
	startTime, err := strconv.ParseInt(fields[19], 10, 64)
	if err != nil || startTime <= 0 {
		return 0, errors.New("nativeevents: parse peer start time")
	}
	return startTime, nil
}
