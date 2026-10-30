//go:build linux

package agentlink

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"

	"github.com/smellak/gifford/internal/protocol"
)

// dialVerifiedPath checks the filesystem object before connecting and then
// returns the kernel-authenticated peer pid. The later registry lookup binds
// that pid to the live Hub session; filesystem permissions alone are never
// treated as process authentication.
func dialVerifiedPath(ctx context.Context, path string) (*net.UnixConn, peerCredentials, error) {
	if err := privateSocket(path); err != nil {
		return nil, peerCredentials{}, err
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", path)
	if err != nil {
		return nil, peerCredentials{}, fmt.Errorf("%w: dial agent socket: %w", ErrLinkDown, err)
	}
	unixConn, ok := conn.(*net.UnixConn)
	if !ok {
		_ = conn.Close()
		return nil, peerCredentials{}, fmt.Errorf("%w: not a unix connection", ErrPeerMismatch)
	}
	raw, err := unixConn.SyscallConn()
	if err != nil {
		_ = unixConn.Close()
		return nil, peerCredentials{}, fmt.Errorf("%w: access peer credentials: %w", ErrPeerMismatch, err)
	}
	var (
		credential *unix.Ucred
		credErr    error
	)
	if err := raw.Control(func(fd uintptr) {
		credential, credErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
	}); err != nil {
		_ = unixConn.Close()
		return nil, peerCredentials{}, fmt.Errorf("%w: read peer credentials: %w", ErrPeerMismatch, err)
	}
	if credErr != nil {
		_ = unixConn.Close()
		return nil, peerCredentials{}, fmt.Errorf("%w: get peer credentials: %w", ErrPeerMismatch, credErr)
	}
	if credential == nil {
		_ = unixConn.Close()
		return nil, peerCredentials{}, fmt.Errorf("%w: empty peer credentials", ErrPeerMismatch)
	}
	if int(credential.Uid) != os.Getuid() || credential.Pid <= 0 {
		_ = unixConn.Close()
		return nil, peerCredentials{}, ErrPeerMismatch
	}
	return unixConn, peerCredentials{
		pid: int64(credential.Pid), uid: int64(credential.Uid), gid: int64(credential.Gid),
	}, nil
}

// verifyPeerProcess compares the SO_PEERCRED identity with the exact process
// stored by register_agent. The start time closes PID-reuse races using the
// same /proc field convention as registry.CurrentProcess.
func verifyPeerProcess(peer peerCredentials, expected protocol.ProcessIdentity) error {
	if peer.pid != expected.PID || peer.uid != expected.UID || peer.gid != expected.GID {
		return ErrPeerMismatch
	}
	raw, err := os.ReadFile("/proc/" + strconv.FormatInt(peer.pid, 10) + "/stat")
	if err != nil {
		return fmt.Errorf("%w: read peer start time", ErrPeerMismatch)
	}
	closing := strings.LastIndexByte(string(raw), ')')
	if closing < 0 {
		return fmt.Errorf("%w: parse peer start time", ErrPeerMismatch)
	}
	fields := strings.Fields(string(raw[closing+1:]))
	if len(fields) < 20 {
		return fmt.Errorf("%w: parse peer start time", ErrPeerMismatch)
	}
	start, err := strconv.ParseInt(fields[19], 10, 64)
	if err != nil || start != expected.ProcessStartTime {
		return fmt.Errorf("%w: peer start time", ErrPeerMismatch)
	}
	return nil
}

func privateSocket(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("%w: inspect socket: %w", ErrLinkDown, err)
	}
	if info.Mode().Type() != os.ModeSocket || info.Mode().Perm() != 0o600 || !ownedByCurrentUser(info) {
		return ErrInsecureSocket
	}
	parent, err := os.Lstat(filepath.Dir(path))
	if err != nil {
		return fmt.Errorf("%w: inspect socket directory: %w", ErrInsecureSocket, err)
	}
	if !parent.IsDir() || parent.Mode().Perm() != 0o700 || !ownedByCurrentUser(parent) {
		return ErrInsecureSocket
	}
	return nil
}

func ownedByCurrentUser(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && int(stat.Uid) == os.Getuid()
}
