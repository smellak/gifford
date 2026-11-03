//go:build linux

package hub

import (
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

// peerPidfd returns a pidfd for the process that connected, from
// SO_PEERPIDFD. A kernel without it fails closed.
func peerPidfd(conn *net.UnixConn) (int, error) {
	raw, err := conn.SyscallConn()
	if err != nil {
		return -1, fmt.Errorf("peer pidfd: %w", err)
	}
	fd, optErr := -1, error(nil)
	if err := raw.Control(func(socket uintptr) {
		fd, optErr = unix.GetsockoptInt(int(socket), unix.SOL_SOCKET, unix.SO_PEERPIDFD)
	}); err != nil {
		return -1, fmt.Errorf("peer pidfd control: %w", err)
	}
	if optErr != nil {
		return -1, fmt.Errorf("peer pidfd socket option: %w", optErr)
	}
	return fd, nil
}

// pidfdAlive reports whether the process behind fd has not exited. Signal 0
// sends nothing; EPERM means it exists and belongs to another user.
func pidfdAlive(fd int) bool {
	err := unix.PidfdSendSignal(fd, 0, nil, 0)
	return err == nil || errors.Is(err, unix.EPERM)
}

func closePidfd(fd int) { _ = unix.Close(fd) }

// checkNodeSocketDir refuses a directory that is missing, a symlink, owned
// by another uid, or writable by group or others.
func checkNodeSocketDir(dir string) error {
	fi, err := os.Lstat(dir)
	if err != nil {
		return fmt.Errorf("node socket directory %s: %w", dir, err)
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok || !fi.IsDir() || fi.Mode()&fs.ModeSymlink != 0 ||
		int64(st.Uid) != int64(os.Geteuid()) || fi.Mode().Perm()&0o022 != 0 {
		return fmt.Errorf("node socket directory %s: must be this uid's own directory, writable only by it: %w",
			dir, errInvalidArgument)
	}
	return nil
}
