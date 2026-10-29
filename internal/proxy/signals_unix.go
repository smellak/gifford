//go:build unix

package proxy

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

// handledSignals lists what Run forwards or acts on: SIGWINCH resizes the
// child PTY, the rest go to the harness's process group.
func handledSignals() []os.Signal {
	return []os.Signal{
		unix.SIGWINCH,
		unix.SIGHUP,
		unix.SIGINT,
		unix.SIGQUIT,
		unix.SIGTERM,
		unix.SIGUSR1,
		unix.SIGUSR2,
	}
}

// signalGroup sends sig to the harness process group; a gone group counts
// as done.
func signalGroup(pid int, sig syscall.Signal) error {
	if err := unix.Kill(-pid, sig); err != nil {
		if errors.Is(err, unix.ESRCH) {
			return os.ErrProcessDone
		}
		return fmt.Errorf("signal group: %w", err)
	}
	return nil
}

// exitOf reports how the reaped harness ended.
func exitOf(ps *os.ProcessState) Exit {
	if status, ok := ps.Sys().(syscall.WaitStatus); ok && status.Signaled() {
		return Exit{Code: -1, Signal: status.Signal()}
	}
	return Exit{Code: ps.ExitCode()}
}

// daemonRunning probes the daemon socket once: a dial, not a stat, since a
// crashed daemon leaves its socket file behind.
func daemonRunning(ctx context.Context, path string) bool {
	if path == "" {
		return false
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", path)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}
