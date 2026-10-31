//go:build unix

package nodeproc

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/smellak/gifford/internal/xdg"
)

// stopGrace is how long a node that never became ready gets after SIGTERM
// before SIGKILL.
const stopGrace = 10 * time.Second

// Start runs command, the node, in a session of its own with no terminal,
// its output appended to the log file, and records its PID. It returns the
// PID once the node prints its ready line. When the node exits first, or
// is not ready within readyTimeout, it fails, after stopping the node it
// started.
func Start(dirs xdg.Dirs, command []string, readyTimeout time.Duration) (int, error) {
	running, err := Running(dirs.Runtime)
	if err != nil {
		return 0, err
	}
	if running {
		return 0, ErrRunning
	}
	logPath := filepath.Join(dirs.Data, LogFile)
	logFile, err := os.OpenFile(logPath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o600)
	if err != nil {
		return 0, fmt.Errorf("open the node log: %w", err)
	}
	defer func() { _ = logFile.Close() }()
	info, err := logFile.Stat()
	if err != nil {
		return 0, fmt.Errorf("open the node log: %w", err)
	}
	devNull, err := os.Open(os.DevNull)
	if err != nil {
		return 0, fmt.Errorf("open %s: %w", os.DevNull, err)
	}
	defer func() { _ = devNull.Close() }()

	// The node outlives this command, so no context bounds it.
	cmd := exec.Command(command[0], command[1:]...) //nolint:gosec,noctx // The gifford executable itself.
	cmd.Stdin, cmd.Stdout, cmd.Stderr = devNull, logFile, logFile
	// A session of its own has no controlling terminal: closing this
	// terminal does not stop the node.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		return 0, fmt.Errorf("start the node: %w", err)
	}
	exited := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(exited)
	}()
	pid := cmd.Process.Pid
	if err := writePID(dirs.Runtime, pid); err != nil {
		terminate(cmd.Process, exited)
		return 0, err
	}
	fail := func(err error) (int, error) {
		terminate(cmd.Process, exited)
		_ = removePID(dirs.Runtime)
		return 0, err
	}

	deadline := time.NewTimer(readyTimeout)
	defer deadline.Stop()
	tick := time.NewTicker(pollEvery)
	defer tick.Stop()
	for {
		ready, err := readyAfter(logPath, info.Size())
		if err != nil {
			return fail(err)
		}
		if ready {
			return pid, nil
		}
		select {
		case <-exited:
			return fail(fmt.Errorf("the node stopped before it was ready; see %s", logPath))
		case <-deadline.C:
			return fail(fmt.Errorf("the node was not ready within %s and was stopped; see %s", readyTimeout, logPath))
		case <-tick.C:
		}
	}
}

// terminate stops a node that never became ready, and reaps it: SIGTERM,
// then SIGKILL if it still runs after the grace period.
func terminate(p *os.Process, exited <-chan struct{}) {
	_ = p.Signal(syscall.SIGTERM)
	grace := time.NewTimer(stopGrace)
	defer grace.Stop()
	select {
	case <-exited:
		return
	case <-grace.C:
	}
	_ = p.Kill()
	<-exited
}

// Stop sends SIGTERM to the node gifford up started, the exact process its
// PID file names, and waits until the runtime lock is free. It returns the
// PID. It fails with ErrNotRunning when no node runs, after removing a
// stale PID file, and with ErrNotStartedByUp when no PID file names the
// running node, or the process it names is not command.
func Stop(dirs xdg.Dirs, command []string, stopTimeout time.Duration) (int, error) {
	running, err := Running(dirs.Runtime)
	if err != nil {
		return 0, err
	}
	pid, pidErr := ReadPID(dirs.Runtime)
	if !running {
		if pidErr == nil {
			if err := removePID(dirs.Runtime); err != nil {
				return 0, err
			}
		}
		return 0, ErrNotRunning
	}
	if pidErr != nil {
		return 0, pidErr
	}
	same, err := runs(pid, command)
	if err != nil {
		return 0, err
	}
	if !same {
		return 0, fmt.Errorf("PID %d is not %q: %w", pid, strings.Join(command, " "), ErrNotStartedByUp)
	}
	if err := syscall.Kill(pid, syscall.SIGTERM); err != nil {
		return 0, fmt.Errorf("signal the node (PID %d): %w", pid, err)
	}
	deadline := time.NewTimer(stopTimeout)
	defer deadline.Stop()
	tick := time.NewTicker(pollEvery)
	defer tick.Stop()
	for {
		running, err := Running(dirs.Runtime)
		if err != nil {
			return pid, err
		}
		if !running {
			return pid, removePID(dirs.Runtime)
		}
		select {
		case <-deadline.C:
			return pid, fmt.Errorf("the node (PID %d) still holds the runtime lock after %s", pid, stopTimeout)
		case <-tick.C:
		}
	}
}
