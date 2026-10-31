// Package nodeproc runs the node in the background for gifford up, and
// stops it for gifford down. The node is the gifford hub process, detached
// from the terminal, with its output in a log file in the data directory.
// Its PID file, in the runtime directory, names the exact process gifford
// down signals. The runtime lock the hub holds is the proof that a node
// runs: gifford down waits for its release.
package nodeproc

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/smellak/gifford/internal/cli/runtimelock"
)

const (
	// PIDFile is the file, in the runtime directory, that holds the PID of
	// the node gifford up started.
	PIDFile = "hub.pid"
	// LogFile is the file, in the data directory, that takes the node's
	// stdout and stderr.
	LogFile = "hub.log"
	// ReadyTimeout bounds gifford up's wait for the hub's ready line.
	ReadyTimeout = 60 * time.Second
	// StopTimeout bounds gifford down's wait for the runtime lock.
	StopTimeout = 30 * time.Second

	// readyPrefix starts the line the hub prints once it serves.
	readyPrefix = "gifford hub: ready: "
	pollEvery   = 50 * time.Millisecond
	// maxLogScan bounds how much new log Start reads looking for the
	// ready line.
	maxLogScan = 1 << 20
)

var (
	// ErrRunning means a node already holds the runtime lock.
	ErrRunning = errors.New("the node is already running")
	// ErrNotRunning means no node holds the runtime lock.
	ErrNotRunning = errors.New("the node is not running")
	// ErrNotStartedByUp means a node runs that gifford up did not start, so
	// there is no process gifford down may signal.
	ErrNotStartedByUp = errors.New("the running node was not started by gifford up")
	// ErrUnsupported means the platform cannot run the node in the
	// background.
	ErrUnsupported = errors.New("running the node in the background is not supported on this platform")
)

// NodeCommand is the node's command line: this gifford executable, as
// gifford hub. gifford up starts it, and gifford down signals only a
// process that runs it.
func NodeCommand() ([]string, error) {
	exe, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("find the gifford executable: %w", err)
	}
	return []string{exe, "hub"}, nil
}

// Running reports whether a node holds the runtime lock in runtimeDir. It
// takes the lock for an instant when it is free, so only gifford up and
// gifford down call it.
func Running(runtimeDir string) (bool, error) {
	if _, err := os.Stat(runtimeDir); errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	lock, err := runtimelock.Acquire(runtimeDir)
	if errors.Is(err, runtimelock.ErrHeld) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("check the runtime lock: %w", err)
	}
	if err := lock.Close(); err != nil {
		return false, fmt.Errorf("check the runtime lock: %w", err)
	}
	return false, nil
}

// ReadPID returns the PID gifford up recorded in runtimeDir, or
// ErrNotStartedByUp when there is none.
func ReadPID(runtimeDir string) (int, error) {
	path := filepath.Join(runtimeDir, PIDFile)
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, ErrNotStartedByUp
	}
	if err != nil {
		return 0, fmt.Errorf("read %s: %w", path, err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil || pid <= 0 {
		return 0, fmt.Errorf("%s holds no PID", path)
	}
	return pid, nil
}

func writePID(runtimeDir string, pid int) error {
	path := filepath.Join(runtimeDir, PIDFile)
	if err := os.WriteFile(path, []byte(strconv.Itoa(pid)+"\n"), 0o600); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	return nil
}

func removePID(runtimeDir string) error {
	err := os.Remove(filepath.Join(runtimeDir, PIDFile))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove the PID file: %w", err)
	}
	return nil
}

// readyAfter reports whether the log, from offset on, holds the hub's
// ready line.
func readyAfter(logPath string, offset int64) (bool, error) {
	f, err := os.Open(logPath)
	if err != nil {
		return false, fmt.Errorf("open %s: %w", logPath, err)
	}
	defer func() { _ = f.Close() }()
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return false, fmt.Errorf("read %s: %w", logPath, err)
	}
	scanner := bufio.NewScanner(io.LimitReader(f, maxLogScan))
	for scanner.Scan() {
		if strings.HasPrefix(scanner.Text(), readyPrefix) {
			return true, nil
		}
	}
	if err := scanner.Err(); err != nil {
		return false, fmt.Errorf("read %s: %w", logPath, err)
	}
	return false, nil
}
