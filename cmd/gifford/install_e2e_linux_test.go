package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// installEnv is the environment of a user who never ran gifford: HOME is
// a fresh directory and no XDG variable is set, so every directory
// derives from HOME.
func installEnv(t *testing.T) ([]string, string) {
	t.Helper()
	// t.TempDir is too long: the runtime directory, with its sockets, lives
	// under HOME.
	home, err := os.MkdirTemp("", "gifford-home-*") //nolint:usetesting
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(home)) })
	var env []string
	for _, kv := range os.Environ() {
		key, _, _ := strings.Cut(kv, "=")
		if key == "HOME" || strings.HasPrefix(key, "XDG_") {
			continue
		}
		env = append(env, kv)
	}
	return append(env, "HOME="+home, "GORACE=atexit_sleep_ms=0"), home
}

// runGifford runs the e2e binary and returns its stdout and exit code.
func runGifford(ctx context.Context, t *testing.T, env []string, args ...string) (string, int) {
	t.Helper()
	cmd := exec.CommandContext(ctx, e2eGiffordBinary, args...)
	cmd.Env = env
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return string(out) + stderr.String(), exitErr.ExitCode()
	}
	require.NoError(t, err, "stderr: %s", stderr.String())
	return string(out), 0
}

// exited reports whether process pid is gone: no such process, or a zombie
// its new parent has yet to reap.
func exited(pid int) bool {
	if errors.Is(syscall.Kill(pid, 0), syscall.ESRCH) {
		return true
	}
	stat, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return true
	}
	// The state follows the command name, which is in parentheses.
	i := bytes.LastIndexByte(stat, ')')
	return i >= 0 && i+2 < len(stat) && stat[i+2] == 'Z'
}

// TestInstallFromScratch follows the README's install block for a user who
// never ran gifford: init, doctor, up, status, down, with the real binary.
func TestInstallFromScratch(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	env, home := installEnv(t)
	runtimeDir := filepath.Join(home, ".local", "share", "gifford", "run")
	pidPath := filepath.Join(runtimeDir, "hub.pid")
	// Whatever fails, no node outlives the test.
	t.Cleanup(func() {
		if b, err := os.ReadFile(pidPath); err == nil {
			if pid, err := strconv.Atoi(strings.TrimSpace(string(b))); err == nil {
				_ = syscall.Kill(pid, syscall.SIGKILL)
			}
		}
	})

	out, code := runGifford(ctx, t, env, "init", "--node-id", "node-install")
	require.Equal(t, 0, code, out)
	require.Contains(t, out, filepath.Join(home, ".config", "gifford", "config.yaml"))
	out, code = runGifford(ctx, t, env, "init")
	require.Equal(t, 4, code, "init never overwrites config.yaml: %s", out)
	out, code = runGifford(ctx, t, env, "doctor")
	require.Equal(t, 0, code, out)

	out, code = runGifford(ctx, t, env, "up")
	require.Equal(t, 0, code, out)
	b, err := os.ReadFile(pidPath)
	require.NoError(t, err)
	pid, err := strconv.Atoi(strings.TrimSpace(string(b)))
	require.NoError(t, err)
	require.Contains(t, out, fmt.Sprintf("node ready (PID %d)", pid))
	log, err := os.ReadFile(filepath.Join(home, ".local", "share", "gifford", "hub.log"))
	require.NoError(t, err)
	require.Contains(t, string(log), "gifford hub: ready: ")
	out, code = runGifford(ctx, t, env, "up")
	require.Equal(t, 0, code, out)
	require.Contains(t, out, "already running")

	out, code = runGifford(ctx, t, env, "status")
	require.Equal(t, 0, code, out)
	require.Contains(t, out, fmt.Sprintf("node     up (PID %d)\n", pid))
	require.Contains(t, out, "sockets  hub.sock ok, daemon.sock ok\n")
	require.Contains(t, out, "channel  ACTIVE\n")
	require.Contains(t, out, "agents   0\n")

	out, code = runGifford(ctx, t, env, "down")
	require.Equal(t, 0, code, out)
	require.Equal(t, fmt.Sprintf("gifford down: node stopped (PID %d)\n", pid), out)
	require.Eventually(t, func() bool { return exited(pid) }, 5*time.Second, 20*time.Millisecond, "the hub exited")
	requireNoE2ESockets(t, runtimeDir)
	_, err = os.Stat(pidPath)
	require.ErrorIs(t, err, os.ErrNotExist)

	out, code = runGifford(ctx, t, env, "status")
	require.Equal(t, 1, code, out)
	require.Contains(t, out, "node     down\n")
	out, code = runGifford(ctx, t, env, "down")
	require.Equal(t, 0, code, out)
	require.Contains(t, out, "not running")
}
