//go:build linux

package hub

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/smellak/gifford/internal/notifications"
	"github.com/smellak/gifford/internal/protocol"
)

const hubTMUXWaitDelay = 100 * time.Millisecond

type hubRealTMUXServer struct {
	command string
	socket  string
	pid     int
}

func startHubRealTMUXServer(t *testing.T) *hubRealTMUXServer {
	t.Helper()
	command, err := exec.LookPath("tmux")
	if err != nil {
		t.Skip("tmux is not installed")
	}
	socket := filepath.Join(t.TempDir(), "tmux.sock")
	runHubRealTMUX(t, command, "-S", socket, "new-session", "-d", "-s", "hub-test")
	pidText := runHubRealTMUX(t, command,
		"-S", socket, "display-message", "-p", "-t", "hub-test", "#{pid}")
	pid, err := strconv.Atoi(strings.TrimSpace(pidText))
	require.NoError(t, err)
	server := &hubRealTMUXServer{command: command, socket: socket, pid: pid}
	t.Cleanup(func() {
		server.resume()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, server.command, "-S", server.socket, "kill-server")
		cmd.Env = hubTMUXEnvironment()
		cmd.WaitDelay = hubTMUXWaitDelay
		_ = cmd.Run()
	})
	return server
}

func runHubRealTMUX(t *testing.T, command string, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, command, args...)
	cmd.Env = hubTMUXEnvironment()
	cmd.WaitDelay = hubTMUXWaitDelay
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, string(output))
	return string(output)
}

func hubTMUXEnvironment() []string {
	environment := make([]string, 0, len(os.Environ())+1)
	for _, variable := range os.Environ() {
		if strings.HasPrefix(variable, "TMUX=") || strings.HasPrefix(variable, "TMUX_PANE=") ||
			strings.HasPrefix(variable, "TERM=") {
			continue
		}
		environment = append(environment, variable)
	}
	return append(environment, "TERM=xterm-256color")
}

func (s *hubRealTMUXServer) stop(t *testing.T) {
	t.Helper()
	require.NoError(t, unix.Kill(s.pid, unix.SIGSTOP))
}

func (s *hubRealTMUXServer) resume() {
	_ = unix.Kill(s.pid, unix.SIGCONT)
}

type tmuxNotificationRunner struct {
	adapter notifications.Adapter
	done    chan error
}

func (r *tmuxNotificationRunner) Run(ctx context.Context) {
	now := time.Date(2026, time.October, 31, 0, 0, 0, 0, time.UTC)
	r.done <- r.adapter.Render(ctx, notifications.Notification{
		ID: "ntf_01JB0000000000000000000001", Class: notifications.ClassMessage,
		ObjectID: "01JB0000000000000000000002", Sender: "agent-sender",
		LocalRecipients: []protocol.AgentID{"agent-recipient"},
		Priority:        protocol.PriorityHigh, Reason: notifications.ReasonMessageAvailable,
		State: notifications.StatePending, Adapter: notifications.AdapterTMUX,
		CreatedAt: now, UpdatedAt: now,
	})
}

func writeTMUXMarkerWrapper(t *testing.T, command, marker string) string {
	t.Helper()
	t.Setenv("GIFFORD_TEST_TMUX_COMMAND", command)
	t.Setenv("GIFFORD_TEST_TMUX_MARKER", marker)
	path := filepath.Join(t.TempDir(), "tmux-wrapper")
	contents := `#!/bin/sh
printf '%s\n' "$$" > "$GIFFORD_TEST_TMUX_MARKER"
exec "$GIFFORD_TEST_TMUX_COMMAND" "$@"
`
	require.NoError(t, os.WriteFile(path, []byte(contents), 0o700))
	return path
}

func waitForRealTMUXClient(t *testing.T, marker string) {
	t.Helper()
	require.Eventually(t, func() bool {
		pidText, err := os.ReadFile(marker)
		if err != nil {
			return false
		}
		pid, err := strconv.Atoi(strings.TrimSpace(string(pidText)))
		if err != nil {
			return false
		}
		name, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "comm"))
		return err == nil && strings.HasPrefix(strings.TrimSpace(string(name)), "tmux")
	}, 2*time.Second, 10*time.Millisecond)
}

func TestNotificationLoopStopBoundsStoppedTMUX(t *testing.T) {
	server := startHubRealTMUXServer(t)
	server.stop(t)
	marker := filepath.Join(t.TempDir(), "client.pid")
	command := writeTMUXMarkerWrapper(t, server.command, marker)
	adapter, err := notifications.NewTMUXAdapter(notifications.TMUXOptions{
		Command: command, Socket: server.socket, Client: "/dev/pts/gifford-test",
		Duration: time.Second, Timeout: 30 * time.Second,
	})
	require.NoError(t, err)
	runner := &tmuxNotificationRunner{adapter: adapter, done: make(chan error, 1)}
	loop := startNotificationLoop(t.Context(), runner)
	stopped := false
	t.Cleanup(func() {
		server.resume()
		if !stopped {
			loop.stop()
		}
	})
	waitForRealTMUXClient(t, marker)

	started := time.Now()
	stopDone := make(chan struct{})
	go func() {
		loop.stop()
		close(stopDone)
	}()
	select {
	case <-stopDone:
		stopped = true
	case <-time.After(2 * time.Second):
		server.resume()
		<-stopDone
		stopped = true
		t.Fatal("notification loop stop exceeded fixed margin")
	}
	server.resume()
	require.Less(t, time.Since(started), 2*time.Second)
	require.Error(t, <-runner.done)
}
