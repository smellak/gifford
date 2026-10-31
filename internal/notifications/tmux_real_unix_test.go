//go:build unix

package notifications

import (
	"context"
	"database/sql"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/creack/pty"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/smellak/gifford/internal/protocol"
)

type realTMUXServer struct {
	command string
	socket  string
	pid     int
}

func startRealTMUXServer(t *testing.T) *realTMUXServer {
	t.Helper()
	command, err := exec.LookPath("tmux")
	if err != nil {
		t.Skip("tmux is not installed")
	}
	socket := filepath.Join(t.TempDir(), "tmux.sock")
	runRealTMUX(t, command, "-S", socket, "new-session", "-d", "-s", "notification-test")
	pidText := runRealTMUX(t, command,
		"-S", socket, "display-message", "-p", "-t", "notification-test", "#{pid}")
	pid, err := strconv.Atoi(strings.TrimSpace(pidText))
	require.NoError(t, err)
	server := &realTMUXServer{command: command, socket: socket, pid: pid}
	t.Cleanup(func() {
		server.resume()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, server.command, "-S", server.socket, "kill-server")
		cmd.Env = tmuxTestEnvironment()
		cmd.WaitDelay = adapterWaitDelay
		_ = cmd.Run()
	})
	return server
}

func runRealTMUX(t *testing.T, command string, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, command, args...)
	cmd.Env = tmuxTestEnvironment()
	cmd.WaitDelay = adapterWaitDelay
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, string(output))
	return string(output)
}

func tmuxTestEnvironment() []string {
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

func (s *realTMUXServer) stop(t *testing.T) {
	t.Helper()
	require.NoError(t, unix.Kill(s.pid, unix.SIGSTOP))
}

func (s *realTMUXServer) resume() {
	_ = unix.Kill(s.pid, unix.SIGCONT)
}

func attachRealTMUXClient(t *testing.T, server *realTMUXServer) string {
	t.Helper()
	attached := exec.CommandContext(t.Context(), server.command,
		"-S", server.socket, "attach-session", "-t", "notification-test")
	attached.Env = tmuxTestEnvironment()
	terminal, err := pty.Start(attached)
	require.NoError(t, err)
	drained := make(chan struct{})
	go func() {
		_, _ = io.Copy(io.Discard, terminal)
		close(drained)
	}()
	t.Cleanup(func() {
		_ = attached.Process.Kill()
		_ = terminal.Close()
		_ = attached.Wait()
		<-drained
	})
	var client string
	require.Eventually(t, func() bool {
		client = strings.TrimSpace(runRealTMUX(t, server.command,
			"-S", server.socket, "list-clients", "-F", "#{client_tty}"))
		return client != ""
	}, 2*time.Second, 10*time.Millisecond)
	return client
}

func TestTMUXAdapterRealServerRejectsMissingClient(t *testing.T) {
	server := startRealTMUXServer(t)
	const missingClient = "/dev/pts/gifford-missing"
	adapter, err := NewTMUXAdapter(TMUXOptions{
		Command: server.command, Socket: server.socket, Client: missingClient,
		Duration: time.Second, Timeout: time.Second,
	})
	require.NoError(t, err)

	fixture := newNotificationFixture(t)
	service := newNotificationService(t, fixture, adapter, 91, nil)
	require.NoError(t, service.Initialize(t.Context()))
	messageID := notificationMessageID(91)
	insertSourceMessage(t, fixture, sourceMessage{
		id: messageID, control: protocol.ControlStateAllowed,
		recipients: []protocol.DeliveryMode{protocol.DeliveryModeWhenSafe}, envelope: true,
	})
	_, err = service.RunOnce(t.Context())
	require.NoError(t, err)

	var state State
	var reason FailureReason
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT state,
		last_error_reason FROM human_notifications
		WHERE class = 'MESSAGE' AND object_id = ?`, messageID).Scan(&state, &reason))
	require.Equal(t, StatePending, state)
	require.Equal(t, FailureClientUnavailable, reason)
	require.Zero(t, eventCount(t, fixture, protocol.EventTypeHumanNotificationRendered))
}

func TestTMUXAdapterClientDisconnectBeforeDisplayStaysPending(t *testing.T) {
	server := startRealTMUXServer(t)
	client := attachRealTMUXClient(t, server)
	t.Setenv("GIFFORD_TEST_REAL_TMUX", server.command)
	t.Setenv("GIFFORD_TEST_TMUX_SOCKET", server.socket)
	t.Setenv("GIFFORD_TEST_TMUX_CLIENT", client)
	wrapper := filepath.Join(t.TempDir(), "tmux-disconnect")
	require.NoError(t, os.WriteFile(wrapper, []byte(`#!/bin/sh
if [ "$3" = "display-message" ]; then
  "$GIFFORD_TEST_REAL_TMUX" -S "$GIFFORD_TEST_TMUX_SOCKET" detach-client -t "$GIFFORD_TEST_TMUX_CLIENT" || exit $?
  attempts=0
  while [ "$attempts" -lt 100 ]; do
    clients=$("$GIFFORD_TEST_REAL_TMUX" -S "$GIFFORD_TEST_TMUX_SOCKET" list-clients -F '#{client_tty}') || exit $?
    attached=false
    for current in $clients; do
      if [ "$current" = "$GIFFORD_TEST_TMUX_CLIENT" ]; then
        attached=true
        break
      fi
    done
    if [ "$attached" = false ]; then
      break
    fi
    attempts=$((attempts + 1))
    sleep 0.02
  done
  if [ "$attached" = true ]; then
    echo "timed out waiting for tmux client to detach" >&2
    exit 70
  fi
fi
exec "$GIFFORD_TEST_REAL_TMUX" "$@"
`), 0o700))
	adapter, err := NewTMUXAdapter(TMUXOptions{
		Command: wrapper, Socket: server.socket, Client: client,
		Duration: time.Second, Timeout: time.Second,
	})
	require.NoError(t, err)

	fixture := newNotificationFixture(t)
	service := newNotificationService(t, fixture, adapter, 97, nil)
	require.NoError(t, service.Initialize(t.Context()))
	messageID := notificationMessageID(97)
	insertSourceMessage(t, fixture, sourceMessage{
		id: messageID, control: protocol.ControlStateAllowed,
		recipients: []protocol.DeliveryMode{protocol.DeliveryModeWhenSafe}, envelope: true,
	})
	_, err = service.RunOnce(t.Context())
	require.NoError(t, err)
	require.Empty(t, strings.TrimSpace(runRealTMUX(t, server.command,
		"-S", server.socket, "list-clients", "-F", "#{client_tty}")))

	var state State
	var reason sql.NullString
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT state,
		last_error_reason FROM human_notifications
		WHERE class = 'MESSAGE' AND object_id = ?`, messageID).Scan(&state, &reason))
	require.Equal(t, StatePending, state)
	require.True(t, reason.Valid)
	require.Equal(t, FailureClientUnavailable, FailureReason(reason.String))
	require.Zero(t, eventCount(t, fixture, protocol.EventTypeHumanNotificationRendered))
}

func TestTMUXAdapterStoppedServerHonorsTimeout(t *testing.T) {
	server := startRealTMUXServer(t)
	server.stop(t)
	const adapterTimeout = 50 * time.Millisecond
	adapter, err := NewTMUXAdapter(TMUXOptions{
		Command: server.command, Socket: server.socket, Client: "/dev/pts/gifford-test",
		Duration: time.Second, Timeout: adapterTimeout,
	})
	require.NoError(t, err)

	type renderResult struct {
		err     error
		elapsed time.Duration
	}
	result := make(chan renderResult, 1)
	go func() {
		started := time.Now()
		result <- renderResult{
			err:     adapter.Render(t.Context(), validAdapterNotification()),
			elapsed: time.Since(started),
		}
	}()
	const fixedMargin = time.Second
	var got renderResult
	select {
	case got = <-result:
	case <-time.After(adapterTimeout + fixedMargin):
		server.resume()
		got = <-result
		t.Fatalf("render exceeded timeout margin: %s", got.elapsed)
	}
	server.resume()

	var adapterErr *AdapterError
	require.ErrorAs(t, got.err, &adapterErr)
	require.Equal(t, FailureAdapterTimeout, adapterErr.Reason)
	require.True(t, adapterErr.Retryable)
	require.Less(t, got.elapsed, adapterTimeout+fixedMargin)
}
