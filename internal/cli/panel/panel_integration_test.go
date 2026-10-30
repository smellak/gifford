package panel

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/charmbracelet/x/ansi"
	"github.com/creack/pty"
	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/cli/ctl"
	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/controlapi"
	"github.com/smellak/gifford/internal/eventstore"
)

func TestPanelShowsCtlChannelChange(t *testing.T) {
	configHome := t.TempDir()
	runtimeHome := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", configHome)
	t.Setenv("XDG_RUNTIME_DIR", runtimeHome)
	configDir := filepath.Join(configHome, "gifford")
	require.NoError(t, os.Mkdir(configDir, 0o700))
	require.NoError(t, os.WriteFile(
		filepath.Join(configDir, "config.yaml"),
		[]byte("schema_version: 1\nnode_id: node-a\n"),
		0o600,
	))
	startPanelControlServer(t, filepath.Join(runtimeHome, "gifford"))

	master, terminal, err := pty.Open()
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = terminal.Close()
		_ = master.Close()
	})
	require.NoError(t, pty.Setsize(master, &pty.Winsize{Rows: 40, Cols: 120}))

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	command := NewCmd(&deps.Deps{})
	command.SetIn(terminal)
	command.SetOut(terminal)
	command.SetErr(io.Discard)
	done := make(chan error, 1)
	go func() { done <- command.ExecuteContext(ctx) }()
	requirePanelOutput(t, master, "ACTIVE", 2*time.Second)

	control := ctl.NewCmd(&deps.Deps{})
	control.SetOut(io.Discard)
	control.SetErr(io.Discard)
	control.SetArgs([]string{"channel", "set", "paused", "--reason", "panel refresh test"})
	started := time.Now()
	require.NoError(t, control.ExecuteContext(t.Context()))
	requirePanelOutput(t, master, "PAUSED", time.Second)
	require.Less(t, time.Since(started), time.Second)

	_, err = master.Write([]byte("q"))
	require.NoError(t, err)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		cancel()
		t.Fatal("panel did not exit after q")
	}
}

func startPanelControlServer(t *testing.T, runtimeDir string) {
	t.Helper()

	store, err := eventstore.Open(t.Context(), filepath.Join(t.TempDir(), "gifford.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	engine, err := channel.New(channel.Options{Clock: clock.System{}})
	require.NoError(t, err)
	controller, err := channel.NewController(channel.ControllerOptions{
		DB: store.DB(), Engine: engine, Clock: clock.System{}, NodeID: "node-a",
		Entropy: bytes.NewReader(make([]byte, 1024)),
	})
	require.NoError(t, err)
	require.NoError(t, controller.Reload(t.Context()))
	server, err := controlapi.New(controlapi.Options{
		RuntimeDir: runtimeDir,
		DB:         store.DB(),
		Controller: controller,
	})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	waitForPanelSocket(t, server.Path())
	t.Cleanup(func() {
		cancel()
		require.NoError(t, <-done)
	})
}

func waitForPanelSocket(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := os.Stat(path); err == nil {
			return
		} else if !errors.Is(err, os.ErrNotExist) {
			require.NoError(t, err)
		}
		require.True(t, time.Now().Before(deadline), "control socket was not created")
		time.Sleep(5 * time.Millisecond)
	}
}

func requirePanelOutput(t *testing.T, terminal *os.File, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var output strings.Builder
	buffer := make([]byte, 4096)
	for time.Now().Before(deadline) {
		require.NoError(t, terminal.SetReadDeadline(time.Now().Add(100*time.Millisecond)))
		n, err := terminal.Read(buffer)
		if n > 0 {
			output.Write(buffer[:n])
			if strings.Contains(ansi.Strip(output.String()), want) {
				return
			}
		}
		var networkError net.Error
		if err != nil && (!errors.As(err, &networkError) || !networkError.Timeout()) {
			require.NoError(t, err)
		}
	}
	t.Fatalf("panel output did not contain %q; stripped output: %q", want, ansi.Strip(output.String()))
}
