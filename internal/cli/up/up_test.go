package up

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/cli/exitcode"
)

func TestUpWithoutDevRemainsExplicitlyUnimplemented(t *testing.T) {
	cmd := NewCmd(&deps.Deps{})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	var exit *deps.ExitError
	require.ErrorAs(t, cmd.Execute(), &exit)
	require.Equal(t, exitcode.NotImplemented, exit.Code)
	require.Contains(t, exit.Error(), "use --dev")
}

func TestUpDevRunsUnifiedNodeInForeground(t *testing.T) {
	base := t.TempDir()
	configHome := filepath.Join(base, "config")
	runtimeHome := filepath.Join(base, "run")
	t.Setenv("XDG_CONFIG_HOME", configHome)
	t.Setenv("XDG_DATA_HOME", filepath.Join(base, "data"))
	t.Setenv("XDG_RUNTIME_DIR", runtimeHome)
	require.NoError(t, os.MkdirAll(filepath.Join(configHome, "gifford"), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(configHome, "gifford", "config.yaml"),
		[]byte("schema_version: 1\nnode_id: node-up-dev\n"), 0o600))

	ctx, cancel := context.WithCancel(t.Context())
	cmd := NewCmd(&deps.Deps{})
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"--dev"})
	done := make(chan error, 1)
	go func() { done <- cmd.ExecuteContext(ctx) }()
	runtimeDir := filepath.Join(runtimeHome, "gifford")
	require.Eventually(t, func() bool {
		_, hubErr := os.Stat(filepath.Join(runtimeDir, "hub.sock"))
		_, daemonErr := os.Stat(filepath.Join(runtimeDir, "daemon.sock"))
		return hubErr == nil && daemonErr == nil
	}, 10*time.Second, 10*time.Millisecond)
	cancel()
	require.NoError(t, <-done)
}
