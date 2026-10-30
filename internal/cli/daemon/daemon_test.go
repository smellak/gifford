package daemon

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/cli/deps"
	hubcmd "github.com/smellak/gifford/internal/cli/hub"
)

func TestDaemonRunsUnifiedNode(t *testing.T) {
	configHome, runtimeHome := daemonTestEnvironment(t)
	require.NoError(t, os.MkdirAll(filepath.Join(configHome, "gifford"), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(configHome, "gifford", "config.yaml"),
		[]byte("schema_version: 1\nnode_id: node-daemon\n"), 0o600))

	ctx, cancel := context.WithCancel(t.Context())
	cmd := NewCmd(&deps.Deps{})
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	done := make(chan error, 1)
	go func() { done <- cmd.ExecuteContext(ctx) }()
	runtimeDir := filepath.Join(runtimeHome, "gifford")
	require.Eventually(t, func() bool {
		_, hubErr := os.Stat(filepath.Join(runtimeDir, "hub.sock"))
		_, daemonErr := os.Stat(filepath.Join(runtimeDir, "daemon.sock"))
		return hubErr == nil && daemonErr == nil
	}, 10*time.Second, 10*time.Millisecond)
	second := hubcmd.NewCmd(&deps.Deps{})
	second.SetOut(io.Discard)
	second.SetErr(io.Discard)
	err := second.ExecuteContext(t.Context())
	require.ErrorContains(t, err, "another hub")
	cancel()
	require.NoError(t, <-done)
}

func daemonTestEnvironment(t *testing.T) (string, string) {
	t.Helper()
	base := t.TempDir()
	configHome := filepath.Join(base, "config")
	runtimeHome := filepath.Join(base, "run")
	t.Setenv("XDG_CONFIG_HOME", configHome)
	t.Setenv("XDG_DATA_HOME", filepath.Join(base, "data"))
	t.Setenv("XDG_RUNTIME_DIR", runtimeHome)
	return configHome, runtimeHome
}
