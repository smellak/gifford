//go:build unix

package run

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/creack/pty"
	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/nativeevents"
)

func configureNativeEventRun(t *testing.T, runtimeHome string) {
	t.Helper()
	configHome := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", configHome)
	t.Setenv("XDG_RUNTIME_DIR", runtimeHome)
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	t.Setenv("GORACE", "atexit_sleep_ms=0")
	dir := filepath.Join(configHome, "gifford")
	require.NoError(t, os.MkdirAll(dir, 0o700))
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, "config.yaml"),
		[]byte("schema_version: 1\nnode_id: node-native\n"), 0o600,
	))
	startFakeHub(t, filepath.Join(runtimeHome, "gifford", "hub.sock"))
}

func runNativeEnvironmentHelper(
	t *testing.T,
	agent string,
	reportPath string,
) string {
	t.Helper()
	outer, slave, err := pty.Open()
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = outer.Close()
		_ = slave.Close()
	})
	require.NoError(t, pty.Setsize(outer, &pty.Winsize{Rows: 24, Cols: 80}))
	cmd := NewCmd(nil)
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	cmd.SetIn(slave)
	cmd.SetOut(slave)
	var stderr strings.Builder
	cmd.SetErr(&stderr)
	cmd.SetArgs(append(
		[]string{agentFlag, agent, harnessFlag, genericHarness, "--"},
		cliHelperArgvExtra("native-environment", reportPath)...,
	))
	require.NoError(t, cmd.Execute())
	return stderr.String()
}

func TestRunCreatesAndPassesPrivateNativeEventEntry(t *testing.T) {
	runtimeHome := t.TempDir()
	configureNativeEventRun(t, runtimeHome)
	t.Setenv(nativeevents.SocketEnvironment, "/stale/inherited.sock")
	reportPath := filepath.Join(t.TempDir(), "native-entry.txt")
	stderr := runNativeEnvironmentHelper(t, "agent-native", reportPath)
	require.NotContains(t, stderr, "no native event entry")

	report, err := os.ReadFile(reportPath)
	require.NoError(t, err)
	lines := strings.Split(strings.TrimSpace(string(report)), "\n")
	require.Equal(t, []string{
		filepath.Join(runtimeHome, "gifford", "native-events", "agent-native.sock"),
		"700", "600",
	}, lines)
	_, err = os.Stat(lines[0])
	require.ErrorIs(t, err, os.ErrNotExist, "the entry lives exactly as long as the run")
}

func TestRunNativeEntryFailureKeepsHarnessRunningWithoutStaleEnv(t *testing.T) {
	runtimeHome := t.TempDir()
	configureNativeEventRun(t, runtimeHome)
	badDir := filepath.Join(runtimeHome, "gifford", "native-events")
	require.NoError(t, os.MkdirAll(badDir, 0o755))
	require.NoError(t, os.Chmod(badDir, 0o755))
	t.Setenv(nativeevents.SocketEnvironment, "/stale/inherited.sock")
	reportPath := filepath.Join(t.TempDir(), "native-entry.txt")
	stderr := runNativeEnvironmentHelper(t, "agent-native-fallback", reportPath)
	require.Contains(t, stderr, "gifford: no native event entry for this run:")
	report, err := os.ReadFile(reportPath)
	require.NoError(t, err)
	require.Equal(t, "unset\n", string(report))
}
