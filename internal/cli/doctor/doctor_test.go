package doctor

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/cli/exitcode"
)

func TestDoctorCleanHome(t *testing.T) {
	home := t.TempDir()
	cfgHome := t.TempDir()
	dataHome := t.TempDir()
	runHome := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_CONFIG_HOME", cfgHome)
	t.Setenv("XDG_DATA_HOME", dataHome)
	t.Setenv("XDG_RUNTIME_DIR", runHome)

	cmd := NewCmd(&deps.Deps{})
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetArgs([]string{})
	require.NoError(t, cmd.Execute())

	report := out.String()
	for _, token := range []string{"CONFIG", "DATA", "RUNTIME", "CONFIG.YAML", "OK"} {
		require.Contains(t, report, token)
	}

	for _, p := range []string{
		filepath.Join(cfgHome, "gifford"),
		filepath.Join(dataHome, "gifford"),
		filepath.Join(runHome, "gifford"),
	} {
		info, err := os.Stat(p)
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o700), info.Mode().Perm(), p)
	}
}

func TestDoctorWrongModeReported(t *testing.T) {
	home := t.TempDir()
	cfgHome := t.TempDir()
	dataHome := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_CONFIG_HOME", cfgHome)
	t.Setenv("XDG_DATA_HOME", dataHome)
	t.Setenv("XDG_RUNTIME_DIR", "")

	// Pre-create the runtime fallback directory with the wrong mode.
	runDir := filepath.Join(dataHome, "gifford", "run")
	require.NoError(t, os.MkdirAll(runDir, 0o755))

	cmd := NewCmd(&deps.Deps{})
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetArgs([]string{})

	var ec *deps.ExitError
	require.ErrorAs(t, cmd.Execute(), &ec)
	require.Equal(t, exitcode.Error, ec.Code)
	require.Contains(t, out.String(), "RUNTIME")
	require.Contains(t, out.String(), "FAIL")

	info, err := os.Stat(runDir)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o755), info.Mode().Perm())
}

func TestDoctorDataFileNotDirectory(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	dataHome := t.TempDir()
	t.Setenv("XDG_DATA_HOME", dataHome)
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())

	dataFile := filepath.Join(dataHome, "gifford")
	require.NoError(t, os.WriteFile(dataFile, nil, 0o700))

	cmd := NewCmd(&deps.Deps{})
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetArgs([]string{})

	var ec *deps.ExitError
	require.ErrorAs(t, cmd.Execute(), &ec)
	require.Equal(t, exitcode.Error, ec.Code)
	report := out.String()
	require.Contains(t, report, "DATA")
	require.Contains(t, report, "FAIL")
	require.Contains(t, report, "not a directory")

	info, err := os.Stat(dataFile)
	require.NoError(t, err)
	require.True(t, info.Mode().IsRegular())
	require.Equal(t, os.FileMode(0o700), info.Mode().Perm())
}

func TestDoctorRuntimeFileNotDirectory(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	runHome := t.TempDir()
	t.Setenv("XDG_RUNTIME_DIR", runHome)

	runFile := filepath.Join(runHome, "gifford")
	require.NoError(t, os.WriteFile(runFile, nil, 0o700))

	cmd := NewCmd(&deps.Deps{})
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetArgs([]string{})

	var ec *deps.ExitError
	require.ErrorAs(t, cmd.Execute(), &ec)
	require.Equal(t, exitcode.Error, ec.Code)
	report := out.String()
	require.Contains(t, report, "RUNTIME")
	require.Contains(t, report, "FAIL")
	require.Contains(t, report, "not a directory")

	info, err := os.Stat(runFile)
	require.NoError(t, err)
	require.True(t, info.Mode().IsRegular())
	require.Equal(t, os.FileMode(0o700), info.Mode().Perm())
}

const doctorTmuxLocksConfig = `tmux_locks:
  peer_owner: peer_writer
  peer_writers:
    - peer_dispatch
    - peer_writer
  host: host-7
  mode_path: /etc/example/pane-mode
  admin_uids_path: /etc/example/pane-admin-uids
  operation_schema: example-pane-op/1
  initial_take_drain: 30s
`

func runDoctorWithConfig(t *testing.T, document string) (string, error) {
	t.Helper()
	return runDoctorWithConfigAndPanelLockRoot(t, document, filepath.Join(t.TempDir(), "absent"))
}

func runDoctorWithConfigAndPanelLockRoot(
	t *testing.T,
	document string,
	panelLockRoot string,
) (string, error) {
	t.Helper()
	home := t.TempDir()
	t.Setenv("HOME", home)
	cfgHome := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", cfgHome)
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
	require.NoError(t, os.MkdirAll(filepath.Join(cfgHome, "gifford"), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(cfgHome, "gifford", "config.yaml"), []byte(document), 0o600))

	cmd := newCmd(panelLockRoot)
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetArgs([]string{})
	err := cmd.Execute()
	return out.String(), err
}

func TestDoctorTmuxLocksWithoutWriterLogWarns(t *testing.T) {
	report, err := runDoctorWithConfig(t, doctorTmuxLocksConfig)
	require.NoError(t, err)
	require.Contains(t, report, "WARN")
	require.Contains(t, report, "writer_log_socket")
}

func TestDoctorTmuxLocksWithWriterLogDoesNotWarn(t *testing.T) {
	report, err := runDoctorWithConfig(t, doctorTmuxLocksConfig+"  writer_log_socket: /run/example/writer-log.sock\n")
	require.NoError(t, err)
	require.NotContains(t, report, "WARN")
}

func TestDoctorPanelLockRootWithoutTmuxLocksWarns(t *testing.T) {
	report, err := runDoctorWithConfigAndPanelLockRoot(t, "", t.TempDir())
	require.NoError(t, err)
	require.Contains(t, report, "TMUXLOCKS")
	require.Contains(t, report, "WARN")
	require.Contains(t, report, "the node will reject all tmux panel writes")
	require.Contains(t, report, "add a tmux_locks: block to config.yaml")
}

func TestDoctorPanelLockRootWithTmuxLocksDoesNotWarn(t *testing.T) {
	report, err := runDoctorWithConfigAndPanelLockRoot(t,
		doctorTmuxLocksConfig+"  writer_log_socket: /run/example/writer-log.sock\n",
		t.TempDir(),
	)
	require.NoError(t, err)
	require.NotContains(t, report, "WARN")
}

func TestDoctorWithoutPanelLockRootDoesNotWarn(t *testing.T) {
	report, err := runDoctorWithConfigAndPanelLockRoot(t, "", filepath.Join(t.TempDir(), "absent"))
	require.NoError(t, err)
	require.NotContains(t, report, "WARN")
}

func TestDoctorPeerFlag(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())

	cmd := NewCmd(&deps.Deps{})
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"--peer", "node-b"})
	require.NoError(t, cmd.Execute())
	require.Contains(t, out.String(), "SKIP")
}
