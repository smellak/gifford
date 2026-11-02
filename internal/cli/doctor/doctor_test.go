package doctor

import (
	"bytes"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/cli/exitcode"
	"github.com/smellak/gifford/internal/socketpath"
)

const socketsReportLabel = "SOCKETS"

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

func TestDoctorReportsSocketCapacityAndAgentBoundary(t *testing.T) {
	for _, test := range []struct {
		name       string
		agent      string
		wantErr    bool
		wantReport []string
	}{
		{
			name: "capacity without agent",
			wantReport: []string{
				socketsReportLabel, "OK", "max_agent_bytes native-events=4 agents=11 adapters=9",
			},
		},
		{
			name:  "agent at exact limit",
			agent: "aaaa",
			wantReport: []string{
				socketsReportLabel, "OK", "agent=aaaa fits",
			},
		},
		{
			name:    "agent one byte over",
			agent:   "aaaaa",
			wantErr: true,
			wantReport: []string{
				socketsReportLabel, "FAIL", "agent=aaaaa", "native-events: socket path ",
				" is " + strconv.Itoa(socketpath.MaxBytes+1) + " bytes, the limit is " +
					strconv.Itoa(socketpath.MaxBytes),
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			runtimeDir := doctorSocketEnvironment(t, socketpath.MaxBytes-24)
			cmd := NewCmd(&deps.Deps{})
			var out bytes.Buffer
			cmd.SetOut(&out)
			if test.agent != "" {
				cmd.SetArgs([]string{"--agent", test.agent})
			} else {
				cmd.SetArgs([]string{})
			}

			err := cmd.Execute()
			if test.wantErr {
				var exitErr *deps.ExitError
				require.ErrorAs(t, err, &exitErr)
				require.Equal(t, exitcode.Error, exitErr.Code)
			} else {
				require.NoError(t, err)
			}
			require.Len(t, filepath.Join(runtimeDir, "native-events", test.agent+".sock"),
				socketpath.MaxBytes+len(test.agent)-4)
			for _, token := range test.wantReport {
				require.Contains(t, out.String(), token)
			}
		})
	}
}

func TestDoctorFailsBeforeDaemonTemporaryPathCanOverflow(t *testing.T) {
	runtimeDir := doctorSocketEnvironment(t, socketpath.MaxBytes-23)
	cmd := NewCmd(&deps.Deps{})
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetArgs([]string{})

	var exitErr *deps.ExitError
	require.ErrorAs(t, cmd.Execute(), &exitErr)
	require.Equal(t, exitcode.Error, exitErr.Code)
	report := out.String()
	require.Contains(t, report, socketsReportLabel)
	require.Contains(t, report, "FAIL")
	worstCase := filepath.Join(runtimeDir, ".daemon.sock-9999999999")
	require.Len(t, worstCase, socketpath.MaxBytes+1)
	require.Contains(t, report, worstCase)
	require.Contains(t, report,
		"is "+strconv.Itoa(socketpath.MaxBytes+1)+" bytes, the limit is "+strconv.Itoa(socketpath.MaxBytes))
}

func doctorSocketEnvironment(t *testing.T, runtimeLength int) string {
	t.Helper()
	t.Setenv("HOME", t.TempDir())
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	runtimeHomeLength := runtimeLength - len("/gifford")
	base, err := os.MkdirTemp("", "gd") //nolint:usetesting // exact Unix socket lengths need a short stable base
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(base)) })
	padding := runtimeHomeLength - len(base) - 1
	require.Positive(t, padding)
	runtimeHome := filepath.Join(base, strings.Repeat("r", padding))
	require.Len(t, runtimeHome, runtimeHomeLength)
	t.Setenv("XDG_RUNTIME_DIR", runtimeHome)
	runtimeDir := filepath.Join(runtimeHome, "gifford")
	require.Len(t, runtimeDir, runtimeLength)
	return runtimeDir
}
