package run

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/cli/exitcode"
	"github.com/smellak/gifford/internal/proxy"
)

// agentFlag selects the agent id in the run tests.
const agentFlag = "--agent"

func TestCommandLine(t *testing.T) {
	t.Parallel()

	cases := []struct {
		args string
		code int
	}{
		{"run", exitcode.Usage},
		{"run claude", exitcode.Usage},
		{"run --", exitcode.Usage},
		{"run x -- y", exitcode.Usage},
		{"run --bogus -- y", exitcode.Usage},
		{"run --agent", exitcode.Usage},
	}
	for _, tc := range cases {
		t.Run(tc.args, func(t *testing.T) {
			t.Parallel()
			cmd := NewCmd(&deps.Deps{})
			cmd.SetOut(io.Discard)
			cmd.SetErr(io.Discard)
			cmd.SetIn(strings.NewReader(""))
			cmd.SetArgs(strings.Fields(tc.args))
			var ec *deps.ExitError
			require.ErrorAs(t, cmd.Execute(), &ec)
			require.Equal(t, tc.code, ec.Code)
		})
	}
}

// TestRunAgentInvalid refuses a bad agent id as usage before starting
// anything: no session, no recording, no child.
func TestRunAgentInvalid(t *testing.T) {
	// Serial: it sets process environment.
	dataHome := t.TempDir()
	t.Setenv("XDG_DATA_HOME", dataHome)

	in, _, err := os.Pipe()
	require.NoError(t, err)
	_, out, err := os.Pipe()
	require.NoError(t, err)
	cmd := NewCmd(&deps.Deps{})
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	cmd.SetIn(in)
	cmd.SetOut(out)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{agentFlag, "NOT AN AGENT!", "--", "true"})
	var ec *deps.ExitError
	require.ErrorAs(t, cmd.Execute(), &ec)
	require.Equal(t, exitcode.Usage, ec.Code)
	require.ErrorContains(t, ec.Err, "agent")
	_, statErr := os.Stat(filepath.Join(dataHome, "gifford"))
	require.True(t, os.IsNotExist(statErr), "invalid --agent must not create anything")
}

func TestExitError(t *testing.T) {
	t.Parallel()

	// nil -> nil
	require.NoError(t, exitError(proxy.Exit{}, nil))

	// harness exited 7
	err := exitError(proxy.Exit{Code: 7}, nil)
	var ec *deps.ExitError
	require.ErrorAs(t, err, &ec)
	require.Equal(t, 7, ec.Code)
	require.ErrorContains(t, ec.Err, "harness exited with status 7")

	// killed by signal
	err = exitError(proxy.Exit{Code: -1, Signal: 15}, nil)
	require.ErrorAs(t, err, &ec)
	require.Equal(t, 143, ec.Code)

	// unsupported
	err = exitError(proxy.Exit{}, errors.ErrUnsupported)
	require.ErrorAs(t, err, &ec)
	require.Equal(t, exitcode.NotImplemented, ec.Code)

	// not terminal
	err = exitError(proxy.Exit{}, proxy.ErrNotTerminal)
	require.ErrorAs(t, err, &ec)
	require.Equal(t, exitcode.Error, ec.Code)

	// other error with exit status 0 still is error
	err = exitError(proxy.Exit{Code: 0}, errors.New("boom"))
	require.ErrorAs(t, err, &ec)
	require.Equal(t, exitcode.Error, ec.Code)

	// a canceled context overrides the harness status with gifford's own error
	err = exitError(proxy.Exit{Code: 7}, context.Canceled)
	require.ErrorAs(t, err, &ec)
	require.Equal(t, exitcode.Error, ec.Code)
}
