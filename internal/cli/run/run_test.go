package run

import (
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/cli/exitcode"
	"github.com/smellak/gifford/internal/proxy"
)

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
}
