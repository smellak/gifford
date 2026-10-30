package panel

import (
	"io"
	"strings"
	"testing"

	"github.com/creack/pty"
	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/cli/exitcode"
)

func TestPanelRequiresInteractiveInput(t *testing.T) {
	t.Parallel()

	cmd := NewCmd(&deps.Deps{})
	cmd.SetIn(strings.NewReader(""))
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)

	var exit *deps.ExitError
	require.ErrorAs(t, cmd.Execute(), &exit)
	require.Equal(t, exitcode.Error, exit.Code)
	require.ErrorContains(t, exit, "stdin is not an interactive terminal")
}

func TestPanelRequiresInteractiveOutput(t *testing.T) {
	t.Parallel()

	master, terminal, err := pty.Open()
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, terminal.Close())
		require.NoError(t, master.Close())
	})

	cmd := NewCmd(&deps.Deps{})
	cmd.SetIn(terminal)
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)

	var exit *deps.ExitError
	require.ErrorAs(t, cmd.Execute(), &exit)
	require.Equal(t, exitcode.Error, exit.Code)
	require.ErrorContains(t, exit, "stdout is not an interactive terminal")
}

func TestPanelRejectsArguments(t *testing.T) {
	t.Parallel()

	cmd := NewCmd(&deps.Deps{})
	cmd.SetArgs([]string{"unexpected"})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)

	require.ErrorContains(t, cmd.Execute(), "unknown command")
}
