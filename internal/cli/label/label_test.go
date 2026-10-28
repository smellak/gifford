package label

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/cli/exitcode"
)

func TestLabelNotImplemented(t *testing.T) {
	t.Parallel()

	for _, args := range []string{
		"transcript",
		"annotate",
		"align",
	} {
		t.Run(args, func(t *testing.T) {
			t.Parallel()

			cmd := NewCmd(&deps.Deps{})
			cmd.SetOut(io.Discard)
			cmd.SetArgs(strings.Fields(args))

			var ec *deps.ExitError
			require.ErrorAs(t, cmd.Execute(), &ec)
			require.Equal(t, exitcode.NotImplemented, ec.Code)
		})
	}
}

func TestLabelBareGroupShowsHelp(t *testing.T) {
	t.Parallel()

	cmd := NewCmd(&deps.Deps{})
	cmd.SetOut(io.Discard)
	cmd.SetArgs([]string{})
	require.NoError(t, cmd.Execute())
}

func TestHookAlwaysExitsZero(t *testing.T) {
	t.Parallel()

	for _, args := range []string{
		"hook",
		"hook --event PreToolUse --foo",
	} {
		t.Run(args, func(t *testing.T) {
			t.Parallel()

			cmd := NewCmd(&deps.Deps{})
			var out bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetIn(strings.NewReader(`{"event":"test"}`))
			cmd.SetArgs(strings.Fields(args))
			require.NoError(t, cmd.Execute())
			require.Empty(t, out.String())
		})
	}
}
