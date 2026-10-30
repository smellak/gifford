package capture

import (
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/cli/exitcode"
)

func TestCaptureNotImplemented(t *testing.T) {
	t.Parallel()

	for _, args := range []string{
		"attach --agent agent-1 --tmux-pane %1 --harness claude",
		"attach --agent agent-1 --tmux-pane %1 --harness claude --replace",
		"status",
		"status --agent agent-1",
		"stop --agent agent-1",
		"stop --agent agent-1 --reason done",
		"reconcile",
		// A directory, the day0 format and asciicast v3 have no import yet.
		"import . --format day0 --agent agent-1 --harness codex --environment-domain bank_generic",
		"import . --format asciicast-v2 --agent agent-1 --harness codex --environment-domain bank_generic",
		"import missing.cast --format asciicast-v3 --agent agent-1 --harness codex --environment-domain fleet_managed",
		"export --session s1 --format asciicast-v3",
		"supervise",
		"ingest --session s1",
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

func TestCaptureBareGroupShowsHelp(t *testing.T) {
	t.Parallel()

	cmd := NewCmd(&deps.Deps{})
	cmd.SetOut(io.Discard)
	cmd.SetArgs([]string{})
	require.NoError(t, cmd.Execute())
}

func TestIngestIsHidden(t *testing.T) {
	t.Parallel()

	cmd := NewCmd(&deps.Deps{})
	var found bool
	for _, c := range cmd.Commands() {
		if c.Name() == "ingest" {
			found = true
			require.True(t, c.Hidden)
		}
	}
	require.True(t, found, "ingest subcommand must exist")
}
