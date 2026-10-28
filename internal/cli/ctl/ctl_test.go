package ctl

import (
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/cli/exitcode"
)

func TestCtlNotImplemented(t *testing.T) {
	t.Parallel()

	for _, args := range []string{
		"pause agent-1",
		"resume agent-1",
		"hold-message m1",
		"deliver-next agent-1",
		"inspect obj-1",
		"retry-artifact art-1",
		"unlock agent-1",
		"revoke-node node-b",
		"replay obj-1",
		"model status",
		"model activate adapter-1 v1",
		"model rollback",
		"channel status",
		"channel set active",
		"channel set draining --then off",
		"channel set-node node-b paused",
		"channel explain --message m1",
		"channel explain --from a1 --to a2",
		"agent agent-1",
		"agent agent-1 channel status",
		"link set a1 a2 allow",
		"link set a1 a2 deny --bidirectional",
		"link status a1 a2",
		"link list",
		"link list --agent a1",
		"link clear a1 a2",
		"loop create --thread t1 --participants a1,a2 --mode auto",
		"loop status loop-1",
		"loop list",
		"loop list --active",
		"loop set-mode loop-1 auto",
		"loop next loop-1",
		"loop budget loop-1 --add-turns 2",
		"loop stop loop-1 --reason done",
		"loop complete loop-1",
		"approvals",
		"approvals --pending",
		"approve ap-1",
		"reject ap-1 --reason no",
		"approve-message m1",
		"reject-message m1 --reason no",
		"release-hold m1",
	} {
		t.Run(args, func(t *testing.T) {
			t.Parallel()

			cmd := NewCmd(&deps.Deps{})
			cmd.SetOut(io.Discard)
			cmd.SetErr(io.Discard)
			cmd.SetArgs(strings.Fields(args))

			var ec *deps.ExitError
			require.ErrorAs(t, cmd.Execute(), &ec)
			require.Equal(t, exitcode.NotImplemented, ec.Code)
		})
	}
}

func TestCtlBareGroupsShowHelp(t *testing.T) {
	t.Parallel()

	for _, args := range []string{
		"",
		"model",
		"channel",
		"link",
		"loop",
	} {
		t.Run("group/"+args, func(t *testing.T) {
			t.Parallel()

			cmd := NewCmd(&deps.Deps{})
			cmd.SetOut(io.Discard)
			cmd.SetArgs(strings.Fields(args))
			require.NoError(t, cmd.Execute())
		})
	}
}
