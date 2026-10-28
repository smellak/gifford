// Package run implements the `gifford run` subcommand: it has no behaviour
// yet.
package run

import (
	"github.com/spf13/cobra"

	"github.com/smellak/gifford/internal/cli/deps"
)

// NewCmd returns the gifford run command.
func NewCmd(_ *deps.Deps) *cobra.Command {
	cmd := deps.Leaf("run", "PTY proxy for one agent")
	cmd.Flags().String("agent", "", "agent id to run the proxy for")
	cmd.Flags().String("harness", "", "harness profile to emulate")
	return cmd
}
