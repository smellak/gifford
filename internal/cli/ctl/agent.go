package ctl

import (
	"github.com/spf13/cobra"

	"github.com/smellak/gifford/internal/cli/deps"
)

// newAgentCmd returns the `gifford ctl agent` command. Cobra cannot dispatch
// past an arbitrary agent id to a subcommand, so `agent` carries its own
// RunE (exit 3 for any arguments) and the `channel` child below only exists
// so `--help` documents the full surface.
func newAgentCmd() *cobra.Command {
	agent := deps.Leaf("agent <id>", "Operate one agent's channel")
	agent.AddCommand(deps.Group("channel", "Operate one agent's channel switch",
		deps.Leaf("status", "Show one agent's channel state"),
		deps.Leaf("set", "Set one agent's channel state"),
		deps.Leaf("mode", "Show one agent's channel mode"),
	))
	return agent
}
