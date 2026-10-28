package ctl

import (
	"github.com/spf13/cobra"

	"github.com/smellak/gifford/internal/cli/deps"
)

// newLinkCmd returns the `gifford ctl link` group: operate directional
// links between agents.
func newLinkCmd() *cobra.Command {
	set := deps.Leaf("set <from> <to> <allow|hold|require-approval|deny>", "Set the directional link mode between two agents")
	set.Flags().String("plane", "", "plane the link mode applies to")
	set.Flags().Bool("bidirectional", false, "apply the mode in both directions")
	set.Flags().Duration("ttl", 0, "how long the override applies")
	set.Flags().String("reason", "", "why the link mode is set")

	list := deps.Leaf("list", "List directional links, optionally filtered by agent")
	list.Flags().String("agent", "", "only list links involving this agent")

	return deps.Group("link", "Operate directional links between agents",
		set,
		deps.Leaf("status <from> <to>", "Show the link state between two agents"),
		list,
		deps.Leaf("clear <from> <to>", "Remove a link override and fall back to the default"),
	)
}
