// Package daemon implements the `gifford daemon` subcommand: it has no
// behaviour yet.
package daemon

import (
	"github.com/spf13/cobra"

	"github.com/smellak/gifford/internal/cli/deps"
)

// NewCmd returns the gifford daemon command.
func NewCmd(_ *deps.Deps) *cobra.Command {
	return deps.Leaf("daemon", "Node state, queues, Analyst, Orchestrator and notifications")
}
