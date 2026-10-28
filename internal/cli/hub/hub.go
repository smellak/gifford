// Package hub implements the `gifford hub` subcommand: it has no behaviour
// yet.
package hub

import (
	"github.com/spf13/cobra"

	"github.com/smellak/gifford/internal/cli/deps"
)

// NewCmd returns the gifford hub command.
func NewCmd(_ *deps.Deps) *cobra.Command {
	return deps.Leaf("hub", "Local messaging and federation")
}
