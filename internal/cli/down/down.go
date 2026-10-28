// Package down implements the `gifford down` subcommand: it has no
// behaviour yet.
package down

import (
	"github.com/spf13/cobra"

	"github.com/smellak/gifford/internal/cli/deps"
)

// NewCmd returns the gifford down command.
func NewCmd(_ *deps.Deps) *cobra.Command {
	return deps.Leaf("down", "Stop the node's supervised services")
}
