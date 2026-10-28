// Package initcmd implements the `gifford init` subcommand: it has no
// behaviour yet.
package initcmd

import (
	"github.com/spf13/cobra"

	"github.com/smellak/gifford/internal/cli/deps"
)

// NewCmd returns the gifford init command.
func NewCmd(_ *deps.Deps) *cobra.Command {
	return deps.Leaf("init", "Guided setup of the node; produces config.yaml")
}
