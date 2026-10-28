// Package relay implements the `gifford relay` subcommand: it has no
// behaviour yet.
package relay

import (
	"github.com/spf13/cobra"

	"github.com/smellak/gifford/internal/cli/deps"
)

// NewCmd returns the gifford relay command.
func NewCmd(_ *deps.Deps) *cobra.Command {
	return deps.Leaf("relay", "Artifact Relay")
}
