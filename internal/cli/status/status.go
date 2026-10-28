// Package status implements the `gifford status` subcommand: it has no
// behaviour yet.
package status

import (
	"github.com/spf13/cobra"

	"github.com/smellak/gifford/internal/cli/deps"
)

// NewCmd returns the gifford status command.
func NewCmd(_ *deps.Deps) *cobra.Command {
	return deps.Leaf("status", "Report whether the node's services are running")
}
