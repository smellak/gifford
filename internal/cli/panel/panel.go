// Package panel implements the `gifford panel` subcommand: it has no
// behaviour yet.
package panel

import (
	"github.com/spf13/cobra"

	"github.com/smellak/gifford/internal/cli/deps"
)

// NewCmd returns the gifford panel command.
func NewCmd(_ *deps.Deps) *cobra.Command {
	return deps.Leaf("panel", "TUI for observation and governance")
}
