// Package up implements the `gifford up` subcommand: it has no behaviour
// yet.
package up

import (
	"github.com/spf13/cobra"

	"github.com/smellak/gifford/internal/cli/deps"
)

// NewCmd returns the gifford up command.
func NewCmd(_ *deps.Deps) *cobra.Command {
	cmd := deps.Leaf("up", "Start and supervise the node's services")
	cmd.Flags().Bool("dev", false, "supervise daemon, hub and relay for development")
	return cmd
}
