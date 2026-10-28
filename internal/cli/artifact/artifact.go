// Package artifact implements the `gifford artifact` subcommand tree: send
// and track artifacts. It has no behaviour yet.
package artifact

import (
	"github.com/spf13/cobra"

	"github.com/smellak/gifford/internal/cli/deps"
)

// NewCmd returns the gifford artifact command.
func NewCmd(_ *deps.Deps) *cobra.Command {
	send := deps.Leaf("send <file>", "Send a local file to another agent as an artifact")
	send.Flags().String("to", "", "agent to send the artifact to")
	send.Flags().String("purpose", "", "why the artifact is sent")

	return deps.Group("artifact", "Send and track artifacts", send)
}
