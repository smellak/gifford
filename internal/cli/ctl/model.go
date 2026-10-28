package ctl

import (
	"github.com/spf13/cobra"

	"github.com/smellak/gifford/internal/cli/deps"
)

// newModelCmd returns the `gifford ctl model` group: manage the active
// model adapter.
func newModelCmd() *cobra.Command {
	return deps.Group("model", "Manage the active model adapter",
		deps.Leaf("status", "Show the active adapter, its revisions and gate status"),
		deps.Leaf("activate <adapter> <revision>", "Activate an adapter revision (only if its gate passed); the activation is recorded"),
		deps.Leaf("rollback", "Roll back to the base model immediately, without a redeploy"),
	)
}
