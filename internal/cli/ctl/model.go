package ctl

import (
	"github.com/spf13/cobra"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/protocol"
)

const modelStatusUse = "status"

// newModelCmd returns the `gifford ctl model` group: manage the active
// model adapter.
func newModelCmd() *cobra.Command {
	return deps.Group("model", "Manage the active model adapter",
		newModelStatusCmd(),
		deps.Leaf("activate <adapter> <revision>", "Activate an adapter revision (only if its gate passed); the activation is recorded"),
		deps.Leaf("rollback", "Roll back to the base model immediately, without a redeploy"),
	)
}

func newModelStatusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   modelStatusUse,
		Short: "Show the observed model profile and deployment metadata",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			var result protocol.ModelStatusResult
			if err := call(cmd, protocol.ControlMethodModelStatus, protocol.EmptyParams{}, &result); err != nil {
				return err
			}
			return printJSON(cmd, result)
		},
	}
}
