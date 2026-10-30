// Package daemon implements the `gifford daemon` alias for the foreground
// node process.
package daemon

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/cli/exitcode"
	hubcmd "github.com/smellak/gifford/internal/cli/hub"
)

// NewCmd returns the gifford daemon command.
func NewCmd(_ *deps.Deps) *cobra.Command {
	return &cobra.Command{
		Use:   "daemon",
		Short: "Run the node, queues, Orchestrator and local sockets",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := hubcmd.Run(cmd.Context(), cmd.ErrOrStderr()); err != nil {
				return &deps.ExitError{
					Code: exitcode.Error,
					Err:  fmt.Errorf("gifford daemon: %w", err),
				}
			}
			return nil
		},
	}
}
