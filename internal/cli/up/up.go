// Package up implements the foreground development node entry point.
package up

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/cli/exitcode"
	hubcmd "github.com/smellak/gifford/internal/cli/hub"
)

// NewCmd returns the gifford up command.
func NewCmd(_ *deps.Deps) *cobra.Command {
	var dev bool
	cmd := &cobra.Command{
		Use:   "up",
		Short: "Start the node's services",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if !dev {
				return &deps.ExitError{
					Code: exitcode.NotImplemented,
					Err:  fmt.Errorf("gifford up: service installation is not implemented; use --dev for the foreground node"),
				}
			}
			if err := hubcmd.Run(cmd.Context(), cmd.ErrOrStderr()); err != nil {
				return &deps.ExitError{
					Code: exitcode.Error,
					Err:  fmt.Errorf("gifford up --dev: %w", err),
				}
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&dev, "dev", false, "run the unified node in the foreground")
	return cmd
}
