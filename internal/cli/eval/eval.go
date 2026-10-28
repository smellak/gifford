// Package eval implements the `gifford eval` subcommand: it has no
// behaviour yet.
package eval

import (
	"github.com/spf13/cobra"

	"github.com/smellak/gifford/internal/cli/deps"
)

// NewCmd returns the gifford eval command.
func NewCmd(_ *deps.Deps) *cobra.Command {
	cmd := deps.Leaf("eval", "Evaluate Glimmer (base or Analyst LoRA) and the protocol")
	cmd.Flags().String("profile", "", "inference profile to evaluate")
	cmd.Flags().String("split", "", "dataset split to evaluate on")
	cmd.Flags().String("out", "", "where to write the results report")
	return cmd
}
