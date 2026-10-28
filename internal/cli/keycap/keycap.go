// Package keycap implements the `gifford keycap` subcommand: it has no
// behaviour yet.
package keycap

import (
	"github.com/spf13/cobra"

	"github.com/smellak/gifford/internal/cli/deps"
)

// NewCmd returns the gifford keycap command.
func NewCmd(_ *deps.Deps) *cobra.Command {
	cmd := deps.Leaf("keycap", "Measure a harness's keyboard behaviour to build its profile")
	cmd.Flags().String("harness", "", "harness profile to measure")
	return cmd
}
