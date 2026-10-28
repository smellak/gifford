// Package fuzz implements the `gifford fuzz` subcommand: it has no
// behaviour yet.
package fuzz

import (
	"github.com/spf13/cobra"

	"github.com/smellak/gifford/internal/cli/deps"
)

// NewCmd returns the gifford fuzz command.
func NewCmd(_ *deps.Deps) *cobra.Command {
	cmd := deps.Leaf("fuzz", "Run the keyboard-vs-injection race fuzzer")
	cmd.Flags().Int("scenarios", 0, "number of race scenarios to run")
	cmd.Flags().Bool("baseline", false, "also run the naive-injection baseline")
	return cmd
}
