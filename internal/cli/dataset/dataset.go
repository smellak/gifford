// Package dataset implements the `gifford dataset` subcommand tree: build,
// inspect and redact the Analyst's dataset. It has no behaviour yet.
package dataset

import (
	"github.com/spf13/cobra"

	"github.com/smellak/gifford/internal/cli/deps"
)

// NewCmd returns the gifford dataset command.
func NewCmd(_ *deps.Deps) *cobra.Command {
	build := withVersionFlag(deps.Leaf("build", "Build a versioned training dataset from labeled captures"))
	inspect := withVersionFlag(deps.Leaf("inspect", "Inspect a dataset version's contents and statistics"))
	redact := withVersionFlag(deps.Leaf("redact", "Apply the local redactor to a dataset before export"))
	return deps.Group("dataset", "Build, inspect and redact the Analyst's dataset", build, inspect, redact)
}

// withVersionFlag adds the shared --version flag to a dataset subcommand.
func withVersionFlag(cmd *cobra.Command) *cobra.Command {
	cmd.Flags().String("version", "", "dataset version")
	return cmd
}
