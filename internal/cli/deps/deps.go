// Package deps holds what every gifford subcommand package needs to build
// itself without importing package cli (which would be an import cycle,
// since cli imports every subcommand package to assemble the root command).
package deps

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/smellak/gifford/internal/cli/exitcode"
)

// Deps holds the dependencies that subcommands share. It grows as packages
// land (config service, clock, logger); subcommands never read globals.
type Deps struct{}

// ExitError carries a specific exit code up to Main.
type ExitError struct {
	Code int
	Err  error
}

func (e *ExitError) Error() string { return e.Err.Error() }
func (e *ExitError) Unwrap() error { return e.Err }

// Leaf returns a cobra.Command that accepts any arguments and always fails
// with exitcode.NotImplemented. Every command of the tree that has no
// behaviour yet is built with this.
func Leaf(use, short string) *cobra.Command {
	return &cobra.Command{
		Use:   use,
		Short: short,
		Args:  cobra.ArbitraryArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return &ExitError{
				Code: exitcode.NotImplemented,
				Err:  fmt.Errorf("%s: not implemented yet", cmd.CommandPath()),
			}
		},
	}
}

// Group returns a cobra.Command that only dispatches to children. It has no
// RunE, so cobra's default behaviour applies: running it with no recognised
// child prints help and exits 0. Only leaf commands return NotImplemented.
func Group(use, short string, children ...*cobra.Command) *cobra.Command {
	cmd := &cobra.Command{Use: use, Short: short}
	cmd.AddCommand(children...)
	return cmd
}
