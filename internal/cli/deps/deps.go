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

// NotImplementedAnnotation marks every command Leaf builds, so the tree's
// stubs can be listed.
const NotImplementedAnnotation = "gifford/not-implemented"

// notImplementedNotice is what a command without behaviour says.
const notImplementedNotice = "not implemented in v0.1-hackathon, see README#roadmap"

// NotImplemented is the error of a command, or an option of one, that has
// no behaviour yet. It exits with exitcode.NotImplemented.
func NotImplemented(cmd *cobra.Command) error {
	return &ExitError{
		Code: exitcode.NotImplemented,
		Err:  fmt.Errorf("%s: %s", cmd.CommandPath(), notImplementedNotice),
	}
}

// Leaf returns a cobra.Command that accepts any arguments and always fails
// with NotImplemented. Every command of the tree that has no behaviour yet
// is built with this. It is hidden from --help, and so is a group left
// with no other command.
func Leaf(use, short string) *cobra.Command {
	return &cobra.Command{
		Use:         use,
		Short:       short,
		Hidden:      true,
		Annotations: map[string]string{NotImplementedAnnotation: "true"},
		Args:        cobra.ArbitraryArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return NotImplemented(cmd)
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
