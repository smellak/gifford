// Package down implements `gifford down`: it stops the node gifford up
// started.
package down

import (
	"errors"
	"fmt"
	"io"

	"github.com/spf13/cobra"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/cli/exitcode"
	"github.com/smellak/gifford/internal/cli/nodeproc"
	"github.com/smellak/gifford/internal/xdg"
)

// NewCmd returns the gifford down command.
func NewCmd(_ *deps.Deps) *cobra.Command {
	return &cobra.Command{
		Use:   "down",
		Short: "Stop the node gifford up started",
		Long: `Stop the node gifford up started. It sends SIGTERM to the exact hub
process the PID file in the runtime directory names, then waits up to 30 s
until the hub releases the runtime lock. A node that is not running is not
an error. A node gifford up did not start, such as gifford hub in a
terminal, is left alone.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return run(cmd.OutOrStdout())
		},
	}
}

func run(out io.Writer) error {
	dirs, err := xdg.Resolve()
	if err != nil {
		return exit(exitcode.Error, err)
	}
	command, err := nodeproc.NodeCommand()
	if err != nil {
		return exit(exitcode.Error, err)
	}
	pid, err := nodeproc.Stop(dirs, command, nodeproc.StopTimeout)
	switch {
	case errors.Is(err, nodeproc.ErrNotRunning):
		return say(out, "gifford down: the node is not running\n")
	case errors.Is(err, nodeproc.ErrNotStartedByUp):
		return exit(exitcode.Refused, fmt.Errorf("%w; stop it where it runs", err))
	case errors.Is(err, nodeproc.ErrUnsupported):
		return exit(exitcode.NotImplemented, err)
	case err != nil:
		return exit(exitcode.Error, err)
	}
	return say(out, "gifford down: node stopped (PID %d)\n", pid)
}

func say(out io.Writer, format string, args ...any) error {
	if _, err := fmt.Fprintf(out, format, args...); err != nil {
		return exit(exitcode.Error, err)
	}
	return nil
}

func exit(code int, err error) error {
	return &deps.ExitError{Code: code, Err: fmt.Errorf("gifford down: %w", err)}
}
