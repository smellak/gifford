// Package up implements `gifford up`: it starts the node in the
// background, detached from the terminal, and returns once the hub is
// ready. With --dev the node runs in the foreground instead.
package up

import (
	"errors"
	"fmt"
	"io"
	"path/filepath"

	"github.com/spf13/cobra"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/cli/exitcode"
	hubcmd "github.com/smellak/gifford/internal/cli/hub"
	"github.com/smellak/gifford/internal/cli/nodeproc"
	"github.com/smellak/gifford/internal/config"
	"github.com/smellak/gifford/internal/xdg"
)

// NewCmd returns the gifford up command.
func NewCmd(_ *deps.Deps) *cobra.Command {
	var dev bool
	cmd := &cobra.Command{
		Use:   "up",
		Short: "Start the node in the background",
		Long: `Start the node in the background: the same process as gifford hub,
detached from this terminal, with its output in hub.log in the data
directory. It returns once the hub prints its ready line, and fails if the
hub stops first or is not ready within 60 s. A node that already runs is
left as it is. gifford down stops the node gifford up started.

--dev runs the node in the foreground instead, until Ctrl-C.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if dev {
				if err := hubcmd.Run(cmd.Context(), cmd.ErrOrStderr()); err != nil {
					return failed(fmt.Errorf("gifford up --dev: %w", err))
				}
				return nil
			}
			return background(cmd.OutOrStdout())
		},
	}
	cmd.Flags().BoolVar(&dev, "dev", false, "run the node in the foreground")
	return cmd
}

func background(out io.Writer) error {
	dirs, err := xdg.Resolve()
	if err != nil {
		return failed(fmt.Errorf("gifford up: %w", err))
	}
	path := config.DefaultPath(dirs.Config)
	cfg, err := config.Load(path)
	if err != nil {
		return failed(fmt.Errorf("gifford up: %w", err))
	}
	if !cfg.NodeID.Valid() {
		return failed(fmt.Errorf("gifford up: %s has no node_id; run gifford init first", path))
	}
	if _, err := xdg.EnsureAll(dirs); err != nil {
		return failed(fmt.Errorf("gifford up: %w; see gifford doctor", err))
	}
	command, err := nodeproc.NodeCommand()
	if err != nil {
		return failed(fmt.Errorf("gifford up: %w", err))
	}
	pid, err := nodeproc.Start(dirs, command, nodeproc.ReadyTimeout)
	switch {
	case errors.Is(err, nodeproc.ErrRunning):
		return say(out, "gifford up: the node is already running\n")
	case errors.Is(err, nodeproc.ErrUnsupported):
		return &deps.ExitError{Code: exitcode.NotImplemented, Err: fmt.Errorf("gifford up: %w; use --dev", err)}
	case err != nil:
		return failed(fmt.Errorf("gifford up: %w", err))
	}
	return say(out, "gifford up: node ready (PID %d), log %s\n", pid, filepath.Join(dirs.Data, nodeproc.LogFile))
}

func say(out io.Writer, format string, args ...any) error {
	if _, err := fmt.Fprintf(out, format, args...); err != nil {
		return failed(fmt.Errorf("gifford up: %w", err))
	}
	return nil
}

func failed(err error) error { return &deps.ExitError{Code: exitcode.Error, Err: err} }
