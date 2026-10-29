// Package run implements the `gifford run` subcommand: it runs the command
// after -- on a child PTY through the session wrapper and exits with its
// status.
package run

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/cli/exitcode"
	"github.com/smellak/gifford/internal/proxy"
	"github.com/smellak/gifford/internal/xdg"
)

// sessionParams carries what runSession needs on every platform: the
// harness command, the profile name, the outer terminal and the notice
// writer.
type sessionParams struct {
	Command []string
	Harness string
	In      *os.File
	Out     *os.File
	Notice  io.Writer
}

// runSession runs the harness wrapped in the session on unix, or straight
// through the proxy core where no PTY wrapper exists. The platform files
// own it; run.go only defines its parameters above.

// daemonSocketName is the daemon's socket in the runtime directory. A
// placeholder: the subphase that builds the daemon owns the name.
const daemonSocketName = "daemon.sock"

// NewCmd returns the gifford run command.
func NewCmd(_ *deps.Deps) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "run [flags] -- <command> [args...]",
		Short: "PTY proxy for one agent",
		Long: `Run <command> on a child pseudo-terminal and copy every byte between it and
this terminal unchanged. Everything after -- is the command line.

If the gifford daemon is not running, one line on stderr says so before the
command starts. --agent is accepted and not used yet; --harness selects
the harness profile that parses the keys.

Exit status: the command's own status, or 128+N when signal N killed it.
gifford's own failures exit 1 (error), 2 (usage) or 3 (not implemented on
this platform).`,
		Args: cobra.ArbitraryArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if cmd.ArgsLenAtDash() != 0 || len(args) == 0 {
				return &deps.ExitError{
					Code: exitcode.Usage,
					Err:  errors.New("gifford run: expected -- <command> [args...]"),
				}
			}
			in, ok := cmd.InOrStdin().(*os.File)
			if !ok || in == nil {
				return &deps.ExitError{
					Code: exitcode.Error,
					Err:  fmt.Errorf("stdin: %w", proxy.ErrNotTerminal),
				}
			}
			out, ok := cmd.OutOrStdout().(*os.File)
			if !ok || out == nil {
				return &deps.ExitError{
					Code: exitcode.Error,
					Err:  fmt.Errorf("stdout: %w", proxy.ErrNotTerminal),
				}
			}
			harness, err := cmd.Flags().GetString("harness")
			if err != nil {
				return &deps.ExitError{
					Code: exitcode.Usage,
					Err:  fmt.Errorf("gifford run: %w", err),
				}
			}
			exit, err := runSession(cmd.Context(), sessionParams{
				Command: args,
				Harness: harness,
				In:      in,
				Out:     out,
				Notice:  cmd.ErrOrStderr(),
			})
			return exitError(exit, err)
		},
	}
	cmd.Flags().String("agent", "", "agent id to run the proxy for")
	cmd.Flags().String("harness", "", "harness profile to parse the keys with")
	cmd.SetFlagErrorFunc(func(_ *cobra.Command, err error) error {
		return &deps.ExitError{
			Code: exitcode.Usage,
			Err:  fmt.Errorf("gifford run: %w", err),
		}
	})
	return cmd
}

func daemonSocket() string {
	dirs, err := xdg.Resolve()
	if err != nil {
		return ""
	}
	return filepath.Join(dirs.Runtime, daemonSocketName)
}

func exitError(exit proxy.Exit, err error) error {
	if err != nil {
		if errors.Is(err, errors.ErrUnsupported) {
			return &deps.ExitError{
				Code: exitcode.NotImplemented,
				Err:  fmt.Errorf("gifford run: %w", err),
			}
		}
		return &deps.ExitError{
			Code: exitcode.Error,
			Err:  fmt.Errorf("gifford run: %w", err),
		}
	}
	if exit.Status() != 0 {
		return &deps.ExitError{
			Code: exit.Status(),
			Err:  fmt.Errorf("gifford run: %s", exit.String()),
		}
	}
	return nil
}
