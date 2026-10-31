// Package keycap implements the `gifford keycap` subcommand: it guides
// a human key by key while the harness runs under the PTY core, records
// the exact bytes of each key, and writes profiles/<id>.yaml with the
// measured sequences, the negotiated modes and the detected harness
// version. Guided keys never reach the harness, and out-of-tour typing
// is swallowed too, so nothing typed is ever sent; harness output flows
// to the terminal untouched. keycap keeps no recording: only the
// negotiated-mode markers are scanned from the output stream.
package keycap

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"regexp"
	"time"

	"github.com/spf13/cobra"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/cli/exitcode"
	"github.com/smellak/gifford/internal/proxy"
)

// flowParams carries everything the measurement loop needs. Zero waits
// mean the defaults; tests set short ones. OutDir holds
// profiles/<id>.yaml, "profiles" from the repository root.
type flowParams struct {
	Harness        string
	Command        []string
	In, Out        *os.File
	ErrW           io.Writer
	OutDir         string
	EscWait        time.Duration
	PasteIdle      time.Duration
	VersionTimeout time.Duration
	DrainWait      time.Duration
}

// idPattern admits profile file names: lowercase letters, digits and
// dashes, so the id is always a safe profiles/<id>.yaml name.
var idPattern = regexp.MustCompile(`^[a-z0-9-]+$`)

// NewCmd returns the gifford keycap command.
func NewCmd(_ *deps.Deps) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "keycap --harness <id> [-- <command> [args...]]",
		Short: "Measure a harness's keyboard behaviour to build its profile",
		Long: `Guide a human key by key while the harness runs under the PTY core,
record the exact bytes of every key, and write profiles/<id>.yaml with
the measured sequences, the negotiated modes and the detected harness
version. Guided keys never reach the harness, so nothing typed is sent.

The harness is the command after --; without it, the harness id itself
is looked up on PATH. The harness version comes from "<command> --version".
The profile's input_line, input_placeholder and input_bounds are never
measured: they are preserved from the existing file, or input_line is
scaffolded for a new one, and stay hand-tuned.`,
		Args: cobra.ArbitraryArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			harness, err := cmd.Flags().GetString("harness")
			if err != nil {
				return &deps.ExitError{
					Code: exitcode.Usage,
					Err:  fmt.Errorf("gifford keycap: %w", err),
				}
			}
			if harness == "" || !idPattern.MatchString(harness) {
				return &deps.ExitError{
					Code: exitcode.Usage,
					Err:  fmt.Errorf("gifford keycap: --harness is required, lowercase letters, digits and dashes (profiles/<id>.yaml)"),
				}
			}
			command, err := resolveCommand(cmd, harness, args)
			if err != nil {
				return err
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
			if err := runFlow(cmd.Context(), flowParams{
				Harness: harness,
				Command: command,
				In:      in,
				Out:     out,
				ErrW:    cmd.ErrOrStderr(),
				OutDir:  "profiles",
			}); err != nil {
				return exitError(err)
			}
			return nil
		},
	}
	cmd.Flags().String("harness", "", "harness profile to measure (profiles/<id>.yaml)")
	cmd.SetFlagErrorFunc(func(_ *cobra.Command, err error) error {
		return &deps.ExitError{
			Code: exitcode.Usage,
			Err:  fmt.Errorf("gifford keycap: %w", err),
		}
	})
	return cmd
}

// resolveCommand finds the harness command: the words after --, or the
// harness id itself on PATH. Anything else on the command line is a
// usage error, like gifford run requires --.
func resolveCommand(cmd *cobra.Command, harness string, args []string) ([]string, error) {
	var command []string
	switch dash := cmd.ArgsLenAtDash(); {
	case dash < 0 && len(args) > 0:
		return nil, &deps.ExitError{
			Code: exitcode.Usage,
			Err:  errors.New("gifford keycap: expected -- <command> [args...] after the flags"),
		}
	case dash > 0:
		return nil, &deps.ExitError{
			Code: exitcode.Usage,
			Err:  errors.New("gifford keycap: expected -- <command> [args...] after the flags"),
		}
	case dash == 0:
		command = args
	}
	if len(command) == 0 {
		if _, err := exec.LookPath(harness); err != nil {
			return nil, &deps.ExitError{
				Code: exitcode.Usage,
				Err:  fmt.Errorf("gifford keycap: harness %q not on PATH, pass -- <command> [args...]", harness),
			}
		}
		return []string{harness}, nil
	}
	return command, nil
}

func exitError(err error) error {
	if errors.Is(err, errors.ErrUnsupported) {
		return &deps.ExitError{
			Code: exitcode.NotImplemented,
			Err:  fmt.Errorf("gifford keycap: %w", err),
		}
	}
	return &deps.ExitError{
		Code: exitcode.Error,
		Err:  fmt.Errorf("gifford keycap: %w", err),
	}
}
