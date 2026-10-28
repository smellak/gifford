// Package label implements the `gifford label` subcommand tree: ground
// truth labeling to evaluate the Analyst. It has no behaviour yet, except
// for `hook`, which always exits 0 by design.
package label

import (
	"io"

	"github.com/spf13/cobra"

	"github.com/smellak/gifford/internal/cli/deps"
)

// NewCmd returns the gifford label command.
func NewCmd(_ *deps.Deps) *cobra.Command {
	return deps.Group("label", "Ground truth labeling to evaluate the Analyst",
		newHookCmd(),
		deps.Leaf("transcript", "Read a CLI's own session transcript to derive weak labels"),
		deps.Leaf("annotate", "Manual annotation TUI for ambiguous windows (keys 1-7)"),
		deps.Leaf("align", "Align collected labels with the recorded PTY output by wall-clock time"),
	)
}

// newHookCmd returns the `gifford label hook` command. Hook events arrive on
// stdin from Claude Code itself, so this command always exits 0 and never
// writes to stdout: its output would re-enter the calling model's context,
// and a nonzero exit could block the tool that triggered the hook.
func newHookCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "hook",
		Short: "Record a Claude Code hook event from stdin (always exits 0)",
		Args:  cobra.ArbitraryArgs,
		// Hooks may pass flags we do not know; parsing them would turn an
		// unknown flag into exit 1, and this command must always exit 0.
		DisableFlagParsing: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			_, _ = io.Copy(io.Discard, cmd.InOrStdin())
			return nil
		},
	}
}
