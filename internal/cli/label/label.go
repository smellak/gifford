// Package label implements the `gifford label` subcommand tree: ground
// truth labeling to evaluate the Analyst. `hook` records Claude Code hook
// events and always exits 0 by design; `annotate` is the manual labeling
// TUI. Everything stays in labels/ of the local data dir.
package label

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"
	"golang.org/x/term"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/cli/exitcode"
	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/label"
	"github.com/smellak/gifford/internal/xdg"
)

// sourceOpener opens the windows to annotate.
type sourceOpener func(ctx context.Context) (dataset.RecordingSource, error)

// noRecordingReader is the opener until the recording readers land (S1).
func noRecordingReader(context.Context) (dataset.RecordingSource, error) {
	return nil, &deps.ExitError{
		Code: exitcode.NotImplemented,
		Err:  errors.New("gifford label annotate: no recording reader yet"),
	}
}

// NewCmd returns the gifford label command.
func NewCmd(_ *deps.Deps) *cobra.Command {
	return newCmd(noRecordingReader)
}

func newCmd(open sourceOpener) *cobra.Command {
	return deps.Group("label", "Ground truth labeling to evaluate the Analyst",
		newHookCmd(),
		deps.Leaf("transcript", "Read a CLI's own session transcript to derive weak labels"),
		newAnnotateCmd(open),
		deps.Leaf("align", "Align collected labels with the recorded PTY output by wall-clock time"),
	)
}

// newHookCmd returns the `gifford label hook` command. Hook events arrive on
// stdin from Claude Code itself, so this command always exits 0 and never
// writes to stdout: its output would re-enter the calling model's context,
// and a nonzero exit could block the tool that triggered the hook. A
// failure is noted on stderr only.
func newHookCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "hook",
		Short: "Record a Claude Code hook event from stdin (always exits 0)",
		Args:  cobra.ArbitraryArgs,
		// Hooks may pass flags we do not know; parsing them would turn an
		// unknown flag into exit 1, and this command must always exit 0.
		DisableFlagParsing: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := recordHook(cmd.InOrStdin()); err != nil {
				_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "gifford label hook: %v\n", err)
			}
			return nil
		},
	}
}

func recordHook(stdin io.Reader) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()
	dirs, err := xdg.Resolve()
	if err != nil {
		_, _ = io.Copy(io.Discard, stdin)
		return fmt.Errorf("resolve data dir: %w", err)
	}
	if err := label.RecordHook(label.Dir(dirs.Data), clock.System{}.Now(), stdin); err != nil {
		return fmt.Errorf("record: %w", err)
	}
	return nil
}

func newAnnotateCmd(open sourceOpener) *cobra.Command {
	var annotator string
	cmd := &cobra.Command{
		Use:   "annotate",
		Short: "Manual annotation TUI for ambiguous windows (keys 1-7)",
		Long: "Shows each window's reconstructed screen and activity and reads one key:\n" +
			"1-7 label it A-G, s skips it, u undoes, q saves and quits. Labels go to\n" +
			"labels/" + label.AnnotationsFile + " in the data dir, never the screen.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runAnnotate(cmd, open, annotator)
		},
	}
	cmd.Flags().StringVar(&annotator, "annotator", "", "who labels (1-64 of A-Z a-z 0-9 . _ @ -)")
	_ = cmd.MarkFlagRequired("annotator")
	return cmd
}

func runAnnotate(cmd *cobra.Command, open sourceOpener, annotator string) error {
	// A bad name is a usage error whatever the source: check it first.
	if err := label.CheckAnnotator(annotator); err != nil {
		return &deps.ExitError{Code: exitcode.Usage, Err: err}
	}
	dirs, err := xdg.Resolve()
	if err != nil {
		return fmt.Errorf("resolve data dir: %w", err)
	}
	src, err := open(cmd.Context())
	if err != nil {
		return err
	}
	keys := cmd.InOrStdin()
	if f, ok := keys.(*os.File); ok && term.IsTerminal(int(f.Fd())) {
		old, err := term.MakeRaw(int(f.Fd()))
		if err != nil {
			return fmt.Errorf("raw terminal: %w", err)
		}
		defer func() { _ = term.Restore(int(f.Fd()), old) }()
	}
	anns, annErr := label.Annotate(cmd.Context(), label.AnnotateOptions{
		Source:    src,
		Keys:      keys,
		Screen:    cmd.OutOrStdout(),
		Annotator: annotator,
		Clock:     clock.System{},
	})
	// Labels made before a failure are kept.
	if err := label.SaveAnnotations(label.Dir(dirs.Data), anns); err != nil {
		return errors.Join(annErr, fmt.Errorf("save labels: %w", err))
	}
	if annErr != nil {
		return fmt.Errorf("gifford label annotate: %w", annErr)
	}
	_, _ = fmt.Fprintf(cmd.OutOrStdout(), "\r\nsaved %d labels\r\n", len(anns))
	return nil
}
