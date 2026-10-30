// Package label implements the `gifford label` subcommand tree: ground
// truth labeling to evaluate the Analyst. `hook` records projected harness
// events and always exits 0 by design; `weak import` validates auditable weak
// labels; `annotate` is the manual labeling TUI; `align` matches the sources
// to recorded windows. Everything stays in labels/ of the local data dir.
package label

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"

	"github.com/spf13/cobra"
	"golang.org/x/term"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/cli/exitcode"
	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/dataset/replay"
	"github.com/smellak/gifford/internal/label"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy/session"
	"github.com/smellak/gifford/internal/xdg"
)

const captureSessionEnv = "GIFFORD_CAPTURE_SESSION_ID"

// sourceOpener opens the windows to label; warn takes one line per
// session it had to skip.
type sourceOpener func(ctx context.Context, dataDir string, warn io.Writer) (dataset.RecordingSource, error)

// recordingReader replays every `gifford run` session in the data dir. A
// session that does not replay whole (still running, cut, out of order,
// dropped events) is skipped with a warning and yields no window.
func recordingReader(_ context.Context, dataDir string, warn io.Writer) (dataset.RecordingSource, error) {
	ids, err := session.ListSessions(dataDir)
	if err != nil {
		return nil, fmt.Errorf("list recordings: %w", err)
	}
	srcs := make([]dataset.RecordingSource, 0, len(ids))
	for _, id := range ids {
		src, err := replay.Open(replay.Options{DataDir: dataDir, SessionID: id})
		if err != nil {
			_, _ = fmt.Fprintf(warn, "gifford label: session %s skipped: %v\n", id, err)
			continue
		}
		srcs = append(srcs, src)
	}
	return &chain{srcs: srcs}, nil
}

// chain reads its sources one after the other.
type chain struct{ srcs []dataset.RecordingSource }

func (c *chain) Next(ctx context.Context) (dataset.Recording, error) {
	for len(c.srcs) > 0 {
		rec, err := c.srcs[0].Next(ctx)
		if !errors.Is(err, io.EOF) {
			return rec, err //nolint:wrapcheck // the source's own error
		}
		c.srcs = c.srcs[1:]
	}
	return dataset.Recording{}, io.EOF
}

// NewCmd returns the gifford label command.
func NewCmd(_ *deps.Deps) *cobra.Command {
	return newCmd(recordingReader)
}

func newCmd(open sourceOpener) *cobra.Command {
	return deps.Group("label", "Ground truth labeling to evaluate the Analyst",
		newHookCmd(),
		newWeakCmd(),
		deps.Leaf("transcript", "Read a CLI's own session transcript to derive weak labels"),
		newAnnotateCmd(open),
		newAlignCmd(open),
	)
}

func newWeakCmd() *cobra.Command {
	return deps.Group("weak", "Import auditable weak labels", newWeakImportCmd())
}

func newWeakImportCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "import <file>",
		Short: "Validate and append weak labels from a JSONL file",
		Long: "Validates the entire JSONL file before appending any record to labels/" + label.WeakFile + ".\n\n" +
			"Under " + label.WeakCriterionV1 + ", method " + label.WeakMethodAgentReview +
			", labels A-F require reason " + string(label.WeakReasonVisibleState) +
			" and evidence observed in the window, not the intended script. Label G\n" +
			"requires reason " + string(label.WeakReasonNoDeterminant) +
			" and is valid only after reviewing a valid, complete, sanitised window\n" +
			"that truly has no determinant state indicator. Missing hooks or root links,\n" +
			"label conflicts, blank screens and lost captures do not justify G.\n\n" +
			"reviewer must match [a-z0-9][a-z0-9-]{0,63}; labeled_at must be RFC3339\n" +
			"UTC. source may be omitted or be weak; stored records always say weak.\n" +
			"Import does not enable weak.keep, which remains off by default.",
		Args: func(_ *cobra.Command, args []string) error {
			if len(args) != 1 {
				return &deps.ExitError{Code: exitcode.Usage, Err: errors.New("gifford label weak import: expected one file")}
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			f, err := os.Open(args[0])
			if err != nil {
				return fmt.Errorf("gifford label weak import: %w", err)
			}
			defer func() { _ = f.Close() }()
			dirs, err := xdg.Resolve()
			if err != nil {
				return fmt.Errorf("gifford label weak import: %w", err)
			}
			n, err := label.ImportWeak(label.Dir(dirs.Data), f)
			if err != nil {
				return fmt.Errorf("gifford label weak import: %w", err)
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "imported %d weak labels\n", n)
			return err //nolint:wrapcheck // writing stdout needs no extra context
		},
	}
}

// newHookCmd returns the `gifford label hook <harness>` command. Hook events
// arrive on stdin from the harness itself, so this command always exits 0 and never
// writes to stdout: its output would re-enter the calling model's context,
// and a nonzero exit could block the tool that triggered the hook. A
// failure is noted on stderr only.
func newHookCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "hook <harness>",
		Short: "Record a harness hook event from stdin (always exits 0)",
		Args:  cobra.ArbitraryArgs,
		// Hooks may pass flags we do not know; parsing them would turn an
		// unknown flag into exit 1, and this command must always exit 0.
		DisableFlagParsing: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 || !protocol.Harness(args[0]).Valid() {
				_, _ = io.Copy(io.Discard, cmd.InOrStdin())
				_, _ = fmt.Fprintln(cmd.ErrOrStderr(), "gifford label hook: invalid harness")
				return nil
			}
			captureID := protocol.OpaqueID(os.Getenv(captureSessionEnv))
			if !label.ValidCaptureSessionID(captureID) {
				_, _ = io.Copy(io.Discard, cmd.InOrStdin())
				_, _ = fmt.Fprintln(cmd.ErrOrStderr(), "gifford label hook: invalid capture session")
				return nil
			}
			if err := recordHook(protocol.Harness(args[0]), captureID, cmd.InOrStdin()); err != nil {
				_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "gifford label hook: %v\n", err)
			}
			return nil
		},
	}
}

func recordHook(harness protocol.Harness, captureID protocol.OpaqueID, stdin io.Reader) (err error) {
	defer func() {
		if recover() != nil {
			err = errors.New("internal failure")
		}
	}()
	dirs, err := xdg.Resolve()
	if err != nil {
		_, _ = io.Copy(io.Discard, stdin)
		return fmt.Errorf("resolve data dir: %w", err)
	}
	if err := label.RecordHook(label.Dir(dirs.Data), clock.System{}.Now(), harness, captureID, stdin); err != nil {
		return fmt.Errorf("record: %w", err)
	}
	return nil
}

func newAnnotateCmd(open sourceOpener) *cobra.Command {
	var annotator, configPath string
	var doubtful bool
	cmd := &cobra.Command{
		Use:   "annotate",
		Short: "Manual annotation TUI for ambiguous windows (keys 1-7)",
		Long: "Shows each window's reconstructed screen and activity and reads one key:\n" +
			"1-7 label it A-G, s skips it, u undoes, q saves and quits. Labels go to\n" +
			"labels/" + label.AnnotationsFile + " in the data dir, never the screen.\n" +
			"With --doubtful it asks only the windows align cannot settle: conflicts\n" +
			"between hook and weak labels first, then unlabeled, then weak only, each\n" +
			"by session and time.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runAnnotate(cmd, open, annotator, doubtful, configPath)
		},
	}
	cmd.Flags().StringVar(&annotator, "annotator", "", "who labels (1-64 of A-Z a-z 0-9 . _ @ -)")
	cmd.Flags().BoolVar(&doubtful, "doubtful", false, "ask only the doubtful windows")
	cmd.Flags().StringVar(&configPath, "config", "", "align config YAML for --doubtful (defaults when empty or missing)")
	_ = cmd.MarkFlagRequired("annotator")
	return cmd
}

func runAnnotate(cmd *cobra.Command, open sourceOpener, annotator string, doubtful bool, configPath string) error {
	// A bad name is a usage error whatever the source: check it first.
	if err := label.CheckAnnotator(annotator); err != nil {
		return &deps.ExitError{Code: exitcode.Usage, Err: err}
	}
	cfg, err := loadConfig(configPath)
	if err != nil {
		return err
	}
	dirs, err := xdg.Resolve()
	if err != nil {
		return fmt.Errorf("resolve data dir: %w", err)
	}
	var src dataset.RecordingSource
	if doubtful {
		src, err = doubtfulSource(cmd, open, cfg, dirs.Data)
	} else {
		src, err = open(cmd.Context(), dirs.Data, cmd.ErrOrStderr())
	}
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

func newAlignCmd(open sourceOpener) *cobra.Command {
	var configPath string
	cmd := &cobra.Command{
		Use:   "align",
		Short: "Align collected labels with the recorded PTY output by wall-clock time",
		Long: "Labels each recorded window from labels/ in the data dir: a manual annotation\n" +
			"if there is one, else a hook label (harnesses with hooks), else a weak label\n" +
			"from output activity. Windows within the margin of a transition are dropped,\n" +
			"annotated or not.\n" +
			"Writes labels/" + label.AlignedFile + " (window_id, label, source; never the screen).",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runAlign(cmd, open, configPath)
		},
	}
	cmd.Flags().StringVar(&configPath, "config", "", "align config YAML (defaults when empty or missing)")
	return cmd
}

func loadConfig(path string) (label.AlignConfig, error) {
	cfg, err := label.LoadAlignConfig(path)
	if errors.Is(err, label.ErrBadAlignConfig) {
		return label.AlignConfig{}, &deps.ExitError{Code: exitcode.Usage, Err: err}
	}
	if err != nil {
		return label.AlignConfig{}, fmt.Errorf("gifford label: %w", err)
	}
	return cfg, nil
}

// alignDataDir aligns every session of the data dir with its hooks and
// annotations.
func alignDataDir(cmd *cobra.Command, open sourceOpener, cfg label.AlignConfig, dataDir string, warn io.Writer) ([]label.AlignedLabel, label.AlignStats, error) {
	dir := label.Dir(dataDir)
	src, err := open(cmd.Context(), dataDir, warn)
	if err != nil {
		return nil, label.AlignStats{}, err
	}
	hooks, badHooks, err := label.ReadHooks(dir)
	if err != nil {
		return nil, label.AlignStats{}, fmt.Errorf("gifford label: %w", err)
	}
	anns, badAnns, err := label.ReadAnnotations(dir)
	if err != nil {
		return nil, label.AlignStats{}, fmt.Errorf("gifford label: %w", err)
	}
	weak, badWeak, err := label.ReadWeak(dir)
	if err != nil {
		return nil, label.AlignStats{}, fmt.Errorf("gifford label: %w", err)
	}
	labels, st, err := label.AlignWithWeak(cmd.Context(), cfg, src, hooks, anns, weak)
	if err != nil {
		return nil, label.AlignStats{}, fmt.Errorf("gifford label: %w", err)
	}
	st.BadHookLines, st.BadAnnotationLines, st.BadWeakLines = badHooks, badAnns, badWeak
	return labels, st, nil
}

// doubtfulSource yields only the doubtful windows, in queue order. It
// reads the sessions twice: once to align them, once for the screens of
// the windows asked.
func doubtfulSource(cmd *cobra.Command, open sourceOpener, cfg label.AlignConfig, dataDir string) (dataset.RecordingSource, error) {
	labels, _, err := alignDataDir(cmd, open, cfg, dataDir, cmd.ErrOrStderr())
	if err != nil {
		return nil, err
	}
	queue := label.DoubtfulQueue(labels)
	wanted := make(map[protocol.ULID]bool, len(queue))
	for _, id := range queue {
		wanted[id] = true
	}
	src, err := open(cmd.Context(), dataDir, io.Discard)
	if err != nil {
		return nil, err
	}
	found := make(map[protocol.ULID]dataset.Recording, len(queue))
	for {
		rec, err := src.Next(cmd.Context())
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("gifford label: read window: %w", err)
		}
		if wanted[rec.WindowID] {
			found[rec.WindowID] = rec
		}
	}
	recs := make([]dataset.Recording, 0, len(queue))
	for _, id := range queue {
		if rec, ok := found[id]; ok {
			recs = append(recs, rec)
		}
	}
	return &sliceSource{recs: recs}, nil
}

// sliceSource yields recordings already in memory.
type sliceSource struct{ recs []dataset.Recording }

func (s *sliceSource) Next(context.Context) (dataset.Recording, error) {
	if len(s.recs) == 0 {
		return dataset.Recording{}, io.EOF
	}
	r := s.recs[0]
	s.recs = s.recs[1:]
	return r, nil
}

func runAlign(cmd *cobra.Command, open sourceOpener, configPath string) error {
	cfg, err := loadConfig(configPath)
	if err != nil {
		return err
	}
	dirs, err := xdg.Resolve()
	if err != nil {
		return fmt.Errorf("resolve data dir: %w", err)
	}
	labels, st, err := alignDataDir(cmd, open, cfg, dirs.Data, cmd.ErrOrStderr())
	if err != nil {
		return err
	}
	if err := label.SaveAligned(label.Dir(dirs.Data), labels); err != nil {
		return fmt.Errorf("gifford label align: %w", err)
	}
	// Counts only: never a screen or any text.
	out := cmd.OutOrStdout()
	_, _ = fmt.Fprintf(out,
		"windows %d: manual %d, hook %d, weak %d, unlabeled %d, discarded %d; skipped lines: hooks %d, annotations %d, weak %d\n",
		st.Windows, st.Manual, st.Hook, st.Weak, st.Unlabeled, st.Discarded, st.BadHookLines, st.BadAnnotationLines, st.BadWeakLines)
	_, _ = fmt.Fprintf(out, "discarded by a weak transition only: %d\n", st.DiscardedWeakOnly)
	for _, h := range sortedKeys(st.DiscardedWeakOnlyByHarness) {
		_, _ = fmt.Fprintf(out, "discarded by a weak transition only %s: %d\n", h, st.DiscardedWeakOnlyByHarness[h])
	}
	printDoubts(out, "doubtful", st.Doubtful)
	for _, h := range sortedKeys(st.DoubtfulByHarness) {
		printDoubts(out, "doubtful "+string(h), st.DoubtfulByHarness[h])
	}
	return nil
}

func sortedKeys[V any](m map[protocol.Harness]V) []protocol.Harness {
	keys := make([]protocol.Harness, 0, len(m))
	for h := range m {
		keys = append(keys, h)
	}
	slices.Sort(keys)
	return keys
}

func printDoubts(w io.Writer, what string, c label.DoubtCounts) {
	_, _ = fmt.Fprintf(w, "%s: conflict %d, unlabeled %d, weak_only %d\n", what, c.Conflict, c.Unlabeled, c.WeakOnly)
}
