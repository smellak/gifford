package dataset

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/cli/exitcode"
	"github.com/smellak/gifford/internal/config"
	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/dataset/replay"
	"github.com/smellak/gifford/internal/label"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/screenproj"
	"github.com/smellak/gifford/internal/xdg"
)

// buildFlags are the flags of gifford dataset build.
type buildFlags struct {
	out, mask, alignConfig string
	noMask                 bool
	screenLines            int
	every                  time.Duration
}

func newBuildCmd() *cobra.Command {
	var f buildFlags
	cmd := &cobra.Command{
		Use:   "build --out DIR [--screen-lines N] [--mask FILE | --no-mask] [--every D]",
		Short: "Build a versioned training dataset from labeled captures",
		Long: `Replays every recorded session of the data dir, labels its windows as
gifford label align does (hooks and annotations from labels/), and keeps only
windows with a final label: a doubtful one (conflict, unlabeled, weak only) or
one within the margin of a transition is dropped and counted. Each kept window's
screen goes through the screen projection the Analyst serves with: the human
input excluded (human-input-v1: unless the draft is known closed, the whole
screen is blanked), secrets redacted, the listed domain names masked
(domain-mask-v1) or none, and the last N rows kept. N and the mask come from
config.yaml, ml.screen_lines and ml.mask ("none" is the unmasked variant B, or
an absolute path to the list); both are required, and --screen-lines, --mask
and --no-mask, when given, must say the same. A window before its session's
first output is dropped and counted, as is one whose cursor is unknown or hidden
while the draft is not closed, each reason apart; the other windows keep their
ids and labels. Writes DIR/manifest.json and DIR/windows/<window_id>.json; DIR
must not exist and appears only whole. Prints numbers only. The same sources
give the same bytes.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runBuild(cmd, f)
		},
	}
	fl := cmd.Flags()
	fl.StringVar(&f.out, "out", "", "dataset directory to create; must not exist")
	fl.IntVar(&f.screenLines, "screen-lines", 0, "screen lines per window; must match ml.screen_lines")
	fl.StringVar(&f.mask, "mask", "", "domain mask list; must be the file ml.mask names")
	fl.BoolVar(&f.noMask, "no-mask", false, "mask no domain name (variant B); needs ml.mask: none")
	fl.DurationVar(&f.every, "every", replay.DefaultEvery, "window spacing on the wall clock")
	fl.StringVar(&f.alignConfig, "align-config", "", "align config YAML, as gifford label align --config")
	cmd.SetFlagErrorFunc(func(_ *cobra.Command, err error) error {
		return buildUsage(err.Error())
	})
	return cmd
}

func buildUsage(msg string) error {
	return &deps.ExitError{Code: exitcode.Usage, Err: fmt.Errorf("gifford dataset build: %s", msg)}
}

// discards count the windows left out, by reason.
type discards struct {
	margin, conflict, unlabeled, weakOnly, noOutputYet, noCursor, cursorHidden, sessions int
}

func runBuild(cmd *cobra.Command, f buildFlags) error {
	switch {
	case f.out == "":
		return buildUsage("--out is required")
	case f.mask != "" && f.noMask:
		return buildUsage("--mask and --no-mask exclude each other")
	}
	if _, err := os.Lstat(f.out); !errors.Is(err, os.ErrNotExist) {
		return buildUsage(filepath.Base(f.out) + " already exists")
	}
	cfg, err := label.LoadAlignConfig(f.alignConfig)
	if errors.Is(err, label.ErrBadAlignConfig) {
		return buildUsage(err.Error())
	}
	if err != nil {
		return fmt.Errorf("gifford dataset build: %w", err)
	}
	dirs, err := xdg.Resolve()
	if err != nil {
		return fmt.Errorf("gifford dataset build: %w", err)
	}
	conf, err := config.Load(config.DefaultPath(dirs.Config))
	if errors.Is(err, config.ErrInvalidProjection) {
		return buildUsage(err.Error())
	}
	if err != nil {
		return fmt.Errorf("gifford dataset build: %w", err)
	}
	if err := flagsMatch(cmd, f, conf.ML); err != nil {
		return err
	}
	proj, err := conf.ML.ProjectionPolicies()
	if errors.Is(err, config.ErrInvalidProjection) {
		return buildUsage(err.Error())
	}
	if err != nil {
		return fmt.Errorf("gifford dataset build: %w", err)
	}
	ctx := cmd.Context()
	if ctx == nil {
		ctx = context.Background()
	}
	b := builder{dataDir: dirs.Data, every: f.every, align: cfg, warn: cmd.ErrOrStderr()}
	src, d, err := b.labelled(ctx)
	if err != nil {
		return err
	}
	readiness := conf.ML.Readiness
	inv, err := dataset.RunPipeline(ctx, dataset.PipelineDeps{
		Source:     src,
		Projection: proj,
		Splitter:   dataset.SplitV1{},
	}, dataset.PipelineOptions{MaxWindows: dataset.MaxPipelineWindows, Readiness: &readiness})
	if err != nil {
		return fmt.Errorf("gifford dataset build: %w", err)
	}
	if err := dataset.WriteDir(f.out, inv); err != nil {
		return fmt.Errorf("gifford dataset build: %w", err)
	}
	printBuild(cmd.OutOrStdout(), inv.Manifest, d)
	return nil
}

// flagsMatch refuses a flag that says otherwise than config.yaml: the
// build runs under ml.screen_lines and ml.mask, the Analyst's own, and a
// flag may only repeat them.
func flagsMatch(cmd *cobra.Command, f buildFlags, ml config.ML) error {
	if cmd.Flags().Changed("screen-lines") && f.screenLines != ml.ScreenLines {
		return buildUsage("--screen-lines does not match ml.screen_lines in config.yaml")
	}
	if f.noMask && ml.Mask != screenproj.DomainMaskNone {
		return buildUsage("--no-mask does not match ml.mask in config.yaml")
	}
	if f.mask != "" {
		abs, err := filepath.Abs(f.mask)
		if err != nil || ml.Mask == screenproj.DomainMaskNone || abs != filepath.Clean(ml.Mask) {
			return buildUsage("--mask does not match ml.mask in config.yaml")
		}
	}
	return nil
}

// builder reads the data dir's sessions and labels.
type builder struct {
	dataDir string
	every   time.Duration
	align   label.AlignConfig
	warn    io.Writer
}

// labelled aligns the sessions with their hooks and annotations, then
// returns a second replay of them that yields only the windows with a
// final label, each labeled, and counts the rest.
func (b builder) labelled(ctx context.Context) (dataset.RecordingSource, *discards, error) {
	d := &discards{}
	first, err := replay.OpenAll(b.dataDir, b.every, func(id string, err error) {
		d.sessions++
		_, _ = fmt.Fprintf(b.warn, "gifford dataset build: session %s skipped: %v\n", id, err)
	})
	if err != nil {
		return nil, nil, fmt.Errorf("gifford dataset build: %w", err)
	}
	dir := label.Dir(b.dataDir)
	hooks, _, err := label.ReadHooks(dir)
	if err != nil {
		return nil, nil, fmt.Errorf("gifford dataset build: %w", err)
	}
	anns, _, err := label.ReadAnnotations(dir)
	if err != nil {
		return nil, nil, fmt.Errorf("gifford dataset build: %w", err)
	}
	aligned, st, err := label.Align(ctx, b.align, first, hooks, anns)
	if err != nil {
		return nil, nil, fmt.Errorf("gifford dataset build: %w", err)
	}
	d.margin = st.Discarded
	final := make(map[protocol.ULID]label.AlignedLabel, len(aligned))
	for _, al := range aligned {
		switch {
		case al.Doubt == label.DoubtConflict:
			d.conflict++
		case al.Doubt == label.DoubtWeakOnly:
			d.weakOnly++
		case al.Doubt != "" || al.Label == "":
			d.unlabeled++
		default:
			final[al.WindowID] = al
		}
	}
	// The second pass reads the sessions again, quietly: the first
	// already reported the ones it skipped.
	second, err := replay.OpenAll(b.dataDir, b.every, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("gifford dataset build: %w", err)
	}
	return &finalOnly{src: second, final: final, d: d}, d, nil
}

// finalOnly yields the windows with a final label, labeled.
type finalOnly struct {
	src   dataset.RecordingSource
	final map[protocol.ULID]label.AlignedLabel
	d     *discards
}

func (s *finalOnly) Next(ctx context.Context) (dataset.Recording, error) {
	for {
		rec, err := s.src.Next(ctx)
		if err != nil {
			return dataset.Recording{}, err //nolint:wrapcheck // the replay's own error, or io.EOF
		}
		al, ok := s.final[rec.WindowID]
		if !ok {
			continue
		}
		// A window before the session's first output has no quiet time
		// to give: it is left out, counted, and keeps its place, so the
		// windows after it keep their ids and their labels.
		if rec.NoOutputYet {
			s.d.noOutputYet++
			continue
		}
		// Unless the draft is known closed, a window whose cursor is
		// unknown, or hidden and so not where the draft is, is left out,
		// each counted apart.
		if !rec.Draft.Closed() {
			switch {
			case !rec.Cursor.Known:
				s.d.noCursor++
				continue
			case !rec.Cursor.Visible:
				s.d.cursorHidden++
				continue
			}
		}
		rec.Label, rec.LabelSource = al.Label, labelSource(al.Source)
		return rec, nil
	}
}

func labelSource(s label.Source) dataset.LabelSource {
	switch s {
	case label.SourceManual:
		return dataset.LabelSourceManual
	case label.SourceHook:
		return dataset.LabelSourceHook
	default:
		return dataset.LabelSourceWeak
	}
}

// printBuild prints the build: numbers and names only.
func printBuild(out io.Writer, m dataset.Manifest, d *discards) {
	printManifest(out, m)
	l := m.Counts.ByLabel
	_, _ = fmt.Fprintf(out, "by label: A %d, B %d, C %d, D %d, E %d, F %d, G %d\n", l.A, l.B, l.C, l.D, l.E, l.F, l.G)
	byHarness := map[protocol.Harness]int{}
	for _, e := range m.Entries {
		byHarness[e.Harness]++
	}
	hs := make([]string, 0, len(byHarness))
	for h := range byHarness {
		hs = append(hs, string(h))
	}
	slices.Sort(hs)
	parts := make([]string, 0, len(hs))
	for _, h := range hs {
		parts = append(parts, fmt.Sprintf("%s %d", h, byHarness[protocol.Harness(h)]))
	}
	_, _ = fmt.Fprintf(out, "by harness: %s\n", strings.Join(parts, ", "))
	_, _ = fmt.Fprintf(out, "discarded: margin %d, conflict %d, unlabeled %d, weak_only %d, no_output_yet %d, no_cursor %d, cursor_hidden %d; sessions skipped %d\n",
		d.margin, d.conflict, d.unlabeled, d.weakOnly, d.noOutputYet, d.noCursor, d.cursorHidden, d.sessions)
}
