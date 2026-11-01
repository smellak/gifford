package dataset

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
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
	"github.com/smellak/gifford/internal/profiles"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy/session"
	"github.com/smellak/gifford/internal/screenproj"
	"github.com/smellak/gifford/internal/xdg"
)

// buildFlags are the flags of gifford dataset build.
type buildFlags struct {
	out, mask, alignConfig, splitFile, syntheticDir string
	noMask, scriptedInput, liveForm, liveFormV2     bool
	nativeLabels                                    bool
	screenLines                                     int
	every                                           time.Duration
}

func newBuildCmd() *cobra.Command {
	var f buildFlags
	cmd := &cobra.Command{
		Use:   "build --out DIR [--synthetic-dir DIR] [--screen-lines N] [--mask FILE | --no-mask] [--every D] [--split-file FILE] [--live-form | --live-form-v2] [--native-labels]",
		Short: "Build a versioned training dataset from labeled captures",
		Long: `Replays every recorded session of the data dir, labels its windows as
gifford label align does (scripted timelines, hooks and annotations from
labels/), and keeps only
windows with a final label: a doubtful one (conflict or unlabeled) or one within
the margin of a transition is dropped and counted. Activity-derived weak-only
windows are always diagnostic and dropped. An audited weak.jsonl import is kept
only when align config says weak.keep: train and its group's split is train;
weak labels never enter validation or test. Each kept window's
screen goes through the screen projection the Analyst serves with: the human
input excluded (human-input-v1: unless the draft is known closed, the whole
screen is blanked), secrets redacted, the listed domain names masked
(domain-mask-v1) or none, and the last N rows kept. N and the mask come from
config.yaml, ml.screen_lines and ml.mask ("none" is the unmasked variant B, or
an absolute path to the list); both are required, and --screen-lines, --mask
and --no-mask, when given, must say the same. A window before its session's
first output is dropped and counted, as is one whose cursor is unknown or hidden
while the draft is not closed, each reason apart; the other windows keep their
ids and labels. Each session's windows share one split: split-v1 assigns it
from the session alone, train, validation or test. With --split-file, the file
assigns it: one JSON object per line, {"session":"<id>","split":"train"},
"validation" or "test", each session once, and every session of the data dir
must be in it, so none falls into a split by default; the manifest keeps the
file's sha256 as split_file. --synthetic-dir adds the prebuilt schema-1 windows
named <window_id>.json directly inside DIR. Every one must validate normally,
state both source_quality=synthetic and target.source=synthetic, use label D, E
or F, belong to train, and carry no captured-observation metadata. They join the
captured windows before the manifest and readiness calculation, so the configured
synthetic percentage applies to the final total of each label. --scripted-input says a script typed every session's input, never a
human keyboard: every window's draft counts as closed, so no screen is blanked
for a draft and no window is left out for its cursor, and the manifest says so
as scripted_input. It needs --split-file, and every session of the data dir
must have its script's timeline in labels/timeline.jsonl. --live-form cuts
every screen as a live session reports it to the Analyst, through the same
gate under the profile gifford run --agent picks for the recorded harness
version: only while the input guard the input.draft_* events report is
closed, only with exactly one measured input box, empty, on the cursor's
row, and with that box masked. Every other window is dropped and counted by
reason, harness and label, and the manifest says so as screen_form. It takes
the draft from the guard, so it excludes --scripted-input. --native-labels
requires --live-form and exact native_sources in the align config. It reads the
durable node database read-only, projects only B-F from complete native event
epochs, and writes a sealed, content-free native-labels.json sidecar for final
native targets. Without that flag the database is not opened and existing
dataset bytes and digests are unchanged. Writes DIR/manifest.json and
DIR/windows/<window_id>.json; DIR must not exist and appears only whole. Prints
numbers only. The same sources give the same bytes.`,
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
	fl.StringVar(&f.splitFile, "split-file", "", "assign each session train, validation or test from this file, not split-v1")
	fl.StringVar(&f.syntheticDir, "synthetic-dir", "", "add validated synthetic D, E or F train windows from DIR")
	fl.BoolVar(&f.scriptedInput, "scripted-input", false, "a script typed every session's input, not a human: count every draft closed; needs --split-file")
	fl.BoolVar(&f.liveForm, "live-form", false, "cut every screen as the live Analyst gets it; drop and count the windows it gets none for")
	fl.BoolVar(&f.liveFormV2, "live-form-v2", false, "cut empty-input and causally proven permission-dialog screens under live-input-v2")
	fl.BoolVar(&f.nativeLabels, "native-labels", false, "use enabled durable native events as optional B-F labels; needs --live-form")
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
	margin, conflict, unlabeled, weakOnly, weakNotTrain, weakBlank int
	noOutputYet, noCursor, cursorHidden, sessions                  int
	nativeEnabled                                                  bool
	nativeCandidate, nativeScreenSupported, nativeWithheld         int
	nativeProjectionFailed, nativeConflict, nativeUnclosed         int
	// withheld counts the windows live form left out, by harness and
	// label, then by why the live session withholds them.
	withheld map[liveBucket]map[string]int
}

// liveBucket is a harness and a label.
type liveBucket struct {
	harness protocol.Harness
	label   dataset.Label
}

func (d *discards) withhold(b liveBucket, reason string) {
	if d.withheld[b] == nil {
		d.withheld[b] = map[string]int{}
	}
	d.withheld[b][reason]++
}

func runBuild(cmd *cobra.Command, f buildFlags) error {
	switch {
	case f.out == "":
		return buildUsage("--out is required")
	case f.mask != "" && f.noMask:
		return buildUsage("--mask and --no-mask exclude each other")
	case f.scriptedInput && f.splitFile == "":
		return buildUsage("--scripted-input needs --split-file")
	case f.liveForm && f.scriptedInput:
		return buildUsage("--live-form and --scripted-input exclude each other")
	case f.liveFormV2 && f.scriptedInput:
		return buildUsage("--live-form-v2 and --scripted-input exclude each other")
	case f.liveForm && f.liveFormV2:
		return buildUsage("--live-form and --live-form-v2 exclude each other")
	case f.liveFormV2 && f.every != time.Second:
		return buildUsage("--live-form-v2 requires --every 1s")
	case f.liveFormV2 && f.splitFile == "":
		return buildUsage("--live-form-v2 requires --split-file")
	case f.nativeLabels && !f.liveForm:
		return buildUsage("--native-labels needs --live-form")
	}
	if _, err := os.Lstat(f.out); !errors.Is(err, os.ErrNotExist) {
		return buildUsage(filepath.Base(f.out) + " already exists")
	}
	cfg, alignDigest, err := loadAlignConfig(f.alignConfig)
	if errors.Is(err, label.ErrBadAlignConfig) {
		return buildUsage(err.Error())
	}
	if err != nil {
		return fmt.Errorf("gifford dataset build: %w", err)
	}
	if f.liveFormV2 && cfg.Margin != 600*time.Millisecond {
		return buildUsage("--live-form-v2 requires align margin_ms: 600")
	}
	if f.nativeLabels && len(cfg.NativeSources) == 0 {
		return buildUsage("--native-labels needs native_sources in the align config")
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
	splitter, splitFile, err := splitterFor(f.splitFile, dirs.Data)
	if err != nil {
		return err
	}
	ctx := cmd.Context()
	if ctx == nil {
		ctx = context.Background()
	}
	synthetic, err := loadSyntheticWindows(ctx, f.syntheticDir)
	if err != nil {
		return fmt.Errorf("gifford dataset build: %w", err)
	}
	var nativeRuns []label.NativeRun
	if f.nativeLabels {
		ids, listErr := session.ListSessions(dirs.Data)
		if listErr != nil {
			return fmt.Errorf("gifford dataset build: list native label sessions: %w", listErr)
		}
		sessions := make([]protocol.OpaqueID, 0, len(ids))
		for _, id := range ids {
			sessions = append(sessions, protocol.OpaqueID(id))
		}
		db, openErr := openNativeReadDB(ctx, filepath.Join(dirs.Data, "gifford.db"))
		if openErr != nil {
			return fmt.Errorf("gifford dataset build: %w", openErr)
		}
		nativeRuns, err = label.ReadNativeRuns(ctx, db, sessions)
		if closeErr := db.Close(); err == nil {
			err = closeErr
		}
		if err != nil {
			return fmt.Errorf("gifford dataset build: %w", err)
		}
	}
	b := builder{
		dataDir: dirs.Data, every: f.every, align: cfg, projection: proj, splitter: splitter,
		scripted: f.scriptedInput, native: f.nativeLabels, nativeRuns: nativeRuns,
		warn: cmd.ErrOrStderr(),
	}
	screenForm := ""
	if f.liveForm {
		ps, err := profiles.Load()
		if err != nil {
			return fmt.Errorf("gifford dataset build: %w", err)
		}
		b.live, screenForm = &ps, dataset.ScreenFormLive
	}
	if f.liveFormV2 {
		ps, err := profiles.Load()
		if err != nil {
			return fmt.Errorf("gifford dataset build: %w", err)
		}
		b.liveV2, b.permissionSources, screenForm = &ps, conf.ML.PermissionDialogSources, dataset.ScreenFormLiveV2
	}
	src, d, nativeEvidence, err := b.labelled(ctx)
	if err != nil {
		return err
	}
	readiness := conf.ML.Readiness
	inv, err := dataset.RunPipeline(ctx, dataset.PipelineDeps{
		Source:     src,
		Projection: proj,
		Splitter:   splitter,
	}, dataset.PipelineOptions{
		MaxWindows: dataset.MaxPipelineWindows, Readiness: &readiness, SplitFile: splitFile,
		Align:            &dataset.AlignPolicy{MarginMS: cfg.Margin.Milliseconds(), ConfigDigest: alignDigest},
		ScriptedInput:    f.scriptedInput,
		ScreenForm:       screenForm,
		SyntheticWindows: synthetic,
	})
	if err != nil {
		return fmt.Errorf("gifford dataset build: %w", err)
	}
	if f.nativeLabels {
		evidence := make([]dataset.NativeLabelEvidence, 0, len(nativeEvidence))
		for _, window := range inv.Windows {
			if window.Target.Source != dataset.LabelSourceNative {
				continue
			}
			item, ok := nativeEvidence[window.WindowID]
			if !ok {
				return fmt.Errorf("gifford dataset build: native label evidence missing")
			}
			evidence = append(evidence, item)
		}
		inv, err = dataset.AttachNativeLabels(inv, evidence)
		if err != nil {
			return fmt.Errorf("gifford dataset build: %w", err)
		}
		d.nativeScreenSupported = len(evidence)
	}
	if err := dataset.WriteDir(f.out, inv); err != nil {
		return fmt.Errorf("gifford dataset build: %w", err)
	}
	printBuild(cmd.OutOrStdout(), inv.Manifest, d)
	return nil
}

// loadSyntheticWindows reads a flat directory of standard window documents.
// Filenames bind each document to its id, and no other file type is admitted,
// so a manifest, checksum file or nested corpus cannot be mistaken for input.
func loadSyntheticWindows(ctx context.Context, dir string) ([]dataset.Window, error) {
	if dir == "" {
		return nil, nil
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read synthetic dir: %w", err)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, fmt.Errorf("open synthetic dir: %w", err)
	}
	defer func() { _ = root.Close() }()

	windows := make([]dataset.Window, 0, len(entries))
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("read synthetic dir: %w", err)
		}
		name := entry.Name()
		id, ok := strings.CutSuffix(name, ".json")
		if entry.IsDir() || entry.Type()&os.ModeSymlink != 0 || !ok || !protocol.ULID(id).Valid() {
			return nil, fmt.Errorf("synthetic dir: %s is not a regular <window_id>.json file", name)
		}
		entryInfo, err := entry.Info()
		if err != nil {
			return nil, fmt.Errorf("stat synthetic window %s: %w", name, err)
		}
		if !entryInfo.Mode().IsRegular() {
			return nil, fmt.Errorf("synthetic dir: %s is not a regular file", name)
		}
		if len(windows) == dataset.MaxPipelineWindows {
			return nil, fmt.Errorf("synthetic dir: more than %d windows", dataset.MaxPipelineWindows)
		}
		f, err := root.Open(name)
		if err != nil {
			return nil, fmt.Errorf("open synthetic window %s: %w", name, err)
		}
		info, statErr := f.Stat()
		if statErr != nil || !info.Mode().IsRegular() {
			_ = f.Close()
			if statErr != nil {
				return nil, fmt.Errorf("stat synthetic window %s: %w", name, statErr)
			}
			return nil, fmt.Errorf("synthetic dir: %s is not a regular file", name)
		}
		w, parseErr := dataset.ParseWindow(f)
		closeErr := f.Close()
		if parseErr != nil {
			return nil, fmt.Errorf("parse synthetic window %s: %w", name, parseErr)
		}
		if closeErr != nil {
			return nil, fmt.Errorf("close synthetic window %s: %w", name, closeErr)
		}
		if w.WindowID != protocol.ULID(id) {
			return nil, fmt.Errorf("synthetic window %s: filename does not match window_id", name)
		}
		if err := dataset.ValidateSyntheticTrainingWindow(w); err != nil {
			return nil, fmt.Errorf("synthetic window %s: %w", name, err)
		}
		windows = append(windows, w)
	}
	if len(windows) == 0 {
		return nil, errors.New("synthetic dir: no windows")
	}
	return windows, nil
}

// loadAlignConfig is label.LoadAlignConfig, reading the file once, with
// the SHA-256 of the bytes it parsed for the manifest. The digest is nil
// when align runs with its defaults: no path, or no file at it.
func loadAlignConfig(path string) (label.AlignConfig, *protocol.Digest, error) {
	if path == "" {
		return label.DefaultAlignConfig(), nil, nil
	}
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return label.DefaultAlignConfig(), nil, nil
	}
	if err != nil {
		return label.AlignConfig{}, nil, fmt.Errorf("read align config: %w", err)
	}
	cfg, err := label.ParseAlignConfig(bytes.NewReader(b))
	if err != nil {
		return label.AlignConfig{}, nil, err //nolint:wrapcheck // ErrBadAlignConfig, matched by the caller
	}
	sum := sha256.Sum256(b)
	d := protocol.Digest("sha256:" + hex.EncodeToString(sum[:]))
	return cfg, &d, nil
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
	dataDir    string
	every      time.Duration
	align      label.AlignConfig
	projection screenproj.Policies
	splitter   dataset.Splitter
	scripted   bool
	native     bool
	nativeRuns []label.NativeRun
	// live, when set, cuts the windows the build keeps in live form.
	live              *profiles.Profiles
	liveV2            *profiles.Profiles
	permissionSources []session.PermissionDialogSource
	warn              io.Writer
}

// labelled aligns the sessions with their hooks and annotations, then
// returns a second replay of them that yields only the windows with a
// final label, each labeled, and counts the rest.
func (b builder) labelled(ctx context.Context) (
	dataset.RecordingSource,
	*discards,
	map[protocol.ULID]dataset.NativeLabelEvidence,
	error,
) {
	d := &discards{nativeEnabled: b.native, withheld: map[liveBucket]map[string]int{}}
	skipped := map[string]bool{}
	first, err := replay.OpenAll(b.dataDir, b.every, func(id string, err error) {
		skipped[id] = true
		d.sessions++
		_, _ = fmt.Fprintf(b.warn, "gifford dataset build: session %s skipped: %v\n", id, err)
	})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("gifford dataset build: %w", err)
	}
	dir := label.Dir(b.dataDir)
	hooks, _, err := label.ReadHooks(dir)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("gifford dataset build: %w", err)
	}
	anns, _, err := label.ReadAnnotations(dir)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("gifford dataset build: %w", err)
	}
	weak, _, err := label.ReadWeak(dir)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("gifford dataset build: %w", err)
	}
	timeline, err := label.ReadTimeline(dir)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("gifford dataset build: %w", err)
	}
	if b.scripted {
		if err := everySessionScripted(b.dataDir, timeline); err != nil {
			return nil, nil, nil, err
		}
	}
	aligned, st, err := label.AlignWithTimeline(ctx, b.align, first, hooks, anns, weak, timeline)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("gifford dataset build: %w", err)
	}
	d.margin = st.Discarded
	nativeEvidence := map[protocol.ULID]dataset.NativeLabelEvidence{}
	if b.native {
		nativeSource, openErr := replay.OpenAll(b.dataDir, b.every, nil)
		if openErr != nil {
			return nil, nil, nil, fmt.Errorf("gifford dataset build: %w", openErr)
		}
		projected, nativeStats, alignErr := label.AlignNative(ctx, b.align, nativeSource, b.nativeRuns)
		if alignErr != nil {
			return nil, nil, nil, fmt.Errorf("gifford dataset build: %w", alignErr)
		}
		var mergeStats label.NativeMergeStats
		aligned, nativeEvidence, mergeStats = label.MergeNative(aligned, projected)
		d.margin += mergeStats.Discarded
		d.nativeCandidate = nativeStats.Candidates
		d.nativeConflict = nativeStats.Conflicts + mergeStats.Conflicts
		d.nativeUnclosed = nativeStats.Unclosed
	}
	final := finalLabels(aligned, b.align.WeakKeep, d)
	// The second pass reads the sessions again. It reports only new v2 seal
	// failures; ordinary corrupt sessions were already reported by the first
	// pass. Labels never read the screen, so only this pass cuts in live form.
	var second dataset.RecordingSource
	switch {
	case b.liveV2 != nil:
		second, err = replay.OpenAllLiveV2(b.dataDir, b.every, *b.liveV2, b.permissionSources, func(id string, err error) {
			if skipped[id] {
				return
			}
			d.sessions++
			_, _ = fmt.Fprintf(b.warn, "gifford dataset build: live-input-v2 session %s skipped: %v\n", id, err)
		})
	case b.live != nil:
		second, err = replay.OpenAllLive(b.dataDir, b.every, *b.live, nil)
	default:
		second, err = replay.OpenAll(b.dataDir, b.every, nil)
	}
	if err != nil {
		return nil, nil, nil, fmt.Errorf("gifford dataset build: %w", err)
	}
	return &finalOnly{
		src: second, final: final, keepWeak: b.align.WeakKeep, projection: b.projection, splitter: b.splitter,
		scripted: b.scripted, nativeEvidence: nativeEvidence, d: d,
		liveV2: b.liveV2 != nil, seenPermissionAnchors: map[protocol.Digest]bool{},
	}, d, nativeEvidence, nil
}

// everySessionScripted refuses a data dir with a session that has no
// line in the timeline: input counts as scripted only for a session a
// driver wrote its timeline for. No session is skipped.
func everySessionScripted(dataDir string, timeline []label.TimelineRecord) error {
	scripted := map[protocol.OpaqueID]bool{}
	for _, r := range timeline {
		scripted[r.Session] = true
	}
	ids, err := session.ListSessions(dataDir)
	if err != nil {
		return fmt.Errorf("gifford dataset build: %w", err)
	}
	for _, id := range ids {
		if !scripted[protocol.OpaqueID(id)] {
			return buildUsage("--scripted-input: session " + id + " has no timeline in labels/" + label.TimelineFile)
		}
	}
	return nil
}

func finalLabels(aligned []label.AlignedLabel, keepWeak label.WeakKeep, d *discards) map[protocol.ULID]label.AlignedLabel {
	final := make(map[protocol.ULID]label.AlignedLabel, len(aligned))
	for _, al := range aligned {
		switch {
		case al.Doubt == label.DoubtConflict:
			d.conflict++
		case al.Source == label.SourceWeak:
			if al.Doubt == label.DoubtWeakOnly && keepWeak == label.WeakKeepTrain &&
				al.WeakOrigin == label.WeakOriginImport {
				final[al.WindowID] = al
			} else {
				d.weakOnly++
			}
		case al.Doubt == label.DoubtWeakOnly:
			d.weakOnly++
		case al.Doubt != "" || al.Label == "":
			d.unlabeled++
		default:
			final[al.WindowID] = al
		}
	}
	return final
}

// finalOnly yields the windows with a final label, labeled.
type finalOnly struct {
	src            dataset.RecordingSource
	final          map[protocol.ULID]label.AlignedLabel
	nativeEvidence map[protocol.ULID]dataset.NativeLabelEvidence
	keepWeak       label.WeakKeep
	projection     screenproj.Policies
	// splitter is the pipeline's: a weak window is kept only in train.
	splitter dataset.Splitter
	// scripted counts every draft as closed: a script typed the input.
	scripted              bool
	liveV2                bool
	seenPermissionAnchors map[protocol.Digest]bool
	d                     *discards
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
		native := al.Source == label.SourceNative
		if native {
			evidence, exists := s.nativeEvidence[rec.WindowID]
			if !exists || evidence.Label != al.Label {
				return dataset.Recording{}, errors.New("native label evidence missing or mismatched")
			}
		}
		if al.Source == label.SourceWeak {
			if al.WeakOrigin != label.WeakOriginImport {
				s.d.weakOnly++
				continue
			}
			split, splitErr := s.splitter.Split(rec.GroupDigest)
			if splitErr != nil {
				return dataset.Recording{}, fmt.Errorf("split weak window: %w", splitErr)
			}
			if s.keepWeak != label.WeakKeepTrain || split != dataset.SplitTrain {
				s.d.weakNotTrain++
				continue
			}
		}
		// A window before the session's first output has no quiet time
		// to give: it is left out, counted, and keeps its place, so the
		// windows after it keep their ids and their labels.
		if rec.NoOutputYet {
			s.d.noOutputYet++
			if native {
				s.d.nativeProjectionFailed++
			}
			continue
		}
		// A window the live Analyst would get no screen for is no model
		// input: left out, counted by why, and keeps its place too.
		if rec.LiveWithheld != "" {
			s.d.withhold(liveBucket{harness: rec.Features.Harness, label: al.Label}, rec.LiveWithheld)
			if native {
				s.d.nativeWithheld++
			}
			continue
		}
		// What a script typed is no human draft: the draft is closed,
		// and the screen stays as the harness drew it.
		if s.scripted {
			rec.Draft = dataset.DraftClosed
		}
		// Unless the draft is known closed, a window whose cursor is
		// unknown, or hidden and so not where the draft is, is left out,
		// each counted apart.
		if !rec.Draft.Closed() {
			switch {
			case !rec.Cursor.Known:
				s.d.noCursor++
				if native {
					s.d.nativeProjectionFailed++
				}
				continue
			case !rec.Cursor.Visible:
				s.d.cursorHidden++
				if native {
					s.d.nativeProjectionFailed++
				}
				continue
			}
		}
		if s.liveV2 {
			switch {
			case rec.ObservationForm == dataset.ObservationFormPermissionDialogV1 &&
				!permissionDialogDTruth(al):
				s.d.withhold(liveBucket{harness: rec.Features.Harness, label: al.Label}, "permission_dialog_label_truth")
				continue
			case al.Label == dataset.LabelD && rec.ObservationForm != dataset.ObservationFormPermissionDialogV1:
				s.d.withhold(liveBucket{harness: rec.Features.Harness, label: al.Label}, "permission_dialog_form_missing")
				continue
			case rec.ObservationForm == dataset.ObservationFormPermissionDialogV1 && s.seenPermissionAnchors[rec.ObservationAnchorDigest]:
				s.d.withhold(liveBucket{harness: rec.Features.Harness, label: al.Label}, "permission_dialog_duplicate_anchor")
				continue
			}
			if rec.ObservationForm == dataset.ObservationFormPermissionDialogV1 {
				s.seenPermissionAnchors[rec.ObservationAnchorDigest] = true
			}
		}
		supported, supportErr := s.hasLabelScreen(rec, al, native)
		if supportErr != nil {
			return dataset.Recording{}, supportErr
		}
		if !supported {
			continue
		}
		rec.Label, rec.LabelSource = al.Label, labelSource(al.Source)
		return rec, nil
	}
}

// permissionDialogDTruth accepts only the interval source that actually won
// D. A manual or weak D, or a hook interval of another state, can never borrow
// truth from the screen matcher.
func permissionDialogDTruth(al label.AlignedLabel) bool {
	if al.Label != dataset.LabelD {
		return false
	}
	switch al.Source {
	case label.SourceScript:
		return al.ScriptLabel == dataset.LabelD
	case label.SourceHook:
		return al.HookLabel == dataset.LabelD
	default:
		return false
	}
}

func (s *finalOnly) hasLabelScreen(
	rec dataset.Recording,
	al label.AlignedLabel,
	native bool,
) (bool, error) {
	if !native && (al.Source != label.SourceWeak || al.Label != dataset.LabelG) {
		return true, nil
	}
	usable, err := hasUsableProjectedScreen(rec, s.projection)
	if err != nil {
		return false, fmt.Errorf("project label window: %w", err)
	}
	if usable {
		return true, nil
	}
	if native {
		s.d.nativeProjectionFailed++
	} else {
		s.d.weakBlank++
	}
	return false, nil
}

func hasUsableProjectedScreen(rec dataset.Recording, projection screenproj.Policies) (bool, error) {
	draft := protocol.DraftStateUnknown
	if rec.Draft.Closed() {
		draft = protocol.DraftStateClosed
	}
	projected, err := screenproj.Project(screenproj.Input{
		Rows: rec.Screen.Rows, Cols: rec.Screen.Cols, Cursor: rec.Cursor, Draft: draft,
	}, projection)
	if err != nil {
		return false, err //nolint:wrapcheck // the caller names the labeling projection stage
	}
	return len(projected.Lines) != 0, nil
}

func labelSource(s label.Source) dataset.LabelSource {
	switch s {
	case label.SourceManual:
		return dataset.LabelSourceManual
	case label.SourceScript:
		return dataset.LabelSourceScript
	case label.SourceHook:
		return dataset.LabelSourceHook
	case label.SourceNative:
		return dataset.LabelSourceNative
	default:
		return dataset.LabelSourceWeak
	}
}

// printBuild prints the build: numbers and names only.
func printBuild(out io.Writer, m dataset.Manifest, d *discards) {
	printManifest(out, m)
	if counts := m.ObservationCounts; counts != nil {
		_, _ = fmt.Fprintf(out, "permission anchors %d, unique episodes %d\n", counts.PermissionAnchors, counts.PermissionEpisodes)
	}
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
	_, _ = fmt.Fprintf(out, "discarded: margin %d, conflict %d, unlabeled %d, weak_only %d, weak_not_train %d, weak_blank %d, no_output_yet %d, no_cursor %d, cursor_hidden %d; sessions skipped %d\n",
		d.margin, d.conflict, d.unlabeled, d.weakOnly, d.weakNotTrain, d.weakBlank, d.noOutputYet, d.noCursor, d.cursorHidden, d.sessions)
	if d.nativeEnabled {
		_, _ = fmt.Fprintf(out, "native labels: native_candidate %d, native_screen_supported %d, native_withheld %d, native_projection_failed %d, native_conflict %d, native_unclosed %d\n",
			d.nativeCandidate, d.nativeScreenSupported, d.nativeWithheld, d.nativeProjectionFailed, d.nativeConflict, d.nativeUnclosed)
	}
	if m.ScreenForm != "" {
		printWithheld(out, m, d.withheld)
	}
}

// printWithheld prints, for each harness and label, the windows a live
// form build kept and the ones it left out, by reason.
func printWithheld(out io.Writer, m dataset.Manifest, withheld map[liveBucket]map[string]int) {
	kept := map[liveBucket]int{}
	for _, e := range m.Entries {
		kept[liveBucket{harness: e.Harness, label: e.Label}]++
	}
	buckets := make([]liveBucket, 0, len(kept)+len(withheld))
	for b := range kept {
		buckets = append(buckets, b)
	}
	for b := range withheld {
		if _, ok := kept[b]; !ok {
			buckets = append(buckets, b)
		}
	}
	slices.SortFunc(buckets, func(a, b liveBucket) int {
		return cmp.Or(strings.Compare(string(a.harness), string(b.harness)), strings.Compare(string(a.label), string(b.label)))
	})
	total, lines := 0, make([]string, 0, len(buckets))
	for _, b := range buckets {
		reasons := make([]string, 0, len(withheld[b]))
		for r := range withheld[b] {
			reasons = append(reasons, r)
		}
		slices.Sort(reasons)
		n, parts := 0, make([]string, 0, len(reasons))
		for _, r := range reasons {
			n += withheld[b][r]
			parts = append(parts, fmt.Sprintf("%s %d", r, withheld[b][r]))
		}
		total += n
		line := fmt.Sprintf("live form %s %s: kept %d, withheld %d", b.harness, b.label, kept[b], n)
		if n > 0 {
			line += " (" + strings.Join(parts, ", ") + ")"
		}
		lines = append(lines, line)
	}
	_, _ = fmt.Fprintf(out, "withheld in live form: %d\n", total)
	for _, line := range lines {
		_, _ = fmt.Fprintln(out, line)
	}
}
