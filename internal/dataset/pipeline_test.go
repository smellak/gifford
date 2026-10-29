package dataset

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/redact"
	"github.com/smellak/gifford/internal/redact/redacttest"
	"github.com/smellak/gifford/internal/screenproj"
)

// Invented screen content: a draft the human typed at the prompt, a
// prompt row with the draft closed and a domain name the mask must hide.
const (
	humanDraft = "> sentinel human draft"
	promptRow  = "> run the tests"
	domainName = "sentinelcorp"
	maskedName = "[domain]"
)

// errSentinel is a stage failure whose text must never reach the run's
// error.
var errSentinel = errors.New("sentinel stage detail")

type sliceSource struct {
	recs   []Recording
	next   int
	failAt int // -1 never fails
}

func (s *sliceSource) Next(context.Context) (Recording, error) {
	if s.next == s.failAt {
		return Recording{}, errSentinel
	}
	if s.next == len(s.recs) {
		return Recording{}, io.EOF
	}
	s.next++
	return s.recs[s.next-1], nil
}

type failingRedactor struct{}

func (failingRedactor) RedactScreen(redact.Screen) (redact.ScreenResult, error) {
	return redact.ScreenResult{}, errSentinel
}

type versionlessRedactor struct{ inner screenproj.Redactor }

func (v versionlessRedactor) RedactScreen(s redact.Screen) (redact.ScreenResult, error) {
	res, err := v.inner.RedactScreen(s)
	res.PolicyVersion = ""
	return res, err
}

// mapSplitter assigns train unless the group is listed.
type mapSplitter struct {
	splits map[protocol.Digest]Split
	err    error
}

func (m mapSplitter) Split(group protocol.Digest) (Split, error) {
	if m.err != nil {
		return "", m.err
	}
	if s, ok := m.splits[group]; ok {
		return s, nil
	}
	return SplitTrain, nil
}

// testSecret is a synthetic credential planted on the raw screen.
func testSecret(t *testing.T) string {
	t.Helper()
	v, err := redacttest.Value("github_pat")
	require.NoError(t, err)
	return v
}

// recordingOf turns a fixture window back into what a source reads: its
// features without screen lines, and a raw screen holding a domain name,
// a secret and a prompt row, with the draft known closed.
func recordingOf(w Window, secret string) Recording {
	features := w.Input
	features.ScreenLines = nil
	return Recording{
		WindowID: w.WindowID,
		Contract: w.Contract,
		Features: features,
		Screen: redact.Screen{Cols: 80, Rows: []string{
			"tests: 9 passed, 0 failed",
			"deploy to " + domainName + " staging",
			"export GITHUB_TOKEN=" + secret,
			promptRow,
		}},
		Label:             w.Target.Label,
		LabelSource:       w.Target.Source,
		Provenance:        w.Provenance,
		EnvironmentDomain: w.EnvironmentDomain,
		GroupDigest:       w.Split.GroupDigest,
		Draft:             DraftClosed,
	}
}

func testRecordings(secret string) []Recording {
	ws := testWindows()
	recs := make([]Recording, 0, len(ws))
	for _, w := range ws {
		recs = append(recs, recordingOf(w, secret))
	}
	return recs
}

// testLines is N, fewer than the fixture screen's rows.
const testLines = 3

func testMask(t *testing.T) screenproj.Mask {
	t.Helper()
	m, err := screenproj.NewMask([]string{domainName})
	require.NoError(t, err)
	return m
}

func testDeps(t *testing.T, recs []Recording) PipelineDeps {
	t.Helper()
	r, err := redact.New(redact.Options{})
	require.NoError(t, err)
	return PipelineDeps{
		Source:     &sliceSource{recs: recs, failAt: -1},
		Projection: screenproj.Policies{Lines: testLines, Redactor: r, Mask: testMask(t)},
		Splitter: mapSplitter{splits: map[protocol.Digest]Split{
			testDigest("group-2"): SplitValidation,
			testDigest("group-3"): SplitTest,
		}},
	}
}

var testOptions = PipelineOptions{MaxWindows: 10}

func TestRunPipeline(t *testing.T) {
	t.Parallel()
	secret := testSecret(t)
	inv, err := RunPipeline(context.Background(), testDeps(t, testRecordings(secret)), testOptions)
	require.NoError(t, err)
	require.Len(t, inv.Windows, 4)

	for _, w := range inv.Windows {
		require.NoError(t, w.Validate())
		require.Len(t, w.Input.ScreenLines, testLines, "the last n rows")
		require.Equal(t, "deploy to "+maskedName+" staging", w.Input.ScreenLines[0], "the domain name is masked")
		require.True(t, strings.HasPrefix(w.Input.ScreenLines[1], "export GITHUB_TOKEN=••••"), "the row around the secret stays")
		require.NotContains(t, strings.Join(w.Input.ScreenLines, "\n"), secret, "the redactor masked the secret")
		require.Equal(t, promptRow, w.Input.ScreenLines[2], "a closed draft keeps the prompt row")
		require.Equal(t, int64(80), w.Screen.Cols)
		require.Equal(t, int64(4), w.Screen.Rows, "evidence is the recorded pane size")
		require.Equal(t, ScreenDigest(80, 4, w.Input.ScreenLines), w.Screen.Digest)
		require.Equal(t, Policies{
			RedactionPolicyVersion:  redact.PolicyVersion,
			HumanInputPolicyVersion: screenproj.HumanInputPolicyV1,
			DomainMaskPolicyVersion: testMask(t).Version(),
		}, w.Policies)
	}

	want, err := BuildManifest(inv.Windows)
	require.NoError(t, err)
	require.Equal(t, want, inv.Manifest)
	require.Equal(t, ManifestCounts{
		Windows:  4,
		BySplit:  SplitCounts{Train: 2, Validation: 1, Test: 1},
		ByLabel:  LabelCounts{A: 1, C: 2, G: 1},
		ByDomain: DomainCounts{BankGeneric: 2, FleetManaged: 2},
		BySplitLabelHarness: []CountCell{
			{Split: SplitTrain, Label: LabelA, Harness: protocol.HarnessClaudeCode, N: 1},
			{Split: SplitTrain, Label: LabelC, Harness: protocol.HarnessClaudeCode, N: 1},
			{Split: SplitValidation, Label: LabelC, Harness: protocol.HarnessClaudeCode, N: 1},
			{Split: SplitTest, Label: LabelG, Harness: protocol.HarnessClaudeCode, N: 1},
		},
	}, inv.Manifest.Counts)
}

// TestRunPipelineExcludesTheDraft: the window's draft state reaches the
// projection, and only a draft known closed keeps the screen; the zero
// value counts as possibly open.
func TestRunPipelineExcludesTheDraft(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		draft DraftState
		kept  bool
	}{
		{"", false}, {DraftUnknown, false}, {DraftOpen, false}, {DraftClosed, true},
	} {
		rec := recordingOf(validWindow(), testSecret(t))
		rec.Screen.Rows[3] = humanDraft
		rec.Draft = tc.draft
		rec.Cursor = screenproj.Cursor{Row: 3, Known: true, Visible: true}
		inv, err := RunPipeline(context.Background(), testDeps(t, []Recording{rec}), testOptions)
		require.NoError(t, err)
		w := inv.Windows[0]
		require.Equal(t, tc.kept, strings.Contains(strings.Join(w.Input.ScreenLines, "\n"), humanDraft), "draft %q", tc.draft)
		if !tc.kept {
			require.Equal(t, []string{}, w.Input.ScreenLines, "draft %q: the whole screen is blanked", tc.draft)
			enc, err := json.Marshal(w)
			require.NoError(t, err)
			require.NotContains(t, string(enc), "human draft", "draft %q", tc.draft)
		}
	}
}

func TestRunPipelineReadinessThresholds(t *testing.T) {
	t.Parallel()
	inv, err := RunPipeline(context.Background(), testDeps(t, testRecordings(testSecret(t))), testOptions)
	require.NoError(t, err)
	require.Equal(t, DefaultReadinessThresholds(), inv.Manifest.Readiness.Thresholds)

	th := ReadinessThresholds{}
	opts := testOptions
	opts.Readiness = &th
	inv, err = RunPipeline(context.Background(), testDeps(t, testRecordings(testSecret(t))), opts)
	require.NoError(t, err)
	require.Equal(t, th, inv.Manifest.Readiness.Thresholds, "config thresholds reach the manifest")
}

func TestRunPipelineShortScreen(t *testing.T) {
	t.Parallel()
	rec := recordingOf(validWindow(), testSecret(t))
	rec.Screen.Rows = nil
	inv, err := RunPipeline(context.Background(), testDeps(t, []Recording{rec}), testOptions)
	require.NoError(t, err)
	require.Equal(t, []string{}, inv.Windows[0].Input.ScreenLines, "an empty pane gives no lines, not null")
	require.Equal(t, int64(1), inv.Windows[0].Screen.Rows)
}

func TestRunPipelineFailsClosed(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		mutate func(*PipelineDeps, *PipelineOptions, []Recording)
	}{
		{"no source", func(d *PipelineDeps, _ *PipelineOptions, _ []Recording) { d.Source = nil }},
		{"no splitter", func(d *PipelineDeps, _ *PipelineOptions, _ []Recording) { d.Splitter = nil }},
		{"zero screen lines", func(d *PipelineDeps, _ *PipelineOptions, _ []Recording) { d.Projection.Lines = 0 }},
		{"too many screen lines", func(d *PipelineDeps, _ *PipelineOptions, _ []Recording) {
			d.Projection.Lines = screenproj.MaxLines + 1
		}},
		{"no redactor", func(d *PipelineDeps, _ *PipelineOptions, _ []Recording) { d.Projection.Redactor = nil }},
		{"no mask chosen", func(d *PipelineDeps, _ *PipelineOptions, _ []Recording) { d.Projection.Mask = screenproj.Mask{} }},
		{"zero max windows", func(_ *PipelineDeps, o *PipelineOptions, _ []Recording) { o.MaxWindows = 0 }},
		{"more recordings than max", func(_ *PipelineDeps, o *PipelineOptions, _ []Recording) { o.MaxWindows = 3 }},
		{"no recordings", func(d *PipelineDeps, _ *PipelineOptions, _ []Recording) {
			d.Source = &sliceSource{failAt: -1}
		}},
		{"source fails mid-run", func(d *PipelineDeps, _ *PipelineOptions, recs []Recording) {
			d.Source = &sliceSource{recs: recs, failAt: 2}
		}},
		{"redactor fails", func(d *PipelineDeps, _ *PipelineOptions, _ []Recording) { d.Projection.Redactor = failingRedactor{} }},
		{"redactor names no version", func(d *PipelineDeps, _ *PipelineOptions, _ []Recording) {
			d.Projection.Redactor = versionlessRedactor{inner: d.Projection.Redactor}
		}},
		{"splitter fails", func(d *PipelineDeps, _ *PipelineOptions, _ []Recording) {
			d.Splitter = mapSplitter{err: errSentinel}
		}},
		{"draft open, cursor unknown", func(_ *PipelineDeps, _ *PipelineOptions, recs []Recording) {
			recs[1].Draft = DraftOpen
		}},
		{"draft unknown, cursor hidden", func(_ *PipelineDeps, _ *PipelineOptions, recs []Recording) {
			recs[1].Draft, recs[1].Cursor = DraftUnknown, screenproj.Cursor{Row: 3, Known: true}
		}},
		{"no width", func(_ *PipelineDeps, _ *PipelineOptions, recs []Recording) { recs[1].Screen.Cols = 0 }},
		{"invalid window", func(_ *PipelineDeps, _ *PipelineOptions, recs []Recording) { recs[1].Label = "H" }},
		{"repeated window id", func(_ *PipelineDeps, _ *PipelineOptions, recs []Recording) {
			recs[2].WindowID = recs[0].WindowID
		}},
		{"split moves within a group", func(d *PipelineDeps, _ *PipelineOptions, _ []Recording) {
			calls := 0
			d.Splitter = splitterFunc(func(protocol.Digest) (Split, error) {
				calls++
				if calls == 3 {
					return SplitTest, nil
				}
				return SplitTrain, nil
			})
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			secret := testSecret(t)
			recs := testRecordings(secret)
			deps := testDeps(t, recs)
			opts := testOptions
			tc.mutate(&deps, &opts, recs)
			inv, err := RunPipeline(context.Background(), deps, opts)
			require.ErrorIs(t, err, ErrPipeline)
			require.Equal(t, Inventory{}, inv, "no partial inventory")
			require.NotContains(t, err.Error(), "sentinel", "no stage detail or screen content")
			require.NotContains(t, err.Error(), secret)
		})
	}
}

// TestRunPipelineAbortsOnAProjectionError: a window the projection
// refuses ends the run, naming the recording and the reason only.
func TestRunPipelineAbortsOnAProjectionError(t *testing.T) {
	t.Parallel()
	recs := testRecordings(testSecret(t))
	recs[2].Draft, recs[2].Screen.Rows[3] = DraftOpen, humanDraft
	inv, err := RunPipeline(context.Background(), testDeps(t, recs), testOptions)
	require.ErrorIs(t, err, ErrPipeline)
	require.ErrorIs(t, err, screenproj.ErrNoCursor)
	require.Contains(t, err.Error(), "recording 2")
	require.NotContains(t, err.Error(), "human draft")
	require.Equal(t, Inventory{}, inv)

	deps := testDeps(t, testRecordings(testSecret(t)))
	deps.Projection.Redactor = failingRedactor{}
	_, err = RunPipeline(context.Background(), deps, testOptions)
	require.ErrorIs(t, err, screenproj.ErrRedaction)
	require.NotErrorIs(t, err, errSentinel, "the redactor's own error is not wrapped")
}

type splitterFunc func(protocol.Digest) (Split, error)

func (f splitterFunc) Split(g protocol.Digest) (Split, error) { return f(g) }

func TestRunPipelineCancelled(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	inv, err := RunPipeline(ctx, testDeps(t, testRecordings(testSecret(t))), testOptions)
	require.ErrorIs(t, err, ErrPipeline)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, Inventory{}, inv)
}

func TestRunPipelineNamesTheFailingWindowField(t *testing.T) {
	t.Parallel()
	recs := testRecordings(testSecret(t))
	recs[1].Label = "H"
	_, err := RunPipeline(context.Background(), testDeps(t, recs), testOptions)
	var fe *FieldError
	require.ErrorAs(t, err, &fe)
	require.Equal(t, fieldLabel, fe.Field)
	require.Contains(t, err.Error(), "recording 1")
}

// cancelAtEOF cancels the run as it reports the end of its recordings.
type cancelAtEOF struct {
	inner  *sliceSource
	cancel context.CancelFunc
}

func (c *cancelAtEOF) Next(ctx context.Context) (Recording, error) {
	rec, err := c.inner.Next(ctx)
	if errors.Is(err, io.EOF) {
		c.cancel()
	}
	return rec, err
}

func TestRunPipelineCancelledAtEOF(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	deps := testDeps(t, testRecordings(testSecret(t)))
	deps.Source = &cancelAtEOF{inner: deps.Source.(*sliceSource), cancel: cancel}
	inv, err := RunPipeline(ctx, deps, testOptions)
	require.ErrorIs(t, err, ErrPipeline)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, Inventory{}, inv, "no inventory from a cancelled run")
}

// TestRunPipelineCleaningNeverHidesASecret: controls that show nothing
// are dropped and the secret they split comes out redacted; any other
// control, or a byte that is not UTF-8, ends the run, and no window
// holds the screen.
func TestRunPipelineCleaningNeverHidesASecret(t *testing.T) {
	t.Parallel()
	secret := testSecret(t)
	rec := recordingOf(validWindow(), secret)
	rec.Screen.Rows[2] = "export GITHUB_TOKEN=" + secret[:5] + "\a" + secret[5:10] + "\x00" + secret[10:15] + "\x7f" + secret[15:]
	inv, err := RunPipeline(context.Background(), testDeps(t, []Recording{rec}), testOptions)
	require.NoError(t, err)
	enc, err := json.Marshal(inv.Windows[0])
	require.NoError(t, err)
	for _, part := range []string{secret[:5], secret[5:10], secret[10:15], secret[15:]} {
		require.NotContains(t, string(enc), part, "inert controls inside a secret: it comes out redacted")
	}

	for name, mark := range map[string]string{
		"an escape sequence": "\x1b[0m", "a backspace": "\b", "a carriage return": "\r",
		"a tab": "\t", "a C1 control": "\u009b", "a byte not UTF-8": "\xff",
	} {
		rec := recordingOf(validWindow(), secret)
		rec.Screen.Rows[2] = "export GITHUB_TOKEN=" + secret[:12] + mark + secret[12:]
		inv, err := RunPipeline(context.Background(), testDeps(t, []Recording{rec}), testOptions)
		require.ErrorIs(t, err, screenproj.ErrInvalidInput, name)
		require.Equal(t, Inventory{}, inv, "%s: no window", name)
		require.NotContains(t, err.Error(), secret[:12], name)
	}
}
