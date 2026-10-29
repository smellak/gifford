package dataset

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/redact"
	"github.com/smellak/gifford/internal/redact/redacttest"
)

// Invented screen content: a draft the human typed at the prompt and a
// domain name the mask must hide.
const (
	humanDraft = "> sentinel human draft"
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

// promptExcluder blanks every prompt row.
type promptExcluder struct {
	version string
	err     error
	cols    int // when set, reports a changed width
}

func (e promptExcluder) ExcludeHumanInput(s redact.Screen) (redact.Screen, string, error) {
	if e.err != nil {
		return redact.Screen{}, "", e.err
	}
	rows := make([]string, len(s.Rows))
	for i, r := range s.Rows {
		if !strings.HasPrefix(r, "> ") {
			rows[i] = r
		}
	}
	cols := s.Cols
	if e.cols != 0 {
		cols = e.cols
	}
	return redact.Screen{Rows: rows, Cols: cols}, e.version, nil
}

// nameMasker replaces the domain name.
type nameMasker struct {
	version string
	err     error
	extra   bool // when set, adds a row
}

func (m nameMasker) MaskDomain(s redact.Screen) (redact.Screen, string, error) {
	if m.err != nil {
		return redact.Screen{}, "", m.err
	}
	rows := make([]string, 0, len(s.Rows)+1)
	for _, r := range s.Rows {
		rows = append(rows, strings.ReplaceAll(r, domainName, maskedName))
	}
	if m.extra {
		rows = append(rows, "")
	}
	return redact.Screen{Rows: rows, Cols: s.Cols}, m.version, nil
}

type failingRedactor struct{}

func (failingRedactor) RedactScreen(redact.Screen) (redact.ScreenResult, error) {
	return redact.ScreenResult{}, errSentinel
}

type versionlessRedactor struct{ inner ScreenRedactor }

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
// features without screen lines, and a raw screen holding a secret, a
// human draft and a domain name.
func recordingOf(w Window, secret string) Recording {
	features := w.Input
	features.ScreenLines = nil
	return Recording{
		WindowID: w.WindowID,
		Contract: w.Contract,
		Features: features,
		Screen: redact.Screen{Cols: 80, Rows: []string{
			"deploy to " + domainName + " staging",
			"export GITHUB_TOKEN=" + secret,
			humanDraft,
			"tests: 9 passed, 0 failed",
		}},
		Label:             w.Target.Label,
		Provenance:        w.Provenance,
		EnvironmentDomain: w.EnvironmentDomain,
		GroupDigest:       w.Split.GroupDigest,
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

func testDeps(t *testing.T, recs []Recording) PipelineDeps {
	t.Helper()
	r, err := redact.New(redact.Options{})
	require.NoError(t, err)
	return PipelineDeps{
		Source:   &sliceSource{recs: recs, failAt: -1},
		Excluder: promptExcluder{version: "1"},
		Redactor: r,
		Masker:   nameMasker{version: "1"},
		Splitter: mapSplitter{splits: map[protocol.Digest]Split{
			testDigest("group-2"): SplitValidation,
			testDigest("group-3"): SplitTest,
		}},
	}
}

var testOptions = PipelineOptions{ScreenLines: 3, MaxWindows: 10}

func TestRunPipeline(t *testing.T) {
	t.Parallel()
	secret := testSecret(t)
	inv, err := RunPipeline(context.Background(), testDeps(t, testRecordings(secret)), testOptions)
	require.NoError(t, err)
	require.Len(t, inv.Windows, 4)

	for _, w := range inv.Windows {
		require.NoError(t, w.Validate())
		require.Len(t, w.Input.ScreenLines, testOptions.ScreenLines, "the last n rows")
		joined := strings.Join(w.Input.ScreenLines, "\n")
		require.NotContains(t, joined, secret, "the redactor masked the secret")
		require.NotContains(t, joined, "sentinel", "human input and domain names are gone")
		require.Contains(t, joined, "export GITHUB_TOKEN=", "the row around the secret stays")
		require.Empty(t, w.Input.ScreenLines[1], "the prompt row is excluded")
		require.Equal(t, int64(80), w.Screen.Cols)
		require.Equal(t, int64(4), w.Screen.Rows, "evidence is the recorded pane size")
		require.Equal(t, ScreenDigest(80, 4, w.Input.ScreenLines), w.Screen.Digest)
		require.Equal(t, Policies{RedactionPolicyVersion: redact.PolicyVersion, HumanInputPolicyVersion: "1", DomainMaskPolicyVersion: "1"}, w.Policies)
	}

	want, err := BuildManifest(inv.Windows)
	require.NoError(t, err)
	require.Equal(t, want, inv.Manifest)
	require.Equal(t, ManifestCounts{
		Windows:  4,
		BySplit:  SplitCounts{Train: 2, Validation: 1, Test: 1},
		ByLabel:  LabelCounts{A: 1, C: 2, G: 1},
		ByDomain: DomainCounts{BankGeneric: 2, FleetManaged: 2},
	}, inv.Manifest.Counts)
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
		{"no excluder", func(d *PipelineDeps, _ *PipelineOptions, _ []Recording) { d.Excluder = nil }},
		{"no redactor", func(d *PipelineDeps, _ *PipelineOptions, _ []Recording) { d.Redactor = nil }},
		{"no masker", func(d *PipelineDeps, _ *PipelineOptions, _ []Recording) { d.Masker = nil }},
		{"no splitter", func(d *PipelineDeps, _ *PipelineOptions, _ []Recording) { d.Splitter = nil }},
		{"zero screen lines", func(_ *PipelineDeps, o *PipelineOptions, _ []Recording) { o.ScreenLines = 0 }},
		{"too many screen lines", func(_ *PipelineDeps, o *PipelineOptions, _ []Recording) { o.ScreenLines = maxScreenRows + 1 }},
		{"zero max windows", func(_ *PipelineDeps, o *PipelineOptions, _ []Recording) { o.MaxWindows = 0 }},
		{"more recordings than max", func(_ *PipelineDeps, o *PipelineOptions, _ []Recording) { o.MaxWindows = 3 }},
		{"no recordings", func(d *PipelineDeps, _ *PipelineOptions, _ []Recording) {
			d.Source = &sliceSource{failAt: -1}
		}},
		{"source fails mid-run", func(d *PipelineDeps, _ *PipelineOptions, recs []Recording) {
			d.Source = &sliceSource{recs: recs, failAt: 2}
		}},
		{"excluder fails", func(d *PipelineDeps, _ *PipelineOptions, _ []Recording) {
			d.Excluder = promptExcluder{err: errSentinel}
		}},
		{"redactor fails", func(d *PipelineDeps, _ *PipelineOptions, _ []Recording) { d.Redactor = failingRedactor{} }},
		{"masker fails", func(d *PipelineDeps, _ *PipelineOptions, _ []Recording) { d.Masker = nameMasker{err: errSentinel} }},
		{"splitter fails", func(d *PipelineDeps, _ *PipelineOptions, _ []Recording) {
			d.Splitter = mapSplitter{err: errSentinel}
		}},
		{"excluder names no version", func(d *PipelineDeps, _ *PipelineOptions, _ []Recording) {
			d.Excluder = promptExcluder{}
		}},
		{"redactor names no version", func(d *PipelineDeps, _ *PipelineOptions, _ []Recording) {
			d.Redactor = versionlessRedactor{inner: d.Redactor}
		}},
		{"masker names no version", func(d *PipelineDeps, _ *PipelineOptions, _ []Recording) {
			d.Masker = nameMasker{}
		}},
		{"a stage changes the width", func(d *PipelineDeps, _ *PipelineOptions, _ []Recording) {
			d.Excluder = promptExcluder{version: "1", cols: 81}
		}},
		{"a stage adds a row", func(d *PipelineDeps, _ *PipelineOptions, _ []Recording) {
			d.Masker = nameMasker{version: "1", extra: true}
		}},
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
