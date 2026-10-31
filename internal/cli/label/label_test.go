package label

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/cli/exitcode"
	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/label"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/redact"
)

func TestLabelNotImplemented(t *testing.T) {
	t.Parallel()

	for _, args := range []string{
		"transcript",
	} {
		t.Run(args, func(t *testing.T) {
			t.Parallel()

			cmd := NewCmd(&deps.Deps{})
			cmd.SetOut(io.Discard)
			cmd.SetArgs(strings.Fields(args))

			var ec *deps.ExitError
			require.ErrorAs(t, cmd.Execute(), &ec)
			require.Equal(t, exitcode.NotImplemented, ec.Code)
		})
	}
}

func TestLabelBareGroupShowsHelp(t *testing.T) {
	t.Parallel()

	cmd := NewCmd(&deps.Deps{})
	cmd.SetOut(io.Discard)
	cmd.SetArgs([]string{})
	require.NoError(t, cmd.Execute())
}

// The tests below set XDG_DATA_HOME, so they do not run in parallel.

func labelsDir(t *testing.T) string {
	t.Helper()
	data := t.TempDir()
	t.Setenv("XDG_DATA_HOME", data)
	t.Setenv(captureSessionEnv, "run-123-1761739200000000000")
	return filepath.Join(data, "gifford", "labels")
}

const (
	hookCmd      = "hook claude-code"
	stopHookJSON = `{"hook_event_name":"Stop"}`
)

func TestHookAlwaysExitsZero(t *testing.T) {
	for _, tc := range []struct {
		name, args, stdin string
		recorded, warned  bool
	}{
		{"valid", hookCmd, stopHookJSON, true, false},
		{"unknown flags", "hook claude-code --event PreToolUse --foo", `{"hook_event_name":"PreToolUse"}`, false, true},
		{"missing harness", "hook", stopHookJSON, false, true},
		{"invalid harness", "hook editor", stopHookJSON, false, true},
		{"invalid", hookCmd, `{not json`, false, true},
		{"empty", hookCmd, ``, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := labelsDir(t)
			cmd := NewCmd(&deps.Deps{})
			var out, errOut bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetErr(&errOut)
			cmd.SetIn(strings.NewReader(tc.stdin))
			cmd.SetArgs(strings.Fields(tc.args))
			require.NoError(t, cmd.Execute())
			require.Empty(t, out.String())

			_, err := os.Stat(filepath.Join(dir, label.HooksFile))
			require.Equal(t, tc.recorded, err == nil)
			require.Equal(t, tc.warned, strings.HasPrefix(errOut.String(), "gifford label hook: "), errOut.String())
		})
	}
}

func TestHookUnwritableDataDirExitsZero(t *testing.T) {
	dir := labelsDir(t)
	require.NoError(t, os.MkdirAll(filepath.Dir(dir), 0o700))
	require.NoError(t, os.WriteFile(dir, nil, 0o600)) // labels is a file

	cmd := NewCmd(&deps.Deps{})
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetIn(strings.NewReader(stopHookJSON))
	cmd.SetArgs(strings.Fields(hookCmd))
	require.NoError(t, cmd.Execute())
	require.Empty(t, out.String())
	require.NotEmpty(t, errOut.String())
}

func TestHookMissingCaptureIDExitsZero(t *testing.T) {
	data := t.TempDir()
	t.Setenv("XDG_DATA_HOME", data)
	t.Setenv(captureSessionEnv, "")
	cmd := NewCmd(&deps.Deps{})
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetIn(strings.NewReader(`{"hook_event_name":"Stop","session_id":"native-1"}`))
	cmd.SetArgs(strings.Fields(hookCmd))
	require.NoError(t, cmd.Execute())
	require.Empty(t, out.String())
	require.Contains(t, errOut.String(), "invalid capture session")
	_, err := os.Stat(filepath.Join(data, "gifford", "labels", label.HooksFile))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestHookDoesNotLogOrPersistInputContent(t *testing.T) {
	dir := labelsDir(t)
	const canary = "CANARY-hook-private-content"

	cmd := NewCmd(&deps.Deps{})
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetIn(strings.NewReader(`{"hook_event_name":"Notification","session_id":"s-1","notification_type":"` + canary + `","prompt":"` + canary + `","tool_input":"` + canary + `","tool_output":"` + canary + `","tool_response":"` + canary + `","message":"` + canary + `","title":"` + canary + `","last_assistant_message":"` + canary + `","error_details":"` + canary + `","cwd":"` + canary + `","transcript_path":"` + canary + `","questions":"` + canary + `","answers":"` + canary + `"}`))
	cmd.SetArgs(strings.Fields(hookCmd))
	require.NoError(t, cmd.Execute())
	require.NotContains(t, out.String(), canary)
	require.NotContains(t, errOut.String(), canary)
	raw, err := os.ReadFile(filepath.Join(dir, label.HooksFile))
	require.NoError(t, err)
	require.NotContains(t, string(raw), canary)
	require.Contains(t, string(raw), `"notification_type":"unknown"`)

	cmd = NewCmd(&deps.Deps{})
	errOut.Reset()
	cmd.SetOut(io.Discard)
	cmd.SetErr(&errOut)
	cmd.SetIn(strings.NewReader(`{"hook_event_name":"` + canary + ` space","prompt":"` + canary + `"}`))
	cmd.SetArgs(strings.Fields(hookCmd))
	require.NoError(t, cmd.Execute())
	require.NotContains(t, errOut.String(), canary)
}

func TestOpenCodeHookDoesNotLogOrPersistBridgeContent(t *testing.T) {
	dir := labelsDir(t)
	const canary = "CANARY-opencode-private-content"
	events := []string{
		`{"type":"session.created","properties":{"sessionID":"s1","info":{"id":"s1","title":"` + canary + `","directory":"/` + canary + `"}}}`,
		`{"type":"permission.asked","properties":{"id":"per-1","sessionID":"s1","tool_name":"question","patterns":["` + canary + `"]}}`,
		`{"type":"question.asked","properties":{"id":"que-1","sessionID":"s1","questions":["` + canary + `"]}}`,
	}
	var stdout, stderr bytes.Buffer
	for _, event := range events {
		cmd := NewCmd(&deps.Deps{})
		cmd.SetOut(&stdout)
		cmd.SetErr(&stderr)
		cmd.SetIn(strings.NewReader(event))
		cmd.SetArgs([]string{"hook", "opencode"})
		require.NoError(t, cmd.Execute())
	}
	require.NotContains(t, stdout.String(), canary)
	require.NotContains(t, stderr.String(), canary)
	raw, err := os.ReadFile(filepath.Join(dir, label.HooksFile))
	require.NoError(t, err)
	require.NotContains(t, string(raw), canary)
	require.Contains(t, string(raw), `"has_parent":false`)
	require.Contains(t, string(raw), `"tool_name":"question"`)
}

func TestWeakImportWritesOnlyWeakFile(t *testing.T) {
	dir := labelsDir(t)
	source := filepath.Join(t.TempDir(), "weak.jsonl")
	line := `{"window_id":"01JB0000000000000000000001","label":"G","reason":"no_determinant_indicator","method":"agent_review","criterion_version":"weak-review-v1","reviewer":"reviewer-1","labeled_at":"2026-10-29T12:00:00Z"}` + "\n"
	require.NoError(t, os.WriteFile(source, []byte(line), 0o600))

	cmd := NewCmd(&deps.Deps{})
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"weak", "import", source})
	require.NoError(t, cmd.Execute())
	require.Equal(t, "imported 1 weak labels\n", out.String())
	raw, err := os.ReadFile(filepath.Join(dir, label.WeakFile))
	require.NoError(t, err)
	require.Contains(t, string(raw), `"source":"weak"`)
	require.Contains(t, string(raw), `"label":"G"`)
	_, err = os.Stat(filepath.Join(dir, label.AnnotationsFile))
	require.ErrorIs(t, err, os.ErrNotExist, "weak import never writes manual annotations")
}

func TestWeakImportRejectsWholeFile(t *testing.T) {
	dir := labelsDir(t)
	source := filepath.Join(t.TempDir(), "weak.jsonl")
	valid := `{"window_id":"01JB0000000000000000000001","label":"G","reason":"no_determinant_indicator","method":"agent_review","criterion_version":"weak-review-v1","reviewer":"reviewer-1","labeled_at":"2026-10-29T12:00:00Z"}`
	require.NoError(t, os.WriteFile(source, []byte(valid+"\n"+`{"screen":"CANARY-private"}`+"\n"), 0o600))

	cmd := NewCmd(&deps.Deps{})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"weak", "import", source})
	err := cmd.Execute()
	require.Error(t, err)
	require.NotContains(t, err.Error(), "CANARY")
	_, statErr := os.Stat(filepath.Join(dir, label.WeakFile))
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestWeakImportHelpDocumentsCriterion(t *testing.T) {
	t.Parallel()

	cmd := newWeakImportCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"--help"})
	require.NoError(t, cmd.Execute())
	help := out.String()
	for _, want := range []string{
		label.WeakCriterionV1,
		label.WeakMethodAgentReview,
		string(label.WeakReasonVisibleState),
		string(label.WeakReasonNoDeterminant),
		"not the intended script",
		"blank screens",
		"weak.keep, which remains off by default",
	} {
		require.Contains(t, help, want)
	}
}

type oneWindow struct{ done bool }

func (s *oneWindow) Next(context.Context) (dataset.Recording, error) {
	if s.done {
		return dataset.Recording{}, io.EOF
	}
	s.done = true
	return dataset.Recording{
		WindowID: protocol.ULID("01JB0000000000000000000001"),
		Screen:   redact.Screen{Rows: []string{"SCREEN-ONLY-CONTENT"}, Cols: 80},
	}, nil
}

func execAnnotate(t *testing.T, open sourceOpener, keys string, args ...string) (string, error) {
	t.Helper()
	cmd := newCmd(open)
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(io.Discard)
	cmd.SetIn(strings.NewReader(keys))
	cmd.SetArgs(append([]string{"annotate"}, args...))
	err := cmd.Execute()
	return out.String(), err
}

func TestAnnotateSavesLabels(t *testing.T) {
	dir := labelsDir(t)
	open := func(context.Context, string, io.Writer) (dataset.RecordingSource, error) { return &oneWindow{}, nil }
	out, err := execAnnotate(t, open, "4", "--annotator", benchAnnotator)
	require.NoError(t, err)
	require.Contains(t, out, "SCREEN-ONLY-CONTENT")
	require.Contains(t, out, "saved 1 labels")

	raw, err := os.ReadFile(filepath.Join(dir, label.AnnotationsFile))
	require.NoError(t, err)
	require.Contains(t, string(raw), `"label":"D","annotator":"bench"`)
	require.NotContains(t, string(raw), "SCREEN-ONLY")
}

func TestAnnotateUsage(t *testing.T) {
	labelsDir(t)
	open := func(context.Context, string, io.Writer) (dataset.RecordingSource, error) { return &oneWindow{}, nil }

	_, err := execAnnotate(t, open, "")
	require.ErrorContains(t, err, "annotator")

	// The name is checked before the source is opened, whatever it is.
	never := func(context.Context, string, io.Writer) (dataset.RecordingSource, error) {
		t.Error("source opened for a bad annotator")
		return nil, io.EOF
	}
	for _, o := range []sourceOpener{never, recordingReader} {
		_, err = execAnnotate(t, o, "", "--annotator", "a b")
		var ec *deps.ExitError
		require.ErrorAs(t, err, &ec)
		require.Equal(t, exitcode.Usage, ec.Code)
	}
}

// TestAnnotateReadsRecordings: the shipped reader replays the data dir's
// sessions and skips, with a warning, one that does not replay whole.
func TestAnnotateReadsRecordings(t *testing.T) {
	dir := labelsDir(t)
	rec := filepath.Join(filepath.Dir(dir), "recordings")
	require.NoError(t, os.MkdirAll(rec, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(rec, "cut-1.raw"), nil, 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(rec, "cut-1.events.jsonl"), []byte(`{"session":"cut-1"`), 0o600))

	cmd := NewCmd(&deps.Deps{})
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetIn(strings.NewReader(""))
	cmd.SetArgs([]string{"annotate", "--annotator", benchAnnotator})
	require.NoError(t, cmd.Execute())
	require.Contains(t, out.String(), "saved 0 labels")
	require.Contains(t, errOut.String(), "session cut-1 skipped")
}

const (
	alignCmd       = "align"
	benchAnnotator = "bench"
)

func TestAlignWritesLabels(t *testing.T) {
	dir := labelsDir(t)
	require.NoError(t, label.SaveAnnotations(dir, []label.Annotation{{
		WindowID: protocol.ULID("01JB0000000000000000000001"), Label: dataset.LabelB, Annotator: benchAnnotator,
	}}))
	open := func(context.Context, string, io.Writer) (dataset.RecordingSource, error) { return &oneWindow{}, nil }

	cmd := newCmd(open)
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetArgs([]string{alignCmd})
	require.NoError(t, cmd.Execute())
	require.Equal(t, "windows 1: manual 1, script 0, hook 0, weak 0, unlabeled 0, discarded 0; skipped lines: hooks 0, annotations 0, weak 0\n"+
		"discarded by a weak transition only: 0\n"+
		"doubtful: conflict 0, unlabeled 0, weak_only 0\n", out.String())

	raw, err := os.ReadFile(filepath.Join(dir, label.AlignedFile))
	require.NoError(t, err)
	require.JSONEq(t, `{"window_id":"01JB0000000000000000000001","label":"B","source":"manual"}`, string(raw))
}

func TestAlignReadsImportedWeakLabels(t *testing.T) {
	dir := labelsDir(t)
	line := `{"window_id":"01JB0000000000000000000001","label":"G","reason":"no_determinant_indicator","method":"agent_review","criterion_version":"weak-review-v1","reviewer":"reviewer-1","labeled_at":"2026-10-29T12:00:00Z"}`
	_, err := label.ImportWeak(dir, strings.NewReader(line))
	require.NoError(t, err)
	open := func(context.Context, string, io.Writer) (dataset.RecordingSource, error) { return &oneWindow{}, nil }

	cmd := newCmd(open)
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{alignCmd})
	require.NoError(t, cmd.Execute())
	raw, err := os.ReadFile(filepath.Join(dir, label.AlignedFile))
	require.NoError(t, err)
	require.JSONEq(t, `{"window_id":"01JB0000000000000000000001","label":"G","source":"weak","weak_label":"G","weak_origin":"import","doubt":"WEAK_ONLY"}`, string(raw))
}

func TestAlignBadConfigIsUsage(t *testing.T) {
	labelsDir(t)
	path := filepath.Join(t.TempDir(), "align.yaml")
	require.NoError(t, os.WriteFile(path, []byte("margin_ms: -5\n"), 0o600))
	open := func(context.Context, string, io.Writer) (dataset.RecordingSource, error) { return &oneWindow{}, nil }

	cmd := newCmd(open)
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{alignCmd, "--config", path})
	var ec *deps.ExitError
	require.ErrorAs(t, cmd.Execute(), &ec)
	require.Equal(t, exitcode.Usage, ec.Code)
}

func TestAlignWithNoRecordings(t *testing.T) {
	labelsDir(t)
	cmd := NewCmd(&deps.Deps{})
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{alignCmd})
	require.NoError(t, cmd.Execute())
	require.Contains(t, out.String(), "windows 0:")
}

// doubtWindows: three generic-harness windows with no hooks, all without
// a label, so all doubtful (UNLABELED), from two sessions out of order.
func doubtWindows() []dataset.Recording {
	base := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	mk := func(id, sess string, sec int, screen string) dataset.Recording {
		at := base.Add(time.Duration(sec) * time.Second)
		return dataset.Recording{
			WindowID: protocol.ULID(id),
			Features: dataset.AnalyzeInput{Harness: protocol.HarnessGeneric, SecondsSinceLastOutput: 0.1, NewChildProcesses: []string{}},
			Screen:   redact.Screen{Rows: []string{screen}, Cols: 80},
			Provenance: dataset.Provenance{
				SessionID: protocol.OpaqueID(sess), AnchorAt: at, WindowStart: at.Add(-time.Second), WindowEnd: at,
			},
		}
	}
	return []dataset.Recording{
		mk("01JB0000000000000000000003", "sess-b", 5, "SCREEN-THREE CANARY-CLI"),
		mk("01JB0000000000000000000001", "sess-a", 9, "SCREEN-ONE CANARY-CLI"),
		mk("01JB0000000000000000000002", "sess-a", 20, "SCREEN-TWO CANARY-CLI"),
	}
}

type recSource struct{ recs []dataset.Recording }

func (s *recSource) Next(context.Context) (dataset.Recording, error) {
	if len(s.recs) == 0 {
		return dataset.Recording{}, io.EOF
	}
	r := s.recs[0]
	s.recs = s.recs[1:]
	return r, nil
}

func TestAnnotateDoubtful(t *testing.T) {
	dir := labelsDir(t)
	// One window is already annotated: it leaves the queue.
	require.NoError(t, label.SaveAnnotations(dir, []label.Annotation{{
		WindowID: "01JB0000000000000000000002", Label: dataset.LabelA, Annotator: benchAnnotator,
	}}))
	open := func(context.Context, string, io.Writer) (dataset.RecordingSource, error) {
		return &recSource{recs: doubtWindows()}, nil
	}

	// Skip the first window asked, then quit: the screens show the order.
	out, err := execAnnotate(t, open, "sq", "--annotator", benchAnnotator, "--doubtful")
	require.NoError(t, err)
	one, three := strings.Index(out, "SCREEN-ONE"), strings.Index(out, "SCREEN-THREE")
	require.Positive(t, one)
	require.Greater(t, three, one, "sess-a before sess-b")
	require.NotContains(t, out, "SCREEN-TWO", "annotated windows are not asked")

	cmd := newCmd(open)
	var alignOut bytes.Buffer
	cmd.SetOut(&alignOut)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{alignCmd})
	require.NoError(t, cmd.Execute())
	require.Equal(t, "windows 3: manual 1, script 0, hook 0, weak 0, unlabeled 2, discarded 0; skipped lines: hooks 0, annotations 0, weak 0\n"+
		"discarded by a weak transition only: 0\n"+
		"doubtful: conflict 0, unlabeled 2, weak_only 0\n"+
		"doubtful generic: conflict 0, unlabeled 2, weak_only 0\n", alignOut.String())

	for _, name := range []string{label.AlignedFile, label.AnnotationsFile} {
		raw, err := os.ReadFile(filepath.Join(dir, name))
		require.NoError(t, err)
		require.NotContains(t, string(raw), "CANARY", name)
		require.NotContains(t, string(raw), "SCREEN", name)
	}
}
