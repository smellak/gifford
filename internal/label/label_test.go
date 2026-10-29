package label

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/redact"
)

const (
	secretRow = "token on screen: SCREEN-ONLY-CONTENT"
	idA       = protocol.ULID("01JB0000000000000000000001")
	idB       = protocol.ULID("01JB0000000000000000000002")
	idC       = protocol.ULID("01JB0000000000000000000003")
)

var t0 = time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)

type fixedClock struct{ at time.Time }

func (c *fixedClock) Now() time.Time { c.at = c.at.Add(time.Second); return c.at }

type sliceSource struct {
	recs []dataset.Recording
	err  error
}

func (s *sliceSource) Next(context.Context) (dataset.Recording, error) {
	if len(s.recs) == 0 {
		if s.err != nil {
			return dataset.Recording{}, s.err
		}
		return dataset.Recording{}, io.EOF
	}
	r := s.recs[0]
	s.recs = s.recs[1:]
	return r, nil
}

func fixtures() []dataset.Recording {
	recs := make([]dataset.Recording, 0, 3)
	for _, id := range []protocol.ULID{idA, idB, idC} {
		recs = append(recs, dataset.Recording{
			WindowID: id,
			Features: dataset.AnalyzeInput{
				SecondsSinceLastOutput: 1.5,
				OutputBytes:            dataset.OutputBytes{Last500ms: 1, Last2s: 2, Last10s: 3},
			},
			Screen: redact.Screen{Rows: []string{"$ claude", secretRow + "\x1b]52;c;evil\x07\x1b[2J"}, Cols: 80},
		})
	}
	return recs
}

func annotate(t *testing.T, keys string, src dataset.RecordingSource) ([]Annotation, string) {
	t.Helper()
	var screen strings.Builder
	anns, err := Annotate(context.Background(), AnnotateOptions{
		Source: src, Keys: strings.NewReader(keys), Screen: &screen,
		Annotator: "cto", Clock: &fixedClock{at: t0},
	})
	require.NoError(t, err)
	return anns, screen.String()
}

func labelsOf(anns []Annotation) []string {
	out := make([]string, 0, len(anns))
	for _, a := range anns {
		out = append(out, string(a.WindowID)+"="+string(a.Label))
	}
	return out
}

func TestAnnotateKeys(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name, keys string
		want       []string
	}{
		{"label all", "172", []string{string(idA) + "=A", string(idB) + "=G", string(idC) + "=B"}},
		{"skip", "s3s", []string{string(idB) + "=C"}},
		{"undo relabels", "12u5", []string{string(idA) + "=A", string(idB) + "=E"}},
		{"undo a skip", "su4", []string{string(idA) + "=D"}},
		{"undo past start", "uu1", []string{string(idA) + "=A"}},
		{"quit saves", "1q7", []string{string(idA) + "=A"}},
		{"ctrl-c saves", "6\x03", []string{string(idA) + "=F"}},
		{"ctrl-d saves", "6\x04", []string{string(idA) + "=F"}},
		{"keys end", "12", []string{string(idA) + "=A", string(idB) + "=B"}},
		{"unknown keys ignored", "x9 1", []string{string(idA) + "=A"}},
		{"after last undo", "111u2", []string{string(idA) + "=A", string(idB) + "=A", string(idC) + "=B"}},
		{"after last quits", "111x1", []string{string(idA) + "=A", string(idB) + "=A", string(idC) + "=A"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			anns, _ := annotate(t, tc.keys, &sliceSource{recs: fixtures()})
			require.Equal(t, tc.want, labelsOf(anns))
			for _, a := range anns {
				require.Equal(t, "cto", a.Annotator)
				require.True(t, a.LabeledAt.After(t0))
			}
		})
	}
}

func TestAnnotateRendersWithoutControlRunes(t *testing.T) {
	t.Parallel()

	_, screen := annotate(t, "q", &sliceSource{recs: fixtures()})
	require.Contains(t, screen, secretRow)
	require.Contains(t, screen, "quiet 1.5s")
	require.Contains(t, screen, "1 IDLE_AT_PROMPT")
	require.Contains(t, screen, "7 UNKNOWN")
	// Only the TUI's own clear-screen escapes (\x1b[H\x1b[2J) are left.
	require.Equal(t, 2, strings.Count(screen, "\x1b"))
	require.NotContains(t, screen, "\x07")
}

func TestAnnotateSourceError(t *testing.T) {
	t.Parallel()

	boom := errors.New("boom")
	var screen strings.Builder
	anns, err := Annotate(context.Background(), AnnotateOptions{
		Source: &sliceSource{recs: fixtures()[:1], err: boom}, Keys: strings.NewReader("1"),
		Screen: &screen, Annotator: "cto", Clock: &fixedClock{at: t0},
	})
	require.ErrorIs(t, err, boom)
	require.Equal(t, []string{string(idA) + "=A"}, labelsOf(anns), "labels before the failure are kept")
}

func TestAnnotateRejectsBadAnnotator(t *testing.T) {
	t.Parallel()

	for _, a := range []string{"", "a b", "x\x1b", strings.Repeat("a", 65), "é"} {
		_, err := Annotate(context.Background(), AnnotateOptions{
			Source: &sliceSource{}, Keys: strings.NewReader(""), Screen: io.Discard,
			Annotator: a, Clock: &fixedClock{},
		})
		require.ErrorIs(t, err, ErrBadAnnotator, "annotator %q", a)
	}
}

func fileLines(t *testing.T, path string) []string {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	defer func() { require.NoError(t, f.Close()) }()
	var lines []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		lines = append(lines, sc.Text())
	}
	require.NoError(t, sc.Err())
	return lines
}

func requirePrivate(t *testing.T, dir, name string) {
	t.Helper()
	st, err := os.Stat(dir)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o700), st.Mode().Perm())
	st, err = os.Stat(filepath.Join(dir, name))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), st.Mode().Perm())
}

func TestSaveAnnotationsHoldsNoScreen(t *testing.T) {
	t.Parallel()

	dir := Dir(t.TempDir())
	anns, _ := annotate(t, "12", &sliceSource{recs: fixtures()})
	require.NoError(t, SaveAnnotations(dir, anns))
	require.NoError(t, SaveAnnotations(dir, anns[:1]))
	require.NoError(t, SaveAnnotations(dir, nil))
	requirePrivate(t, dir, AnnotationsFile)

	lines := fileLines(t, filepath.Join(dir, AnnotationsFile))
	require.Len(t, lines, 3)
	for _, l := range lines {
		require.NotContains(t, l, "SCREEN-ONLY")
		var m map[string]any
		require.NoError(t, json.Unmarshal([]byte(l), &m))
		require.ElementsMatch(t, []string{"window_id", "label", "annotator", "labeled_at"}, keysOf(m))
	}
}

func keysOf(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func TestRecordHook(t *testing.T) {
	t.Parallel()

	dir := Dir(t.TempDir())
	require.NoError(t, RecordHook(dir, t0, strings.NewReader(" {\"hook_event_name\": \"Stop\",\n \"x\": [1, 2]}\n")))
	require.NoError(t, RecordHook(dir, t0.Add(time.Second), strings.NewReader(`{"hook_event_name":"PreToolUse"}`)))
	requirePrivate(t, dir, HooksFile)

	lines := fileLines(t, filepath.Join(dir, HooksFile))
	require.Equal(t, []string{
		`{"received_at":"2026-10-29T12:00:00Z","event":{"hook_event_name":"Stop","x":[1,2]}}`,
		`{"received_at":"2026-10-29T12:00:01Z","event":{"hook_event_name":"PreToolUse"}}`,
	}, lines)
}

func TestRecordHookRejects(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name, in string
		want     error
	}{
		{"empty", "", ErrEmptyHook},
		{"blank", " \n\t", ErrEmptyHook},
		{"invalid", "{not json", ErrBadHook},
		{"array", "[1]", ErrBadHook},
		{"null", "null", ErrBadHook},
		{"trailing", `{"a":1} {"b":2}`, ErrBadHook},
		{"trailing bracket", `{"a":1}]`, ErrBadHook},
		{"too large", `{"a":"` + strings.Repeat("x", maxHookBytes) + `"}`, ErrHookTooLarge},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			dir := Dir(t.TempDir())
			require.ErrorIs(t, RecordHook(dir, t0, strings.NewReader(tc.in)), tc.want)
			_, err := os.Stat(filepath.Join(dir, HooksFile))
			require.ErrorIs(t, err, os.ErrNotExist)
		})
	}
}

func TestAppendRefusesOpenPerms(t *testing.T) {
	t.Parallel()

	dir := Dir(t.TempDir())
	require.NoError(t, os.MkdirAll(dir, 0o700))
	path := filepath.Join(dir, HooksFile)
	require.NoError(t, os.WriteFile(path, nil, 0o600))
	require.NoError(t, os.Chmod(path, 0o644))
	require.ErrorIs(t, RecordHook(dir, t0, strings.NewReader(`{}`)), ErrUnsafeFile)

	open := Dir(t.TempDir())
	require.NoError(t, os.MkdirAll(open, 0o700))
	require.NoError(t, os.Chmod(open, 0o755))
	require.ErrorIs(t, SaveAnnotations(open, []Annotation{{WindowID: idA}}), ErrUnsafeFile)
}
