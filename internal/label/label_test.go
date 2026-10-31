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
	secretRow     = "token on screen: SCREEN-ONLY-CONTENT"
	hookCaptureID = protocol.OpaqueID("run-123-1761739200000000000")
	idA           = protocol.ULID("01JB0000000000000000000001")
	idB           = protocol.ULID("01JB0000000000000000000002")
	idC           = protocol.ULID("01JB0000000000000000000003")
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
	require.NoError(t, RecordHook(dir, t0, protocol.HarnessClaudeCode, hookCaptureID, strings.NewReader(" {\"hook_event_name\": \"Stop\",\n \"session_id\": \"ab-12\", \"x\": [1, 2]}\n")))
	require.NoError(t, RecordHook(dir, t0.Add(time.Second), protocol.HarnessClaudeCode, hookCaptureID, strings.NewReader(`{"hook_event_name":"PreToolUse"}`)))
	requirePrivate(t, dir, HooksFile)

	lines := fileLines(t, filepath.Join(dir, HooksFile))
	require.Equal(t, []string{
		`{"received_at":"2026-10-29T12:00:00Z","harness":"claude-code","capture_session_id":"run-123-1761739200000000000","event":{"hook_event_name":"Stop","session_id":"ab-12"}}`,
		`{"received_at":"2026-10-29T12:00:01Z","harness":"claude-code","capture_session_id":"run-123-1761739200000000000","event":{"hook_event_name":"PreToolUse"}}`,
	}, lines)
}

// TestRecordHookKeepsNoContent: the prompt, the tool input and output,
// the paths and the message never reach hooks.jsonl.
func TestRecordHookKeepsNoContent(t *testing.T) {
	t.Parallel()

	const canary = "CANARY-7f3a"
	dir := Dir(t.TempDir())
	for _, ev := range []string{
		`{"hook_event_name":"UserPromptSubmit","session_id":"s-1","prompt":"` + canary + `","cwd":"/home/` + canary + `","transcript_path":"/tmp/` + canary + `.jsonl","title":"` + canary + `"}`,
		`{"hook_event_name":"PreToolUse","session_id":"s-1","tool_name":"Bash","tool_input":{"command":"echo ` + canary + `"},"questions":["` + canary + `"],"answers":{"a":"` + canary + `"}}`,
		`{"hook_event_name":"PostToolUse","session_id":"s-1","tool_input":{"c":"` + canary + `"},"tool_output":"` + canary + `","tool_response":{"stdout":"` + canary + `"},"last_assistant_message":"` + canary + `"}`,
		`{"hook_event_name":"Notification","session_id":"s-1","notification_type":"permission_prompt","message":"` + canary + `","error_details":"` + canary + `"}`,
		`{"hook_event_name":"Notification","session_id":"s-1","message":"Claude needs your permission to use ` + canary + `"}`,
		`{"hook_event_name":"Notification","session_id":"s-1","notification_type":"` + canary + `"}`,
		`{"hook_event_name":"Stop","session_id":"` + canary + ` x","stop_hook_active":false}`,
	} {
		require.NoError(t, RecordHook(dir, t0, protocol.HarnessClaudeCode, hookCaptureID, strings.NewReader(ev)))
	}
	raw, err := os.ReadFile(filepath.Join(dir, HooksFile))
	require.NoError(t, err)
	require.NotContains(t, string(raw), canary)
	require.NotContains(t, string(raw), "CANARY")

	lines := fileLines(t, filepath.Join(dir, HooksFile))
	events := make([]string, 0, len(lines))
	for _, l := range lines {
		var r HookRecord
		require.NoError(t, json.Unmarshal([]byte(l), &r))
		events = append(events, r.Event.Name+"/"+r.Event.SessionID+"/"+r.Event.NotificationType)
	}
	require.Equal(t, []string{
		"UserPromptSubmit/s-1/", "PreToolUse/s-1/", "PostToolUse/s-1/",
		"Notification/s-1/permission_prompt", "Notification/s-1/",
		"Notification/s-1/unknown", "Stop//",
	}, events)
}

func TestRecordHookNormalisesEnumsAndMetadata(t *testing.T) {
	t.Parallel()

	dir := Dir(t.TempDir())
	for _, tc := range []struct {
		harness protocol.Harness
		event   string
	}{
		{protocol.HarnessClaudeCode, `{"hook_event_name":"StopFailure","session_id":"cc-1","error":"CANARY-error"}`},
		{protocol.HarnessClaudeCode, `{"hook_event_name":"StopFailure","session_id":"cc-1","error":"rate_limit"}`},
		{protocol.HarnessClaudeCode, `{"hook_event_name":"PreToolUse","session_id":"cc-1","tool_name":"AskUserQuestion","post_stop":true}`},
		{protocol.HarnessClaudeCode, `{"hook_event_name":"PreToolUse","session_id":"cc-1","tool_name":"Bash","post_stop":"true"}`},
		{protocol.HarnessCodex, `{"hook_event_name":"PreToolUse","session_id":"cx-1","tool_name":"request_user_input"}`},
		{protocol.HarnessOpencode, `{"type":"permission.asked","properties":{"sessionID":"oc-1"},"tool_name":"question"}`},
		{protocol.HarnessOpencode, `{"type":"session.error","properties":{"sessionID":"oc-1","error":{"name":"MessageAbortedError","data":"CANARY-detail"}}}`},
	} {
		require.NoError(t, RecordHook(dir, t0, tc.harness, hookCaptureID, strings.NewReader(tc.event)))
	}
	raw, err := os.ReadFile(filepath.Join(dir, HooksFile))
	require.NoError(t, err)
	require.NotContains(t, string(raw), "CANARY")

	lines := fileLines(t, filepath.Join(dir, HooksFile))
	got := make([]HookRecord, 0, len(lines))
	for _, line := range lines {
		var r HookRecord
		require.NoError(t, json.Unmarshal([]byte(line), &r))
		got = append(got, r)
	}
	require.Equal(t, unknownHookEnum, got[0].Event.ErrorType)
	require.Equal(t, "rate_limit", got[1].Event.ErrorType)
	require.Equal(t, "AskUserQuestion", got[2].Event.ToolName)
	require.NotNil(t, got[2].Event.PostStop)
	require.True(t, *got[2].Event.PostStop)
	require.Empty(t, got[3].Event.ToolName)
	require.Nil(t, got[3].Event.PostStop)
	require.Equal(t, "request_user_input", got[4].Event.ToolName)
	require.Equal(t, "permission.asked", got[5].Event.Name)
	require.Equal(t, "oc-1", got[5].Event.SessionID)
	require.Equal(t, "question", got[5].Event.ToolName)
	require.Equal(t, openCodeAborted, got[6].Event.ErrorType)
}

func TestRecordHookNormalisesWaitNotificationsByHarness(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		harness protocol.Harness
		kind    string
		want    string
	}{
		{"claude permission", protocol.HarnessClaudeCode, permissionPrompt, permissionPrompt},
		{"claude question is unknown", protocol.HarnessClaudeCode, questionPrompt, unknownHookEnum},
		{"muse permission", protocol.HarnessMuseCode, permissionPrompt, permissionPrompt},
		{"muse question", protocol.HarnessMuseCode, questionPrompt, questionPrompt},
		{"muse arbitrary is unknown", protocol.HarnessMuseCode, "CANARY-private-kind", unknownHookEnum},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := Dir(t.TempDir())
			event := `{"hook_event_name":"Notification","session_id":"native-1","notification_type":"` + tc.kind + `"}`
			require.NoError(t, RecordHook(dir, t0, tc.harness, hookCaptureID, strings.NewReader(event)))
			records, bad, err := ReadHooks(dir)
			require.NoError(t, err)
			require.Zero(t, bad)
			require.Len(t, records, 1)
			require.Equal(t, tc.want, records[0].Event.NotificationType)
			require.NotContains(t, records[0].Event.NotificationType, "CANARY")
		})
	}
}

func TestRecordHookSessionStartSourceIsClosed(t *testing.T) {
	t.Parallel()

	for _, harness := range []protocol.Harness{
		protocol.HarnessClaudeCode, protocol.HarnessCodex, protocol.HarnessMuseCode,
	} {
		dir := Dir(t.TempDir())
		for _, source := range []string{sessionStartStartup, sessionStartResume, sessionStartClear, sessionStartCompact, "CANARY-arbitrary-source"} {
			event := `{"hook_event_name":"SessionStart","session_id":"native-1","source":"` + source + `"}`
			require.NoError(t, RecordHook(dir, t0, harness, hookCaptureID, strings.NewReader(event)))
		}
		raw, err := os.ReadFile(filepath.Join(dir, HooksFile))
		require.NoError(t, err)
		require.NotContains(t, string(raw), "CANARY")
		lines := fileLines(t, filepath.Join(dir, HooksFile))
		got := make([]string, 0, len(lines))
		for _, line := range lines {
			var r HookRecord
			require.NoError(t, json.Unmarshal([]byte(line), &r))
			got = append(got, r.Event.Source)
		}
		require.Equal(t, []string{sessionStartStartup, sessionStartResume, sessionStartClear, sessionStartCompact, ""}, got, harness)
	}
}

func TestRecordHookRejectsUnsupportedSessionStartEvents(t *testing.T) {
	t.Parallel()

	for _, harness := range []protocol.Harness{protocol.HarnessOpencode, protocol.HarnessAntigravity} {
		dir := Dir(t.TempDir())
		event := `{"hook_event_name":"SessionStart","session_id":"native-1","source":"startup"}`
		require.ErrorIs(t, RecordHook(dir, t0, harness, hookCaptureID, strings.NewReader(event)), ErrBadHook)
		_, err := os.Stat(filepath.Join(dir, HooksFile))
		require.ErrorIs(t, err, os.ErrNotExist)
	}
}

func TestRecordHookProjectsOpenCodeRootEvidence(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		event     string
		sessionID string
		hasParent *bool
	}{
		{
			name:      "root",
			event:     `{"type":"session.created","properties":{"sessionID":"s1","info":{"id":"s1","title":"CANARY-title","directory":"/CANARY-directory"}}}`,
			sessionID: "s1", hasParent: boolPointer(false),
		},
		{
			name:      "child",
			event:     openChildEvent,
			sessionID: "s2", hasParent: boolPointer(true),
		},
		{
			name:      "null is unknown",
			event:     `{"type":"session.created","properties":{"sessionID":"s1","info":{"id":"s1","parentID":null}}}`,
			sessionID: "s1",
		},
		{
			name:      "wrong parent type is unknown",
			event:     `{"type":"session.created","properties":{"sessionID":"s1","info":{"id":"s1","parentID":7}}}`,
			sessionID: "s1",
		},
		{
			name:  "incomplete info",
			event: `{"type":"session.created","properties":{"sessionID":"s1","info":{}}}`,
		},
		{
			name:  "mismatched ids",
			event: `{"type":"session.created","properties":{"sessionID":"s2","info":{"id":"s1"}}}`,
		},
		{
			name:  "missing outer id",
			event: `{"type":"session.created","properties":{"info":{"id":"s1"}}}`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := Dir(t.TempDir())
			require.NoError(t, RecordHook(dir, t0, protocol.HarnessOpencode, hookCaptureID, strings.NewReader(tc.event)))
			raw, err := os.ReadFile(filepath.Join(dir, HooksFile))
			require.NoError(t, err)
			require.NotContains(t, string(raw), "CANARY")
			records, bad, err := ReadHooks(dir)
			require.NoError(t, err)
			require.Zero(t, bad)
			require.Len(t, records, 1)
			require.Equal(t, tc.sessionID, records[0].Event.SessionID)
			require.Equal(t, tc.hasParent, records[0].Event.HasParent)
		})
	}
}

func TestRecordHookProjectsClosedOpenCodeWaitMetadata(t *testing.T) {
	t.Parallel()

	dir := Dir(t.TempDir())
	const canary = "CANARY-opencode-private"
	events := []string{
		`{"type":"permission.asked","properties":{"id":"per-1","sessionID":"s1","permission":"question","tool_name":"question","patterns":["` + canary + `"]}}`,
		`{"type":"permission.replied","properties":{"requestID":"per-1","sessionID":"s1","reply":"once","message":"` + canary + `"}}`,
		`{"type":"question.asked","properties":{"id":"que-1","sessionID":"s1","questions":["` + canary + `"]}}`,
		`{"type":"question.replied","properties":{"requestID":"que-1","sessionID":"s1","answers":["` + canary + `"]}}`,
		`{"type":"session.error","properties":{"sessionID":"s1","error":{"name":"ContextOverflowError","data":{"message":"` + canary + `"}}}}`,
	}
	for _, event := range events {
		require.NoError(t, RecordHook(dir, t0, protocol.HarnessOpencode, hookCaptureID, strings.NewReader(event)))
	}
	raw, err := os.ReadFile(filepath.Join(dir, HooksFile))
	require.NoError(t, err)
	require.NotContains(t, string(raw), canary)
	records, bad, err := ReadHooks(dir)
	require.NoError(t, err)
	require.Zero(t, bad)
	require.Len(t, records, len(events))
	require.Equal(t, "per-1", records[0].Event.RequestID)
	require.Equal(t, "question", records[0].Event.ToolName)
	require.Equal(t, "per-1", records[1].Event.RequestID)
	require.Equal(t, "once", records[1].Event.ReplyType)
	require.Equal(t, "que-1", records[2].Event.RequestID)
	require.Equal(t, "que-1", records[3].Event.RequestID)
	require.Equal(t, "context_overflow", records[4].Event.ErrorType)
}

func boolPointer(value bool) *bool { return &value }

type hookErrorReader struct{ err error }

func (r hookErrorReader) Read([]byte) (int, error) { return 0, r.err }

func TestRecordHookErrorsHoldNoContent(t *testing.T) {
	t.Parallel()

	const canary = "CANARY-private-hook-text"
	for _, err := range []error{
		RecordHook(Dir(t.TempDir()), t0, protocol.HarnessClaudeCode, hookCaptureID, hookErrorReader{err: errors.New(canary)}),
		RecordHook(Dir(t.TempDir()), t0, protocol.HarnessClaudeCode, hookCaptureID, strings.NewReader(`{"hook_event_name":"`+canary+` space","prompt":"`+canary+`"}`)),
	} {
		require.Error(t, err)
		require.NotContains(t, err.Error(), canary)
	}
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
		{"no name", `{"session_id":"s"}`, ErrBadHook},
		{"bad name", `{"hook_event_name":"Stop now"}`, ErrBadHook},
		{"name not a string", `{"hook_event_name":1}`, ErrBadHook},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			dir := Dir(t.TempDir())
			require.ErrorIs(t, RecordHook(dir, t0, protocol.HarnessClaudeCode, hookCaptureID, strings.NewReader(tc.in)), tc.want)
			_, err := os.Stat(filepath.Join(dir, HooksFile))
			require.ErrorIs(t, err, os.ErrNotExist)
		})
	}
}

func TestRecordHookEventNamesAreClosedByHarness(t *testing.T) {
	t.Parallel()

	const canary = "CANARY_private_event"
	for _, tc := range []struct {
		name    string
		harness protocol.Harness
		event   string
	}{
		{"unknown Claude event", protocol.HarnessClaudeCode, `{"hook_event_name":"` + canary + `","session_id":"s1"}`},
		{"OpenCode event on Claude", protocol.HarnessClaudeCode, `{"hook_event_name":"session.created","session_id":"s1"}`},
		{"type fallback on Claude", protocol.HarnessClaudeCode, `{"type":"Stop","session_id":"s1"}`},
		{"Claude envelope on OpenCode", protocol.HarnessOpencode, `{"hook_event_name":"session.created","properties":{"sessionID":"s1","info":{"id":"s1"}}}`},
		{"Muse-only event on Codex", protocol.HarnessCodex, `{"hook_event_name":"ToolUseStart","session_id":"s1"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := Dir(t.TempDir())
			err := RecordHook(dir, t0, tc.harness, hookCaptureID, strings.NewReader(tc.event))
			require.ErrorIs(t, err, ErrBadHook)
			require.NotContains(t, err.Error(), canary)
			_, statErr := os.Stat(filepath.Join(dir, HooksFile))
			require.ErrorIs(t, statErr, os.ErrNotExist)
		})
	}
}

func TestReadHooksDiscardsUnknownEventNames(t *testing.T) {
	t.Parallel()

	dir := Dir(t.TempDir())
	require.NoError(t, RecordHook(dir, t0, protocol.HarnessClaudeCode, hookCaptureID, strings.NewReader(evStop)))
	unknown := `{"received_at":"2026-10-29T12:00:01Z","harness":"claude-code",` +
		`"capture_session_id":"run-123-1761739200000000000","event":{"hook_event_name":"CANARY_private_event","session_id":"s1"}}` + "\n"
	require.NoError(t, appendLine(dir, HooksFile, []byte(unknown)))

	records, bad, err := ReadHooks(dir)
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.Equal(t, 1, bad)
}

func TestRecordHookRejectsHarness(t *testing.T) {
	t.Parallel()

	dir := Dir(t.TempDir())
	require.ErrorIs(t, RecordHook(dir, t0, "editor", hookCaptureID, strings.NewReader(`{"hook_event_name":"Stop"}`)), ErrBadHookHarness)
	_, err := os.Stat(filepath.Join(dir, HooksFile))
	require.ErrorIs(t, err, os.ErrNotExist)
	for _, captureID := range []protocol.OpaqueID{
		"capture-1", "run-0-1", "run-01-1", "run-1-0", "run-1--1", "run-a-1",
		"run-1-999999999999999999999999999999999999",
	} {
		require.ErrorIs(t, RecordHook(dir, t0, protocol.HarnessClaudeCode, captureID, strings.NewReader(`{"hook_event_name":"Stop"}`)), ErrBadHookCapture)
	}
}

func TestAppendRefusesOpenPerms(t *testing.T) {
	t.Parallel()

	dir := Dir(t.TempDir())
	require.NoError(t, os.MkdirAll(dir, 0o700))
	path := filepath.Join(dir, HooksFile)
	require.NoError(t, os.WriteFile(path, nil, 0o600))
	require.NoError(t, os.Chmod(path, 0o644))
	require.ErrorIs(t, RecordHook(dir, t0, protocol.HarnessClaudeCode, hookCaptureID, strings.NewReader(`{"hook_event_name":"Stop"}`)), ErrUnsafeFile)

	open := Dir(t.TempDir())
	require.NoError(t, os.MkdirAll(open, 0o700))
	require.NoError(t, os.Chmod(open, 0o755))
	require.ErrorIs(t, SaveAnnotations(open, []Annotation{{WindowID: idA}}), ErrUnsafeFile)
}
