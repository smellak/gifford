package label

import (
	"context"
	"encoding/json"
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
	idleHook = "A/hook"
	evStop   = `{"hook_event_name":"Stop","session_id":"s1"}`
	evPrompt = `{"hook_event_name":"UserPromptSubmit","session_id":"s1"}`
	evPre    = `{"hook_event_name":"PreToolUse","session_id":"s1"}`
	evPost   = `{"hook_event_name":"PostToolUse","session_id":"s1"}`
	evAsk    = `{"hook_event_name":"Notification","session_id":"s1","notification_type":"permission_prompt","message":"Claude needs your permission to use Bash"}`
	evAskOld = `{"hook_event_name":"Notification","session_id":"s1","message":"Claude needs your permission to use Bash"}`
	evIdle   = `{"hook_event_name":"Notification","session_id":"s1","notification_type":"idle_prompt","message":"Claude is waiting for your input"}`
	evStart  = `{"hook_event_name":"SessionStart","session_id":"s1"}`
)

func at(sec float64) time.Time { return t0.Add(time.Duration(sec * float64(time.Second))) }

// hk records event through RecordHook's projection, as align reads it.
func hk(sec float64, event string) HookRecord {
	var obj map[string]json.RawMessage
	if json.Unmarshal([]byte(event), &obj) != nil {
		return HookRecord{ReceivedAt: at(sec)}
	}
	ev, _ := projectHook(obj)
	return HookRecord{ReceivedAt: at(sec), Event: ev}
}

// noSession drops an event's session; badSession makes it malformed.
func noSession(event string) string {
	return strings.Replace(event, `,"session_id":"s1"`, "", 1)
}

func badSession(event string) string {
	return strings.Replace(event, `"s1"`, `"s 1"`, 1)
}

// inS2 moves an event to a second session.
func inS2(event string) string {
	return strings.Replace(event, `"s1"`, `"s2"`, 1)
}

// rec is a synthetic recording anchored at sec.
func rec(n int, harness protocol.Harness, sec, quiet float64, burst int64, children []string) dataset.Recording {
	return dataset.Recording{
		WindowID: protocol.ULID("01JB00000000000000000000" + string(rune('A'+n/10)) + string(rune('0'+n%10))),
		Features: dataset.AnalyzeInput{
			Harness: harness, SecondsSinceLastOutput: quiet,
			OutputBytes:       dataset.OutputBytes{Last2s: burst, Last10s: burst},
			NewChildProcesses: children,
		},
		Screen: redact.Screen{Rows: []string{"SCREEN-ONLY-CONTENT"}, Cols: 80},
		Provenance: dataset.Provenance{
			SessionID: "sess-1", AnchorAt: at(sec),
			WindowStart: at(sec - 1), WindowEnd: at(sec),
		},
	}
}

func claudeAt(sec float64) dataset.Recording {
	return rec(0, protocol.HarnessClaudeCode, sec, 0.1, 10, []string{})
}

func align(t *testing.T, cfg AlignConfig, recs []dataset.Recording, hooks []HookRecord, anns []Annotation) ([]AlignedLabel, AlignStats) {
	t.Helper()
	got, st, err := Align(context.Background(), cfg, &sliceSource{recs: recs}, hooks, anns)
	require.NoError(t, err)
	return got, st
}

func oneLabel(t *testing.T, cfg AlignConfig, r dataset.Recording, hooks []HookRecord) string {
	t.Helper()
	got, _ := align(t, cfg, []dataset.Recording{r}, hooks, nil)
	if len(got) == 0 || got[0].Label == "" {
		return "" // discarded, or doubtful with no label
	}
	require.Len(t, got, 1)
	return string(got[0].Label) + "/" + string(got[0].Source)
}

func TestAlignHookRules(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		hooks  []HookRecord
		window float64
		want   string
	}{
		{"prompt to next is thinking", []HookRecord{hk(0, evPrompt), hk(10, evPre)}, 5, "B/hook"},
		{"pre to post is running tool", []HookRecord{hk(0, evPre), hk(10, evPost)}, 5, "C/hook"},
		{"after post is not known", []HookRecord{hk(0, evPost), hk(10, evStop)}, 5, ""},
		{"permission notification is waiting", []HookRecord{hk(0, evPre), hk(1, evAsk), hk(10, evPost)}, 5, "D/hook"},
		{"permission from message only", []HookRecord{hk(1, evAskOld), hk(10, evPost)}, 5, "D/hook"},
		{"before permission is still running", []HookRecord{hk(0, evPre), hk(8, evAsk), hk(10, evPost)}, 5, "C/hook"},
		{"stop to prompt is idle", []HookRecord{hk(0, evStop), hk(10, evPrompt)}, 5, idleHook},
		{"idle notification moves nothing", []HookRecord{hk(0, evStop), hk(5, evIdle), hk(10, evPrompt)}, 5, idleHook},
		{"other events move nothing", []HookRecord{hk(0, evStop), hk(5, evStart), hk(10, evPrompt)}, 5, idleHook},
		{"last state has no known end", []HookRecord{hk(0, evStop)}, 5, ""},
		{"state lasts at most max interval", []HookRecord{hk(0, evStop), hk(1000, evPrompt)}, 400, ""},
		{"within max interval", []HookRecord{hk(0, evStop), hk(1000, evPrompt)}, 200, idleHook},
		{"out of order lines", []HookRecord{hk(10, evPrompt), hk(0, evStop)}, 5, idleHook},
		{"unreadable event skipped", []HookRecord{hk(0, evStop), hk(5, `"x"`), hk(10, evPrompt)}, 5, idleHook},
		{"no hooks", nil, 5, ""},
		{"events without session", []HookRecord{hk(0, noSession(evStop)), hk(10, noSession(evPrompt))}, 5, ""},
		{"events with a malformed session", []HookRecord{hk(0, inS2(evStop)), hk(10, badSession(evPrompt))}, 5, ""},
		{"session-less event between", []HookRecord{hk(0, evStop), hk(5, noSession(evPre)), hk(10, evPrompt)}, 5, idleHook},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, oneLabel(t, DefaultAlignConfig(), claudeAt(tc.window), tc.hooks))
		})
	}
}

func TestAlignHookSessions(t *testing.T) {
	t.Parallel()

	idle := []HookRecord{hk(0, evStop), hk(10, evPrompt)}
	for _, tc := range []struct {
		name  string
		hooks []HookRecord
		want  string
	}{
		{"sessions agree", append(idle, hk(0, inS2(evStop)), hk(10, inS2(evPrompt))), idleHook},
		{"sessions disagree", append(idle, hk(0, inS2(evPre)), hk(10, inS2(evPost))), ""},
		{"other session has no state here", append(idle, hk(0, inS2(evPre)), hk(2, inS2(evPost))), idleHook},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, oneLabel(t, DefaultAlignConfig(), claudeAt(5), tc.hooks))
		})
	}
}

func TestAlignDropsWindowsNearTransitions(t *testing.T) {
	t.Parallel()

	hooks := []HookRecord{hk(0, evStop), hk(10, evPrompt)}
	span := DefaultAlignConfig()
	span.EventWindow = EventWindowSpan
	for _, tc := range []struct {
		name   string
		cfg    AlignConfig
		window float64
		want   string
	}{
		{"299 ms after", DefaultAlignConfig(), 0.299, ""},
		{"300 ms after", DefaultAlignConfig(), 0.3, idleHook},
		{"300 ms before", DefaultAlignConfig(), 9.7, idleHook},
		{"299 ms before", DefaultAlignConfig(), 9.701, ""},
		{"on the transition", DefaultAlignConfig(), 10, ""},
		// The span is [sec-1, sec]: its start must also clear the margin.
		{"span start too close", span, 1.2, ""},
		{"span clear", span, 1.3, idleHook},
		{"span end too close", span, 9.8, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, oneLabel(t, tc.cfg, claudeAt(tc.window), hooks))
		})
	}
}

func TestAlignWeakRules(t *testing.T) {
	t.Parallel()

	c := protocol.HarnessCodex
	recs := []dataset.Recording{
		rec(1, c, 10, 5, 0, nil),                // quiet 5 s: turn over at 5
		rec(2, c, 11, 0.1, 500, nil),            // burst after silence: turn starts in [10, 10.9]
		rec(3, c, 12, 0.2, 100, nil),            // thinking
		rec(4, c, 13, 0.2, 100, nil),            // child spawned after this anchor: too close
		rec(5, c, 15, 0.1, 50, []string{"git"}), // tool starts in [13, 15]
		rec(6, c, 17, 0.5, 20, []string{}),      // running tool
		rec(7, c, 25, 6, 0, nil),                // quiet 6 s: tool over at 19
		rec(8, c, 26, 0.1, 10, nil),             // small output after silence: no turn
		rec(9, c, 30, 4, 0, nil),                // quiet: last output at 26
	}
	got, st := align(t, DefaultAlignConfig(), recs, nil, nil)
	byID := map[protocol.ULID]string{}
	for _, l := range got {
		if l.Label != "" {
			byID[l.WindowID] = string(l.Label) + "/" + string(l.Source)
		}
	}
	want := map[int]string{3: "B/weak", 6: "C/weak"}
	for i, r := range recs {
		require.Equal(t, want[i+1], byID[r.WindowID], "window %d", i+1)
	}
	require.Equal(t, 9, st.Windows)
	require.Equal(t, 2, st.Weak)
	require.Equal(t, 9, st.Weak+st.Unlabeled+st.Discarded)
}

func TestAlignHookOnlyForHookHarnesses(t *testing.T) {
	t.Parallel()

	hooks := []HookRecord{hk(0, evStop), hk(30, evPrompt)}
	c := protocol.HarnessClaudeCode
	// Weak transitions at 10-10.9 discard the first two windows; the
	// third gets the hook label, its weak one having no known end.
	recs := []dataset.Recording{rec(1, c, 10, 5, 0, nil), rec(2, c, 11, 0.1, 500, nil), rec(3, c, 12, 0.2, 100, nil)}
	got, st := align(t, DefaultAlignConfig(), recs, hooks, nil)
	require.Equal(t, 2, st.Discarded)
	require.Len(t, got, 1)
	require.Equal(t, recs[2].WindowID, got[0].WindowID)
	require.Equal(t, SourceHook, got[0].Source)
	require.Equal(t, dataset.LabelA, got[0].Label)
	require.Empty(t, got[0].Doubt)

	// Hook events never label a harness without hooks.
	codex := rec(1, protocol.HarnessCodex, 5, 0.1, 10, nil)
	require.Empty(t, oneLabel(t, DefaultAlignConfig(), codex, []HookRecord{hk(0, evStop), hk(10, evPrompt)}))
}

// public drops what AlignedLabel does not save.
func public(ls []AlignedLabel) []AlignedLabel {
	out := make([]AlignedLabel, 0, len(ls))
	for _, l := range ls {
		out = append(out, AlignedLabel{WindowID: l.WindowID, Label: l.Label, Source: l.Source, HookLabel: l.HookLabel, WeakLabel: l.WeakLabel, Doubt: l.Doubt})
	}
	return out
}

func TestAlignManualWins(t *testing.T) {
	t.Parallel()

	hooks := []HookRecord{hk(0, evStop), hk(10, evPrompt)}
	labeled := claudeAt(5)
	nearTransition := rec(1, protocol.HarnessClaudeCode, 10, 0.1, 10, nil)
	weak := rec(2, protocol.HarnessCodex, 5, 0.1, 10, nil)
	anns := []Annotation{
		{WindowID: labeled.WindowID, Label: dataset.LabelE, Annotator: "a", LabeledAt: at(100)},
		{WindowID: labeled.WindowID, Label: dataset.LabelG, Annotator: "b", LabeledAt: at(200)},
		{WindowID: labeled.WindowID, Label: dataset.LabelF, Annotator: "c", LabeledAt: at(150)}, // older
		{WindowID: nearTransition.WindowID, Label: dataset.LabelB, Annotator: "a", LabeledAt: at(100)},
		{WindowID: weak.WindowID, Label: dataset.LabelD, Annotator: "a", LabeledAt: at(100)},
	}
	got, st := align(t, DefaultAlignConfig(), []dataset.Recording{labeled, nearTransition, weak}, hooks, anns)
	require.Equal(t, []AlignedLabel{
		{WindowID: labeled.WindowID, Label: dataset.LabelG, Source: SourceManual, HookLabel: dataset.LabelA},
		{WindowID: nearTransition.WindowID, Label: dataset.LabelB, Source: SourceManual},
		{WindowID: weak.WindowID, Label: dataset.LabelD, Source: SourceManual},
	}, public(got))
	require.Equal(t, 3, st.Manual)
}

func TestAlignRejectsBadInput(t *testing.T) {
	t.Parallel()

	bad := DefaultAlignConfig()
	bad.EventWindow = "frame"
	_, _, err := Align(context.Background(), bad, &sliceSource{}, nil, nil)
	require.ErrorIs(t, err, ErrBadAlignConfig)

	boom := &sliceSource{err: os.ErrClosed}
	_, _, err = Align(context.Background(), DefaultAlignConfig(), boom, nil, nil)
	require.ErrorIs(t, err, os.ErrClosed)
}

func TestAlignConfig(t *testing.T) {
	t.Parallel()

	cfg, err := ParseAlignConfig(strings.NewReader(""))
	require.NoError(t, err)
	require.Equal(t, DefaultAlignConfig(), cfg)

	cfg, err = ParseAlignConfig(strings.NewReader(`
event_window: span
margin_ms: 500
max_interval_seconds: 60
hook_harnesses: [claude-code, opencode]
weak:
  silence_seconds: 1.5
  burst_bytes: 10
`))
	require.NoError(t, err)
	require.Equal(t, AlignConfig{
		EventWindow: EventWindowSpan, Margin: 500 * time.Millisecond, MaxInterval: time.Minute,
		HookHarnesses: []protocol.Harness{protocol.HarnessClaudeCode, protocol.HarnessOpencode},
		Silence:       1500 * time.Millisecond, BurstBytes: 10,
	}, cfg)

	for _, in := range []string{
		"unknown: 1", "event_window: frame", "margin_ms: -1", "margin_ms: 60001",
		"max_interval_seconds: 0", "hook_harnesses: [vim]", "weak: {silence_seconds: 0}",
		"weak: {burst_bytes: 0}", "weak: {silence_seconds: .nan}", "margin_ms: [1]",
	} {
		_, err := ParseAlignConfig(strings.NewReader(in))
		require.ErrorIs(t, err, ErrBadAlignConfig, in)
	}

	cfg, err = LoadAlignConfig(filepath.Join(t.TempDir(), "missing.yaml"))
	require.NoError(t, err)
	require.Equal(t, DefaultAlignConfig(), cfg)
}

func TestAlignedFileRoundTrip(t *testing.T) {
	t.Parallel()

	dir := Dir(t.TempDir())
	require.NoError(t, RecordHook(dir, at(0), strings.NewReader(evStop)))
	require.NoError(t, RecordHook(dir, at(10), strings.NewReader(evPrompt)))
	require.NoError(t, appendLine(dir, HooksFile, []byte("{torn\n")))
	require.NoError(t, SaveAnnotations(dir, []Annotation{{WindowID: idA, Label: dataset.LabelC, Annotator: "a", LabeledAt: at(1)}}))
	require.NoError(t, appendLine(dir, AnnotationsFile, []byte(`{"window_id":"x","label":"Z"}`+"\n")))

	hooks, badHooks, err := ReadHooks(dir)
	require.NoError(t, err)
	require.Len(t, hooks, 2)
	require.Equal(t, 1, badHooks)
	anns, badAnns, err := ReadAnnotations(dir)
	require.NoError(t, err)
	require.Len(t, anns, 1)
	require.Equal(t, 1, badAnns)

	got, _ := align(t, DefaultAlignConfig(), []dataset.Recording{claudeAt(5)}, hooks, nil)
	require.NoError(t, SaveAligned(dir, got))
	require.NoError(t, SaveAligned(dir, got)) // replaced, not appended
	requirePrivate(t, dir, AlignedFile)
	lines := fileLines(t, filepath.Join(dir, AlignedFile))
	require.Len(t, lines, 1)
	require.NotContains(t, lines[0], "SCREEN-ONLY")
	var m map[string]any
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &m))
	require.ElementsMatch(t, []string{"window_id", "label", "source", "hook_label"}, keysOf(m))

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, e := range entries {
		require.False(t, strings.HasPrefix(e.Name(), "."), "temporary file left: %s", e.Name())
	}

	missing := Dir(t.TempDir())
	hooks, _, err = ReadHooks(missing)
	require.NoError(t, err)
	require.Empty(t, hooks)
}
