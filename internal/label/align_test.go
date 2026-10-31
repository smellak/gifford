package label

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/redact"
)

const (
	idleHook           = "A/hook"
	testHarnessVersion = "test-v1"
	evStop             = `{"hook_event_name":"Stop","session_id":"s1"}`
	evPrompt           = `{"hook_event_name":"UserPromptSubmit","session_id":"s1"}`
	evPre              = `{"hook_event_name":"PreToolUse","session_id":"s1"}`
	evPost             = `{"hook_event_name":"PostToolUse","session_id":"s1"}`
	evPermission       = `{"hook_event_name":"PermissionRequest","session_id":"s1"}`
	evAsk              = `{"hook_event_name":"Notification","session_id":"s1","notification_type":"permission_prompt","message":"Claude needs your permission to use Bash"}`
	evAskOld           = `{"hook_event_name":"Notification","session_id":"s1","message":"Claude needs your permission to use Bash"}`
	evIdle             = `{"hook_event_name":"Notification","session_id":"s1","notification_type":"idle_prompt","message":"Claude is waiting for your input"}`
	evStart            = `{"hook_event_name":"SessionStart","session_id":"s1"}`
	evRoot             = `{"hook_event_name":"SessionStart","session_id":"s1","source":"startup"}`
	claudeQuestion     = `{"hook_event_name":"PreToolUse","session_id":"s1","tool_name":"AskUserQuestion"}`
	userInputQuestion  = `{"hook_event_name":"PreToolUse","session_id":"s1","tool_name":"request_user_input"}`
	openRootEvent      = `{"type":"session.created","properties":{"sessionID":"s1","info":{"id":"s1"}}}`
	openChildEvent     = `{"type":"session.created","properties":{"sessionID":"s2","info":{"id":"s2","parentID":"s1"}}}`
	openQuestionAsked  = `{"type":"question.asked","properties":{"id":"que-1","sessionID":"s1"}}`
	openQuestionReply  = `{"type":"question.replied","properties":{"requestID":"que-1","sessionID":"s1"}}`
)

func at(sec float64) time.Time { return t0.Add(time.Duration(sec * float64(time.Second))) }

// hk records event through RecordHook's projection, as align reads it.
func hk(sec float64, event string) HookRecord {
	return hookAt(sec, protocol.HarnessClaudeCode, event)
}

func hookAt(sec float64, harness protocol.Harness, event string) HookRecord {
	var obj map[string]json.RawMessage
	if json.Unmarshal([]byte(event), &obj) != nil {
		return HookRecord{ReceivedAt: at(sec), Harness: harness, CaptureSessionID: hookCaptureID}
	}
	ev, _ := projectHook(harness, obj)
	return HookRecord{ReceivedAt: at(sec), Harness: harness, CaptureSessionID: hookCaptureID, Event: ev}
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
			SessionID: "sess-1", CaptureSessionID: hookCaptureID,
			HarnessVersion: testHarnessVersion, AnchorAt: at(sec),
			WindowStart: at(sec - 1), WindowEnd: at(sec),
		},
	}
}

func claudeAt(sec float64) dataset.Recording {
	return rec(0, protocol.HarnessClaudeCode, sec, 0.1, 10, []string{})
}

func align(t *testing.T, cfg AlignConfig, recs []dataset.Recording, hooks []HookRecord, anns []Annotation) ([]AlignedLabel, AlignStats) {
	t.Helper()
	// Hook-focused fixtures use one root whose startup evidence has been
	// verified for test-v1. Dedicated link tests call Align directly.
	if len(hooks) > 0 && len(cfg.RootSessionSources) == 0 {
		cfg.RootSessionSources = []WaitSource{{Harness: protocol.HarnessClaudeCode, HarnessVersion: testHarnessVersion}}
		hooks = append([]HookRecord{hk(-1, evRoot)}, hooks...)
	}
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

func foldedLabel(cfg AlignConfig, harness protocol.Harness, records []HookRecord) dataset.Label {
	return foldedLabelAt(cfg, harness, records, 5)
}

func foldedLabelAt(cfg AlignConfig, harness protocol.Harness, records []HookRecord, sec float64) dataset.Label {
	ts := foldHookTimeline(cfg, harness, testHarnessVersion, records)
	return labelIn(intervals(ts, cfg.MaxInterval), at(sec), at(sec), cfg.Margin)
}

func TestAlignHookRules(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		hooks  []HookRecord
		window float64
		wait   bool
		want   string
	}{
		{"prompt to next is thinking", []HookRecord{hk(0, evPrompt), hk(10, evPre)}, 5, false, "B/hook"},
		{"pre to post is running tool", []HookRecord{hk(0, evPre), hk(10, evPost)}, 5, false, "C/hook"},
		{"after post is not known", []HookRecord{hk(0, evPost), hk(10, evStop)}, 5, false, ""},
		{"observed permission wait", []HookRecord{hk(0, evPre), hk(1, evPermission), hk(2, evAsk), hk(10, evPost)}, 5, true, "D/hook"},
		{"wait source is off by default", []HookRecord{hk(0, evPre), hk(1, evPermission), hk(2, evAsk), hk(10, evPost)}, 5, false, ""},
		{"permission from message only", []HookRecord{hk(1, evAskOld), hk(10, evPost)}, 5, true, ""},
		{"pending wait is not backdated", []HookRecord{hk(0, evPre), hk(1, evPermission), hk(8, evAsk), hk(10, evPost)}, 5, true, ""},
		{"before request is still running", []HookRecord{hk(0, evPre), hk(8, evPermission), hk(9, evAsk), hk(10, evPost)}, 5, true, "C/hook"},
		{"response before notification cancels wait", []HookRecord{hk(0, evPre), hk(1, evPermission), hk(2, evPost), hk(3, evAsk), hk(10, evStop)}, 5, true, ""},
		{"stop to prompt is idle", []HookRecord{hk(0, evStop), hk(10, evPrompt)}, 5, false, idleHook},
		{"idle notification moves nothing", []HookRecord{hk(0, evStop), hk(5, evIdle), hk(10, evPrompt)}, 5, false, idleHook},
		{"other events move nothing", []HookRecord{hk(0, evStop), hk(5, evStart), hk(10, evPrompt)}, 5, false, idleHook},
		{"last state has no known end", []HookRecord{hk(0, evStop)}, 5, false, ""},
		{"state lasts at most max interval", []HookRecord{hk(0, evStop), hk(1000, evPrompt)}, 400, false, ""},
		{"within max interval", []HookRecord{hk(0, evStop), hk(1000, evPrompt)}, 200, false, idleHook},
		{"out of order lines", []HookRecord{hk(10, evPrompt), hk(0, evStop)}, 5, false, idleHook},
		{"unreadable event skipped", []HookRecord{hk(0, evStop), hk(5, `"x"`), hk(10, evPrompt)}, 5, false, idleHook},
		{"no hooks", nil, 5, false, ""},
		{"events without session", []HookRecord{hk(0, noSession(evStop)), hk(10, noSession(evPrompt))}, 5, false, ""},
		{"events with a malformed session", []HookRecord{hk(0, inS2(evStop)), hk(10, badSession(evPrompt))}, 5, false, ""},
		{"session-less event between", []HookRecord{hk(0, evStop), hk(5, noSession(evPre)), hk(10, evPrompt)}, 5, false, idleHook},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := DefaultAlignConfig()
			if tc.wait {
				cfg.ApprovalWaitSources = []WaitSource{{Harness: protocol.HarnessClaudeCode, HarnessVersion: testHarnessVersion}}
			}
			require.Equal(t, tc.want, oneLabel(t, cfg, claudeAt(tc.window), tc.hooks))
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
		{"subsession disagreement is ignored", append(idle, hk(0, inS2(evPre)), hk(10, inS2(evPost))), idleHook},
		{"other session has no state here", append(idle, hk(0, inS2(evPre)), hk(2, inS2(evPost))), idleHook},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, oneLabel(t, DefaultAlignConfig(), claudeAt(5), tc.hooks))
		})
	}
}

func hooksInCapture(capture protocol.OpaqueID, hooks ...HookRecord) []HookRecord {
	out := slices.Clone(hooks)
	for i := range out {
		out[i].CaptureSessionID = capture
	}
	return out
}

func TestAlignHookLinkIsExact(t *testing.T) {
	t.Parallel()

	capture1 := hookCaptureID
	capture2 := protocol.OpaqueID("run-124-1761739200000000001")
	linked := DefaultAlignConfig()
	linked.RootSessionSources = []WaitSource{{Harness: protocol.HarnessClaudeCode, HarnessVersion: testHarnessVersion}}
	root := hk(-1, evRoot)
	idle := []HookRecord{root, hk(0, evStop), hk(10, evPrompt)}
	one := claudeAt(5)

	rawLabel := func(cfg AlignConfig, recs []dataset.Recording, hooks []HookRecord) []AlignedLabel {
		got, _, err := Align(context.Background(), cfg, &sliceSource{recs: recs}, hooks, nil)
		require.NoError(t, err)
		return got
	}
	require.Empty(t, rawLabel(DefaultAlignConfig(), []dataset.Recording{one}, idle)[0].HookLabel, "root evidence is off by default")
	require.Equal(t, dataset.LabelA, rawLabel(linked, []dataset.Recording{one}, idle)[0].HookLabel)
	require.Empty(t, rawLabel(linked, []dataset.Recording{one}, idle[1:])[0].HookLabel, "no root evidence means no hook label")

	ambiguous := append(slices.Clone(idle), hk(-0.5, inS2(evRoot)))
	require.Empty(t, rawLabel(linked, []dataset.Recording{one}, ambiguous)[0].HookLabel, "two roots contradict")
	wrongHarness := HookRecord{
		ReceivedAt: at(-0.5), Harness: protocol.HarnessCodex, CaptureSessionID: capture1,
		Event: HookEvent{Name: "SessionStart", SessionID: "codex-root", Source: sessionStartStartup},
	}
	require.Empty(t, rawLabel(linked, []dataset.Recording{one}, append(slices.Clone(idle), wrongHarness))[0].HookLabel, "another harness contradicts the link")

	subagent := append(slices.Clone(idle), hk(4, inS2(evPre)), hk(6, inS2(evPost)))
	require.Equal(t, dataset.LabelA, rawLabel(linked, []dataset.Recording{one}, subagent)[0].HookLabel, "an inherited capture id does not make a subagent the root")

	second := claudeAt(5)
	second.WindowID = idB
	second.Provenance.CaptureSessionID = capture2
	second.Provenance.SessionID = capture2
	secondHooks := hooksInCapture(capture2, hk(-1, evRoot), hk(0, evPre), hk(10, evPost))
	got := rawLabel(linked, []dataset.Recording{one, second}, append(slices.Clone(idle), secondHooks...))
	require.Equal(t, one.WindowID, got[0].WindowID, "linking never changes a window id")
	require.Equal(t, second.WindowID, got[1].WindowID, "linking never changes a window id")
	require.Equal(t, dataset.LabelA, got[0].HookLabel)
	require.Equal(t, dataset.LabelC, got[1].HookLabel, "two captures never share events or margins")

	mismatch := linked
	mismatch.RootSessionSources = []WaitSource{{Harness: protocol.HarnessClaudeCode, HarnessVersion: "another-v1"}}
	require.Empty(t, rawLabel(mismatch, []dataset.Recording{one}, idle)[0].HookLabel, "the recording's real version must match")
	unknownVersion := one
	unknownVersion.Provenance.HarnessVersion = "unknown"
	require.Empty(t, rawLabel(linked, []dataset.Recording{unknownVersion}, idle)[0].HookLabel, "an unknown measured version never activates a source")
	historical := slices.Clone(idle)
	for i := range historical {
		historical[i].CaptureSessionID = ""
	}
	require.Empty(t, rawLabel(linked, []dataset.Recording{one}, historical)[0].HookLabel, "historical hooks have no exact capture link")
}

func TestAlignObservedWaitsAndTerminalEvents(t *testing.T) {
	t.Parallel()

	approval := DefaultAlignConfig()
	approval.ApprovalWaitSources = []WaitSource{{Harness: protocol.HarnessClaudeCode, HarnessVersion: testHarnessVersion}}
	both := approval
	both.QuestionWaitSources = []WaitSource{{Harness: protocol.HarnessClaudeCode, HarnessVersion: testHarnessVersion}}
	question := claudeQuestion
	interrupt := `{"hook_event_name":"Interrupt","session_id":"s1"}`
	sessionEnd := `{"hook_event_name":"SessionEnd","session_id":"s1"}`
	failure := `{"hook_event_name":"StopFailure","session_id":"s1","error":"rate_limit"}`
	postStop := `{"hook_event_name":"Stop","session_id":"s1","post_stop":true}`

	for _, tc := range []struct {
		name  string
		cfg   AlignConfig
		hooks []HookRecord
		want  string
	}{
		{"approval observed", approval, []HookRecord{hk(0, evPre), hk(1, evPermission), hk(2, evAsk), hk(10, evPost)}, "D/hook"},
		{"question wins over approval", both, []HookRecord{hk(0, question), hk(1, evPermission), hk(2, evAsk), hk(10, evPost)}, "E/hook"},
		{"question source disabled is not approval", approval, []HookRecord{hk(0, question), hk(1, evPermission), hk(2, evAsk), hk(10, evPost)}, ""},
		{"question cancelled before observation", both, []HookRecord{hk(0, question), hk(1, evPermission), hk(2, evPost), hk(3, evAsk), hk(10, evStop)}, ""},
		{"exact version required", func() AlignConfig {
			cfg := DefaultAlignConfig()
			cfg.ApprovalWaitSources = []WaitSource{{Harness: protocol.HarnessClaudeCode, HarnessVersion: "other-v1"}}
			return cfg
		}(), []HookRecord{hk(0, evPre), hk(1, evPermission), hk(2, evAsk), hk(10, evPost)}, ""},
		{"stop failure is error", DefaultAlignConfig(), []HookRecord{hk(0, failure), hk(10, evPrompt)}, "F/hook"},
		{"interrupt proves no idle state", DefaultAlignConfig(), []HookRecord{hk(0, interrupt), hk(10, evPrompt)}, ""},
		{"session end proves no terminal target", DefaultAlignConfig(), []HookRecord{hk(0, sessionEnd), hk(10, evPrompt)}, ""},
		{"post stop event excluded", DefaultAlignConfig(), []HookRecord{hk(0, postStop), hk(10, evPrompt)}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, oneLabel(t, tc.cfg, claudeAt(5), tc.hooks))
		})
	}

	openCfg := DefaultAlignConfig()
	failed := []HookRecord{
		hookAt(0, protocol.HarnessOpencode, `{"type":"session.error","properties":{"sessionID":"s1","error":{"name":"UnknownError"}}}`),
		hookAt(10, protocol.HarnessOpencode, `{"type":"permission.replied","properties":{"sessionID":"s1"}}`),
	}
	require.Equal(t, dataset.LabelF, foldedLabel(openCfg, protocol.HarnessOpencode, failed))
	failed[0] = hookAt(0, protocol.HarnessOpencode, `{"type":"session.error","properties":{"sessionID":"s1","error":{"name":"MessageAbortedError"}}}`)
	require.Empty(t, foldedLabel(openCfg, protocol.HarnessOpencode, failed), "an abort is not F")
}

func TestFoldQuestionToolsStartPendingWithoutRunningTool(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		harness protocol.Harness
		start   string
		close   string
	}{
		{
			name: "claude ask", harness: protocol.HarnessClaudeCode,
			start: claudeQuestion,
			close: evPost,
		},
		{
			name: "codex request user input", harness: protocol.HarnessCodex,
			start: userInputQuestion,
			close: evPost,
		},
		{
			name: "muse pre tool", harness: protocol.HarnessMuseCode,
			start: userInputQuestion,
			close: evPost,
		},
		{
			name: "muse tool use start", harness: protocol.HarnessMuseCode,
			start: `{"hook_event_name":"ToolUseStart","session_id":"s1","tool_name":"request_user_input"}`,
			close: `{"hook_event_name":"PostToolUseFailure","session_id":"s1","tool_name":"request_user_input"}`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := DefaultAlignConfig()
			cfg.HookHarnesses = []protocol.Harness{tc.harness}
			cfg.QuestionWaitSources = []WaitSource{{Harness: tc.harness, HarnessVersion: testHarnessVersion}}
			records := []HookRecord{
				hookAt(0, tc.harness, tc.start),
				hookAt(10, tc.harness, tc.close),
			}
			require.Empty(t, foldedLabel(cfg, tc.harness, records), "a question tool start is pending, never C or E")
		})
	}

	claude := DefaultAlignConfig()
	claude.QuestionWaitSources = []WaitSource{{Harness: protocol.HarnessClaudeCode, HarnessVersion: testHarnessVersion}}
	question := claudeQuestion
	observed := []HookRecord{hk(0, question), hk(1, evPermission), hk(2, evAsk), hk(10, evPost)}
	require.Empty(t, foldedLabelAt(claude, protocol.HarnessClaudeCode, observed, 0.5), "E is not backdated")
	require.Equal(t, dataset.LabelE, foldedLabel(claude, protocol.HarnessClaudeCode, observed))

	cancelled := []HookRecord{hk(0, question), hk(1, evPost), hk(2, evAsk), hk(10, evStop)}
	require.Empty(t, foldedLabel(claude, protocol.HarnessClaudeCode, cancelled), "a late notification cannot reopen a cancelled question")
}

func TestFoldFailureEndsAtObservedIdleOrCompaction(t *testing.T) {
	t.Parallel()

	failure := `{"hook_event_name":"StopFailure","session_id":"s1","error":"rate_limit"}`
	for _, cut := range []string{
		evIdle,
		`{"hook_event_name":"PreCompact","session_id":"s1"}`,
		`{"hook_event_name":"SessionStart","session_id":"s1","source":"compact"}`,
	} {
		records := []HookRecord{hk(0, failure), hk(4, cut), hk(10, evPrompt)}
		require.Equal(t, dataset.LabelF, foldedLabelAt(DefaultAlignConfig(), protocol.HarnessClaudeCode, records, 2))
		require.Empty(t, foldedLabelAt(DefaultAlignConfig(), protocol.HarnessClaudeCode, records, 6), "the observation cuts F to no label")
	}

	cfg := DefaultAlignConfig()
	cfg.QuestionWaitSources = []WaitSource{{Harness: protocol.HarnessClaudeCode, HarnessVersion: testHarnessVersion}}
	question := claudeQuestion
	records := []HookRecord{
		hk(0, question),
		hk(1, `{"hook_event_name":"SessionStart","session_id":"s1","source":"compact"}`),
		hk(2, evAsk),
		hk(10, evStop),
	}
	require.Empty(t, foldedLabel(cfg, protocol.HarnessClaudeCode, records), "compact cancels pending questions without reopening them")
}

func TestFoldCandidateWaitSources(t *testing.T) {
	t.Parallel()

	with := func(harness protocol.Harness, approval, question bool) AlignConfig {
		cfg := DefaultAlignConfig()
		cfg.HookHarnesses = []protocol.Harness{harness}
		if approval {
			cfg.ApprovalWaitSources = []WaitSource{{Harness: harness, HarnessVersion: testHarnessVersion}}
		}
		if question {
			cfg.QuestionWaitSources = []WaitSource{{Harness: harness, HarnessVersion: testHarnessVersion}}
		}
		return cfg
	}
	musePermission := []HookRecord{
		hookAt(0, protocol.HarnessMuseCode, `{"hook_event_name":"PermissionRequest","session_id":"s1"}`),
		hookAt(2, protocol.HarnessMuseCode, `{"hook_event_name":"Notification","session_id":"s1","notification_type":"permission_prompt"}`),
		hookAt(10, protocol.HarnessMuseCode, `{"hook_event_name":"ToolUseStart","session_id":"s1"}`),
	}
	museQuestion := []HookRecord{
		hookAt(0, protocol.HarnessMuseCode, userInputQuestion),
		hookAt(2, protocol.HarnessMuseCode, `{"hook_event_name":"Notification","session_id":"s1","notification_type":"question_prompt"}`),
		hookAt(10, protocol.HarnessMuseCode, `{"hook_event_name":"PostToolUse","session_id":"s1","tool_name":"request_user_input"}`),
	}
	openQuestion := []HookRecord{
		hookAt(0, protocol.HarnessOpencode, openQuestionAsked),
		hookAt(10, protocol.HarnessOpencode, openQuestionReply),
	}

	require.Equal(t, dataset.LabelD, foldedLabel(with(protocol.HarnessMuseCode, true, false), protocol.HarnessMuseCode, musePermission))
	require.Equal(t, dataset.LabelE, foldedLabel(with(protocol.HarnessMuseCode, true, true), protocol.HarnessMuseCode, museQuestion))
	require.Empty(t, foldedLabel(with(protocol.HarnessMuseCode, true, false), protocol.HarnessMuseCode, museQuestion), "a question never falls back to approval")
	require.Empty(t, foldedLabel(with(protocol.HarnessMuseCode, true, false), protocol.HarnessMuseCode, []HookRecord{
		hookAt(0, protocol.HarnessMuseCode, `{"hook_event_name":"PermissionRequest","session_id":"s1"}`),
		hookAt(8, protocol.HarnessMuseCode, `{"hook_event_name":"Notification","session_id":"s1","notification_type":"permission_prompt"}`),
		hookAt(10, protocol.HarnessMuseCode, `{"hook_event_name":"ToolUseStart","session_id":"s1"}`),
	}), "Muse waits are not backdated")
	require.Empty(t, foldedLabel(with(protocol.HarnessMuseCode, true, false), protocol.HarnessMuseCode, []HookRecord{
		hookAt(0, protocol.HarnessMuseCode, `{"hook_event_name":"PermissionRequest","session_id":"s1"}`),
		hookAt(1, protocol.HarnessMuseCode, `{"hook_event_name":"ToolUseStart","session_id":"s1"}`),
		hookAt(2, protocol.HarnessMuseCode, `{"hook_event_name":"Notification","session_id":"s1","notification_type":"permission_prompt"}`),
		hookAt(10, protocol.HarnessMuseCode, `{"hook_event_name":"Stop","session_id":"s1"}`),
	}), "a tool start before observation cancels the pending wait")
	require.Equal(t, dataset.LabelC, foldedLabel(DefaultAlignConfig(), protocol.HarnessMuseCode, []HookRecord{
		hookAt(0, protocol.HarnessMuseCode, `{"hook_event_name":"ToolUseStart","session_id":"s1"}`),
		hookAt(10, protocol.HarnessMuseCode, `{"hook_event_name":"Stop","session_id":"s1"}`),
	}))

	require.Equal(t, dataset.LabelE, foldedLabel(with(protocol.HarnessOpencode, false, true), protocol.HarnessOpencode, openQuestion))
	rejected := slices.Clone(openQuestion)
	rejected[1] = hookAt(10, protocol.HarnessOpencode, `{"type":"question.rejected","properties":{"requestID":"que-1","sessionID":"s1"}}`)
	require.Equal(t, dataset.LabelE, foldedLabel(with(protocol.HarnessOpencode, false, true), protocol.HarnessOpencode, rejected))
	require.Empty(t, foldedLabel(with(protocol.HarnessOpencode, true, false), protocol.HarnessOpencode, openQuestion), "a question never falls back to approval")
	require.Equal(t, dataset.LabelD, foldedLabel(with(protocol.HarnessOpencode, true, false), protocol.HarnessOpencode, []HookRecord{
		hookAt(0, protocol.HarnessOpencode, `{"type":"permission.asked","properties":{"id":"per-1","sessionID":"s1"}}`),
		hookAt(10, protocol.HarnessOpencode, `{"type":"permission.replied","properties":{"requestID":"per-1","sessionID":"s1","reply":"once"}}`),
	}))
	require.Equal(t, dataset.LabelE, foldedLabel(with(protocol.HarnessOpencode, true, true), protocol.HarnessOpencode, []HookRecord{
		hookAt(0, protocol.HarnessOpencode, openQuestionAsked),
		hookAt(1, protocol.HarnessOpencode, `{"type":"permission.asked","properties":{"id":"per-1","sessionID":"s1","tool_name":"question"}}`),
		hookAt(8, protocol.HarnessOpencode, `{"type":"permission.replied","properties":{"requestID":"per-1","sessionID":"s1","reply":"once"}}`),
		hookAt(10, protocol.HarnessOpencode, openQuestionReply),
	}), "E wins when an observed permission is a question")
	require.Empty(t, foldedLabel(with(protocol.HarnessOpencode, false, true), protocol.HarnessOpencode, []HookRecord{
		hookAt(8, protocol.HarnessOpencode, openQuestionAsked),
		hookAt(10, protocol.HarnessOpencode, openQuestionReply),
	}), "OpenCode questions are not backdated")
}

func TestAlignRecordedClaudeWaitSourcesEndToEnd(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		events []string
		want   dataset.Label
	}{
		{"approval", []string{evPre, evPermission, evAsk, evPost}, dataset.LabelD},
		{"question", []string{
			claudeQuestion,
			evPermission, evAsk, evPost,
		}, dataset.LabelE},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := Dir(t.TempDir())
			require.NoError(t, RecordHook(dir, at(-1), protocol.HarnessClaudeCode, hookCaptureID, strings.NewReader(evRoot)))
			for i, event := range tc.events {
				seconds := []float64{0, 1, 2, 10}[i]
				require.NoError(t, RecordHook(dir, at(seconds), protocol.HarnessClaudeCode, hookCaptureID, strings.NewReader(event)))
			}
			hooks, bad, err := ReadHooks(dir)
			require.NoError(t, err)
			require.Zero(t, bad)
			cfg := DefaultAlignConfig()
			source := WaitSource{Harness: protocol.HarnessClaudeCode, HarnessVersion: testHarnessVersion}
			cfg.RootSessionSources = []WaitSource{source}
			cfg.ApprovalWaitSources = []WaitSource{source}
			cfg.QuestionWaitSources = []WaitSource{source}
			got, _, err := Align(context.Background(), cfg, &sliceSource{recs: []dataset.Recording{claudeAt(5)}}, hooks, nil)
			require.NoError(t, err)
			require.Len(t, got, 1)
			require.Equal(t, tc.want, got[0].HookLabel)
			require.Equal(t, tc.want, got[0].Label)
			require.Equal(t, SourceHook, got[0].Source)
		})
	}
}

func TestAlignRecordedRootCandidatesEndToEnd(t *testing.T) {
	t.Parallel()

	type rootCase struct {
		name    string
		harness protocol.Harness
		root    string
		events  []string
		want    dataset.Label
		wait    bool
	}
	for _, tc := range []rootCase{
		{
			name: "claude session startup", harness: protocol.HarnessClaudeCode,
			root: evRoot, events: []string{evStop, evPrompt}, want: dataset.LabelA,
		},
		{
			name: "codex session startup", harness: protocol.HarnessCodex,
			root: evRoot, events: []string{evStop, evPrompt}, want: dataset.LabelA,
		},
		{
			name: "muse session startup", harness: protocol.HarnessMuseCode,
			root: evRoot, events: []string{evStop, evPrompt}, want: dataset.LabelA,
		},
		{
			name: "opencode parentless session created", harness: protocol.HarnessOpencode,
			root: openRootEvent,
			events: []string{
				openQuestionAsked,
				openQuestionReply,
			},
			want: dataset.LabelE, wait: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := Dir(t.TempDir())
			require.NoError(t, RecordHook(dir, at(-1), tc.harness, hookCaptureID, strings.NewReader(tc.root)))
			for i, event := range tc.events {
				require.NoError(t, RecordHook(dir, at(float64(i*10)), tc.harness, hookCaptureID, strings.NewReader(event)))
			}
			hooks, bad, err := ReadHooks(dir)
			require.NoError(t, err)
			require.Zero(t, bad)
			cfg := DefaultAlignConfig()
			cfg.HookHarnesses = []protocol.Harness{tc.harness}
			source := WaitSource{Harness: tc.harness, HarnessVersion: testHarnessVersion}
			cfg.RootSessionSources = []WaitSource{source}
			if tc.wait {
				cfg.QuestionWaitSources = []WaitSource{source}
			}
			r := rec(0, tc.harness, 5, 0.1, 10, nil)
			got, _, err := Align(context.Background(), cfg, &sliceSource{recs: []dataset.Recording{r}}, hooks, nil)
			require.NoError(t, err)
			require.Len(t, got, 1)
			require.Equal(t, tc.want, got[0].HookLabel)

			cfg.RootSessionSources = nil
			got, _, err = Align(context.Background(), cfg, &sliceSource{recs: []dataset.Recording{r}}, hooks, nil)
			require.NoError(t, err)
			require.Empty(t, got[0].HookLabel, "every root candidate is off by default")
		})
	}
}

func TestAlignSessionStartRootsExcludeSubagents(t *testing.T) {
	t.Parallel()

	for _, harness := range []protocol.Harness{
		protocol.HarnessClaudeCode, protocol.HarnessCodex, protocol.HarnessMuseCode,
	} {
		t.Run(string(harness), func(t *testing.T) {
			t.Parallel()
			dir := Dir(t.TempDir())
			events := []struct {
				sec float64
				raw string
			}{
				{-2, evRoot},
				{-1, `{"hook_event_name":"SubagentStart","session_id":"s2","subagent_id":"CANARY-child"}`},
				{0, evStop},
				{4, `{"hook_event_name":"PreToolUse","session_id":"s2"}`},
				{6, `{"hook_event_name":"PostToolUse","session_id":"s2"}`},
				{10, evPrompt},
			}
			for _, event := range events {
				require.NoError(t, RecordHook(dir, at(event.sec), harness, hookCaptureID, strings.NewReader(event.raw)))
			}
			hooks, bad, err := ReadHooks(dir)
			require.NoError(t, err)
			require.Zero(t, bad)
			cfg := DefaultAlignConfig()
			cfg.HookHarnesses = []protocol.Harness{harness}
			cfg.RootSessionSources = []WaitSource{{Harness: harness, HarnessVersion: testHarnessVersion}}
			r := rec(0, harness, 5, 0.1, 10, nil)
			got, st, err := Align(context.Background(), cfg, &sliceSource{recs: []dataset.Recording{r}}, hooks, nil)
			require.NoError(t, err)
			require.Zero(t, st.Discarded, "subagent transitions cannot affect root margins")
			require.Equal(t, dataset.LabelA, got[0].HookLabel)
		})
	}
}

func TestAlignNonStartupSessionSourcesNeverRoot(t *testing.T) {
	t.Parallel()

	for _, harness := range []protocol.Harness{
		protocol.HarnessClaudeCode, protocol.HarnessCodex, protocol.HarnessMuseCode,
	} {
		for _, sourceName := range []string{sessionStartResume, sessionStartClear, sessionStartCompact} {
			t.Run(string(harness)+"/"+sourceName, func(t *testing.T) {
				t.Parallel()
				dir := Dir(t.TempDir())
				root := `{"hook_event_name":"SessionStart","session_id":"s1","source":"` + sourceName + `"}`
				for i, event := range []string{root, evStop, evPrompt} {
					require.NoError(t, RecordHook(dir, at(float64(i*5-1)), harness, hookCaptureID, strings.NewReader(event)))
				}
				hooks, bad, err := ReadHooks(dir)
				require.NoError(t, err)
				require.Zero(t, bad)
				cfg := DefaultAlignConfig()
				cfg.HookHarnesses = []protocol.Harness{harness}
				cfg.RootSessionSources = []WaitSource{{Harness: harness, HarnessVersion: testHarnessVersion}}
				r := rec(0, harness, 5, 0.1, 10, nil)
				got, _, err := Align(context.Background(), cfg, &sliceSource{recs: []dataset.Recording{r}}, hooks, nil)
				require.NoError(t, err)
				require.Empty(t, got[0].HookLabel)
			})
		}
	}
}

func TestAlignOpenCodeRootRejectsChildrenAndMalformedEvidence(t *testing.T) {
	t.Parallel()

	cfg := DefaultAlignConfig()
	cfg.HookHarnesses = []protocol.Harness{protocol.HarnessOpencode}
	source := WaitSource{Harness: protocol.HarnessOpencode, HarnessVersion: testHarnessVersion}
	cfg.RootSessionSources = []WaitSource{source}
	cfg.QuestionWaitSources = []WaitSource{source}
	r := rec(0, protocol.HarnessOpencode, 5, 0.1, 10, nil)
	question := []string{
		openQuestionAsked,
		openQuestionReply,
	}
	alignRecorded := func(t *testing.T, roots ...string) dataset.Label {
		t.Helper()
		dir := Dir(t.TempDir())
		for i, event := range roots {
			require.NoError(t, RecordHook(dir, at(-2+float64(i)/10), protocol.HarnessOpencode, hookCaptureID, strings.NewReader(event)))
		}
		for i, event := range question {
			require.NoError(t, RecordHook(dir, at(float64(i*10)), protocol.HarnessOpencode, hookCaptureID, strings.NewReader(event)))
		}
		hooks, bad, err := ReadHooks(dir)
		require.NoError(t, err)
		require.Zero(t, bad)
		got, _, err := Align(context.Background(), cfg, &sliceSource{recs: []dataset.Recording{r}}, hooks, nil)
		require.NoError(t, err)
		require.Len(t, got, 1)
		return got[0].HookLabel
	}

	root := openRootEvent
	child := openChildEvent
	require.Equal(t, dataset.LabelE, alignRecorded(t, root, child), "a child is excluded, not a second root")
	require.Empty(t, alignRecorded(t, root, `{"type":"session.created","properties":{"sessionID":"s3","info":{"id":"s3"}}}`), "two roots contradict")

	for name, malformed := range map[string]string{
		"null parent":       `{"type":"session.created","properties":{"sessionID":"s1","info":{"id":"s1","parentID":null}}}`,
		"wrong parent type": `{"type":"session.created","properties":{"sessionID":"s1","info":{"id":"s1","parentID":3}}}`,
		"missing info":      `{"type":"session.created","properties":{"sessionID":"s1"}}`,
		"incomplete info":   `{"type":"session.created","properties":{"sessionID":"s1","info":{}}}`,
		"mismatched ids":    `{"type":"session.created","properties":{"sessionID":"s2","info":{"id":"s1"}}}`,
		"missing outer id":  `{"type":"session.created","properties":{"info":{"id":"s1"}}}`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			require.Empty(t, alignRecorded(t, malformed))
		})
	}

	wrongVersion := cfg
	wrongVersion.RootSessionSources = []WaitSource{{Harness: protocol.HarnessOpencode, HarnessVersion: "other-v1"}}
	dir := Dir(t.TempDir())
	require.NoError(t, RecordHook(dir, at(-1), protocol.HarnessOpencode, hookCaptureID, strings.NewReader(root)))
	for i, event := range question {
		require.NoError(t, RecordHook(dir, at(float64(i*10)), protocol.HarnessOpencode, hookCaptureID, strings.NewReader(event)))
	}
	hooks, _, err := ReadHooks(dir)
	require.NoError(t, err)
	got, _, err := Align(context.Background(), wrongVersion, &sliceSource{recs: []dataset.Recording{r}}, hooks, nil)
	require.NoError(t, err)
	require.Empty(t, got[0].HookLabel, "a different measured version never activates the root")
}

func TestAlignOpenCodeRootIsolatesChildMargins(t *testing.T) {
	t.Parallel()

	dir := Dir(t.TempDir())
	events := []struct {
		sec float64
		raw string
	}{
		{-2, openRootEvent},
		{-1, openChildEvent},
		{0, `{"type":"question.asked","properties":{"id":"que-root","sessionID":"s1"}}`},
		{5, `{"type":"question.asked","properties":{"id":"que-child","sessionID":"s2"}}`},
		{6, `{"type":"question.replied","properties":{"requestID":"que-child","sessionID":"s2"}}`},
		{10, `{"type":"question.replied","properties":{"requestID":"que-root","sessionID":"s1"}}`},
	}
	for _, event := range events {
		require.NoError(t, RecordHook(dir, at(event.sec), protocol.HarnessOpencode, hookCaptureID, strings.NewReader(event.raw)))
	}
	hooks, bad, err := ReadHooks(dir)
	require.NoError(t, err)
	require.Zero(t, bad)
	cfg := DefaultAlignConfig()
	cfg.HookHarnesses = []protocol.Harness{protocol.HarnessOpencode}
	source := WaitSource{Harness: protocol.HarnessOpencode, HarnessVersion: testHarnessVersion}
	cfg.RootSessionSources = []WaitSource{source}
	cfg.QuestionWaitSources = []WaitSource{source}
	r := rec(0, protocol.HarnessOpencode, 5, 0.1, 10, nil)
	got, st, err := Align(context.Background(), cfg, &sliceSource{recs: []dataset.Recording{r}}, hooks, nil)
	require.NoError(t, err)
	require.Zero(t, st.Discarded, "a child transition cannot discard a root window")
	require.Equal(t, dataset.LabelE, got[0].HookLabel)
}

func TestFoldMuseQuestionLifecycle(t *testing.T) {
	t.Parallel()

	cfg := DefaultAlignConfig()
	cfg.HookHarnesses = []protocol.Harness{protocol.HarnessMuseCode}
	cfg.ApprovalWaitSources = []WaitSource{{Harness: protocol.HarnessMuseCode, HarnessVersion: testHarnessVersion}}
	cfg.QuestionWaitSources = []WaitSource{{Harness: protocol.HarnessMuseCode, HarnessVersion: testHarnessVersion}}
	questionNotification := `{"hook_event_name":"Notification","session_id":"s1","notification_type":"question_prompt"}`
	for _, start := range []string{
		userInputQuestion,
		`{"hook_event_name":"ToolUseStart","session_id":"s1","tool_name":"request_user_input"}`,
	} {
		records := []HookRecord{
			hookAt(0, protocol.HarnessMuseCode, start),
			hookAt(2, protocol.HarnessMuseCode, questionNotification),
			hookAt(10, protocol.HarnessMuseCode, `{"hook_event_name":"PostToolUseFailure","session_id":"s1","tool_name":"request_user_input"}`),
		}
		require.Empty(t, foldedLabelAt(cfg, protocol.HarnessMuseCode, records, 1), "a tool name does not open E or C")
		require.Equal(t, dataset.LabelE, foldedLabel(cfg, protocol.HarnessMuseCode, records))
	}

	late := []HookRecord{
		hookAt(0, protocol.HarnessMuseCode, userInputQuestion),
		hookAt(1, protocol.HarnessMuseCode, `{"hook_event_name":"PostToolUseFailure","session_id":"s1","tool_name":"request_user_input"}`),
		hookAt(2, protocol.HarnessMuseCode, questionNotification),
		hookAt(10, protocol.HarnessMuseCode, evStop),
	}
	require.Empty(t, foldedLabel(cfg, protocol.HarnessMuseCode, late), "a late notification never reopens a cancelled wait")

	preserve := []HookRecord{
		hookAt(0, protocol.HarnessMuseCode, userInputQuestion),
		hookAt(1, protocol.HarnessMuseCode, questionNotification),
		hookAt(2, protocol.HarnessMuseCode, `{"hook_event_name":"PermissionRequest","session_id":"s1"}`),
		hookAt(3, protocol.HarnessMuseCode, `{"hook_event_name":"Notification","session_id":"s1","notification_type":"permission_prompt"}`),
		hookAt(10, protocol.HarnessMuseCode, evPost),
	}
	require.Equal(t, dataset.LabelE, foldedLabel(cfg, protocol.HarnessMuseCode, preserve), "a permission prompt does not replace an open E")
}

func TestFoldOpenCodeOverlapAndClosures(t *testing.T) {
	t.Parallel()

	cfg := DefaultAlignConfig()
	cfg.HookHarnesses = []protocol.Harness{protocol.HarnessOpencode}
	source := WaitSource{Harness: protocol.HarnessOpencode, HarnessVersion: testHarnessVersion}
	cfg.ApprovalWaitSources = []WaitSource{source}
	cfg.QuestionWaitSources = []WaitSource{source}
	questionAsked := openQuestionAsked
	questionReplied := openQuestionReply
	permission := func(tool string) string {
		return `{"type":"permission.asked","properties":{"id":"per-1","sessionID":"s1","tool_name":"` + tool + `"}}`
	}
	permissionReplied := `{"type":"permission.replied","properties":{"requestID":"per-1","sessionID":"s1","reply":"once"}}`

	for _, records := range [][]HookRecord{
		{
			hookAt(0, protocol.HarnessOpencode, questionAsked),
			hookAt(1, protocol.HarnessOpencode, permission("question")),
			hookAt(8, protocol.HarnessOpencode, permissionReplied),
			hookAt(10, protocol.HarnessOpencode, questionReplied),
		},
		{
			hookAt(0, protocol.HarnessOpencode, permission("question")),
			hookAt(1, protocol.HarnessOpencode, questionAsked),
			hookAt(8, protocol.HarnessOpencode, permissionReplied),
			hookAt(10, protocol.HarnessOpencode, questionReplied),
		},
	} {
		require.Equal(t, dataset.LabelE, foldedLabel(cfg, protocol.HarnessOpencode, records))
		require.Equal(t, dataset.LabelE, foldedLabelAt(cfg, protocol.HarnessOpencode, records, 9), "permission closure does not close the question")
	}
	permissionFirst := []HookRecord{
		hookAt(0, protocol.HarnessOpencode, permission("question")),
		hookAt(1, protocol.HarnessOpencode, questionAsked),
		hookAt(8, protocol.HarnessOpencode, permissionReplied),
		hookAt(10, protocol.HarnessOpencode, questionReplied),
	}
	require.Empty(t, foldedLabelAt(cfg, protocol.HarnessOpencode, permissionFirst, 0.5), "a question permission is never approval D")

	for _, records := range [][]HookRecord{
		{
			hookAt(0, protocol.HarnessOpencode, questionAsked),
			hookAt(1, protocol.HarnessOpencode, permission("bash")),
			hookAt(8, protocol.HarnessOpencode, permissionReplied),
			hookAt(10, protocol.HarnessOpencode, questionReplied),
		},
		{
			hookAt(0, protocol.HarnessOpencode, permission("bash")),
			hookAt(1, protocol.HarnessOpencode, questionAsked),
			hookAt(8, protocol.HarnessOpencode, permissionReplied),
			hookAt(10, protocol.HarnessOpencode, questionReplied),
		},
	} {
		require.Empty(t, foldedLabel(cfg, protocol.HarnessOpencode, records), "an unidentified overlap is never D")
		require.Empty(t, foldedLabelAt(cfg, protocol.HarnessOpencode, records, 9), "a reply cannot guess its way back to E")
	}

	rejected := []HookRecord{
		hookAt(0, protocol.HarnessOpencode, questionAsked),
		hookAt(2, protocol.HarnessOpencode, `{"type":"question.rejected","properties":{"requestID":"que-1","sessionID":"s1"}}`),
		hookAt(10, protocol.HarnessOpencode, `{"type":"session.error","properties":{"sessionID":"s1","error":{"name":"MessageAbortedError"}}}`),
	}
	require.Empty(t, foldedLabel(cfg, protocol.HarnessOpencode, rejected), "question rejection closes E")
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
		if l.Source == SourceWeak {
			require.Equal(t, WeakOriginActivity, l.WeakOrigin)
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

func TestAlignImportedWeakLabels(t *testing.T) {
	t.Parallel()

	plain := rec(1, protocol.HarnessGeneric, 5, 0.1, 10, nil)
	weakRecord := func(id protocol.ULID, l dataset.Label) WeakRecord {
		reason := WeakReasonVisibleState
		if l == dataset.LabelG {
			reason = WeakReasonNoDeterminant
		}
		return WeakRecord{
			WindowID: id, Label: l, Reason: reason, Method: WeakMethodAgentReview,
			CriterionVersion: WeakCriterionV1, Reviewer: "reviewer-1", LabeledAt: at(100), Source: SourceWeak,
		}
	}
	weak := []WeakRecord{weakRecord(plain.WindowID, dataset.LabelG)}
	got, _, err := AlignWithWeak(context.Background(), DefaultAlignConfig(), &sliceSource{recs: []dataset.Recording{plain}}, nil, nil, weak)
	require.NoError(t, err)
	require.Equal(t, []AlignedLabel{{
		WindowID: plain.WindowID, Label: dataset.LabelG, Source: SourceWeak,
		WeakLabel: dataset.LabelG, WeakOrigin: WeakOriginImport, Doubt: DoubtWeakOnly,
	}}, public(got))

	hooked := claudeAt(5)
	hooks := []HookRecord{hk(-1, evRoot), hk(0, evStop), hk(10, evPrompt)}
	hookCfg := DefaultAlignConfig()
	hookCfg.RootSessionSources = []WaitSource{{Harness: protocol.HarnessClaudeCode, HarnessVersion: testHarnessVersion}}
	weak = []WeakRecord{weakRecord(hooked.WindowID, dataset.LabelB)}
	got, _, err = AlignWithWeak(context.Background(), hookCfg, &sliceSource{recs: []dataset.Recording{hooked}}, hooks, nil, weak)
	require.NoError(t, err)
	require.Equal(t, []AlignedLabel{{
		WindowID: hooked.WindowID, Label: dataset.LabelA, Source: SourceHook,
		HookLabel: dataset.LabelA, WeakLabel: dataset.LabelB, WeakOrigin: WeakOriginImport, Doubt: DoubtConflict,
	}}, public(got), "a weak label never replaces conflicting hook evidence")

	anns := []Annotation{{WindowID: hooked.WindowID, Label: dataset.LabelF, Annotator: "reviewer", LabeledAt: at(200)}}
	got, _, err = AlignWithWeak(context.Background(), hookCfg, &sliceSource{recs: []dataset.Recording{hooked}}, hooks, anns, weak)
	require.NoError(t, err)
	require.Equal(t, dataset.LabelF, got[0].Label)
	require.Equal(t, SourceManual, got[0].Source, "manual evidence remains strongest")
}

func TestAlignImportedWeakConflictsAreOrderIndependent(t *testing.T) {
	t.Parallel()

	plain := rec(4, protocol.HarnessGeneric, 5, 0.1, 10, nil)
	weakRecord := func(l dataset.Label, reviewer string, labeledAt time.Time) WeakRecord {
		return WeakRecord{
			WindowID: plain.WindowID, Label: l, Reason: WeakReasonVisibleState,
			Method: WeakMethodAgentReview, CriterionVersion: WeakCriterionV1,
			Reviewer: reviewer, LabeledAt: labeledAt, Source: SourceWeak,
		}
	}
	a := weakRecord(dataset.LabelA, "reviewer-a", at(200))
	b := weakRecord(dataset.LabelB, "reviewer-b", at(100))
	alignWeak := func(records []WeakRecord, anns []Annotation) ([]AlignedLabel, AlignStats) {
		got, st, err := AlignWithWeak(
			context.Background(), DefaultAlignConfig(),
			&sliceSource{recs: []dataset.Recording{plain}}, nil, anns, records,
		)
		require.NoError(t, err)
		return got, st
	}

	for _, records := range [][]WeakRecord{{a, b}, {b, a}} {
		got, st := alignWeak(records, nil)
		require.Len(t, got, 1, "reversing lines never creates another window")
		require.Equal(t, DoubtConflict, got[0].Doubt)
		require.Empty(t, got[0].Label, "labeled_at and line order never arbitrate")
		require.Equal(t, 1, st.Doubtful.Conflict)
	}

	got, st := alignWeak([]WeakRecord{a, a}, nil)
	require.Len(t, got, 1, "identical audited repetitions do not multiply a window")
	require.Equal(t, dataset.LabelA, got[0].Label)
	require.Equal(t, SourceWeak, got[0].Source)
	require.Equal(t, WeakOriginImport, got[0].WeakOrigin)
	require.Equal(t, DoubtWeakOnly, got[0].Doubt)
	require.Equal(t, 1, st.Weak)

	manual := []Annotation{{WindowID: plain.WindowID, Label: dataset.LabelF, Annotator: "human", LabeledAt: at(300)}}
	got, st = alignWeak([]WeakRecord{a, b}, manual)
	require.Equal(t, dataset.LabelF, got[0].Label)
	require.Equal(t, SourceManual, got[0].Source)
	require.Empty(t, got[0].Doubt, "a valid manual annotation remains the explicit override")
	require.Equal(t, 1, st.Manual)

	hooked := claudeAt(5)
	a.WindowID, b.WindowID = hooked.WindowID, hooked.WindowID
	cfg := DefaultAlignConfig()
	cfg.RootSessionSources = []WaitSource{{Harness: protocol.HarnessClaudeCode, HarnessVersion: testHarnessVersion}}
	hooks := []HookRecord{hk(-1, evRoot), hk(0, evStop), hk(10, evPrompt)}
	got, _, err := AlignWithWeak(
		context.Background(), cfg, &sliceSource{recs: []dataset.Recording{hooked}}, hooks, nil, []WeakRecord{b, a},
	)
	require.NoError(t, err)
	require.Equal(t, dataset.LabelA, got[0].Label)
	require.Equal(t, SourceHook, got[0].Source)
	require.Equal(t, DoubtConflict, got[0].Doubt, "hook agreement with one import cannot resolve conflicting imports")
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
		out = append(out, AlignedLabel{
			WindowID: l.WindowID, Label: l.Label, Source: l.Source, HookLabel: l.HookLabel,
			WeakLabel: l.WeakLabel, WeakOrigin: l.WeakOrigin, Doubt: l.Doubt,
		})
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
		{WindowID: weak.WindowID, Label: dataset.LabelD, Source: SourceManual},
	}, public(got), "a manual label near a transition is discarded too")
	require.Equal(t, 2, st.Manual)
	require.Equal(t, 1, st.Discarded)
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
hook_harnesses: [claude-code, codex, muse-code, opencode]
root_session_sources:
  - {harness: claude-code, harness_version: "2.0.14 (Claude Code)"}
  - {harness: codex, harness_version: "0.154.0"}
  - {harness: muse-code, harness_version: "1.4.13"}
  - {harness: opencode, harness_version: "1.18.29"}
wait_sources:
  approval:
    - {harness: claude-code, harness_version: "2.0.14 (Claude Code)"}
  question:
    - {harness: opencode, harness_version: "1.2.3"}
weak:
  silence_seconds: 1.5
  burst_bytes: 10
  keep: train
`))
	require.NoError(t, err)
	require.Equal(t, AlignConfig{
		EventWindow: EventWindowSpan, Margin: 500 * time.Millisecond, MaxInterval: time.Minute,
		HookHarnesses: []protocol.Harness{
			protocol.HarnessClaudeCode, protocol.HarnessCodex, protocol.HarnessMuseCode, protocol.HarnessOpencode,
		},
		RootSessionSources: []WaitSource{
			{Harness: protocol.HarnessClaudeCode, HarnessVersion: "2.0.14 (Claude Code)"},
			{Harness: protocol.HarnessCodex, HarnessVersion: "0.154.0"},
			{Harness: protocol.HarnessMuseCode, HarnessVersion: "1.4.13"},
			{Harness: protocol.HarnessOpencode, HarnessVersion: "1.18.29"},
		},
		ApprovalWaitSources: []WaitSource{{Harness: protocol.HarnessClaudeCode, HarnessVersion: "2.0.14 (Claude Code)"}},
		QuestionWaitSources: []WaitSource{{Harness: protocol.HarnessOpencode, HarnessVersion: "1.2.3"}},
		Silence:             1500 * time.Millisecond, BurstBytes: 10, WeakKeep: WeakKeepTrain,
	}, cfg)

	for _, in := range []string{
		"unknown: 1", "event_window: frame", "margin_ms: -1", "margin_ms: 60001",
		"max_interval_seconds: 0", "hook_harnesses: [vim]", "weak: {silence_seconds: 0}",
		"weak: {burst_bytes: 0}", "weak: {silence_seconds: .nan}", "margin_ms: [1]",
		"weak: {keep: validation}",
		"root_session_sources: [{harness: opencode, harness_version: 1.0}]",
		"hook_harnesses: [antigravity]\nroot_session_sources: [{harness: antigravity, harness_version: 1.0}]",
		"root_session_sources: [{harness: claude-code, harness_version: 1.0}, {harness: claude-code, harness_version: 1.0}]",
		"root_session_sources: [{harness: claude-code, harness_version: unknown}]",
		"wait_sources: {approval: [{harness: vim, harness_version: 1.0}]}",
		"wait_sources: {approval: [{harness: opencode, harness_version: 1.0}]}",
		"wait_sources: {approval: [{harness: claude-code, harness_version: ''}]}",
		"wait_sources: {approval: [{harness: claude-code, harness_version: \"1\\n2\"}]}",
		"wait_sources: {approval: [{harness: claude-code, harness_version: 1.0}, {harness: claude-code, harness_version: 1.0}]}",
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
	require.NoError(t, RecordHook(dir, at(0), protocol.HarnessClaudeCode, hookCaptureID, strings.NewReader(evStop)))
	require.NoError(t, RecordHook(dir, at(10), protocol.HarnessClaudeCode, hookCaptureID, strings.NewReader(evPrompt)))
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
