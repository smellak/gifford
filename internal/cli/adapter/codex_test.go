package adapter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/analyst"
	"github.com/smellak/gifford/internal/nativeevents"
	"github.com/smellak/gifford/internal/protocol"
)

var codexHookTime = time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC)

const (
	codexTestSessionID  = "session-1"
	codexTestTurnID     = "turn-1"
	codexTestToolUseID  = "tool-1"
	codexTestAgentID    = "agent-1"
	codexCanary         = "CANARY-content"
	codexHookGroup      = "hook"
	codexToolUseField   = "tool_use_id"
	codexResponseField  = "tool_response"
	codexSourceField    = "source"
	codexReasonField    = "reason"
	codexAgentIDField   = "agent_id"
	codexAgentTypeField = "agent_type"
	codexTriggerField   = "trigger"
)

func codexHookJSON(t *testing.T, eventName string, fields map[string]any) string {
	t.Helper()
	input := map[string]any{
		"hook_event_name":                        eventName,
		"session_id":                             codexTestSessionID,
		"turn_id":                                codexTestTurnID,
		"transcript_path":                        "/tmp/CANARY-transcript.jsonl",
		"cwd":                                    "/tmp/CANARY-worktree",
		"model":                                  "CANARY-model",
		"permission_mode":                        "default",
		strings.Join([]string{"pro", "mpt"}, ""): codexCanary,
		"tool_name":                              "CANARY-tool",
		"tool_input":                             map[string]any{"command": codexCanary},
		codexResponseField:                       map[string]any{"output": codexCanary},
		"last_assistant_message":                 codexCanary,
	}
	for key, value := range fields {
		input[key] = value
	}
	raw, err := json.Marshal(input)
	require.NoError(t, err)
	return string(raw)
}

func executeCodexHook(
	t *testing.T,
	input string,
	nonces io.Reader,
	send emitFunc,
	args ...string,
) (string, string) {
	t.Helper()
	cmd := newCodexHookCmd(send, func() time.Time { return codexHookTime }, nonces)
	cmd.SetIn(strings.NewReader(input))
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	commandArgs := make([]string, len(args))
	copy(commandArgs, args)
	cmd.SetArgs(commandArgs)
	require.NoError(t, cmd.Execute())
	return stdout.String(), stderr.String()
}

func TestCodexHookTableIsContentFree(t *testing.T) {
	tests := []struct {
		name              string
		eventName         string
		fields            map[string]any
		wantType          protocol.EventType
		wantResult        protocol.NativeEventResult
		wantTool          protocol.OpaqueID
		wantRequestPrefix string
		wantTurn          bool
		wantNoEvent       bool
	}{
		{name: "session startup", eventName: codexHookSessionStart, fields: map[string]any{codexSourceField: codexSourceStartup}, wantType: protocol.EventTypeSessionStarted},
		{name: "session resume", eventName: codexHookSessionStart, fields: map[string]any{codexSourceField: codexSourceResume}, wantType: protocol.EventTypeSessionStarted},
		{name: "session clear", eventName: codexHookSessionStart, fields: map[string]any{codexSourceField: codexSourceClear}, wantType: protocol.EventTypeSessionStarted},
		{name: "session compact", eventName: codexHookSessionStart, fields: map[string]any{codexSourceField: codexSourceCompact}, wantNoEvent: true},
		{name: "session end", eventName: codexHookSessionEnd, fields: map[string]any{codexReasonField: codexReasonOther}, wantType: protocol.EventTypeSessionEnded},
		{name: "user input", eventName: codexHookUserPromptSubmit, wantType: protocol.EventTypePromptSubmitted, wantTurn: true},
		{name: "tool start", eventName: codexHookPreToolUse, fields: map[string]any{codexToolUseField: codexTestToolUseID}, wantType: protocol.EventTypeToolStarted, wantTool: codexTestToolUseID, wantTurn: true},
		{name: "approval", eventName: codexHookPermission, wantType: protocol.EventTypeApprovalRequested, wantRequestPrefix: codexRequestPrefix, wantTurn: true},
		{name: "tool complete", eventName: codexHookPostToolUse, fields: map[string]any{codexToolUseField: codexTestToolUseID}, wantType: protocol.EventTypeToolCompleted, wantResult: protocol.NativeEventResultOK, wantTool: codexTestToolUseID, wantTurn: true},
		{name: "subagent start", eventName: codexHookSubagentStart, fields: map[string]any{codexAgentIDField: codexTestAgentID, codexAgentTypeField: codexCanary}, wantType: protocol.EventTypeSubagentStarted, wantTurn: true},
		{name: "subagent stop", eventName: codexHookSubagentStop, fields: map[string]any{codexAgentIDField: codexTestAgentID}, wantNoEvent: true},
		{name: "pre compact", eventName: codexHookPreCompact, fields: map[string]any{codexTriggerField: codexTriggerManual}, wantNoEvent: true},
		{name: "post compact", eventName: codexHookPostCompact, fields: map[string]any{codexTriggerField: codexTriggerAuto}, wantType: protocol.EventTypeContextCompactionCompleted, wantTurn: true},
		{name: "stop", eventName: codexHookStop, wantNoEvent: true},
		{name: "interrupt", eventName: codexHookInterrupt, wantType: protocol.EventTypeTurnCompleted, wantResult: protocol.NativeEventResultCancelled, wantTurn: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var (
				calls int
				hello nativeevents.Hello
				event nativeevents.Event
			)
			send := func(
				_ context.Context,
				_ string,
				gotHello nativeevents.Hello,
				gotEvent nativeevents.Event,
			) error {
				calls++
				hello, event = gotHello, gotEvent
				return nil
			}
			stdout, stderr := executeCodexHook(
				t, codexHookJSON(t, test.eventName, test.fields),
				bytes.NewReader(bytes.Repeat([]byte{0x11}, 2*codexNonceBytes)), send,
			)
			require.Empty(t, stdout)
			require.Empty(t, stderr)
			if test.wantNoEvent {
				require.Zero(t, calls)
				return
			}
			require.Equal(t, 1, calls)
			require.Equal(t, protocol.HarnessCodex, hello.Harness)
			require.Equal(t, codexHarnessVersion, hello.HarnessVersion)
			require.Equal(t, nativeevents.EmitterTypeHook, hello.EmitterType)
			require.Equal(t, codexEmitterVersion, hello.EmitterVersion)
			require.Equal(t, nativeevents.SchemaVersion, hello.SchemaVersion)
			require.Equal(t, test.wantType, event.Type)
			require.Equal(t, test.wantResult, event.Result)
			require.Equal(t, test.wantTool, event.ToolRef)
			if test.wantRequestPrefix != "" {
				require.True(t, strings.HasPrefix(string(event.RequestRef), test.wantRequestPrefix))
			}
			if test.wantTurn {
				require.Equal(t, protocol.OpaqueID(codexTestTurnID), event.TurnRef)
			} else {
				require.Empty(t, event.TurnRef)
			}
			require.Equal(t, protocol.OpaqueID(codexTestSessionID), event.SessionRef)
			require.True(t, strings.HasPrefix(string(event.EmitterEventID), codexEventIDPrefix))
			require.Empty(t, event.ToolKind)
			require.NoError(t, event.Validate())
			wire, err := json.Marshal(struct {
				Hello nativeevents.Hello `json:"hello"`
				Event nativeevents.Event `json:"event"`
			}{Hello: hello, Event: event})
			require.NoError(t, err)
			require.NotContains(t, string(wire), "CANARY")
		})
	}
}

func TestCodexPermissionRequestsHaveInvocationRefs(t *testing.T) {
	nonceBytes := append(
		bytes.Repeat([]byte{0x11}, codexNonceBytes),
		bytes.Repeat([]byte{0x12}, codexNonceBytes)...,
	)
	nonceBytes = append(nonceBytes, bytes.Repeat([]byte{0x21}, codexNonceBytes)...)
	nonceBytes = append(nonceBytes, bytes.Repeat([]byte{0x22}, codexNonceBytes)...)
	nonces := bytes.NewReader(nonceBytes)
	var got []nativeevents.Event
	send := func(_ context.Context, _ string, _ nativeevents.Hello, event nativeevents.Event) error {
		got = append(got, event)
		return nil
	}
	input := codexHookJSON(t, codexHookPermission, nil)
	executeCodexHook(t, input, nonces, send)
	executeCodexHook(t, input, nonces, send)

	require.Len(t, got, 2)
	require.NotEqual(t, got[0].RequestRef, got[1].RequestRef)
	require.NotEqual(t, got[0].EmitterEventID, got[1].EmitterEventID)
	require.Equal(t, protocol.OpaqueID(codexRequestPrefix+strings.Repeat("11", codexNonceBytes)), got[0].RequestRef)
	require.Equal(t, protocol.OpaqueID(codexEventIDPrefix+strings.Repeat("12", codexNonceBytes)), got[0].EmitterEventID)
}

func TestCodexPermissionOmitsInvalidTurnRef(t *testing.T) {
	event, ok := codexEvent(codexHookInput{
		HookEventName: codexHookPermission, SessionID: codexTestSessionID,
		TurnID: "not/a/native/ref",
	}, codexHookTime.UnixMilli(), bytes.NewReader(bytes.Repeat([]byte{0x33}, 2*codexNonceBytes)))
	require.True(t, ok)
	require.Empty(t, event.TurnRef)
	require.NotEmpty(t, event.RequestRef)
}

func TestCodexRefsHashInvalidValuesDeterministically(t *testing.T) {
	longSession := strings.Repeat("s", 65)
	invalidTurn := "turn/with/slash"
	longTool := strings.Repeat("t", 65)
	input := codexHookInput{
		HookEventName: codexHookPreToolUse, SessionID: longSession,
		TurnID: invalidTurn, ToolUseID: longTool,
	}
	first, ok := codexEvent(input, codexHookTime.UnixMilli(), bytes.NewReader(bytes.Repeat([]byte{0x41}, codexNonceBytes)))
	require.True(t, ok)
	second, ok := codexEvent(input, codexHookTime.UnixMilli(), bytes.NewReader(bytes.Repeat([]byte{0x42}, codexNonceBytes)))
	require.True(t, ok)

	require.Len(t, first.SessionRef, 64)
	require.Len(t, first.TurnRef, 64)
	require.Len(t, first.ToolRef, 64)
	require.Equal(t, first.SessionRef, second.SessionRef)
	require.Equal(t, first.TurnRef, second.TurnRef)
	require.Equal(t, first.ToolRef, second.ToolRef)
	require.NotEqual(t, first.EmitterEventID, second.EmitterEventID)
	require.NotEqual(t, first.SessionRef, first.TurnRef)
	require.NoError(t, first.Validate())
}

func TestCodexOpenEventsExpireOrCancelWithoutInventedClosures(t *testing.T) {
	permission, ok := codexEvent(codexHookInput{
		HookEventName: codexHookPermission, SessionID: codexTestSessionID, TurnID: codexTestTurnID,
	}, codexHookTime.UnixMilli(), bytes.NewReader(bytes.Repeat([]byte{0x51}, 2*codexNonceBytes)))
	require.True(t, ok)
	approval := analyst.NativeEvent{
		EventID: "01ARZ3NDEKTSV4RRFFQ69G5FAV", Type: permission.Type,
		At: codexHookTime, Sequence: 1, TurnRef: permission.TurnRef, RequestRef: permission.RequestRef,
	}
	during := analyst.ReduceNative(
		[]analyst.NativeEvent{approval}, codexHookTime.Add(analyst.NativeOperationTTL-time.Nanosecond),
	)
	require.True(t, during.Blocks)
	require.Equal(t, protocol.AgentPhaseWaitingApproval, during.Phase)
	after := analyst.ReduceNative(
		[]analyst.NativeEvent{approval}, codexHookTime.Add(analyst.NativeOperationTTL),
	)
	require.False(t, after.Blocks)
	require.True(t, after.Stale)
	require.Equal(t, protocol.AgentPhaseUnknown, after.Phase)

	tool, ok := codexEvent(codexHookInput{
		HookEventName: codexHookPreToolUse, SessionID: codexTestSessionID,
		TurnID: codexTestTurnID, ToolUseID: codexTestToolUseID,
	}, codexHookTime.UnixMilli(), bytes.NewReader(bytes.Repeat([]byte{0x52}, codexNonceBytes)))
	require.True(t, ok)
	openTool := analyst.NativeEvent{
		EventID: "01ARZ3NDEKTSV4RRFFQ69G5FAW", Type: tool.Type,
		At: codexHookTime, Sequence: 1, TurnRef: tool.TurnRef, ToolRef: tool.ToolRef,
	}
	running := analyst.ReduceNative(
		[]analyst.NativeEvent{openTool}, codexHookTime.Add(analyst.NativeOperationTTL-time.Nanosecond),
	)
	require.True(t, running.Blocks)
	require.Equal(t, protocol.AgentPhaseRunningTool, running.Phase)
	expired := analyst.ReduceNative(
		[]analyst.NativeEvent{openTool}, codexHookTime.Add(analyst.NativeOperationTTL),
	)
	require.False(t, expired.Blocks)
	require.True(t, expired.Stale)
	require.Equal(t, protocol.AgentPhaseUnknown, expired.Phase)

	interrupt, ok := codexEvent(codexHookInput{
		HookEventName: codexHookInterrupt, SessionID: codexTestSessionID, TurnID: codexTestTurnID,
	}, codexHookTime.Add(time.Second).UnixMilli(), bytes.NewReader(bytes.Repeat([]byte{0x53}, codexNonceBytes)))
	require.True(t, ok)
	state := analyst.ReduceNative([]analyst.NativeEvent{
		openTool,
		{EventID: "01ARZ3NDEKTSV4RRFFQ69G5FAX", Type: interrupt.Type, At: codexHookTime.Add(time.Second), Sequence: 2, TurnRef: interrupt.TurnRef, Result: interrupt.Result},
	}, codexHookTime.Add(time.Second))
	require.False(t, state.Blocks)
}

func TestCodexMalformedOrUnsupportedInputAlwaysSucceeds(t *testing.T) {
	oversized := `{"hook_event_name":"UserPromptSubmit","session_id":"session-1","prompt":"` +
		strings.Repeat("x", maxCodexHookBytes) + `"}`
	for name, input := range map[string]string{
		"empty": "", "malformed": "{", "array": "[]", "trailing": `{} {}`,
		"missing session": `{"hook_event_name":"SessionEnd","reason":"other"}`,
		"missing turn":    `{"hook_event_name":"UserPromptSubmit","session_id":"session-1"}`,
		"unknown":         `{"hook_event_name":"Notification","session_id":"session-1"}`,
		"bad source":      `{"hook_event_name":"SessionStart","session_id":"session-1","source":"fork"}`,
		"bad trigger":     `{"hook_event_name":"PostCompact","session_id":"session-1","turn_id":"turn-1","trigger":"later"}`,
		"oversized":       oversized,
	} {
		t.Run(name, func(t *testing.T) {
			calls := 0
			stdout, stderr := executeCodexHook(t, input, bytes.NewReader(bytes.Repeat([]byte{0x61}, 2*codexNonceBytes)), func(
				context.Context,
				string,
				nativeevents.Hello,
				nativeevents.Event,
			) error {
				calls++
				return nil
			})
			require.Zero(t, calls)
			require.Empty(t, stdout)
			require.Empty(t, stderr)
		})
	}
}

func TestCodexHookAlwaysExitsZero(t *testing.T) {
	valid := codexHookJSON(t, codexHookInterrupt, nil)
	t.Run("send error", func(t *testing.T) {
		executeCodexHook(t, valid, bytes.NewReader(bytes.Repeat([]byte{0x71}, codexNonceBytes)), func(
			context.Context,
			string,
			nativeevents.Hello,
			nativeevents.Event,
		) error {
			return errors.New("send error")
		})
	})
	t.Run("send panic", func(t *testing.T) {
		executeCodexHook(t, valid, bytes.NewReader(bytes.Repeat([]byte{0x72}, codexNonceBytes)), func(
			context.Context,
			string,
			nativeevents.Hello,
			nativeevents.Event,
		) error {
			panic("send panic")
		})
	})
	t.Run("nonce error", func(t *testing.T) {
		executeCodexHook(t, valid, codexErrorReader{}, func(
			context.Context,
			string,
			nativeevents.Hello,
			nativeevents.Event,
		) error {
			t.Fatal("send called")
			return nil
		})
	})
	t.Run("arguments", func(t *testing.T) {
		executeCodexHook(t, valid, bytes.NewReader(nil), func(
			context.Context,
			string,
			nativeevents.Hello,
			nativeevents.Event,
		) error {
			t.Fatal("send called")
			return nil
		}, "--future-flag", "value")
	})
}

type codexErrorReader struct{}

func (codexErrorReader) Read([]byte) (int, error) { return 0, errors.New("read error") }

func findCodexHook(root *cobra.Command) *cobra.Command {
	command, _, err := root.Find([]string{codexHookGroup, "codex"})
	if err != nil {
		return nil
	}
	return command
}

func TestAdapterTreeContainsCodexHook(t *testing.T) {
	require.NotNil(t, findCodexHook(NewCmd(nil)))
}
