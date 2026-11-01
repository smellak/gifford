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

var museHookTime = time.Date(2026, 11, 1, 14, 0, 0, 0, time.UTC)

const (
	museTestSessionID      = "session-1"
	museTestTurnID         = "turn-1"
	museTestToolUseID      = "tool-1"
	museTestChildSessionID = "child-1"
	museCanary             = "CANARY-content"
	museHookGroup          = "hook"
	museToolUseField       = "tool_use_id"
	museChildSessionField  = "child_session_id"
	museSourceField        = "source"
	museTriggerField       = "trigger"
)

// museHookJSON models the measured 1.4.13 hook stdin. The extra fields are
// intentionally content-bearing canaries: the adapter must discard them.
func museHookJSON(t *testing.T, eventName string, fields map[string]any) string {
	t.Helper()
	input := map[string]any{
		strings.Join([]string{"hook_event_", "name"}, ""): eventName,
		strings.Join([]string{"session_", "id"}, ""):      museTestSessionID,
		"turn_id":                                         museTestTurnID,
		strings.Join([]string{"c", "wd"}, ""):             "/tmp/MUSE-CANARY-worktree",
		strings.Join([]string{"mo", "del"}, ""):           "MUSE-CANARY-model",
		strings.Join([]string{"permission_", "mode"}, ""): "CANARY-permission",
		strings.Join([]string{"pro", "mpt"}, ""):          museCanary,
		"tool_name":                                       "CANARY-tool",
		"tool_input":                                      map[string]any{"command": museCanary},
		strings.Join([]string{"tool_", "response"}, ""):   map[string]any{strings.Join([]string{"out", "put"}, ""): museCanary},
		"request_id":                                      "CANARY-request",
		"notification_type":                               "question_prompt",
		"output_text_preview":                             museCanary,
		"error_details":                                   museCanary,
		"last_assistant_message":                          museCanary,
		"reason":                                          museCanary,
	}
	for key, value := range fields {
		input[key] = value
	}
	raw, err := json.Marshal(input)
	require.NoError(t, err)
	return string(raw)
}

func executeMuseHook(
	t *testing.T,
	input string,
	nonces io.Reader,
	send emitFunc,
	args ...string,
) (string, string) {
	t.Helper()
	cmd := newMuseHookCmd(send, func() time.Time { return museHookTime }, nonces)
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

func TestMuseHookFixturesAreContentFree(t *testing.T) {
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
		{name: "session startup", eventName: museHookSessionStart, fields: map[string]any{museSourceField: museSourceStartup}, wantType: protocol.EventTypeSessionStarted},
		{name: "session resume", eventName: museHookSessionStart, fields: map[string]any{museSourceField: museSourceResume}, wantType: protocol.EventTypeSessionStarted},
		{name: "session clear", eventName: museHookSessionStart, fields: map[string]any{museSourceField: museSourceClear}, wantType: protocol.EventTypeSessionStarted},
		{name: "session compact", eventName: museHookSessionStart, fields: map[string]any{museSourceField: museSourceCompact}, wantNoEvent: true},
		{name: "session termination", eventName: museHookSessionEnd, wantType: protocol.EventTypeSessionEnded},
		{name: "user input", eventName: museHookUserPromptSubmit, wantType: protocol.EventTypePromptSubmitted, wantTurn: true},
		{name: "pre LLM", eventName: museHookPreLLMCall, wantType: protocol.EventTypeTurnStarted, wantTurn: true},
		{name: "post LLM", eventName: museHookPostLLMCall, wantType: protocol.EventTypeTurnStreaming, wantTurn: true},
		{name: "tool intent", eventName: museHookPreToolUse, fields: map[string]any{museToolUseField: museTestToolUseID}, wantType: protocol.EventTypeToolStarted, wantTool: museTestToolUseID, wantTurn: true},
		{name: "tool execution signal", eventName: museHookToolUseStart, fields: map[string]any{museToolUseField: museTestToolUseID}, wantNoEvent: true},
		{name: "permission request", eventName: museHookPermission, wantType: protocol.EventTypeApprovalRequested, wantRequestPrefix: museRequestPrefix, wantTurn: true},
		{name: "tool completion", eventName: museHookPostToolUse, fields: map[string]any{museToolUseField: museTestToolUseID}, wantType: protocol.EventTypeToolCompleted, wantResult: protocol.NativeEventResultOK, wantTool: museTestToolUseID, wantTurn: true},
		{name: "tool failure", eventName: museHookPostToolUseFailure, fields: map[string]any{museToolUseField: museTestToolUseID}, wantType: protocol.EventTypeToolFailed, wantResult: protocol.NativeEventResultFailed, wantTool: museTestToolUseID, wantTurn: true},
		{name: "tool batch", eventName: museHookPostToolBatch, wantNoEvent: true},
		{name: "subagent start", eventName: museHookSubagentStart, fields: map[string]any{museChildSessionField: museTestChildSessionID}, wantType: protocol.EventTypeSubagentStarted, wantTurn: true},
		{name: "subagent stop", eventName: museHookSubagentStop, fields: map[string]any{museChildSessionField: museTestChildSessionID}, wantNoEvent: true},
		{name: "pre compact", eventName: museHookPreCompact, fields: map[string]any{museTriggerField: museTriggerManual}, wantNoEvent: true},
		{name: "post compact", eventName: museHookPostCompact, fields: map[string]any{museTriggerField: museTriggerAuto}, wantType: protocol.EventTypeContextCompactionCompleted, wantTurn: true},
		{name: "stop hook", eventName: museHookStop, wantNoEvent: true},
		{name: "stop failure", eventName: museHookStopFailure, wantNoEvent: true},
		{name: "interrupt", eventName: museHookInterrupt, wantType: protocol.EventTypeTurnCompleted, wantResult: protocol.NativeEventResultCancelled, wantTurn: true},
		{name: "session fork", eventName: museHookSessionFork, wantNoEvent: true},
		{name: "notification", eventName: museHookNotification, wantNoEvent: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var (
				calls int
				hello nativeevents.Hello
				event nativeevents.Event
			)
			send := func(
				ctx context.Context,
				_ string,
				gotHello nativeevents.Hello,
				gotEvent nativeevents.Event,
			) error {
				calls++
				hello, event = gotHello, gotEvent
				deadline, ok := ctx.Deadline()
				require.True(t, ok)
				require.LessOrEqual(t, time.Until(deadline), EmitTimeout)
				return nil
			}
			stdout, stderr := executeMuseHook(
				t, museHookJSON(t, test.eventName, test.fields),
				bytes.NewReader(bytes.Repeat([]byte{0x11}, 2*museNonceBytes)), send,
			)
			require.Empty(t, stdout)
			require.Empty(t, stderr)
			if test.wantNoEvent {
				require.Zero(t, calls)
				return
			}
			require.Equal(t, 1, calls)
			require.NoError(t, hello.Validate())
			require.Equal(t, protocol.HarnessMuseCode, hello.Harness)
			require.Equal(t, museHarnessVersion, hello.HarnessVersion)
			require.Equal(t, nativeevents.EmitterTypeHook, hello.EmitterType)
			require.Equal(t, museEmitterVersion, hello.EmitterVersion)
			require.Equal(t, nativeevents.SchemaVersion, hello.SchemaVersion)
			require.Equal(t, test.wantType, event.Type)
			require.Equal(t, test.wantResult, event.Result)
			require.Equal(t, test.wantTool, event.ToolRef)
			if test.wantRequestPrefix != "" {
				require.True(t, strings.HasPrefix(string(event.RequestRef), test.wantRequestPrefix))
			}
			if test.wantTurn {
				require.Equal(t, protocol.OpaqueID(museTestTurnID), event.TurnRef)
			} else {
				require.Empty(t, event.TurnRef)
			}
			require.Equal(t, protocol.OpaqueID(museTestSessionID), event.SessionRef)
			require.True(t, strings.HasPrefix(string(event.EmitterEventID), museEventIDPrefix))
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

func TestMusePermissionRequestsHaveIndependentInvocationRefs(t *testing.T) {
	nonceBytes := append(
		bytes.Repeat([]byte{0x11}, museNonceBytes),
		bytes.Repeat([]byte{0x12}, museNonceBytes)...,
	)
	nonceBytes = append(nonceBytes, bytes.Repeat([]byte{0x21}, museNonceBytes)...)
	nonceBytes = append(nonceBytes, bytes.Repeat([]byte{0x22}, museNonceBytes)...)
	nonces := bytes.NewReader(nonceBytes)
	var got []nativeevents.Event
	send := func(_ context.Context, _ string, _ nativeevents.Hello, event nativeevents.Event) error {
		got = append(got, event)
		return nil
	}
	input := museHookJSON(t, museHookPermission, nil)
	executeMuseHook(t, input, nonces, send)
	executeMuseHook(t, input, nonces, send)

	require.Len(t, got, 2)
	require.NotEqual(t, got[0].RequestRef, got[1].RequestRef)
	require.NotEqual(t, got[0].EmitterEventID, got[1].EmitterEventID)
	require.Equal(t, protocol.OpaqueID(museRequestPrefix+strings.Repeat("11", museNonceBytes)), got[0].RequestRef)
	require.Equal(t, protocol.OpaqueID(museEventIDPrefix+strings.Repeat("12", museNonceBytes)), got[0].EmitterEventID)
}

func TestMusePermissionOmitsInvalidTurnRef(t *testing.T) {
	event, ok := museEvent(museHookInput{
		HookEventName: museHookPermission, SessionID: museTestSessionID,
		TurnID: "not/a/native/ref",
	}, museHookTime.UnixMilli(), bytes.NewReader(bytes.Repeat([]byte{0x33}, 2*museNonceBytes)))
	require.True(t, ok)
	require.Empty(t, event.TurnRef)
	require.NotEmpty(t, event.RequestRef)
}

func TestMuseRefsHashInvalidValuesDeterministically(t *testing.T) {
	input := museHookInput{
		HookEventName: museHookPreToolUse, SessionID: strings.Repeat("s", 65),
		TurnID: "turn/with/slash", ToolUseID: strings.Repeat("t", 65),
	}
	first, ok := museEvent(input, museHookTime.UnixMilli(), bytes.NewReader(bytes.Repeat([]byte{0x41}, museNonceBytes)))
	require.True(t, ok)
	second, ok := museEvent(input, museHookTime.UnixMilli(), bytes.NewReader(bytes.Repeat([]byte{0x42}, museNonceBytes)))
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

func TestMuseApprovalExpiresUnknownAndInterruptCancelsTurn(t *testing.T) {
	permission, ok := museEvent(museHookInput{
		HookEventName: museHookPermission, SessionID: museTestSessionID, TurnID: museTestTurnID,
	}, museHookTime.UnixMilli(), bytes.NewReader(bytes.Repeat([]byte{0x51}, 2*museNonceBytes)))
	require.True(t, ok)
	approval := analyst.NativeEvent{
		EventID: "01ARZ3NDEKTSV4RRFFQ69G5FAY", Type: permission.Type,
		At: museHookTime, Sequence: 1, TurnRef: permission.TurnRef, RequestRef: permission.RequestRef,
	}
	during := analyst.ReduceNative(
		[]analyst.NativeEvent{approval}, museHookTime.Add(analyst.NativeOperationTTL-time.Nanosecond),
	)
	require.True(t, during.Blocks)
	require.Equal(t, protocol.AgentPhaseWaitingApproval, during.Phase)
	after := analyst.ReduceNative(
		[]analyst.NativeEvent{approval}, museHookTime.Add(analyst.NativeOperationTTL),
	)
	require.False(t, after.Blocks)
	require.True(t, after.Stale)
	require.Equal(t, protocol.AgentPhaseUnknown, after.Phase)

	interrupt, ok := museEvent(museHookInput{
		HookEventName: museHookInterrupt, SessionID: museTestSessionID, TurnID: museTestTurnID,
	}, museHookTime.Add(time.Second).UnixMilli(), bytes.NewReader(bytes.Repeat([]byte{0x52}, museNonceBytes)))
	require.True(t, ok)
	state := analyst.ReduceNative([]analyst.NativeEvent{
		approval,
		{EventID: "01ARZ3NDEKTSV4RRFFQ69G5FAZ", Type: interrupt.Type, At: museHookTime.Add(time.Second), Sequence: 2, TurnRef: interrupt.TurnRef, Result: interrupt.Result},
	}, museHookTime.Add(time.Second))
	require.False(t, state.Blocks)
}

func TestMuseMalformedOrUnsupportedInputAlwaysSucceeds(t *testing.T) {
	oversized := `{"hook_event_name":"UserPromptSubmit","session_id":"session-1","prompt":"` +
		strings.Repeat("x", maxMuseHookBytes) + `"}`
	for name, input := range map[string]string{
		"blank input": "", "broken JSON": "{", "JSON array": "[]",
		"multiple values": strings.Join([]string{"{}", "{}"}, " "),
		"missing session": `{"hook_event_name":"SessionEnd"}`,
		"missing turn":    `{"hook_event_name":"UserPromptSubmit","session_id":"session-1"}`,
		"missing tool":    `{"hook_event_name":"PreToolUse","session_id":"session-1","turn_id":"turn-1"}`,
		"missing child":   `{"hook_event_name":"SubagentStart","session_id":"session-1","turn_id":"turn-1"}`,
		"unknown":         `{"hook_event_name":"FutureHook","session_id":"session-1"}`,
		"bad source":      `{"hook_event_name":"SessionStart","session_id":"session-1","source":"fork"}`,
		"bad trigger":     `{"hook_event_name":"PostCompact","session_id":"session-1","turn_id":"turn-1","trigger":"later"}`,
		"over limit":      oversized,
	} {
		t.Run(name, func(t *testing.T) {
			calls := 0
			stdout, stderr := executeMuseHook(t, input, bytes.NewReader(bytes.Repeat([]byte{0x61}, 2*museNonceBytes)), func(
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

func TestMuseHookAlwaysExitsZero(t *testing.T) {
	valid := museHookJSON(t, museHookInterrupt, nil)
	t.Run("send error", func(t *testing.T) {
		executeMuseHook(t, valid, bytes.NewReader(bytes.Repeat([]byte{0x71}, museNonceBytes)), func(
			context.Context,
			string,
			nativeevents.Hello,
			nativeevents.Event,
		) error {
			return errors.New("send error")
		})
	})
	t.Run("send panic", func(t *testing.T) {
		executeMuseHook(t, valid, bytes.NewReader(bytes.Repeat([]byte{0x72}, museNonceBytes)), func(
			context.Context,
			string,
			nativeevents.Hello,
			nativeevents.Event,
		) error {
			panic("send panic")
		})
	})
	t.Run("nonce error", func(t *testing.T) {
		executeMuseHook(t, valid, museErrorReader{}, func(
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
		executeMuseHook(t, valid, bytes.NewReader(nil), func(
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

type museErrorReader struct{}

func (museErrorReader) Read([]byte) (int, error) { return 0, errors.New("read error") }

func findMuseHook(root *cobra.Command) *cobra.Command {
	command, _, err := root.Find([]string{museHookGroup, "muse"})
	if err != nil {
		return nil
	}
	return command
}

func TestAdapterTreeContainsMuseHook(t *testing.T) {
	require.NotNil(t, findMuseHook(NewCmd(nil)))
}
