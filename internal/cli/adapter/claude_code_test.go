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

var claudeHookTime = time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC)

const (
	claudeTestSessionID         = "session-1"
	claudeTestPromptID          = "prompt-1"
	claudeTestTitle             = "CANARY-title"
	claudeTestToolName          = "Bash"
	claudeTestToolUseID         = "tool-1"
	claudeTestCommand           = "CANARY-command"
	claudeTestAgentID           = "agent-1"
	claudeSourceField           = "source"
	claudeCommandField          = "command"
	claudeToolNameField         = "tool_name"
	claudeToolInputField        = "tool_input"
	claudeToolUseIDField        = "tool_use_id"
	claudeErrorField            = "error"
	claudeLastAssistantMsgField = "last_assistant_message"
)

func claudeHookJSON(t *testing.T, eventName string, fields map[string]any) string {
	t.Helper()
	input := map[string]any{
		"hook_event_name": eventName,
		"session_id":      claudeTestSessionID,
		"prompt_id":       claudeTestPromptID,
		"transcript_path": "/tmp/CANARY-transcript.jsonl",
		"cwd":             "/tmp/CANARY-worktree",
		"scratchpad_dir":  "/tmp/CANARY-scratchpad",
		"permission_mode": "default",
		"agent_type":      "CANARY-agent-type",
		"effort":          map[string]any{"level": "high"},
	}
	for key, value := range fields {
		input[key] = value
	}
	raw, err := json.Marshal(input)
	require.NoError(t, err)
	return string(raw)
}

func executeClaudeCodeHook(
	t *testing.T,
	input string,
	nonces io.Reader,
	send emitFunc,
	args ...string,
) {
	t.Helper()
	cmd := newClaudeCodeHookCmd(send, func() time.Time { return claudeHookTime }, nonces)
	cmd.SetIn(strings.NewReader(input))
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	commandArgs := make([]string, len(args))
	copy(commandArgs, args)
	cmd.SetArgs(commandArgs)
	require.NoError(t, cmd.Execute())
}

func TestClaudeCodeHookTableIsContentFree(t *testing.T) {
	tests := []struct {
		name              string
		eventName         string
		fields            map[string]any
		wantType          protocol.EventType
		wantResult        protocol.NativeEventResult
		wantTool          protocol.OpaqueID
		wantRequestPrefix string
		wantNoEvent       bool
	}{
		{
			name: "session start", eventName: "SessionStart",
			fields: map[string]any{
				claudeSourceField: "startup", "model": "CANARY-model", "session_title": claudeTestTitle,
				"seconds_since_last_response": 12, "context_tokens": 34,
				"prompt_cache_likely_expired": true, "estimated_cache_write_usd": 0.01,
			},
			wantType: protocol.EventTypeSessionStarted,
		},
		{
			name: "user prompt", eventName: "UserPromptSubmit",
			fields:   map[string]any{"prompt": "CANARY-prompt", claudeSourceField: "user", "session_title": claudeTestTitle},
			wantType: protocol.EventTypePromptSubmitted,
		},
		{
			name: "tool start", eventName: "PreToolUse",
			fields: map[string]any{
				claudeToolNameField: claudeTestToolName, claudeToolUseIDField: claudeTestToolUseID,
				claudeToolInputField: map[string]any{claudeCommandField: claudeTestCommand},
				"mcp_server":         map[string]any{"name": "CANARY-server"},
			},
			wantType: protocol.EventTypeToolStarted, wantTool: claudeTestToolUseID,
		},
		{
			name: "approval", eventName: "PermissionRequest",
			fields: map[string]any{
				claudeToolNameField:      claudeTestToolName,
				claudeToolInputField:     map[string]any{claudeCommandField: claudeTestCommand},
				"permission_suggestions": []any{map[string]any{"rule": "CANARY-rule"}},
			},
			wantType: protocol.EventTypeApprovalRequested, wantRequestPrefix: "perm-",
		},
		{
			name: "tool complete", eventName: "PostToolUse",
			fields: map[string]any{
				claudeToolNameField: claudeTestToolName, claudeToolUseIDField: claudeTestToolUseID,
				claudeToolInputField: map[string]any{claudeCommandField: claudeTestCommand},
				"tool_response":      map[string]any{"output": "CANARY-output"}, "duration_ms": 12,
			},
			wantType: protocol.EventTypeToolCompleted, wantResult: protocol.NativeEventResultOK,
			wantTool: claudeTestToolUseID,
		},
		{
			name: "tool failure", eventName: "PostToolUseFailure",
			fields: map[string]any{
				claudeToolNameField: claudeTestToolName, claudeToolUseIDField: claudeTestToolUseID,
				claudeToolInputField: map[string]any{claudeCommandField: claudeTestCommand},
				claudeErrorField:     "CANARY-error", "is_interrupt": false, "duration_ms": 12,
			},
			wantType: protocol.EventTypeToolFailed, wantResult: protocol.NativeEventResultFailed,
			wantTool: claudeTestToolUseID,
		},
		{
			name: "notification", eventName: "Notification",
			fields: map[string]any{
				"message": "CANARY-message", "title": claudeTestTitle, "notification_type": "permission_prompt",
			},
			wantNoEvent: true,
		},
		{
			name: "subagent", eventName: "SubagentStart",
			fields:   map[string]any{"agent_id": claudeTestAgentID, "agent_type": "CANARY-agent"},
			wantType: protocol.EventTypeSubagentStarted,
		},
		{
			name: "stop", eventName: "Stop",
			fields: map[string]any{
				"stop_hook_active": false, claudeLastAssistantMsgField: "CANARY-answer",
				"background_tasks": []any{map[string]any{"description": "CANARY-task"}},
				"session_crons":    []any{map[string]any{"prompt": "CANARY-cron"}},
			},
			wantType: protocol.EventTypeTurnCompleted, wantResult: protocol.NativeEventResultOK,
		},
		{
			name: "stop failure", eventName: "StopFailure",
			fields: map[string]any{
				claudeErrorField: "rate_limit", "error_details": "CANARY-details",
				claudeLastAssistantMsgField: "CANARY-error-message",
			},
			wantType: protocol.EventTypeTurnCompleted, wantResult: protocol.NativeEventResultFailed,
		},
		{
			name: "compact", eventName: "PreCompact",
			fields:      map[string]any{"trigger": "manual", "custom_instructions": "CANARY-instructions"},
			wantNoEvent: true,
		},
		{
			name: "session end", eventName: "SessionEnd",
			fields:   map[string]any{"reason": "other"},
			wantType: protocol.EventTypeSessionEnded,
		},
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
			executeClaudeCodeHook(
				t, claudeHookJSON(t, test.eventName, test.fields),
				bytes.NewReader(bytes.Repeat([]byte{0x11}, permissionNonceBytes)), send,
			)
			if test.wantNoEvent {
				require.Zero(t, calls)
				return
			}
			require.Equal(t, 1, calls)
			require.Equal(t, protocol.HarnessClaudeCode, hello.Harness)
			require.Equal(t, claudeCodeHarnessVersion, hello.HarnessVersion)
			require.Equal(t, nativeevents.EmitterTypeHook, hello.EmitterType)
			require.Equal(t, claudeCodeEmitterVersion, hello.EmitterVersion)
			require.Equal(t, nativeevents.SchemaVersion, hello.SchemaVersion)
			require.Equal(t, test.wantType, event.Type)
			require.Equal(t, test.wantResult, event.Result)
			require.Equal(t, test.wantTool, event.ToolRef)
			if test.wantRequestPrefix != "" {
				require.True(t, strings.HasPrefix(string(event.RequestRef), test.wantRequestPrefix))
			}
			require.Equal(t, protocol.OpaqueID(claudeTestSessionID), event.SessionRef)
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

func TestClaudeCodeQuestionUsesToolUseID(t *testing.T) {
	for _, test := range []struct {
		name       string
		eventName  string
		wantType   protocol.EventType
		wantResult protocol.NativeEventResult
	}{
		{name: "requested", eventName: "PreToolUse", wantType: protocol.EventTypeQuestionRequested},
		{name: "resolved", eventName: "PostToolUse", wantType: protocol.EventTypeQuestionResolved, wantResult: protocol.NativeEventResultOK},
		{name: "failed", eventName: "PostToolUseFailure", wantType: protocol.EventTypeQuestionResolved, wantResult: protocol.NativeEventResultFailed},
	} {
		t.Run(test.name, func(t *testing.T) {
			var got nativeevents.Event
			send := func(_ context.Context, _ string, _ nativeevents.Hello, event nativeevents.Event) error {
				got = event
				return nil
			}
			executeClaudeCodeHook(t, claudeHookJSON(t, test.eventName, map[string]any{
				claudeToolNameField: claudeQuestionTool, claudeToolUseIDField: "question-1",
				claudeToolInputField: map[string]any{"questions": []any{"CANARY-question"}},
				"tool_response":      map[string]any{"answers": "CANARY-answer"},
				claudeErrorField:     "CANARY-error",
			}), bytes.NewReader(nil), send)
			require.Equal(t, test.wantType, got.Type)
			require.Equal(t, protocol.OpaqueID("question-1"), got.RequestRef)
			require.Empty(t, got.ToolRef)
			require.Equal(t, test.wantResult, got.Result)
		})
	}
}

func TestClaudeCodePermissionRequestsHaveInvocationRefs(t *testing.T) {
	nonceBytes := append(
		bytes.Repeat([]byte{0x11}, permissionNonceBytes),
		bytes.Repeat([]byte{0x22}, permissionNonceBytes)...,
	)
	nonces := bytes.NewReader(nonceBytes)
	var got []nativeevents.Event
	send := func(_ context.Context, _ string, _ nativeevents.Hello, event nativeevents.Event) error {
		got = append(got, event)
		return nil
	}
	input := claudeHookJSON(t, "PermissionRequest", map[string]any{
		claudeToolNameField:  claudeTestToolName,
		claudeToolInputField: map[string]any{claudeCommandField: "same command"},
	})
	executeClaudeCodeHook(t, input, nonces, send)
	executeClaudeCodeHook(t, input, nonces, send)

	require.Len(t, got, 2)
	require.Equal(t, protocol.OpaqueID(claudeTestPromptID), got[0].TurnRef)
	require.Equal(t, protocol.OpaqueID(claudeTestPromptID), got[1].TurnRef)
	require.NotEqual(t, got[0].RequestRef, got[1].RequestRef)
	require.NotEqual(t, got[0].EmitterEventID, got[1].EmitterEventID)
	require.Equal(t,
		claudeEmitterEventID(
			claudeTestSessionID,
			"PermissionRequest",
			strings.TrimPrefix(string(got[0].RequestRef), "perm-"),
		),
		got[0].EmitterEventID,
	)
}

func TestClaudeCodePermissionWithoutClosureExpiresToUnknown(t *testing.T) {
	event, ok := claudeCodeEvent(claudeCodeHookInput{
		HookEventName: "PermissionRequest", SessionID: claudeTestSessionID, PromptID: claudeTestPromptID,
	}, claudeHookTime.UnixMilli(), bytes.NewReader(bytes.Repeat([]byte{0x33}, permissionNonceBytes)))
	require.True(t, ok)
	native := analyst.NativeEvent{
		EventID: "01ARZ3NDEKTSV4RRFFQ69G5FAV", Type: event.Type,
		At: claudeHookTime, Sequence: 1, TurnRef: event.TurnRef, RequestRef: event.RequestRef,
	}

	during := analyst.ReduceNative(
		[]analyst.NativeEvent{native}, claudeHookTime.Add(analyst.NativeOperationTTL-time.Nanosecond),
	)
	require.True(t, during.Blocks)
	require.Equal(t, protocol.AgentPhaseWaitingApproval, during.Phase)
	after := analyst.ReduceNative(
		[]analyst.NativeEvent{native}, claudeHookTime.Add(analyst.NativeOperationTTL),
	)
	require.False(t, after.Blocks)
	require.True(t, after.Stale)
	require.Equal(t, protocol.AgentPhaseUnknown, after.Phase)
}

func TestClaudeCodeSubagentStartNeverVetoes(t *testing.T) {
	event, ok := claudeCodeEvent(claudeCodeHookInput{
		HookEventName: "SubagentStart", SessionID: claudeTestSessionID, PromptID: claudeTestPromptID,
		AgentID: claudeTestAgentID,
	}, claudeHookTime.UnixMilli(), bytes.NewReader(nil))
	require.True(t, ok)
	state := analyst.ReduceNative([]analyst.NativeEvent{{
		EventID: "01ARZ3NDEKTSV4RRFFQ69G5FAW", Type: event.Type,
		At: claudeHookTime, Sequence: 1, TurnRef: event.TurnRef,
	}}, claudeHookTime)
	require.False(t, state.Blocks)
}

func TestClaudeCodeIgnoresNonInitialSessionStarts(t *testing.T) {
	for _, source := range []string{"clear", "compact"} {
		t.Run(source, func(t *testing.T) {
			calls := 0
			executeClaudeCodeHook(t, claudeHookJSON(t, "SessionStart", map[string]any{
				claudeSourceField: source,
			}), bytes.NewReader(nil), func(
				context.Context,
				string,
				nativeevents.Hello,
				nativeevents.Event,
			) error {
				calls++
				return nil
			})
			require.Zero(t, calls)
		})
	}
}

func TestClaudeCodeMalformedOrOversizedInputAlwaysSucceeds(t *testing.T) {
	oversized := `{"hook_event_name":"UserPromptSubmit","session_id":"session-1","prompt":"` +
		strings.Repeat("x", maxClaudeCodeHookBytes) + `"}`
	for name, input := range map[string]string{
		"empty": "", "malformed": "{", "array": "[]", "trailing": `{} {}`,
		"missing ids": `{"hook_event_name":"Stop"}`, "oversized": oversized,
	} {
		t.Run(name, func(t *testing.T) {
			calls := 0
			executeClaudeCodeHook(t, input, bytes.NewReader(nil), func(
				context.Context,
				string,
				nativeevents.Hello,
				nativeevents.Event,
			) error {
				calls++
				return nil
			})
			require.Zero(t, calls)
		})
	}
}

func TestClaudeCodeHookAlwaysExitsZero(t *testing.T) {
	valid := claudeHookJSON(t, "Stop", map[string]any{claudeLastAssistantMsgField: "CANARY"})
	t.Run("send error", func(t *testing.T) {
		executeClaudeCodeHook(t, valid, bytes.NewReader(nil), func(
			context.Context,
			string,
			nativeevents.Hello,
			nativeevents.Event,
		) error {
			return errors.New("send failed")
		})
	})
	t.Run("send panic", func(t *testing.T) {
		executeClaudeCodeHook(t, valid, bytes.NewReader(nil), func(
			context.Context,
			string,
			nativeevents.Hello,
			nativeevents.Event,
		) error {
			panic("send failed")
		})
	})
	t.Run("nonce error", func(t *testing.T) {
		executeClaudeCodeHook(t, claudeHookJSON(t, "PermissionRequest", nil), errorReader{}, func(
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
		executeClaudeCodeHook(t, valid, bytes.NewReader(nil), func(
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

type errorReader struct{}

func (errorReader) Read([]byte) (int, error) { return 0, errors.New("read failed") }

func findClaudeCodeHook(root *cobra.Command) *cobra.Command {
	command, _, err := root.Find([]string{"hook", "claude-code"})
	if err != nil {
		return nil
	}
	return command
}

func TestAdapterTreeContainsClaudeCodeHook(t *testing.T) {
	require.NotNil(t, findClaudeCodeHook(NewCmd(nil)))
}
