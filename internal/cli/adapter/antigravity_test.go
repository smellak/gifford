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

var antigravityHookTime = time.Date(2026, 11, 1, 15, 0, 0, 0, time.UTC)

const (
	antigravityTestConversationID = "conversation-1"
	antigravityCanary             = "ANTIGRAVITY-CANARY-content"
	antigravityHookGroup          = "hook"
	antigravityInvocationField    = "invocationNum"
	antigravityStepField          = "stepIdx"
	antigravityExecutionField     = "executionNum"
	antigravityFullyIdleField     = "fullyIdle"
	antigravityConversationJSON   = `{"conversationId":"conversation-1"}`
)

func antigravityHookJSON(t *testing.T, fields map[string]any) string {
	t.Helper()
	input := map[string]any{
		"conversationId":        antigravityTestConversationID,
		"workspacePaths":        []string{"/tmp/ANTIGRAVITY-CANARY-worktree"},
		"transcriptPath":        "/tmp/ANTIGRAVITY-CANARY-transcript.jsonl",
		"artifactDirectoryPath": "/tmp/ANTIGRAVITY-CANARY-artifacts",
		"modelName":             "ANTIGRAVITY-CANARY-model",
		"lastUserInput":         antigravityCanary,
		"toolCall": map[string]any{
			strings.Join([]string{"na", "me"}, ""): "ANTIGRAVITY-CANARY-tool",
			"args":                                 map[string]any{strings.Join([]string{"com", "mand"}, ""): antigravityCanary},
		},
		"toolResult":        map[string]any{strings.Join([]string{"out", "put"}, ""): antigravityCanary},
		"terminationReason": antigravityCanary,
	}
	for key, value := range fields {
		input[key] = value
	}
	raw, err := json.Marshal(input)
	require.NoError(t, err)
	return string(raw)
}

func executeAntigravityHook(
	t *testing.T,
	input string,
	nonces io.Reader,
	send emitFunc,
	args ...string,
) (string, string) {
	t.Helper()
	cmd := newAntigravityHookCmd(send, func() time.Time { return antigravityHookTime }, nonces)
	cmd.SetIn(strings.NewReader(input))
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs(append([]string(nil), args...))
	require.NoError(t, cmd.Execute())
	return stdout.String(), stderr.String()
}

func TestAntigravityHookTableIsContentFreeAndNeutral(t *testing.T) {
	tests := []struct {
		name        string
		hook        string
		fields      map[string]any
		wantType    protocol.EventType
		wantResult  protocol.NativeEventResult
		wantTurn    bool
		wantTool    bool
		wantNoEvent bool
	}{
		{name: "session start", hook: antigravityHookSessionStart, wantType: protocol.EventTypeSessionStarted},
		{name: "invocation start zero", hook: antigravityHookPreInvocation, fields: map[string]any{antigravityInvocationField: 0}, wantType: protocol.EventTypeTurnStarted},
		{name: "invocation return", hook: antigravityHookPostInvocation, fields: map[string]any{antigravityInvocationField: 1}, wantType: protocol.EventTypeTurnStreaming},
		{name: "tool start zero", hook: antigravityHookPreToolUse, fields: map[string]any{antigravityStepField: 0, antigravityInvocationField: 7}, wantType: protocol.EventTypeToolStarted, wantTool: true},
		{name: "tool complete absent error", hook: antigravityHookPostToolUse, fields: map[string]any{antigravityStepField: 1}, wantType: protocol.EventTypeToolCompleted, wantResult: protocol.NativeEventResultOK, wantTool: true},
		{name: "tool complete empty error", hook: antigravityHookPostToolUse, fields: map[string]any{antigravityStepField: 2, "error": ""}, wantType: protocol.EventTypeToolCompleted, wantResult: protocol.NativeEventResultOK, wantTool: true},
		{name: "failed tool", hook: antigravityHookPostToolUse, fields: map[string]any{antigravityStepField: 3, "error": antigravityCanary}, wantType: protocol.EventTypeToolFailed, wantResult: protocol.NativeEventResultFailed, wantTool: true},
		{name: "background work zero", hook: antigravityHookStop, fields: map[string]any{antigravityExecutionField: 0, antigravityFullyIdleField: false}, wantType: protocol.EventTypeBackgroundTaskStarted, wantTurn: true},
		{name: "idle stop", hook: antigravityHookStop, fields: map[string]any{antigravityExecutionField: 1, antigravityFullyIdleField: true}, wantNoEvent: true},
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
			stdout, stderr := executeAntigravityHook(
				t, antigravityHookJSON(t, test.fields),
				bytes.NewReader(bytes.Repeat([]byte{0x11}, antigravityNonceBytes)), send, test.hook,
			)
			require.Equal(t, antigravityNeutralOutput(test.hook), stdout)
			require.Empty(t, stderr)
			if test.wantNoEvent {
				require.Zero(t, calls)
				return
			}
			require.Equal(t, 1, calls)
			require.NoError(t, hello.Validate())
			require.Equal(t, protocol.HarnessAntigravity, hello.Harness)
			require.Equal(t, antigravityHarnessVersion, hello.HarnessVersion)
			require.Equal(t, nativeevents.EmitterTypeHook, hello.EmitterType)
			require.Equal(t, antigravityEmitterVersion, hello.EmitterVersion)
			require.Equal(t, nativeevents.SchemaVersion, hello.SchemaVersion)
			require.Equal(t, test.wantType, event.Type)
			require.Equal(t, test.wantResult, event.Result)
			require.Equal(t, protocol.OpaqueID(antigravityTestConversationID), event.SessionRef)
			require.Equal(t, test.wantTurn, event.TurnRef != "")
			require.Equal(t, test.wantTool, event.ToolRef != "")
			if test.wantTool {
				require.Empty(t, event.TurnRef)
			}
			require.Empty(t, event.ToolKind)
			require.True(t, strings.HasPrefix(string(event.EmitterEventID), antigravityEventIDPrefix))
			require.NoError(t, event.Validate())
			wire, err := json.Marshal(struct {
				Hello nativeevents.Hello `json:"hello"`
				Event nativeevents.Event `json:"event"`
			}{Hello: hello, Event: event})
			require.NoError(t, err)
			require.NotContains(t, string(wire), "ANTIGRAVITY-CANARY")
			require.NotContains(t, string(wire), "connection_mode")
		})
	}
}

func TestAntigravityRequiredFieldsArePresentAndTyped(t *testing.T) {
	tests := []struct {
		name      string
		hook      string
		input     string
		wantEvent bool
	}{
		{name: "missing conversation", hook: antigravityHookSessionStart, input: `{}`},
		{name: "null conversation", hook: antigravityHookSessionStart, input: `{"conversationId":null}`},
		{name: "wrong conversation type", hook: antigravityHookSessionStart, input: `{"conversationId":7}`},
		{name: "missing invocation", hook: antigravityHookPreInvocation, input: antigravityConversationJSON},
		{name: "null invocation", hook: antigravityHookPreInvocation, input: `{"conversationId":"conversation-1","invocationNum":null}`},
		{name: "string invocation", hook: antigravityHookPreInvocation, input: `{"conversationId":"conversation-1","invocationNum":"0"}`},
		{name: "fractional invocation", hook: antigravityHookPreInvocation, input: `{"conversationId":"conversation-1","invocationNum":0.5}`},
		{name: "negative invocation", hook: antigravityHookPreInvocation, input: `{"conversationId":"conversation-1","invocationNum":-1}`},
		{name: "zero invocation", hook: antigravityHookPreInvocation, input: `{"conversationId":"conversation-1","invocationNum":0}`, wantEvent: true},
		{name: "missing step", hook: antigravityHookPreToolUse, input: antigravityConversationJSON},
		{name: "null step", hook: antigravityHookPreToolUse, input: `{"conversationId":"conversation-1","stepIdx":null}`},
		{name: "string step", hook: antigravityHookPreToolUse, input: `{"conversationId":"conversation-1","stepIdx":"0"}`},
		{name: "fractional step", hook: antigravityHookPreToolUse, input: `{"conversationId":"conversation-1","stepIdx":0.5}`},
		{name: "negative step", hook: antigravityHookPreToolUse, input: `{"conversationId":"conversation-1","stepIdx":-1}`},
		{name: "zero step", hook: antigravityHookPreToolUse, input: `{"conversationId":"conversation-1","stepIdx":0}`, wantEvent: true},
		{name: "missing fully idle", hook: antigravityHookStop, input: `{"conversationId":"conversation-1","executionNum":0}`},
		{name: "null fully idle", hook: antigravityHookStop, input: `{"conversationId":"conversation-1","executionNum":0,"fullyIdle":null}`},
		{name: "string fully idle", hook: antigravityHookStop, input: `{"conversationId":"conversation-1","executionNum":0,"fullyIdle":"false"}`},
		{name: "true fully idle", hook: antigravityHookStop, input: `{"conversationId":"conversation-1","executionNum":0,"fullyIdle":true}`},
		{name: "missing execution", hook: antigravityHookStop, input: `{"conversationId":"conversation-1","fullyIdle":false}`},
		{name: "null execution", hook: antigravityHookStop, input: `{"conversationId":"conversation-1","executionNum":null,"fullyIdle":false}`},
		{name: "string execution", hook: antigravityHookStop, input: `{"conversationId":"conversation-1","executionNum":"0","fullyIdle":false}`},
		{name: "negative execution", hook: antigravityHookStop, input: `{"conversationId":"conversation-1","executionNum":-1,"fullyIdle":false}`},
		{name: "zero execution", hook: antigravityHookStop, input: `{"conversationId":"conversation-1","executionNum":0,"fullyIdle":false}`, wantEvent: true},
		{name: "absent error", hook: antigravityHookPostToolUse, input: `{"conversationId":"conversation-1","stepIdx":0}`, wantEvent: true},
		{name: "null error", hook: antigravityHookPostToolUse, input: `{"conversationId":"conversation-1","stepIdx":0,"error":null}`},
		{name: "wrong error type", hook: antigravityHookPostToolUse, input: `{"conversationId":"conversation-1","stepIdx":0,"error":{}}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			stdout, stderr := executeAntigravityHook(
				t, test.input, bytes.NewReader(bytes.Repeat([]byte{0x21}, antigravityNonceBytes)),
				func(context.Context, string, nativeevents.Hello, nativeevents.Event) error {
					calls++
					return nil
				}, test.hook,
			)
			require.Equal(t, antigravityNeutralOutput(test.hook), stdout)
			require.Empty(t, stderr)
			if test.wantEvent {
				require.Equal(t, 1, calls)
			} else {
				require.Zero(t, calls)
			}
		})
	}
}

func TestAntigravityReferencesAreDeterministicAndDomainSeparated(t *testing.T) {
	zero := int64(0)
	longConversation := strings.Repeat("conversation/", 7)
	first, ok := antigravityEvent(antigravityHookPreToolUse, antigravityHookInput{
		ConversationID: longConversation,
		StepIdx:        &zero,
		InvocationNum:  &zero,
	}, antigravityHookTime.UnixMilli(), bytes.NewReader(bytes.Repeat([]byte{0x31}, antigravityNonceBytes)))
	require.True(t, ok)
	second, ok := antigravityEvent(antigravityHookPreToolUse, antigravityHookInput{
		ConversationID: longConversation,
		StepIdx:        &zero,
	}, antigravityHookTime.UnixMilli(), bytes.NewReader(bytes.Repeat([]byte{0x32}, antigravityNonceBytes)))
	require.True(t, ok)
	executionRef, ok := antigravityNumberRef("execution", longConversation, &zero)
	require.True(t, ok)

	require.Len(t, first.SessionRef, 64)
	require.Len(t, first.ToolRef, 64)
	require.Equal(t, first.SessionRef, second.SessionRef)
	require.Equal(t, first.ToolRef, second.ToolRef)
	require.NotEqual(t, first.EmitterEventID, second.EmitterEventID)
	require.NotEqual(t, first.SessionRef, first.ToolRef)
	require.NotEqual(t, executionRef, first.ToolRef)
	require.Empty(t, first.TurnRef)
	require.NoError(t, first.Validate())
}

func TestAntigravityResetInvocationCounterDoesNotAssertTurnIdentity(t *testing.T) {
	zero := int64(0)
	input := antigravityHookInput{
		ConversationID: antigravityTestConversationID,
		InvocationNum:  &zero,
	}
	first, ok := antigravityEvent(
		antigravityHookPreInvocation, input, antigravityHookTime.UnixMilli(),
		bytes.NewReader(bytes.Repeat([]byte{0x33}, antigravityNonceBytes)),
	)
	require.True(t, ok)
	second, ok := antigravityEvent(
		antigravityHookPreInvocation, input, antigravityHookTime.Add(time.Second).UnixMilli(),
		bytes.NewReader(bytes.Repeat([]byte{0x34}, antigravityNonceBytes)),
	)
	require.True(t, ok)

	require.Empty(t, first.TurnRef)
	require.Empty(t, second.TurnRef)
	require.NotEqual(t, first.EmitterEventID, second.EmitterEventID)
	require.NoError(t, first.Validate())
	require.NoError(t, second.Validate())
}

func TestAntigravityBackgroundWorkExpiresToUnknown(t *testing.T) {
	zero := int64(0)
	fullyIdle := false
	event, ok := antigravityEvent(antigravityHookStop, antigravityHookInput{
		ConversationID: antigravityTestConversationID,
		ExecutionNum:   &zero,
		FullyIdle:      &fullyIdle,
	}, antigravityHookTime.UnixMilli(), bytes.NewReader(bytes.Repeat([]byte{0x41}, antigravityNonceBytes)))
	require.True(t, ok)
	native := analyst.NativeEvent{
		EventID: "01ARZ3NDEKTSV4RRFFQ69G5FB0", Type: event.Type,
		At: antigravityHookTime, Sequence: 1, TurnRef: event.TurnRef,
	}

	during := analyst.ReduceNative(
		[]analyst.NativeEvent{native}, antigravityHookTime.Add(analyst.NativeOperationTTL-time.Nanosecond),
	)
	require.True(t, during.Blocks)
	require.Equal(t, protocol.AgentPhaseBackgroundWork, during.Phase)
	after := analyst.ReduceNative(
		[]analyst.NativeEvent{native}, antigravityHookTime.Add(analyst.NativeOperationTTL),
	)
	require.False(t, after.Blocks)
	require.True(t, after.Stale)
	require.Equal(t, protocol.AgentPhaseUnknown, after.Phase)
}

func TestAntigravityNeutralOutputSurvivesSendErrorAndPanic(t *testing.T) {
	input := antigravityHookJSON(t, map[string]any{
		antigravityInvocationField: 0,
		antigravityStepField:       0,
		antigravityExecutionField:  0,
		antigravityFullyIdleField:  false,
	})
	for _, hook := range []string{
		antigravityHookSessionStart,
		antigravityHookPreToolUse,
		antigravityHookPostToolUse,
		antigravityHookPreInvocation,
		antigravityHookPostInvocation,
		antigravityHookStop,
	} {
		t.Run(hook+" error", func(t *testing.T) {
			stdout, stderr := executeAntigravityHook(
				t, input, bytes.NewReader(bytes.Repeat([]byte{0x51}, antigravityNonceBytes)),
				func(context.Context, string, nativeevents.Hello, nativeevents.Event) error {
					return errors.New("send error")
				}, hook,
			)
			require.Equal(t, antigravityNeutralOutput(hook), stdout)
			require.Empty(t, stderr)
		})
		t.Run(hook+" panic", func(t *testing.T) {
			stdout, stderr := executeAntigravityHook(
				t, input, bytes.NewReader(bytes.Repeat([]byte{0x52}, antigravityNonceBytes)),
				func(context.Context, string, nativeevents.Hello, nativeevents.Event) error {
					panic("send panic")
				}, hook,
			)
			require.Equal(t, antigravityNeutralOutput(hook), stdout)
			require.Empty(t, stderr)
		})
	}
}

func TestAntigravityMalformedOrUnsupportedInputIsNeutral(t *testing.T) {
	oversized := `{"conversationId":"conversation-1","lastUserInput":"` +
		strings.Repeat("x", maxAntigravityHookBytes) + `"}`
	tests := []struct {
		name string
		args []string
		in   string
		want string
	}{
		{name: "blank", args: []string{antigravityHookPreToolUse}, want: antigravityNeutralOutput(antigravityHookPreToolUse)},
		{name: "broken JSON", args: []string{antigravityHookPreToolUse}, in: `{`, want: antigravityNeutralOutput(antigravityHookPreToolUse)},
		{name: "multiple values", args: []string{antigravityHookPreToolUse}, in: strings.Join([]string{"{}", "{}"}, " "), want: antigravityNeutralOutput(antigravityHookPreToolUse)},
		{name: "over limit", args: []string{antigravityHookPreToolUse}, in: oversized, want: antigravityNeutralOutput(antigravityHookPreToolUse)},
		{name: "nonce error", args: []string{antigravityHookSessionStart}, in: antigravityConversationJSON, want: antigravityNeutralOutput(antigravityHookSessionStart)},
		{name: "extra argument", args: []string{antigravityHookStop, "extra"}, in: `{}`, want: antigravityNeutralOutput(antigravityHookStop)},
		{name: "unknown hook", args: []string{"FutureHook"}, in: `{}`, want: "{}\n"},
		{name: "missing hook", in: `{}`, want: "{}\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			nonces := io.Reader(bytes.NewReader(bytes.Repeat([]byte{0x61}, antigravityNonceBytes)))
			if test.name == "nonce error" {
				nonces = antigravityErrorReader{}
			}
			stdout, stderr := executeAntigravityHook(
				t, test.in, nonces,
				func(context.Context, string, nativeevents.Hello, nativeevents.Event) error {
					calls++
					return nil
				}, test.args...,
			)
			require.Zero(t, calls)
			require.Equal(t, test.want, stdout)
			require.Empty(t, stderr)
		})
	}
}

type antigravityErrorReader struct{}

func (antigravityErrorReader) Read([]byte) (int, error) {
	return 0, errors.New("read error")
}

func findAntigravityHook(root *cobra.Command) *cobra.Command {
	command, _, err := root.Find([]string{antigravityHookGroup, "antigravity"})
	if err != nil {
		return nil
	}
	return command
}

func TestAdapterTreeContainsAntigravityHook(t *testing.T) {
	root := NewCmd(nil)
	require.NotNil(t, findAntigravityHook(root))
	root.SetIn(strings.NewReader(`{}`))
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	root.SetOut(&stdout)
	root.SetErr(&stderr)
	root.SetArgs([]string{antigravityHookGroup, "antigravity", antigravityHookPreToolUse})
	require.NoError(t, root.Execute())
	require.Equal(t, antigravityNeutralOutput(antigravityHookPreToolUse), stdout.String())
	require.Empty(t, stderr.String())
}
