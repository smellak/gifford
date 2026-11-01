package protocol

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func nativeDurableTestEvent() EventEnvelope {
	return EventEnvelope{
		EventID:          "01JB00000000000000000000N1",
		SchemaVersion:    SchemaVersion,
		NodeID:           testNode,
		SessionID:        "session-1",
		AgentID:          testAgentID,
		Harness:          HarnessCodex,
		HarnessVersion:   testVersion,
		AdapterType:      AdapterTypeGenericPTY,
		SourceQuality:    SourceQualityNativeStructured,
		EventType:        EventTypeTurnStarted,
		Stream:           StreamNativeEvent,
		Origin:           OriginHarness,
		WallTime:         time.Date(2026, 11, 1, 5, 0, 0, 0, time.UTC),
		Sequence:         1,
		Payload:          Payload(`{"emitter_event_id":"emitter-1"}`),
		Sensitivity:      SensitivityNormal,
		RedactionApplied: false,
	}
}

func TestNativeDurableEventProfileIsSeparateAndClosed(t *testing.T) {
	t.Parallel()
	event := nativeDurableTestEvent()
	require.NoError(t, ValidateNativeDurableEvent(event))
	require.Error(t, ValidateDurableEventEnvelope(event), "native events never widen the PTY profile")

	for name, patch := range map[string]func(*EventEnvelope){
		"unknown event": func(e *EventEnvelope) { e.EventType = "turn.future" },
		"screen source": func(e *EventEnvelope) { e.SourceQuality = SourceQualityPTYScreen },
		"wrong stream":  func(e *EventEnvelope) { e.Stream = StreamPTY },
		"free text":     func(e *EventEnvelope) { e.Payload = Payload(`{"emitter_event_id":"emitter-1","prompt":"secret"}`) },
		"long ref": func(e *EventEnvelope) {
			e.TurnID = OpaqueID("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abc")
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			candidate := event
			patch(&candidate)
			require.Error(t, ValidateNativeDurableEvent(candidate))
		})
	}
}

func TestNativeDurableRefsAndQuestionResolved(t *testing.T) {
	t.Parallel()

	tool := nativeDurableTestEvent()
	tool.EventType = EventTypeToolStarted
	require.Error(t, ValidateNativeDurableEvent(tool))
	tool.ToolCallID = "tool-1"
	tool.Payload = Payload(`{"emitter_event_id":"emitter-1","tool_kind":"shell"}`)
	require.NoError(t, ValidateNativeDurableEvent(tool))

	question := nativeDurableTestEvent()
	question.EventType = EventTypeQuestionResolved
	question.Payload = Payload(`{"emitter_event_id":"question-event","result":"ok"}`)
	require.Error(t, ValidateNativeDurableEvent(question), "request_ref is mandatory")
	question.CorrelationID = "request-1"
	require.NoError(t, ValidateNativeDurableEvent(question))
	question.Payload = Payload(`{"emitter_event_id":"question-event"}`)
	require.Error(t, ValidateNativeDurableEvent(question), "result is mandatory")
	question.Payload = Payload(`{"emitter_event_id":"question-event","result":"ok","answer":"free text"}`)
	require.Error(t, ValidateNativeDurableEvent(question), "answer text has no schema field")
}

func TestCanonicalNativeDurablePayload(t *testing.T) {
	t.Parallel()
	got, err := CanonicalNativeDurableEventPayload(Payload(
		`{ "tool_kind": "mcp", "emitter_event_id": "event-1", "result": "ok" }`,
	))
	require.NoError(t, err)
	require.JSONEq(t, `{"emitter_event_id":"event-1","result":"ok","tool_kind":"mcp"}`, string(got))
}
