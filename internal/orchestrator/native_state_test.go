package orchestrator

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/analyst"
	"github.com/smellak/gifford/internal/protocol"
)

func durableNativeStateEvent(
	id protocol.ULID,
	sequence int64,
	typeID protocol.EventType,
	at time.Time,
) protocol.EventEnvelope {
	return protocol.EventEnvelope{
		EventID: id, SchemaVersion: protocol.SchemaVersion, NodeID: executorTestNode,
		SessionID: executorTestSession, AgentID: executorTestAgent,
		Harness: protocol.HarnessCodex, HarnessVersion: executorTestHarnessVersion,
		AdapterType:   protocol.AdapterTypeGenericPTY,
		SourceQuality: protocol.SourceQualityNativeStructured,
		EventType:     typeID, Stream: protocol.StreamNativeEvent,
		Origin: protocol.OriginHarness, WallTime: at, Sequence: sequence,
		Payload:     protocol.Payload(`{"emitter_event_id":"emitter-1"}`),
		Sensitivity: protocol.SensitivityNormal,
	}
}

func durableCapabilityStateEvent(
	t *testing.T,
	id protocol.ULID,
	sequence int64,
	at time.Time,
	before, after protocol.Capabilities,
) protocol.EventEnvelope {
	t.Helper()
	payload, err := json.Marshal(nativeCapabilityPayload{
		EventType: protocol.EventTypeAdapterCapabilitiesChanged,
		Before:    before,
		After:     after,
	})
	require.NoError(t, err)
	return protocol.EventEnvelope{
		EventID: id, SchemaVersion: protocol.SchemaVersion, NodeID: executorTestNode,
		SessionID: executorTestSession, AgentID: executorTestAgent,
		Harness: protocol.HarnessCodex, HarnessVersion: executorTestHarnessVersion,
		AdapterType:   protocol.AdapterTypeGenericPTY,
		SourceQuality: protocol.SourceQualityProcessMetrics,
		EventType:     protocol.EventTypeAdapterCapabilitiesChanged,
		Origin:        protocol.OriginSystem, WallTime: at, Sequence: sequence,
		Payload: protocol.Payload(payload), Sensitivity: protocol.SensitivityNormal,
	}
}

func TestNativeStateReferencesHydrateOnlyDurableEvents(t *testing.T) {
	t.Parallel()
	fixture := newExecutorFixture(t)
	at := executorTestTime.Add(time.Second)
	event := durableNativeStateEvent(
		"01ARZ3NDEKTSV4RRFFQ69G1011", 1, protocol.EventTypeApprovalRequested, at,
	)
	event.TurnID, event.CorrelationID = "turn-1", "request-1"
	require.NoError(t, protocol.ValidateNativeDurableEvent(event))
	require.NoError(t, fixture.store.AppendEvents(t.Context(), []protocol.EventEnvelope{event}))
	report := stateObservation("observation-native-1", 2, at.Add(time.Second))
	report.NativeEvents = []protocol.StateObservationEvent{{
		EventID: event.EventID, Type: event.EventType, At: event.WallTime,
	}}

	got, err := loadNativeStateEvents(t.Context(), fixture.store.DB(), report)
	require.NoError(t, err)
	require.Equal(t, []analyst.NativeEvent{{
		EventID: event.EventID, Type: event.EventType, At: event.WallTime,
		Sequence: event.Sequence, TurnRef: event.TurnID, RequestRef: event.CorrelationID,
	}}, got)

	report.NativeEvents[0].EventID = "01ARZ3NDEKTSV4RRFFQ69G1012"
	_, err = loadNativeStateEvents(t.Context(), fixture.store.DB(), report)
	require.ErrorIs(t, err, ErrObservationUnavailable,
		"an observation cannot introduce an event that Core did not store")
}

func TestNativeStateReferenceMustMatchDurableIdentity(t *testing.T) {
	t.Parallel()
	fixture := newExecutorFixture(t)
	at := executorTestTime.Add(time.Second)
	event := durableNativeStateEvent(
		"01ARZ3NDEKTSV4RRFFQ69G1021", 1, protocol.EventTypeToolStarted, at,
	)
	event.ToolCallID = "tool-1"
	require.NoError(t, fixture.store.AppendEvents(t.Context(), []protocol.EventEnvelope{event}))
	base := stateObservation("observation-native-identity", 2, at.Add(time.Second))
	base.NativeEvents = []protocol.StateObservationEvent{{
		EventID: event.EventID, Type: event.EventType, At: event.WallTime,
	}}

	for name, mutate := range map[string]func(*protocol.StateObservationReport){
		"type": func(report *protocol.StateObservationReport) {
			report.NativeEvents[0].Type = protocol.EventTypeToolCompleted
		},
		"time": func(report *protocol.StateObservationReport) {
			report.NativeEvents[0].At = report.NativeEvents[0].At.Add(time.Nanosecond)
		},
		"session": func(report *protocol.StateObservationReport) {
			report.SessionID = "session-other"
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			report := cloneObservation(base)
			mutate(&report)
			_, err := loadNativeStateEvents(t.Context(), fixture.store.DB(), report)
			require.ErrorIs(t, err, ErrObservationUnavailable)
		})
	}
}

func TestNativeStatePreservesLifecycleAcrossUnrelatedFullDegradation(t *testing.T) {
	t.Parallel()
	fixture := newExecutorFixture(t)
	at := executorTestTime.Add(time.Second)
	after := protocol.Capabilities{PTYCapture: true, MCP: true}
	before := after
	before.NativeCancel = true
	event := durableCapabilityStateEvent(
		t, "01ARZ3NDEKTSV4RRFFQ69G1031", 2, at, before, after,
	)
	require.NoError(t, fixture.store.AppendEvents(t.Context(), []protocol.EventEnvelope{event}))
	report := stateObservation("observation-native-neutral-capability", 2, at.Add(time.Second))
	report.NativeEvents = []protocol.StateObservationEvent{{
		EventID: event.EventID, Type: event.EventType, At: event.WallTime,
	}}

	got, err := loadNativeStateEvents(t.Context(), fixture.store.DB(), report)
	require.NoError(t, err)
	require.Equal(t, analyst.NativeUnchanged, got[0].Availability)

	illegalAfter := before
	illegalAfter.NativeCancel = false
	illegalAfter.MCP = false
	illegal := durableCapabilityStateEvent(
		t, "01ARZ3NDEKTSV4RRFFQ69G1032", 3, at, before, illegalAfter,
	)
	require.NoError(t, fixture.store.AppendEvents(t.Context(), []protocol.EventEnvelope{illegal}))
	report.NativeEvents[0] = protocol.StateObservationEvent{
		EventID: illegal.EventID, Type: illegal.EventType, At: illegal.WallTime,
	}
	_, err = loadNativeStateEvents(t.Context(), fixture.store.DB(), report)
	require.ErrorIs(t, err, ErrObservationUnavailable)
}
