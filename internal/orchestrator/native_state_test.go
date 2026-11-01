package orchestrator

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/analyst"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

const nativeStateWindow = 256

type countingNativeStateQueryer struct {
	db         *sql.DB
	rowQueries int
	rangeReads int
}

func (q *countingNativeStateQueryer) QueryRowContext(
	ctx context.Context,
	query string,
	args ...any,
) *sql.Row {
	q.rowQueries++
	return q.db.QueryRowContext(ctx, query, args...)
}

func (q *countingNativeStateQueryer) QueryContext(
	ctx context.Context,
	query string,
	args ...any,
) (*sql.Rows, error) {
	q.rangeReads++
	return q.db.QueryContext(ctx, query, args...)
}

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

func nativeStateReferences(events []protocol.EventEnvelope) []protocol.StateObservationEvent {
	start := len(events) - nativeStateWindow
	if start < 0 {
		start = 0
	}
	references := make([]protocol.StateObservationEvent, 0, len(events)-start)
	for _, event := range events[start:] {
		references = append(references, protocol.StateObservationEvent{
			EventID: event.EventID, Type: event.EventType, At: event.WallTime,
		})
	}
	return references
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
			producer := newTestStateProducer(t, fixture, nil, StateAnalysisProfile{})
			_, err = producer.loadStatePreparation(t.Context(), fixture.store.DB(), report)
			require.ErrorIs(t, err, ErrObservationUnavailable,
				"the incremental production path authenticates the complete suffix")
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

func TestNativeStatePrefixPreservesGapAndBindingState(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name            string
		gapSequence     int
		prefixAvailable bool
		rebound         bool
		wantPhase       protocol.AgentPhase
	}{
		{
			name:        "real gap before cut remains unknown",
			gapSequence: 2, wantPhase: protocol.AgentPhaseUnknown,
		},
		{
			name:        "real gap inside window remains unknown",
			gapSequence: 200, wantPhase: protocol.AgentPhaseUnknown,
		},
		{name: "degradation before cut remains unknown", wantPhase: protocol.AgentPhaseUnknown},
		{
			name:            "binding before cut preserves a fresh operation",
			prefixAvailable: true, wantPhase: protocol.AgentPhaseRunningTool,
		},
		{
			name:    "new binding after degradation restores fresh operation",
			rebound: true, wantPhase: protocol.AgentPhaseRunningTool,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			fixture := newExecutorFixture(t)
			const total = nativeStateWindow + 56
			events := make([]protocol.EventEnvelope, 0, total)
			generic := protocol.Capabilities{PTYCapture: true, MCP: true}
			native := generic
			native.StructuredEvents = true
			native.ToolEvents = true
			native.ApprovalEvents = true
			for sequence := 1; sequence <= total; sequence++ {
				if sequence == test.gapSequence {
					continue
				}
				id := protocol.ULID(fmt.Sprintf(
					"01ARZ3NDEKTSV4RRFFQ69G%04d", sequence,
				))
				at := executorTestTime.Add(time.Duration(sequence) * time.Millisecond)
				var event protocol.EventEnvelope
				switch {
				case test.prefixAvailable && sequence == 2:
					event = durableCapabilityStateEvent(t, id, int64(sequence), at, generic, native)
				case test.gapSequence == 0 && sequence == 2:
					event = durableCapabilityStateEvent(t, id, int64(sequence), at, native, generic)
				case test.rebound && sequence == 200:
					event = durableCapabilityStateEvent(t, id, int64(sequence), at, generic, native)
				case sequence == total:
					event = durableNativeStateEvent(
						id, int64(sequence), protocol.EventTypeToolStarted, at,
					)
					event.ToolCallID = "tool-after-prefix"
				default:
					event = durableNativeStateEvent(
						id, int64(sequence), protocol.EventTypeNotificationReceived, at,
					)
				}
				require.NoError(t, event.Validate())
				events = append(events, event)
			}
			require.NoError(t, fixture.store.AppendEvents(t.Context(), events))

			report := stateObservation(
				protocol.OpaqueID("observation-prefix-"+
					fmt.Sprint(test.gapSequence, test.prefixAvailable, test.rebound)),
				2, events[len(events)-1].WallTime.Add(time.Second),
			)
			report.EventGeneration = int64(len(events))
			report.NativeEvents = nativeStateReferences(events)
			producer := newTestStateProducer(t, fixture, nil, StateAnalysisProfile{})
			prepared, err := producer.loadStatePreparation(t.Context(), fixture.store.DB(), report)
			require.NoError(t, err)
			got := prepared.observation(report).ReduceNative()
			require.Equal(t, test.wantPhase, got.Phase)
			require.Equal(t, test.wantPhase == protocol.AgentPhaseRunningTool, got.Blocks)
		})
	}
}

func TestNativeStateCacheRejectsUnannouncedSequenceEpoch(t *testing.T) {
	firstFixture := newExecutorFixture(t)
	at := executorTestTime.Add(time.Second)
	firstEvent := durableNativeStateEvent(
		"01ARZ3NDEKTSV4RRFFQ69G4001", 1, protocol.EventTypeToolStarted, at,
	)
	firstEvent.ToolCallID = "tool-first-run"
	require.NoError(t, firstFixture.store.AppendEvents(
		t.Context(), []protocol.EventEnvelope{firstEvent},
	))
	firstReport := stateObservation("observation-first-epoch", 1, at.Add(time.Second))
	firstReport.EventGeneration = 1
	firstReport.NativeEvents = nativeStateReferences([]protocol.EventEnvelope{firstEvent})
	firstConfig, err := loadStateConfiguration(
		t.Context(), firstFixture.store.DB(), firstReport,
	)
	require.NoError(t, err)

	cache := &nativeHistoryCache{}
	firstHistory, err := cache.load(
		t.Context(), firstFixture.store.DB(), firstConfig.nativeLineage, firstReport,
	)
	require.NoError(t, err)
	require.Equal(t, protocol.AgentPhaseRunningTool,
		firstHistory.Reduce(nil, firstReport.ObservedAt).Phase)

	// A second capability store has restarted its sequence at one while an
	// otherwise identical session row still advertises the old epoch.
	secondFixture := newExecutorFixture(t)
	secondEvent := durableNativeStateEvent(
		"01ARZ3NDEKTSV4RRFFQ69G4002", 1, protocol.EventTypeApprovalRequested, at,
	)
	secondEvent.CorrelationID = "approval-second-run"
	require.NoError(t, secondFixture.store.AppendEvents(
		t.Context(), []protocol.EventEnvelope{secondEvent},
	))
	secondReport := stateObservation("observation-second-epoch", 1, at.Add(time.Second))
	secondReport.EventGeneration = 1
	secondReport.NativeEvents = nativeStateReferences([]protocol.EventEnvelope{secondEvent})
	secondConfig, err := loadStateConfiguration(
		t.Context(), secondFixture.store.DB(), secondReport,
	)
	require.NoError(t, err)
	require.Equal(t, firstConfig.nativeLineage, secondConfig.nativeLineage)
	_, err = cache.load(
		t.Context(), secondFixture.store.DB(), secondConfig.nativeLineage, secondReport,
	)
	require.ErrorIs(t, err, ErrObservationUnavailable,
		"a sequence reset cannot be mixed into the previous run")

	// Once the durable session identifies the new process epoch, the same
	// session id and reset sequence form a fresh accumulator.
	processJSON, err := json.Marshal(protocol.ProcessIdentity{
		PID: 505, ProcessStartTime: 606, UID: 303, GID: 404,
	})
	require.NoError(t, err)
	newStart := executorTestTime.Add(500 * time.Millisecond)
	_, err = secondFixture.store.DB().ExecContext(t.Context(), `UPDATE sessions
		SET process_json = ?, started_at = ?, updated_at = ? WHERE session_id = ?`,
		string(processJSON), eventstore.FormatTime(newStart), eventstore.FormatTime(newStart),
		executorTestSession)
	require.NoError(t, err)
	secondConfig, err = loadStateConfiguration(
		t.Context(), secondFixture.store.DB(), secondReport,
	)
	require.NoError(t, err)
	require.NotEqual(t, firstConfig.nativeLineage, secondConfig.nativeLineage)
	secondHistory, err := cache.load(
		t.Context(), secondFixture.store.DB(), secondConfig.nativeLineage, secondReport,
	)
	require.NoError(t, err)
	secondState := secondHistory.Reduce(nil, secondReport.ObservedAt)
	require.Equal(t, protocol.AgentPhaseWaitingApproval, secondState.Phase)
	require.Equal(t, []protocol.ULID{secondEvent.EventID}, secondState.Evidence)
}

func TestNativeStateRangeUsesSessionSequenceIndex(t *testing.T) {
	fixture := newExecutorFixture(t)
	report := stateObservation("observation-native-query-plan", 1, executorTestTime.Add(time.Second))
	prepared, err := loadStateConfiguration(t.Context(), fixture.store.DB(), report)
	require.NoError(t, err)
	lineage := prepared.nativeLineage
	rows, err := fixture.store.DB().QueryContext(t.Context(),
		"EXPLAIN QUERY PLAN "+nativeStateRangeSQL,
		lineage.sessionID, -1, 5000, protocol.SchemaVersion,
		lineage.nodeID, lineage.agentID, lineage.adapterType,
		lineage.harness, lineage.harnessVersion, eventstore.FormatTime(lineage.startedAt),
		protocol.EventTypeAdapterCapabilitiesChanged, protocol.StreamNativeEvent,
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()
	var details []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		require.NoError(t, rows.Scan(&id, &parent, &unused, &detail))
		details = append(details, detail)
	}
	require.NoError(t, rows.Err())
	plan := strings.Join(details, "\n")
	require.Contains(t, plan, "SEARCH events USING INDEX events_session_sequence")
	require.NotContains(t, plan, "SCAN events")
}

func TestNativeStateCacheMeasuresFiveThousandEventIncrement(t *testing.T) {
	fixture := newExecutorFixture(t)
	const total = 5000
	events := make([]protocol.EventEnvelope, 0, total+1)
	for sequence := 1; sequence <= total; sequence++ {
		event := durableNativeStateEvent(
			protocol.ULID(fmt.Sprintf("01ARZ3NDEKTSV4RRFFQ69G%04d", sequence)),
			int64(sequence), protocol.EventTypeNotificationReceived,
			executorTestTime.Add(time.Duration(sequence)*time.Microsecond),
		)
		events = append(events, event)
	}
	for start := 0; start < len(events); start += eventstore.MaxBatchEvents {
		end := start + eventstore.MaxBatchEvents
		if end > len(events) {
			end = len(events)
		}
		require.NoError(t, fixture.store.AppendEvents(t.Context(), events[start:end]))
	}
	report := stateObservation(
		"observation-native-5000", total, executorTestTime.Add(time.Second),
	)
	report.EventGeneration = total
	report.NativeEvents = nativeStateReferences(events)
	prepared, err := loadStateConfiguration(t.Context(), fixture.store.DB(), report)
	require.NoError(t, err)
	queryer := &countingNativeStateQueryer{db: fixture.store.DB()}
	cache := &nativeHistoryCache{}

	started := time.Now()
	history, err := cache.load(t.Context(), queryer, prepared.nativeLineage, report)
	firstPass := time.Since(started)
	require.NoError(t, err)
	require.False(t, history.Reduce(nil, report.ObservedAt).Blocks)
	require.Equal(t, 1, queryer.rowQueries)
	require.Equal(t, 1, queryer.rangeReads)

	// A repeated cut is served entirely by the clone of the cached reducer.
	_, err = cache.load(t.Context(), queryer, prepared.nativeLineage, report)
	require.NoError(t, err)
	require.Equal(t, 1, queryer.rowQueries)
	require.Equal(t, 1, queryer.rangeReads)

	increment := durableNativeStateEvent(
		"01ARZ3NDEKTSV4RRFFQ69G5001", total+1,
		protocol.EventTypeToolStarted, executorTestTime.Add(5001*time.Microsecond),
	)
	increment.ToolCallID = "tool-after-5000"
	require.NoError(t, fixture.store.AppendEvents(
		t.Context(), []protocol.EventEnvelope{increment},
	))
	events = append(events, increment)
	report.ObservationID = "observation-native-5001"
	report.ObservationGeneration++
	report.EventGeneration++
	report.NativeEvents = nativeStateReferences(events)
	started = time.Now()
	history, err = cache.load(t.Context(), queryer, prepared.nativeLineage, report)
	incremental := time.Since(started)
	require.NoError(t, err)
	require.Equal(t, protocol.AgentPhaseRunningTool,
		history.Reduce(nil, report.ObservedAt).Phase)
	require.Equal(t, 2, queryer.rowQueries)
	require.Equal(t, 2, queryer.rangeReads)
	t.Logf("native_history_5000 first_pass_ms=%.3f incremental_ms=%.3f",
		float64(firstPass.Microseconds())/1000,
		float64(incremental.Microseconds())/1000)
}
