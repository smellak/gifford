package orchestrator

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/analyst"
	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/daemon/agentlink"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/gateway"
	"github.com/smellak/gifford/internal/protocol"
)

const (
	stateTestProfileRevision     = "profile-v1"
	stateTestCalibrationRevision = "calibration-v1"
	stateTestBaseModelRepo       = "model"
	stateTestBaseModelRevision   = "model-rev-1"
	stateTestRedactionPolicy     = "redact-v1"
	stateTestRuntime             = "test"
)

func (c *executorClock) set(now time.Time) {
	c.mu.Lock()
	c.now = now
	c.mu.Unlock()
}

func stateObservation(id protocol.OpaqueID, generation int64, at time.Time) protocol.StateObservationReport {
	return protocol.StateObservationReport{
		AgentID: executorTestAgent, SessionID: executorTestSession,
		ObservationID: id, ObservedAt: at,
		Harness: protocol.HarnessCodex, HarnessVersion: executorTestHarnessVersion,
		ObservationGeneration: generation, OutputGeneration: generation,
		EventGeneration: generation, InputGuardVersion: 3,
		ScreenState: protocol.StateObservationScreenWithheld,
		ScreenRows:  []string{}, Activity: protocol.StateObservationActivity{},
		NativeEvents: []protocol.StateObservationEvent{},
	}
}

func newTestStateProducer(t *testing.T, fixture *executorFixture, analyzer StateAnalyzer, profile StateAnalysisProfile) *StateProducer {
	t.Helper()
	producer, err := NewStateProducer(StateProducerOptions{
		DB: fixture.store.DB(), Clock: fixture.clock, Channel: fixture.controller,
		Analyzer: analyzer, Profile: profile,
		Entropy: bytes.NewReader(bytes.Repeat([]byte{1}, 4096)),
	})
	require.NoError(t, err)
	return producer
}

func TestStateProducerInitialSafetyPreservesProvenUnsafeGuard(t *testing.T) {
	for _, tc := range []struct {
		name     string
		hasGuard bool
		draft    protocol.DraftState
		activity protocol.ActivityState
		want     protocol.DeliverySafety
	}{
		{name: "no_guard", want: protocol.DeliverySafetyUncertain},
		{
			name: "unknown_guard", hasGuard: true, draft: protocol.DraftStateUnknown,
			activity: protocol.ActivityStateUnknown, want: protocol.DeliverySafetyUncertain,
		},
		{
			name: "closed_idle", hasGuard: true, draft: protocol.DraftStateClosed,
			activity: protocol.ActivityStateIdle, want: protocol.DeliverySafetyUncertain,
		},
		{
			name: "open_draft", hasGuard: true, draft: protocol.DraftStateOpen,
			activity: protocol.ActivityStateIdle, want: protocol.DeliverySafetyUnsafe,
		},
		{
			name: "active_human", hasGuard: true, draft: protocol.DraftStateClosed,
			activity: protocol.ActivityStateActive, want: protocol.DeliverySafetyUnsafe,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newExecutorFixture(t)
			_, err := fixture.store.DB().ExecContext(t.Context(), `DELETE FROM state_snapshots
				WHERE agent_id = ? AND session_id = ?`, executorTestAgent, executorTestSession)
			require.NoError(t, err)
			if tc.hasGuard {
				guards, guardErr := agentlink.NewInputGuardStore(fixture.store.DB(), fixture.clock)
				require.NoError(t, guardErr)
				written, guardErr := guards.WriteInputGuard(t.Context(), protocol.InputGuardReport{
					AgentID: executorTestAgent, SessionID: executorTestSession,
					DraftState: tc.draft, ActivityState: tc.activity,
					DraftGeneration: 3, InputGuardVersion: 4,
					DetectionSource: protocol.InputGuardProviderPTYProxy, Confidence: 1,
				})
				require.NoError(t, guardErr)
				require.True(t, written)
			} else {
				_, err = fixture.store.DB().ExecContext(t.Context(), `DELETE FROM input_guards
					WHERE agent_id = ? AND session_id = ?`, executorTestAgent, executorTestSession)
				require.NoError(t, err)
			}

			producer := newTestStateProducer(t, fixture, nil, StateAnalysisProfile{})
			require.NoError(t, producer.publishInitialStates(t.Context()))
			state, err := ReadLatestState(t.Context(), fixture.store.DB(), executorTestAgent,
				executorTestSession, fixture.clock.Now(), nil)
			require.NoError(t, err)
			require.Equal(t, tc.want, state.Snapshot.DeliverySafety)
			require.Equal(t, protocol.HealthStateDegraded, state.Snapshot.HealthState)
			require.Equal(t, protocol.AgentPhaseUnknown, state.Snapshot.AgentPhase)
			require.True(t, state.Snapshot.Stale)
		})
	}
}

func TestStateProducerLatestWinsTTLRateAndReplay(t *testing.T) {
	t.Parallel()

	fixture := newExecutorFixture(t)
	producer := newTestStateProducer(t, fixture, nil, StateAnalysisProfile{})
	at := executorTestTime.Add(time.Second)
	fixture.clock.set(at)
	older := stateObservation("observation-older", 2, at)
	newer := stateObservation("observation-newer", 3, at.Add(100*time.Millisecond))
	require.NoError(t, producer.Submit(older))
	require.NoError(t, producer.Submit(newer))
	fixture.clock.set(newer.ObservedAt)
	require.NoError(t, producer.produceOne(t.Context(), executorTestAgent))

	state, err := ReadLatestState(t.Context(), fixture.store.DB(), executorTestAgent,
		executorTestSession, at, nil)
	require.NoError(t, err)
	require.Equal(t, newer.ObservationID, state.ObservationID)
	require.Equal(t, newer.ObservedAt.Add(stateTTL), state.ExpiresAt,
		"TTL is derived from observed_at, never commit time")
	require.Equal(t, StateEligibilityStale, state.EligibilityReason)

	replay := newTestStateProducer(t, fixture, nil, StateAnalysisProfile{})
	require.NoError(t, replay.Submit(newer))
	require.ErrorIs(t, replay.produceOne(t.Context(), executorTestAgent), ErrObservationUnavailable)
	var count int64
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT count(*)
		FROM state_snapshots WHERE observation_id = ?`, newer.ObservationID).Scan(&count))
	require.Equal(t, int64(1), count, "replay cannot renew or duplicate a snapshot")

	next := stateObservation("observation-next", 4, at.Add(time.Second))
	require.NoError(t, producer.Submit(next))
	require.Empty(t, producer.dueAgents(), "at most one commit per agent per second")
	fixture.clock.set(newer.ObservedAt.Add(stateCommitInterval))
	require.Equal(t, []protocol.AgentID{executorTestAgent}, producer.dueAgents())
}

func TestStateReaderAcceptsLegacyFiveSecondExpiryWithinNewBound(t *testing.T) {
	t.Parallel()

	fixture := newExecutorFixture(t)
	producer := newTestStateProducer(t, fixture, nil, StateAnalysisProfile{})
	observedAt := executorTestTime.Add(time.Second)
	fixture.clock.set(observedAt)
	report := stateObservation("observation-legacy-expiry", 2, observedAt)
	require.NoError(t, producer.Submit(report))
	require.NoError(t, producer.produceOne(t.Context(), executorTestAgent))

	legacyExpiry := observedAt.Add(5 * time.Second)
	_, err := fixture.store.DB().ExecContext(t.Context(), `UPDATE state_snapshots SET expires_at = ?
		WHERE observation_id = ?`, eventstore.FormatTime(legacyExpiry), report.ObservationID)
	require.NoError(t, err)
	state, err := ReadLatestState(t.Context(), fixture.store.DB(), executorTestAgent,
		executorTestSession, observedAt.Add(time.Second), nil)
	require.NoError(t, err)
	require.Equal(t, legacyExpiry, state.ExpiresAt)

	beyondBound := observedAt.Add(stateTTL + time.Millisecond)
	_, err = fixture.store.DB().ExecContext(t.Context(), `UPDATE state_snapshots SET expires_at = ?
		WHERE observation_id = ?`, eventstore.FormatTime(beyondBound), report.ObservationID)
	require.NoError(t, err)
	_, err = ReadLatestState(t.Context(), fixture.store.DB(), executorTestAgent,
		executorTestSession, observedAt.Add(time.Second), nil)
	require.ErrorIs(t, err, ErrStateObservationInvalid)
}

type blockingStateAnalyzer struct {
	started  chan struct{}
	release  chan struct{}
	recorder *gateway.SQLRecorder
	record   *protocol.GlimmerCallRecord
}

func (a *blockingStateAnalyzer) Analyze(ctx context.Context, _ analyst.Observation) (analyst.Analysis, error) {
	close(a.started)
	<-a.release
	if a.record != nil {
		if err := a.recorder.RecordCall(ctx, *a.record, analyst.PromptVersion); err != nil {
			return analyst.Analysis{}, err
		}
		record := *a.record
		return analyst.Analysis{
			AgentID: executorTestAgent, Route: analyst.RoutePTY,
			Phase: protocol.AgentPhaseIdleAtPrompt, Confidence: .95,
			PromptVersion: analyst.PromptVersion, CallRecord: &record,
		}, nil
	}
	return analyst.Analysis{
		AgentID: executorTestAgent, Route: analyst.RouteNative,
		Phase: protocol.AgentPhaseIdleAtPrompt, Confidence: 1,
	}, nil
}

func TestStateProducerLinksInFlightAnalysisAcrossEffectiveActivityAging(t *testing.T) {
	t.Parallel()
	fixture := newExecutorFixture(t)
	recorder, err := gateway.NewSQLRecorder(fixture.store.DB())
	require.NoError(t, err)
	digest := protocol.SHA256Hex("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	profile := StateAnalysisProfile{
		Revision: stateTestProfileRevision, SHA256: digest,
		CalibrationRevision: stateTestCalibrationRevision, CalibrationSHA256: digest,
		Temperature: 1, IdleThreshold: .9,
	}
	firstAt := executorTestTime.Add(time.Second)
	record := protocol.GlimmerCallRecord{
		CallID: "call-state-effective-aging", Contract: protocol.ModelContractAnalyzeAgentState,
		AgentID: executorTestAgent, InferenceProfile: protocol.InferenceProfileBase,
		BaseModelRepo: stateTestBaseModelRepo, BaseModelRevision: stateTestBaseModelRevision,
		Runtime: stateTestRuntime, RedactionPolicyVersion: stateTestRedactionPolicy,
		InputSHA256: digest, OutputSHA256: digest,
		Probabilities: map[string]float64{
			string(protocol.AgentPhaseIdleAtPrompt): .95,
			string(protocol.AgentPhaseThinking):     .05,
		},
		Status: protocol.GlimmerCallStatusOK, CreatedAt: firstAt,
	}
	blocker := &blockingStateAnalyzer{
		started: make(chan struct{}), release: make(chan struct{}), recorder: recorder, record: &record,
	}
	producer := newTestStateProducer(t, fixture, blocker, profile)
	first := stateObservation("observation-effective-in-flight", 2, firstAt)
	first.OutputGeneration, first.EventGeneration = 188, 0
	first.Activity = protocol.StateObservationActivity{
		BytesLast2s: 15, BytesLast10s: 45, SecondsSinceLastOutput: .5,
	}
	refresh := first
	refresh.ObservationID = "observation-effective-refresh"
	refresh.ObservationGeneration++
	refresh.ObservedAt = first.ObservedAt.Add(time.Second)
	refresh.Activity = protocol.StateObservationActivity{
		BytesLast2s: 15, BytesLast10s: 30, SecondsSinceLastOutput: 1.5,
	}

	fixture.clock.set(first.ObservedAt)
	require.NoError(t, producer.Submit(first))
	require.NoError(t, producer.Demand(executorTestAgent))
	done := make(chan error, 1)
	go func() { done <- producer.produceOne(t.Context(), executorTestAgent) }()
	<-blocker.started
	require.NoError(t, producer.Submit(refresh), "the no-op refresh keeps the effective generations")
	fixture.clock.set(refresh.ObservedAt)
	close(blocker.release)
	require.NoError(t, <-done)
	require.NotContains(t, producer.pending, executorTestAgent)
	require.NotContains(t, producer.demanded, executorTestAgent)

	state, err := ReadLatestState(t.Context(), fixture.store.DB(), executorTestAgent,
		executorTestSession, refresh.ObservedAt, &profile)
	require.NoError(t, err)
	require.Equal(t, record.CallID, state.GlimmerCallID)
	require.Equal(t, first.ObservedAt.Add(stateTTL), state.ExpiresAt,
		"a compatible refresh links the original inference without refreshing its TTL")
}

func TestStateProducerDiscardsAnalysisWhenGenerationsChange(t *testing.T) {
	t.Parallel()

	fixture := newExecutorFixture(t)
	digest := protocol.SHA256Hex("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	profile := StateAnalysisProfile{
		Revision: stateTestProfileRevision, SHA256: digest,
		CalibrationRevision: stateTestCalibrationRevision, CalibrationSHA256: digest,
		Temperature: 1, IdleThreshold: .9,
	}
	blocker := &blockingStateAnalyzer{started: make(chan struct{}), release: make(chan struct{})}
	producer := newTestStateProducer(t, fixture, blocker, profile)
	first := stateObservation("observation-in-flight", 2, executorTestTime.Add(time.Second))
	second := stateObservation("observation-superseding", 3, executorTestTime.Add(2*time.Second))
	fixture.clock.set(first.ObservedAt)
	require.NoError(t, producer.Submit(first))
	require.NoError(t, producer.Demand(executorTestAgent))
	done := make(chan error, 1)
	go func() { done <- producer.produceOne(t.Context(), executorTestAgent) }()
	<-blocker.started
	require.NoError(t, producer.Submit(second))
	close(blocker.release)
	require.ErrorIs(t, <-done, ErrObservationSuperseded)
	require.True(t, producer.demanded[executorTestAgent],
		"superseded evidence cannot consume the outstanding demand")

	var count int64
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT count(*)
		FROM state_snapshots WHERE observation_id = ?`, first.ObservationID).Scan(&count))
	require.Zero(t, count, "an analysis cannot commit across a generation change")
}

func TestStateProducerNativeExpirySupersedesInFlightAnalysis(t *testing.T) {
	t.Parallel()
	fixture := newExecutorFixture(t)
	digest := protocol.SHA256Hex(
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	)
	profile := StateAnalysisProfile{
		Revision: stateTestProfileRevision, SHA256: digest,
		CalibrationRevision: stateTestCalibrationRevision, CalibrationSHA256: digest,
		Temperature: 1, IdleThreshold: .9,
	}
	blocker := &blockingStateAnalyzer{started: make(chan struct{}), release: make(chan struct{})}
	producer := newTestStateProducer(t, fixture, blocker, profile)
	requestedAt := executorTestTime.Add(2 * time.Second)
	requested := durableNativeStateEvent(
		"01ARZ3NDEKTSV4RRFFQ69G1201", 1,
		protocol.EventTypeApprovalRequested, requestedAt,
	)
	requested.CorrelationID = "request-native-in-flight"
	require.NoError(t, protocol.ValidateNativeDurableEvent(requested))
	require.NoError(t, fixture.store.AppendEvents(
		t.Context(), []protocol.EventEnvelope{requested},
	))
	firstAt := requestedAt.Add(100 * time.Millisecond)
	first := stateObservation("observation-native-in-flight", 2, firstAt)
	first.NativeEvents = []protocol.StateObservationEvent{{
		EventID: requested.EventID, Type: requested.EventType, At: requested.WallTime,
	}}
	refresh := first
	refresh.ObservationID = "observation-native-expired-refresh"
	refresh.ObservationGeneration++
	refresh.ObservedAt = requestedAt.Add(analyst.NativeOperationTTL)

	fixture.clock.set(firstAt)
	require.NoError(t, producer.Submit(first))
	require.NoError(t, producer.Demand(executorTestAgent))
	done := make(chan error, 1)
	go func() { done <- producer.produceOne(t.Context(), executorTestAgent) }()
	<-blocker.started
	fixture.clock.set(refresh.ObservedAt)
	require.NoError(t, producer.Submit(refresh))
	close(blocker.release)
	require.ErrorIs(t, <-done, ErrObservationSuperseded,
		"an analysis begun under a live native veto cannot cross its expiry")
	require.True(t, producer.demanded[executorTestAgent],
		"the expiry transition still requires a fresh Glimmer analysis")

	var count int64
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT count(*)
		FROM state_snapshots WHERE observation_id = ?`, first.ObservationID).Scan(&count))
	require.Zero(t, count)
}

func TestStateProducerDiscardsAnalysisWhenGuardChanges(t *testing.T) {
	t.Parallel()

	fixture := newExecutorFixture(t)
	digest := protocol.SHA256Hex("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	profile := StateAnalysisProfile{
		Revision: stateTestProfileRevision, SHA256: digest,
		CalibrationRevision: stateTestCalibrationRevision, CalibrationSHA256: digest,
		Temperature: 1, IdleThreshold: .9,
	}
	blocker := &blockingStateAnalyzer{started: make(chan struct{}), release: make(chan struct{})}
	producer := newTestStateProducer(t, fixture, blocker, profile)
	report := stateObservation("observation-guard-change", 2, executorTestTime.Add(time.Second))
	fixture.clock.set(report.ObservedAt)
	require.NoError(t, producer.Submit(report))
	require.NoError(t, producer.Demand(executorTestAgent))
	done := make(chan error, 1)
	go func() { done <- producer.produceOne(t.Context(), executorTestAgent) }()
	<-blocker.started
	guardStore, err := agentlink.NewInputGuardStore(fixture.store.DB(), fixture.clock)
	require.NoError(t, err)
	written, err := guardStore.WriteInputGuard(t.Context(), protocol.InputGuardReport{
		AgentID: executorTestAgent, SessionID: executorTestSession,
		DraftState: protocol.DraftStateClosed, ActivityState: protocol.ActivityStateIdle,
		DraftGeneration: 2, InputGuardVersion: 4,
		DetectionSource: protocol.InputGuardProviderPTYProxy, Confidence: 1,
	})
	require.NoError(t, err)
	require.True(t, written)
	close(blocker.release)
	require.ErrorIs(t, <-done, ErrObservationSuperseded)

	var count int64
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT count(*)
		FROM state_snapshots WHERE observation_id = ?`, report.ObservationID).Scan(&count))
	require.Zero(t, count, "a candidate cannot cross an input-guard revision")
}

type recordedStateAnalyzer struct {
	recorder *gateway.SQLRecorder
	record   protocol.GlimmerCallRecord
	calls    int
}

type nativeVetoRecordedAnalyzer struct {
	recorder *gateway.SQLRecorder
	record   protocol.GlimmerCallRecord
	calls    int
	failAt   int
}

func (a *nativeVetoRecordedAnalyzer) Analyze(
	ctx context.Context,
	observation analyst.Observation,
) (analyst.Analysis, error) {
	a.calls++
	reduced := observation.ReduceNative()
	if a.calls == a.failAt {
		return analyst.Analysis{
			AgentID: observation.AgentID, Route: analyst.RoutePTY,
			Phase: protocol.AgentPhaseUnknown, PreviousStale: true,
			NativeEvidence: append([]protocol.ULID(nil), reduced.Evidence...),
		}, gateway.ErrCallFailed
	}
	record := a.record
	record.CallID = protocol.OpaqueID(fmt.Sprintf("call-native-veto-%d", a.calls))
	record.CreatedAt = observation.ObservedAt
	if err := a.recorder.RecordCall(ctx, record, analyst.PromptVersion); err != nil {
		return analyst.Analysis{}, err
	}
	if reduced.Blocks {
		return analyst.Analysis{
			AgentID: observation.AgentID, Route: analyst.RouteNative,
			Phase: reduced.Phase, Confidence: reduced.Confidence,
			PreviousStale:   reduced.Stale,
			NativeEvidence:  append([]protocol.ULID(nil), reduced.Evidence...),
			NativeExpiresAt: reduced.ExpiresAt,
		}, nil
	}
	return analyst.Analysis{
		AgentID: observation.AgentID, Route: analyst.RoutePTY,
		Phase: protocol.AgentPhaseIdleAtPrompt, Confidence: .95,
		PromptVersion: analyst.PromptVersion, CallRecord: &record,
		NativeEvidence: append([]protocol.ULID(nil), reduced.Evidence...),
	}, nil
}

func TestStateProducerNativeVetoRequiresFreshGlimmerAfterResolutionOrFallback(t *testing.T) {
	t.Parallel()
	fixture := newExecutorFixture(t)
	recorder, err := gateway.NewSQLRecorder(fixture.store.DB())
	require.NoError(t, err)
	digest := protocol.SHA256Hex("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	profile := StateAnalysisProfile{
		Revision: stateTestProfileRevision, SHA256: digest,
		CalibrationRevision: stateTestCalibrationRevision, CalibrationSHA256: digest,
		Temperature: 1, IdleThreshold: .9,
	}
	analyzer := &nativeVetoRecordedAnalyzer{recorder: recorder, record: protocol.GlimmerCallRecord{
		Contract: protocol.ModelContractAnalyzeAgentState, AgentID: executorTestAgent,
		InferenceProfile: protocol.InferenceProfileBase,
		BaseModelRepo:    stateTestBaseModelRepo, BaseModelRevision: stateTestBaseModelRevision,
		Runtime: stateTestRuntime, RedactionPolicyVersion: stateTestRedactionPolicy,
		InputSHA256: digest, OutputSHA256: digest,
		Probabilities: map[string]float64{
			string(protocol.AgentPhaseIdleAtPrompt): .95,
			string(protocol.AgentPhaseThinking):     .05,
		},
		Status: protocol.GlimmerCallStatusOK,
	}}
	producer := newTestStateProducer(t, fixture, analyzer, profile)
	requestedAt := executorTestTime.Add(2 * time.Second)
	requested := durableNativeStateEvent(
		"01ARZ3NDEKTSV4RRFFQ69G1031", 1,
		protocol.EventTypeApprovalRequested, requestedAt,
	)
	requested.CorrelationID = "request-native-veto"
	require.NoError(t, protocol.ValidateNativeDurableEvent(requested))
	require.NoError(t, fixture.store.AppendEvents(t.Context(), []protocol.EventEnvelope{requested}))
	firstAt := requestedAt.Add(100 * time.Millisecond)
	first := stateObservation("observation-native-veto", 2, firstAt)
	first.OutputGeneration, first.EventGeneration = 1, 1
	first.NativeEvents = []protocol.StateObservationEvent{{
		EventID: requested.EventID, Type: requested.EventType, At: requested.WallTime,
	}}
	fixture.clock.set(firstAt)
	require.NoError(t, producer.Submit(first))
	require.NoError(t, producer.Demand(executorTestAgent))
	require.NoError(t, producer.produceOne(t.Context(), executorTestAgent))
	require.Equal(t, 1, analyzer.calls, "Glimmer still runs before the native veto closes the door")
	blocked, err := ReadLatestState(
		t.Context(), fixture.store.DB(), executorTestAgent, executorTestSession, firstAt, &profile,
	)
	require.NoError(t, err)
	require.Equal(t, protocol.AgentPhaseWaitingApproval, blocked.Snapshot.AgentPhase)
	require.Equal(t, StateEligibilityNativeInformative, blocked.EligibilityReason)
	require.False(t, blocked.AutomaticEligible)
	require.Empty(t, blocked.GlimmerCallID)

	resolvedAt := firstAt.Add(900 * time.Millisecond)
	resolved := durableNativeStateEvent(
		"01ARZ3NDEKTSV4RRFFQ69G1032", 2,
		protocol.EventTypeApprovalResolved, resolvedAt,
	)
	resolved.CorrelationID = requested.CorrelationID
	resolved.Payload = protocol.Payload(
		`{"emitter_event_id":"emitter-2","result":"ok"}`,
	)
	require.NoError(t, protocol.ValidateNativeDurableEvent(resolved))
	require.NoError(t, fixture.store.AppendEvents(t.Context(), []protocol.EventEnvelope{resolved}))
	secondAt := firstAt.Add(stateCommitInterval)
	second := stateObservation("observation-native-resolved", 3, secondAt)
	second.OutputGeneration, second.EventGeneration = first.OutputGeneration, 2
	second.NativeEvents = []protocol.StateObservationEvent{
		{EventID: requested.EventID, Type: requested.EventType, At: requested.WallTime},
		{EventID: resolved.EventID, Type: resolved.EventType, At: resolved.WallTime},
	}
	fixture.clock.set(secondAt)
	require.NoError(t, producer.Submit(second))
	require.NoError(t, producer.Demand(executorTestAgent))
	require.NoError(t, producer.produceOne(t.Context(), executorTestAgent))
	require.Equal(t, 2, analyzer.calls, "the resolved request needs new model evidence")
	open, err := ReadLatestState(
		t.Context(), fixture.store.DB(), executorTestAgent, executorTestSession, secondAt, &profile,
	)
	require.NoError(t, err)
	require.Equal(t, protocol.AgentPhaseIdleAtPrompt, open.Snapshot.AgentPhase)
	require.Equal(t, StateEligibilityEligible, open.EligibilityReason)
	require.True(t, open.AutomaticEligible)
	require.Equal(t, protocol.OpaqueID("call-native-veto-2"), open.GlimmerCallID)

	nativeCapabilities := protocol.Capabilities{
		StructuredEvents: true, ToolEvents: true, ApprovalEvents: true,
	}
	downAt := secondAt.Add(100 * time.Millisecond)
	down := durableCapabilityStateEvent(
		t, "01ARZ3NDEKTSV4RRFFQ69G1033", 3, downAt,
		nativeCapabilities, protocol.Capabilities{},
	)
	require.NoError(t, down.Validate())
	require.NoError(t, fixture.store.AppendEvents(t.Context(), []protocol.EventEnvelope{down}))
	thirdAt := secondAt.Add(stateCommitInterval)
	third := stateObservation("observation-native-down", 4, thirdAt)
	third.OutputGeneration, third.EventGeneration = second.OutputGeneration, 3
	third.NativeEvents = append(append(
		[]protocol.StateObservationEvent(nil), second.NativeEvents...),
		protocol.StateObservationEvent{
			EventID: down.EventID, Type: down.EventType, At: down.WallTime,
		},
	)
	fixture.clock.set(thirdAt)
	require.NoError(t, producer.Submit(third))
	require.NoError(t, producer.Demand(executorTestAgent))
	require.NoError(t, producer.produceOne(t.Context(), executorTestAgent))
	require.Equal(t, 3, analyzer.calls)
	fallback, err := ReadLatestState(
		t.Context(), fixture.store.DB(), executorTestAgent, executorTestSession, thirdAt, &profile,
	)
	require.NoError(t, err)
	require.Equal(t, protocol.AgentPhaseIdleAtPrompt, fallback.Snapshot.AgentPhase)
	require.False(t, fallback.Snapshot.Stale)
	require.Equal(t, StateEligibilityEligible, fallback.EligibilityReason)
	require.True(t, fallback.AutomaticEligible,
		"a fresh PTY analysis recovers without a native reconnect or human key")
	require.Equal(t, protocol.OpaqueID("call-native-veto-3"), fallback.GlimmerCallID)
	require.Equal(t, []protocol.ULID{down.EventID}, fallback.Snapshot.EvidenceEventIDs,
		"the native degradation remains a diagnostic on the PTY snapshot")

	upAt := thirdAt.Add(100 * time.Millisecond)
	up := durableCapabilityStateEvent(
		t, "01ARZ3NDEKTSV4RRFFQ69G1034", 4, upAt,
		protocol.Capabilities{}, nativeCapabilities,
	)
	require.NoError(t, up.Validate())
	require.NoError(t, fixture.store.AppendEvents(t.Context(), []protocol.EventEnvelope{up}))
	fourthAt := thirdAt.Add(stateCommitInterval)
	fourth := stateObservation("observation-native-reactivated", 5, fourthAt)
	fourth.OutputGeneration, fourth.EventGeneration = third.OutputGeneration, 4
	fourth.NativeEvents = append(append(
		[]protocol.StateObservationEvent(nil), third.NativeEvents...),
		protocol.StateObservationEvent{
			EventID: up.EventID, Type: up.EventType, At: up.WallTime,
		},
	)
	fixture.clock.set(fourthAt)
	require.NoError(t, producer.Submit(fourth))
	require.NoError(t, producer.Demand(executorTestAgent))
	require.NoError(t, producer.produceOne(t.Context(), executorTestAgent))
	require.Equal(t, 4, analyzer.calls, "reactivation needs fresh model evidence")
	reactivated, err := ReadLatestState(
		t.Context(), fixture.store.DB(), executorTestAgent, executorTestSession, fourthAt, &profile,
	)
	require.NoError(t, err)
	require.Equal(t, protocol.AgentPhaseIdleAtPrompt, reactivated.Snapshot.AgentPhase)
	require.True(t, reactivated.AutomaticEligible)
	require.Equal(t, protocol.OpaqueID("call-native-veto-4"), reactivated.GlimmerCallID)
}

func TestStateProducerExpiredNativeVetoRequiresFreshSafePTY(t *testing.T) {
	for index, test := range []struct {
		name       string
		failSecond bool
		openDraft  bool
	}{
		{name: "fresh idle recovers"},
		{name: "model failure stays closed", failSecond: true},
		{name: "open draft stays closed", openDraft: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newExecutorFixture(t)
			recorder, err := gateway.NewSQLRecorder(fixture.store.DB())
			require.NoError(t, err)
			digest := protocol.SHA256Hex(
				"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			)
			profile := StateAnalysisProfile{
				Revision: stateTestProfileRevision, SHA256: digest,
				CalibrationRevision: stateTestCalibrationRevision, CalibrationSHA256: digest,
				Temperature: 1, IdleThreshold: .9,
			}
			analyzer := &nativeVetoRecordedAnalyzer{
				recorder: recorder,
				record: protocol.GlimmerCallRecord{
					Contract: protocol.ModelContractAnalyzeAgentState, AgentID: executorTestAgent,
					InferenceProfile: protocol.InferenceProfileBase,
					BaseModelRepo:    stateTestBaseModelRepo, BaseModelRevision: stateTestBaseModelRevision,
					Runtime: stateTestRuntime, RedactionPolicyVersion: stateTestRedactionPolicy,
					InputSHA256: digest, OutputSHA256: digest,
					Probabilities: map[string]float64{
						string(protocol.AgentPhaseIdleAtPrompt): .95,
						string(protocol.AgentPhaseThinking):     .05,
					},
					Status: protocol.GlimmerCallStatusOK,
				},
			}
			if test.failSecond {
				analyzer.failAt = 2
			}
			producer := newTestStateProducer(t, fixture, analyzer, profile)
			requestedAt := executorTestTime.Add(2 * time.Second)
			requested := durableNativeStateEvent(
				protocol.ULID(fmt.Sprintf("01ARZ3NDEKTSV4RRFFQ69G11%02d", index)), 1,
				protocol.EventTypeApprovalRequested, requestedAt,
			)
			requested.CorrelationID = protocol.OpaqueID(fmt.Sprintf("request-expiry-%d", index))
			require.NoError(t, protocol.ValidateNativeDurableEvent(requested))
			require.NoError(t, fixture.store.AppendEvents(
				t.Context(), []protocol.EventEnvelope{requested},
			))

			firstAt := requestedAt.Add(100 * time.Millisecond)
			first := stateObservation(
				protocol.OpaqueID(fmt.Sprintf("observation-expiry-veto-%d", index)), 2, firstAt,
			)
			first.OutputGeneration, first.EventGeneration = 1, 1
			first.NativeEvents = []protocol.StateObservationEvent{{
				EventID: requested.EventID, Type: requested.EventType, At: requested.WallTime,
			}}
			fixture.clock.set(firstAt)
			require.NoError(t, producer.Submit(first))
			require.NoError(t, producer.Demand(executorTestAgent))
			require.NoError(t, producer.produceOne(t.Context(), executorTestAgent))
			require.Equal(t, 1, analyzer.calls)
			blocked, err := ReadLatestState(
				t.Context(), fixture.store.DB(), executorTestAgent, executorTestSession,
				firstAt, &profile,
			)
			require.NoError(t, err)
			require.Equal(t, protocol.AgentPhaseWaitingApproval, blocked.Snapshot.AgentPhase)
			require.Equal(t, requestedAt.Add(analyst.NativeOperationTTL), blocked.ExpiresAt,
				"native authority expires from durable receipt time")
			require.False(t, blocked.AutomaticEligible)

			fallbackAt := requestedAt.Add(analyst.NativeOperationTTL)
			guardVersion := first.InputGuardVersion
			if test.openDraft {
				fixture.clock.set(fallbackAt)
				guards, guardErr := agentlink.NewInputGuardStore(fixture.store.DB(), fixture.clock)
				require.NoError(t, guardErr)
				written, guardErr := guards.WriteInputGuard(t.Context(), protocol.InputGuardReport{
					AgentID: executorTestAgent, SessionID: executorTestSession,
					DraftState: protocol.DraftStateOpen, ActivityState: protocol.ActivityStateIdle,
					DraftGeneration: 2, InputGuardVersion: 4,
					DetectionSource: protocol.InputGuardProviderPTYProxy, Confidence: 1,
				})
				require.NoError(t, guardErr)
				require.True(t, written)
				guardVersion = 4
			}
			fallback := stateObservation(
				protocol.OpaqueID(fmt.Sprintf("observation-expiry-fallback-%d", index)),
				3, fallbackAt,
			)
			fallback.OutputGeneration, fallback.EventGeneration = 1, 1
			fallback.InputGuardVersion = guardVersion
			fallback.NativeEvents = append(
				[]protocol.StateObservationEvent(nil), first.NativeEvents...,
			)
			fixture.clock.set(fallbackAt)
			require.NoError(t, producer.Submit(fallback))
			require.NoError(t, producer.Demand(executorTestAgent))
			require.NoError(t, producer.produceOne(t.Context(), executorTestAgent))
			require.Equal(t, 2, analyzer.calls,
				"crossing the native TTL cannot reuse the earlier Glimmer result")
			latest, err := ReadLatestState(
				t.Context(), fixture.store.DB(), executorTestAgent, executorTestSession,
				fallbackAt, &profile,
			)
			require.NoError(t, err)
			require.Equal(t, []protocol.ULID{requested.EventID}, latest.Snapshot.EvidenceEventIDs)

			switch {
			case test.failSecond:
				require.Equal(t, protocol.AgentPhaseUnknown, latest.Snapshot.AgentPhase)
				require.True(t, latest.Snapshot.Stale)
				require.False(t, latest.AutomaticEligible)
				require.Empty(t, latest.GlimmerCallID)
			case test.openDraft:
				require.Equal(t, protocol.AgentPhaseIdleAtPrompt, latest.Snapshot.AgentPhase)
				require.Equal(t, protocol.DeliverySafetyUnsafe, latest.Snapshot.DeliverySafety)
				require.Equal(t, StateEligibilityHumanInput, latest.EligibilityReason)
				require.False(t, latest.AutomaticEligible)
			default:
				require.Equal(t, protocol.AgentPhaseIdleAtPrompt, latest.Snapshot.AgentPhase)
				require.False(t, latest.Snapshot.Stale)
				require.True(t, latest.AutomaticEligible,
					"fresh PTY evidence recovers without a closing hook or human key")
				require.Equal(t, protocol.OpaqueID("call-native-veto-2"), latest.GlimmerCallID)
				require.Equal(t, fallbackAt.Add(stateTTL), latest.ExpiresAt,
					"the fresh PTY analysis gets the independent ten-second lifetime")
			}
		})
	}
}

func (a *recordedStateAnalyzer) Analyze(ctx context.Context, _ analyst.Observation) (analyst.Analysis, error) {
	a.calls++
	if err := a.recorder.RecordCall(ctx, a.record, analyst.PromptVersion); err != nil {
		return analyst.Analysis{}, err
	}
	record := a.record
	return analyst.Analysis{
		AgentID: executorTestAgent, Route: analyst.RoutePTY,
		Phase: protocol.AgentPhaseIdleAtPrompt, Confidence: .95,
		PromptVersion: analyst.PromptVersion, CallRecord: &record,
	}, nil
}

type mismatchedRecordedAnalyzer struct {
	recorder *gateway.SQLRecorder
	record   protocol.GlimmerCallRecord
}

func (a *mismatchedRecordedAnalyzer) Analyze(ctx context.Context, _ analyst.Observation) (analyst.Analysis, error) {
	requireRecord := a.record
	if err := a.recorder.RecordCall(ctx, requireRecord, analyst.PromptVersion); err != nil {
		return analyst.Analysis{}, err
	}
	returned := a.record
	returned.Probabilities = map[string]float64{
		string(protocol.AgentPhaseIdleAtPrompt): .99,
		string(protocol.AgentPhaseThinking):     .01,
	}
	return analyst.Analysis{
		AgentID: executorTestAgent, Route: analyst.RoutePTY,
		Phase: protocol.AgentPhaseIdleAtPrompt, Confidence: .99,
		PromptVersion: analyst.PromptVersion, CallRecord: &returned,
	}, nil
}

func TestStateProducerDoesNotTrustUndurableCallEvidence(t *testing.T) {
	t.Parallel()

	fixture := newExecutorFixture(t)
	recorder, err := gateway.NewSQLRecorder(fixture.store.DB())
	require.NoError(t, err)
	digest := protocol.SHA256Hex("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	profile := StateAnalysisProfile{
		Revision: stateTestProfileRevision, SHA256: digest,
		CalibrationRevision: stateTestCalibrationRevision, CalibrationSHA256: digest,
		Temperature: 1, IdleThreshold: .9,
	}
	observed := executorTestTime.Add(time.Second)
	analyzer := &mismatchedRecordedAnalyzer{recorder: recorder, record: protocol.GlimmerCallRecord{
		CallID: "call-state-mismatch", Contract: protocol.ModelContractAnalyzeAgentState,
		AgentID: executorTestAgent, InferenceProfile: protocol.InferenceProfileBase,
		BaseModelRepo: stateTestBaseModelRepo, BaseModelRevision: stateTestBaseModelRevision, Runtime: stateTestRuntime,
		RedactionPolicyVersion: stateTestRedactionPolicy, InputSHA256: digest, OutputSHA256: digest,
		Probabilities: map[string]float64{
			string(protocol.AgentPhaseIdleAtPrompt): .95,
			string(protocol.AgentPhaseThinking):     .05,
		},
		Status: protocol.GlimmerCallStatusOK, CreatedAt: observed,
	}}
	producer := newTestStateProducer(t, fixture, analyzer, profile)
	fixture.clock.set(observed)
	report := stateObservation("observation-mismatched-call", 2, observed)
	require.NoError(t, producer.Submit(report))
	require.NoError(t, producer.Demand(executorTestAgent))
	require.NoError(t, producer.produceOne(t.Context(), executorTestAgent))
	require.NotContains(t, producer.demanded, executorTestAgent,
		"an attempted analysis is consumed even when its evidence verifies as UNKNOWN")

	state, err := ReadLatestState(t.Context(), fixture.store.DB(), executorTestAgent,
		executorTestSession, observed, &profile)
	require.NoError(t, err)
	require.Equal(t, protocol.AgentPhaseUnknown, state.Snapshot.AgentPhase)
	require.Equal(t, protocol.DeliverySafetyUncertain, state.Snapshot.DeliverySafety)
	require.Equal(t, StateEligibilityStale, state.EligibilityReason)
	require.Empty(t, state.GlimmerCallID, "undurable evidence cannot be attached to a snapshot")
}

func TestStateProducerDemandWaitsForQuietObservationAndConsumesCache(t *testing.T) {
	t.Parallel()

	fixture := newExecutorFixture(t)
	recorder, err := gateway.NewSQLRecorder(fixture.store.DB())
	require.NoError(t, err)
	digest := protocol.SHA256Hex("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	profile := StateAnalysisProfile{
		Revision: stateTestProfileRevision, SHA256: digest,
		CalibrationRevision: stateTestCalibrationRevision, CalibrationSHA256: digest,
		Temperature: 1, IdleThreshold: .9,
	}
	quietAt := executorTestTime.Add(3 * time.Second)
	analyzer := &recordedStateAnalyzer{recorder: recorder, record: protocol.GlimmerCallRecord{
		CallID: "call-state-demand-quiet", Contract: protocol.ModelContractAnalyzeAgentState,
		AgentID: executorTestAgent, InferenceProfile: protocol.InferenceProfileBase,
		BaseModelRepo: stateTestBaseModelRepo, BaseModelRevision: stateTestBaseModelRevision,
		Runtime: stateTestRuntime, RedactionPolicyVersion: stateTestRedactionPolicy,
		InputSHA256: digest, OutputSHA256: digest,
		Probabilities: map[string]float64{
			string(protocol.AgentPhaseIdleAtPrompt): .95,
			string(protocol.AgentPhaseThinking):     .05,
		},
		Status: protocol.GlimmerCallStatusOK, CreatedAt: quietAt,
	}}
	producer := newTestStateProducer(t, fixture, analyzer, profile)
	require.NoError(t, producer.Demand(executorTestAgent))

	for generation := int64(2); generation <= 3; generation++ {
		at := executorTestTime.Add(time.Duration(generation-1) * time.Second)
		fixture.clock.set(at)
		noisy := stateObservation(
			protocol.OpaqueID(fmt.Sprintf("observation-demand-noisy-%d", generation)), generation, at,
		)
		noisy.OutputGeneration, noisy.EventGeneration = 2, 2
		noisy.Activity.BytesLast500ms = 1
		noisy.Activity.BytesLast2s = 1
		noisy.Activity.BytesLast10s = 1
		require.NoError(t, producer.Submit(noisy))
		require.NoError(t, producer.produceOne(t.Context(), executorTestAgent))
		require.Zero(t, analyzer.calls)
		require.True(t, producer.demanded[executorTestAgent])
	}

	fixture.clock.set(quietAt)
	quiet := stateObservation("observation-demand-quiet", 4, quietAt)
	quiet.OutputGeneration, quiet.EventGeneration = 2, 2
	require.NoError(t, producer.Submit(quiet))
	require.NoError(t, producer.produceOne(t.Context(), executorTestAgent))
	require.Equal(t, 1, analyzer.calls)
	require.NotContains(t, producer.demanded, executorTestAgent)
	first, err := ReadLatestState(t.Context(), fixture.store.DB(), executorTestAgent,
		executorTestSession, quietAt, &profile)
	require.NoError(t, err)
	require.Equal(t, quietAt.Add(stateTTL), first.ExpiresAt)

	cacheAt := quietAt.Add(time.Second)
	fixture.clock.set(cacheAt)
	require.NoError(t, producer.Demand(executorTestAgent))
	cached := stateObservation("observation-demand-cache", 5, cacheAt)
	cached.OutputGeneration, cached.EventGeneration = 2, 2
	require.NoError(t, producer.Submit(cached))
	require.NoError(t, producer.produceOne(t.Context(), executorTestAgent))
	require.Equal(t, 1, analyzer.calls, "a valid cached analysis satisfies demand without a new call")
	require.NotContains(t, producer.demanded, executorTestAgent,
		"a cache-satisfied demand cannot remain latent")
	second, err := ReadLatestState(t.Context(), fixture.store.DB(), executorTestAgent,
		executorTestSession, cacheAt, &profile)
	require.NoError(t, err)
	require.Equal(t, first.GlimmerCallID, second.GlimmerCallID)
	require.Equal(t, first.ExpiresAt, second.ExpiresAt, "cache reuse never renews inference TTL")

	expiredAt := first.ExpiresAt
	fixture.clock.set(expiredAt)
	expired := stateObservation("observation-demand-expired", 6, expiredAt)
	expired.OutputGeneration, expired.EventGeneration = 2, 2
	require.NoError(t, producer.Submit(expired))
	require.NoError(t, producer.produceOne(t.Context(), executorTestAgent))
	require.Equal(t, 1, analyzer.calls, "quiet refreshes never infer without a new demand")
}

func TestStateProducerCommitWithoutDemandPreservesConcurrentDemand(t *testing.T) {
	t.Parallel()

	fixture := newExecutorFixture(t)
	producer := newTestStateProducer(t, fixture, nil, StateAnalysisProfile{})
	report := stateObservation("observation-concurrent-demand", 2, executorTestTime.Add(time.Second))
	require.NoError(t, producer.Submit(report))
	current, _, ok := producer.current(executorTestAgent)
	require.True(t, ok)
	require.NoError(t, producer.Demand(executorTestAgent))
	producer.reconcileAndDiscard(current, stateCommitResult{
		stateID: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
	}, false, statePreparation{})
	require.NotContains(t, producer.pending, executorTestAgent)
	require.True(t, producer.demanded[executorTestAgent],
		"a commit that began without demand cannot consume a later Demand")
}

func TestStateReaderRequiresExactPTYEvidenceAndManifest(t *testing.T) {
	t.Parallel()

	fixture := newExecutorFixture(t)
	recorder, err := gateway.NewSQLRecorder(fixture.store.DB())
	require.NoError(t, err)
	digest := protocol.SHA256Hex("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	profile := StateAnalysisProfile{
		Revision: stateTestProfileRevision, SHA256: digest,
		CalibrationRevision: stateTestCalibrationRevision, CalibrationSHA256: digest,
		Temperature: 1, IdleThreshold: .9,
	}
	record := protocol.GlimmerCallRecord{
		CallID: "call-state-1", Contract: protocol.ModelContractAnalyzeAgentState,
		AgentID: executorTestAgent, InferenceProfile: protocol.InferenceProfileBase,
		BaseModelRepo: stateTestBaseModelRepo, BaseModelRevision: stateTestBaseModelRevision, Runtime: stateTestRuntime,
		RedactionPolicyVersion: stateTestRedactionPolicy, InputSHA256: digest, OutputSHA256: digest,
		Probabilities: map[string]float64{
			string(protocol.AgentPhaseIdleAtPrompt): .95,
			string(protocol.AgentPhaseThinking):     .05,
		},
		Status: protocol.GlimmerCallStatusOK, CreatedAt: executorTestTime.Add(time.Second),
	}
	analyzer := &recordedStateAnalyzer{recorder: recorder, record: record}
	producer := newTestStateProducer(t, fixture, analyzer, profile)
	observed := executorTestTime.Add(time.Second)
	fixture.clock.set(observed)
	report := stateObservation("observation-pty", 2, observed)
	require.NoError(t, producer.Submit(report))
	require.NoError(t, producer.Demand(executorTestAgent))
	require.NoError(t, producer.produceOne(t.Context(), executorTestAgent))

	state, err := ReadLatestState(t.Context(), fixture.store.DB(), executorTestAgent,
		executorTestSession, observed.Add(time.Second), &profile)
	require.NoError(t, err)
	require.True(t, state.AutomaticEligible)
	require.Equal(t, StateEligibilityEligible, state.EligibilityReason)
	require.InDelta(t, .95, state.IdleProbability, 1e-12)
	tx, err := fixture.store.DB().BeginTx(t.Context(), &sql.TxOptions{ReadOnly: true})
	require.NoError(t, err)
	automatic, err := producer.ReadLatestForAutomatic(t.Context(), tx, executorTestAgent,
		executorTestSession, observed.Add(time.Second), &profile)
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())
	require.True(t, automatic.AutomaticEligible,
		"the producer reconciles the durable row to the accepted evidence head")
	pending := stateObservation("observation-pending-newer", 3, observed.Add(500*time.Millisecond))
	require.NoError(t, producer.Submit(pending))
	tx, err = fixture.store.DB().BeginTx(t.Context(), &sql.TxOptions{ReadOnly: true})
	require.NoError(t, err)
	invalidated, err := producer.ReadLatestForAutomatic(t.Context(), tx, executorTestAgent,
		executorTestSession, observed.Add(500*time.Millisecond), &profile)
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())
	require.False(t, invalidated.AutomaticEligible)
	require.Equal(t, StateEligibilityObservationNewer, invalidated.EligibilityReason,
		"a newer pending generation invalidates before the rate-limited commit")
	producer.discardIfCurrent(pending)
	require.NoError(t, producer.Submit(report))
	require.NotContains(t, producer.pending, executorTestAgent,
		"an older replay cannot replace the retained evidence head")
	tx, err = fixture.store.DB().BeginTx(t.Context(), &sql.TxOptions{ReadOnly: true})
	require.NoError(t, err)
	invalidated, err = producer.ReadLatestForAutomatic(t.Context(), tx, executorTestAgent,
		executorTestSession, observed.Add(500*time.Millisecond), &profile)
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())
	require.False(t, invalidated.AutomaticEligible)
	require.Equal(t, StateEligibilityObservationNewer, invalidated.EligibilityReason,
		"queue discard and an older replay cannot re-enable superseded evidence")
	pure, err := ReadLatestState(t.Context(), fixture.store.DB(), executorTestAgent,
		executorTestSession, observed.Add(500*time.Millisecond), &profile)
	require.NoError(t, err)
	require.True(t, pure.AutomaticEligible, "the pure audit/manual read ignores producer memory")
	wrongProfile := profile
	wrongProfile.Temperature = 1.25
	mismatched, err := ReadLatestState(t.Context(), fixture.store.DB(), executorTestAgent,
		executorTestSession, observed.Add(time.Second), &wrongProfile)
	require.NoError(t, err)
	require.Equal(t, StateEligibilityProfileChanged, mismatched.EligibilityReason)

	_, err = fixture.store.DB().ExecContext(t.Context(), `UPDATE glimmer_calls
		SET probabilities_json = '{"IDLE_AT_PROMPT":0.1,"THINKING":0.9}'
		WHERE call_id = ?`, record.CallID)
	require.NoError(t, err)
	mismatched, err = ReadLatestState(t.Context(), fixture.store.DB(), executorTestAgent,
		executorTestSession, observed.Add(time.Second), &profile)
	require.NoError(t, err)
	require.Equal(t, StateEligibilityCallInvalid, mismatched.EligibilityReason,
		"p_idle must be recomputed from the durable call distribution")
	_, err = fixture.store.DB().ExecContext(t.Context(), `UPDATE glimmer_calls
		SET probabilities_json = '{"IDLE_AT_PROMPT":0.95,"THINKING":0.05}'
		WHERE call_id = ?`, record.CallID)
	require.NoError(t, err)

	_, err = fixture.store.DB().ExecContext(t.Context(), `UPDATE sessions
		SET manifest_json = replace(manifest_json, '1.0.0', '1.0.1')
		WHERE session_id = ?`, executorTestSession)
	require.NoError(t, err)
	changed, err := ReadLatestState(t.Context(), fixture.store.DB(), executorTestAgent,
		executorTestSession, observed.Add(time.Second), &profile)
	require.NoError(t, err)
	require.False(t, changed.AutomaticEligible)
	require.Equal(t, StateEligibilitySessionChanged, changed.EligibilityReason,
		"manifest digest changes are coherent even when sessions.updated_at does not move")
	_, err = fixture.store.DB().ExecContext(t.Context(), `UPDATE sessions
		SET manifest_json = replace(manifest_json, '1.0.1', '1.0.0')
		WHERE session_id = ?`, executorTestSession)
	require.NoError(t, err)

	// A newer honest UNKNOWN must shadow the older eligible IDLE row. The
	// reader validates after selecting latest and never recovers an older row.
	unknownProducer := newTestStateProducer(t, fixture, nil, StateAnalysisProfile{})
	unknownAt := observed.Add(stateCommitInterval)
	fixture.clock.set(unknownAt)
	unknown := stateObservation("observation-newer-unknown", 3, unknownAt)
	require.NoError(t, unknownProducer.Submit(unknown))
	require.NoError(t, unknownProducer.produceOne(t.Context(), executorTestAgent))
	latest, err := ReadLatestState(t.Context(), fixture.store.DB(), executorTestAgent,
		executorTestSession, unknownAt.Add(500*time.Millisecond), &profile)
	require.NoError(t, err)
	require.Equal(t, unknown.ObservationID, latest.ObservationID)
	require.Equal(t, StateEligibilityStale, latest.EligibilityReason)
	require.False(t, latest.AutomaticEligible)
}

func TestStateProducerReusesCompatibleInferenceWithoutExtendingItsLifetime(t *testing.T) {
	t.Parallel()

	fixture := newExecutorFixture(t)
	recorder, err := gateway.NewSQLRecorder(fixture.store.DB())
	require.NoError(t, err)
	digest := protocol.SHA256Hex("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	profile := StateAnalysisProfile{
		Revision: stateTestProfileRevision, SHA256: digest,
		CalibrationRevision: stateTestCalibrationRevision, CalibrationSHA256: digest,
		Temperature: 1, IdleThreshold: .9,
	}
	observed := executorTestTime.Add(500 * time.Millisecond)
	record := protocol.GlimmerCallRecord{
		CallID: "call-state-cache", Contract: protocol.ModelContractAnalyzeAgentState,
		AgentID: executorTestAgent, InferenceProfile: protocol.InferenceProfileBase,
		BaseModelRepo: stateTestBaseModelRepo, BaseModelRevision: stateTestBaseModelRevision, Runtime: stateTestRuntime,
		RedactionPolicyVersion: stateTestRedactionPolicy, InputSHA256: digest, OutputSHA256: digest,
		Probabilities: map[string]float64{
			string(protocol.AgentPhaseIdleAtPrompt): .95,
			string(protocol.AgentPhaseThinking):     .05,
		},
		Status: protocol.GlimmerCallStatusOK, CreatedAt: observed,
	}
	analyzer := &recordedStateAnalyzer{recorder: recorder, record: record}
	producer := newTestStateProducer(t, fixture, analyzer, profile)
	beforeCount := stateRowCount(t, fixture)
	fixture.clock.set(observed)
	first := stateObservation("observation-cache-1", 2, observed)
	require.NoError(t, producer.Submit(first))
	require.NoError(t, producer.Demand(executorTestAgent))
	require.ErrorIs(t, producer.produceOne(t.Context(), executorTestAgent), errStateCommitRateLimited)
	require.Equal(t, 1, analyzer.calls)
	require.Equal(t, beforeCount, stateRowCount(t, fixture),
		"a rate-limited candidate cannot commit a row")

	_, err = fixture.controller.SetGlobal(t.Context(), protocol.SetGlobalParams{
		Mode: protocol.ChannelModeOff, ExpectedVersion: 0,
	}, channel.Actor{Type: protocol.ActorTypeHuman, ID: executorTestOperator})
	require.NoError(t, err)
	commitAt := executorTestTime.Add(stateCommitInterval)
	fixture.clock.set(commitAt)
	changed := stateObservation("observation-cache-2", 3, commitAt)
	changed.OutputGeneration, changed.EventGeneration = first.OutputGeneration, first.EventGeneration
	require.NoError(t, producer.Submit(changed))
	require.NoError(t, producer.produceOne(t.Context(), executorTestAgent))
	require.Equal(t, 1, analyzer.calls,
		"a channel-only temporal refresh reuses the still-valid ANALYZE result")
	require.Equal(t, beforeCount+1, stateRowCount(t, fixture))

	latest, err := ReadLatestState(t.Context(), fixture.store.DB(), executorTestAgent,
		executorTestSession, commitAt, &profile)
	require.NoError(t, err)
	require.Equal(t, changed.ObservationID, latest.ObservationID)
	require.Equal(t, record.CallID, latest.GlimmerCallID)
	require.Equal(t, observed.Add(stateTTL), latest.ExpiresAt,
		"a reused call never extends the original inference lifetime")

	expiredAt := observed.Add(stateTTL)
	fixture.clock.set(expiredAt)
	expired := stateObservation("observation-cache-expired", 5, expiredAt)
	expired.OutputGeneration, expired.EventGeneration = first.OutputGeneration, first.EventGeneration
	require.NoError(t, producer.Submit(expired))
	require.NoError(t, producer.produceOne(t.Context(), executorTestAgent))
	require.Equal(t, 1, analyzer.calls, "an expired inference is not renewed without demand")
	stale, err := ReadLatestState(t.Context(), fixture.store.DB(), executorTestAgent,
		executorTestSession, expiredAt, &profile)
	require.NoError(t, err)
	require.False(t, stale.AutomaticEligible)
	require.Equal(t, StateEligibilityStale, stale.EligibilityReason)
}

func TestStateAnalysisCacheKeyRequiresExactAnalystInput(t *testing.T) {
	t.Parallel()

	digest := protocol.SHA256Hex("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	profile := StateAnalysisProfile{
		Revision: stateTestProfileRevision, SHA256: digest,
		CalibrationRevision: stateTestCalibrationRevision, CalibrationSHA256: digest,
		Temperature: 1, IdleThreshold: .9,
	}
	report := stateObservation("observation-cache-key-1", 2, executorTestTime.Add(time.Second))
	report.Activity.SecondsSinceLastOutput = 4
	prepared := statePreparation{
		sessionUpdatedAt: "2026-10-30T18:30:00.000000000Z",
		manifestSHA256:   string(digest),
		previous: &analyst.PreviousState{
			Phase: protocol.AgentPhaseThinking,
			Since: executorTestTime.Add(-3 * time.Second),
		},
	}
	base, err := stateAnalysisCacheKey(report, prepared, profile)
	require.NoError(t, err)

	identityOnly := report
	identityOnly.ObservationID = "observation-cache-key-2"
	identityOnly.ObservationGeneration++
	identityKey, err := stateAnalysisCacheKey(identityOnly, prepared, profile)
	require.NoError(t, err)
	require.Equal(t, base, identityKey,
		"transport identity is not part of the ANALYZE input")
	require.True(t, sameInferenceReportEvidence(report, identityOnly))

	observedAtChanged := identityOnly
	observedAtChanged.ObservedAt = observedAtChanged.ObservedAt.Add(time.Millisecond)
	observedAtKey, err := stateAnalysisCacheKey(observedAtChanged, prepared, profile)
	require.NoError(t, err)
	require.NotEqual(t, base, observedAtKey)
	require.False(t, sameInferenceReportEvidence(report, observedAtChanged))
	require.True(t, compatibleInferenceRefresh(report, observedAtChanged),
		"a later timer-only refresh can consume the original in-flight result")
	require.False(t, compatibleInferenceRefresh(observedAtChanged, report),
		"an older observation is not a compatible refresh")

	agingRefresh := identityOnly
	agingRefresh.ObservedAt = agingRefresh.ObservedAt.Add(time.Second)
	agingRefresh.Activity = protocol.StateObservationActivity{
		BytesLast2s: 10, BytesLast10s: 30, SecondsSinceLastOutput: 5,
	}
	agingBase := report
	agingBase.Activity = protocol.StateObservationActivity{
		BytesLast2s: 20, BytesLast10s: 40, SecondsSinceLastOutput: 4,
	}
	require.True(t, compatibleInferenceRefresh(agingBase, agingRefresh),
		"quiet time may advance and byte windows may drain around the same last output")
	newOutput := agingRefresh
	newOutput.Activity.BytesLast2s = agingBase.Activity.BytesLast2s + 1
	newOutput.Activity.BytesLast10s = agingBase.Activity.BytesLast10s + 1
	require.False(t, compatibleInferenceRefresh(agingBase, newOutput),
		"growing byte windows are new evidence")
	outputAdvanced := observedAtChanged
	outputAdvanced.OutputGeneration++
	require.False(t, compatibleInferenceRefresh(report, outputAdvanced))
	eventsAdvanced := observedAtChanged
	eventsAdvanced.EventGeneration++
	require.False(t, compatibleInferenceRefresh(report, eventsAdvanced))
	guardAdvanced := observedAtChanged
	guardAdvanced.InputGuardVersion++
	require.False(t, compatibleInferenceRefresh(report, guardAdvanced))
	otherSession := observedAtChanged
	otherSession.SessionID = "session-refresh-other"
	require.False(t, compatibleInferenceRefresh(report, otherSession))

	activityChanged := identityOnly
	activityChanged.Activity.SecondsSinceLastOutput++
	activityKey, err := stateAnalysisCacheKey(activityChanged, prepared, profile)
	require.NoError(t, err)
	require.NotEqual(t, base, activityKey)
	require.False(t, compatibleInferenceRefresh(report, activityChanged),
		"an idle-age change without matching clock progress is not temporal aging")

	previousChanged := prepared
	previous := *prepared.previous
	previous.Since = previous.Since.Add(time.Second)
	previousChanged.previous = &previous
	previousKey, err := stateAnalysisCacheKey(identityOnly, previousChanged, profile)
	require.NoError(t, err)
	require.NotEqual(t, base, previousKey)
}

func stateRowCount(t *testing.T, fixture *executorFixture) int64 {
	t.Helper()
	var count int64
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT count(*)
		FROM state_snapshots WHERE producer_version IS NOT NULL`).Scan(&count))
	return count
}
