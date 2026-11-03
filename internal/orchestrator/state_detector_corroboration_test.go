package orchestrator

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"maps"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/analyst"
	"github.com/smellak/gifford/internal/daemon/agentlink"
	"github.com/smellak/gifford/internal/gateway"
	"github.com/smellak/gifford/internal/protocol"
)

const detectorTestDigest protocol.SHA256Hex = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"

type detectorPolicyAnalyzer struct {
	recorder      *gateway.SQLRecorder
	phase         protocol.AgentPhase
	confidence    float64
	probabilities map[string]float64
	calls         int
}

func (a *detectorPolicyAnalyzer) Analyze(
	ctx context.Context,
	observation analyst.Observation,
) (analyst.Analysis, error) {
	a.calls++
	record := protocol.GlimmerCallRecord{
		CallID:   protocol.OpaqueID(fmt.Sprintf("call-detector-policy-%d", a.calls)),
		Contract: protocol.ModelContractAnalyzeAgentState,
		AgentID:  observation.AgentID, InferenceProfile: protocol.InferenceProfileBase,
		BaseModelRepo: stateTestBaseModelRepo, BaseModelRevision: stateTestBaseModelRevision,
		Runtime: stateTestRuntime, RedactionPolicyVersion: stateTestRedactionPolicy,
		InputSHA256: detectorTestDigest, OutputSHA256: detectorTestDigest,
		AnalysisPolicyVersion: stateAnalysisPolicy,
		Probabilities:         maps.Clone(a.probabilities),
		Status:                protocol.GlimmerCallStatusOK, CreatedAt: observation.ObservedAt,
	}
	if err := a.recorder.RecordCall(ctx, record, analyst.PromptVersion); err != nil {
		return analyst.Analysis{}, err
	}
	return analyst.Analysis{
		AgentID: observation.AgentID, Route: analyst.RoutePTY,
		Phase: a.phase, Confidence: a.confidence,
		PromptVersion: analyst.PromptVersion, CallRecord: &record,
	}, nil
}

func detectorTestProfile() StateAnalysisProfile {
	return StateAnalysisProfile{
		Revision: stateTestProfileRevision, SHA256: detectorTestDigest,
		CalibrationRevision: stateTestCalibrationRevision,
		CalibrationSHA256:   detectorTestDigest,
		Temperature:         1, IdleThreshold: .9,
	}
}

func detectorIdleAnalyzer(t *testing.T, fixture *executorFixture) *detectorPolicyAnalyzer {
	t.Helper()
	recorder, err := gateway.NewSQLRecorder(fixture.store.DB())
	require.NoError(t, err)
	return &detectorPolicyAnalyzer{
		recorder: recorder, phase: protocol.AgentPhaseIdleAtPrompt, confidence: .4,
		probabilities: map[string]float64{
			string(protocol.AgentPhaseIdleAtPrompt): .4,
			string(protocol.AgentPhaseThinking):     .6,
		},
	}
}

func configureDetectorRoute(
	t *testing.T,
	fixture *executorFixture,
	harness protocol.Harness,
	adapter protocol.AdapterType,
	provider protocol.InputGuardProvider,
	capture protocol.CaptureMode,
) int64 {
	t.Helper()
	capabilities := protocol.Capabilities{PTYCapture: true}
	if provider == protocol.InputGuardProviderNativeAdapter {
		capabilities.PromptBufferIntrospection = true
	}
	manifest := protocol.CapabilityManifest{
		SchemaVersion: protocol.SchemaVersion, AgentID: executorTestAgent,
		Harness: harness, HarnessVersion: executorTestHarnessVersion,
		AdapterType: adapter, InputGuardProvider: provider,
		CaptureMode: capture, Deliverable: true, Capabilities: capabilities,
	}
	require.NoError(t, manifest.Validate())
	raw, err := json.Marshal(manifest)
	require.NoError(t, err)
	_, err = fixture.store.DB().ExecContext(t.Context(), `UPDATE sessions
		SET capture_mode = ?, manifest_json = ? WHERE session_id = ? AND agent_id = ?`,
		capture, string(raw), executorTestSession, executorTestAgent)
	require.NoError(t, err)

	guards, err := agentlink.NewInputGuardStore(fixture.store.DB(), fixture.clock)
	require.NoError(t, err)
	written, err := guards.WriteInputGuard(t.Context(), protocol.InputGuardReport{
		AgentID: executorTestAgent, SessionID: executorTestSession,
		DraftState: protocol.DraftStateClosed, ActivityState: protocol.ActivityStateIdle,
		DraftGeneration: 2, InputGuardVersion: 4,
		DetectionSource: provider, Confidence: 1,
	})
	require.NoError(t, err)
	require.True(t, written)
	return 4
}

func detectorObservation(
	harness protocol.Harness,
	id protocol.OpaqueID,
	generation, guardVersion int64,
	at time.Time,
) protocol.StateObservationReport {
	report := stateObservation(id, generation, at)
	report.Harness = harness
	report.InputGuardVersion = guardVersion
	report.ScreenState = protocol.StateObservationScreenAvailable
	report.ScreenRows = []string{"detector policy screen"}
	report.Cols = 80
	known := false
	report.Cursor = &protocol.StateObservationCursor{Row: 0, Col: 0, Known: &known}
	return report
}

func detectorCorroboration(
	report protocol.StateObservationReport,
	busy bool,
) tmuxDetectorCorroboration {
	process := protocol.ProcessIdentity{
		PID: 101, ProcessStartTime: 202, UID: 303, GID: 404,
	}
	processJSON, _ := json.Marshal(process)
	return tmuxDetectorCorroboration{
		agentID: report.AgentID, sessionID: report.SessionID,
		instanceID: "01JB0000000000000000000099", paneID: "%9", panePID: 100,
		observationID: report.ObservationID, observedAt: report.ObservedAt,
		harness: report.Harness, harnessVersion: report.HarnessVersion,
		observationGeneration: report.ObservationGeneration,
		outputGeneration:      report.OutputGeneration, eventGeneration: report.EventGeneration,
		inputGuardVersion: report.InputGuardVersion, draftGeneration: 2,
		projectedScreenSHA256: detectorTestDigest,
		processSHA256:         sha256.Sum256(processJSON), policyVersion: stateAnalysisPolicy,
		certain: true, busy: busy, boxBusy: false, isCLI: true, detail: "empty",
	}
}

func produceDetectorState(
	t *testing.T,
	fixture *executorFixture,
	producer *StateProducer,
	report protocol.StateObservationReport,
	corroboration *tmuxDetectorCorroboration,
) StateRead {
	t.Helper()
	fixture.clock.set(report.ObservedAt)
	require.NoError(t, producer.Demand(report.AgentID))
	if corroboration == nil {
		require.NoError(t, producer.Submit(report))
	} else {
		require.NoError(t, producer.submitTmuxCorroborated(report, *corroboration))
	}
	require.NoError(t, producer.produceOne(t.Context(), report.AgentID))
	state, err := ReadLatestState(t.Context(), fixture.store.DB(), report.AgentID,
		report.SessionID, report.ObservedAt.Add(time.Millisecond), &producer.profile)
	require.NoError(t, err)
	return state
}

func TestDetectorCorroborationAllowsEffectiveIdleBelowThresholdForMeasuredHarnesses(t *testing.T) {
	for _, harness := range []protocol.Harness{
		protocol.HarnessCodex,
		protocol.HarnessClaudeCode,
		protocol.HarnessMuseCode,
		protocol.HarnessOpencode,
	} {
		t.Run(string(harness), func(t *testing.T) {
			fixture := newExecutorFixture(t)
			guardVersion := configureDetectorRoute(t, fixture, harness,
				protocol.AdapterTypeGenericPTY, protocol.InputGuardProviderTmuxAttach,
				protocol.CaptureModeTmuxAttach)
			analyzer := detectorIdleAnalyzer(t, fixture)
			producer := newTestStateProducer(t, fixture, analyzer, detectorTestProfile())
			report := detectorObservation(harness, "observation-detector-idle", 2,
				guardVersion, executorTestTime.Add(time.Second))
			corroboration := detectorCorroboration(report, false)
			state := produceDetectorState(t, fixture, producer, report, &corroboration)

			require.InDelta(t, .4, state.IdleProbability, 1e-12,
				"the real calibrated probability remains auditable")
			require.InDelta(t, .9, state.IdleThreshold, 1e-12)
			require.Equal(t, protocol.DeliverySafetySafe, state.Snapshot.DeliverySafety)
			require.True(t, state.AutomaticEligible,
				"reader and producer must apply the same detector-corroborated policy")
			require.Equal(t, StateEligibilityEligible, state.EligibilityReason)
		})
	}
}

func TestDetectorCorroborationBusyAndUnknownVetoSafety(t *testing.T) {
	for _, tc := range []struct {
		name       string
		busy       bool
		phase      protocol.AgentPhase
		confidence float64
		probs      map[string]float64
	}{
		{
			name: "detector_busy", busy: true,
			phase: protocol.AgentPhaseIdleAtPrompt, confidence: .4,
			probs: map[string]float64{
				string(protocol.AgentPhaseIdleAtPrompt): .4,
				string(protocol.AgentPhaseThinking):     .6,
			},
		},
		{
			name: "effective_unknown", phase: protocol.AgentPhaseUnknown, confidence: .6,
			probs: map[string]float64{
				string(protocol.AgentPhaseIdleAtPrompt): .4,
				string(protocol.AgentPhaseUnknown):      .6,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newExecutorFixture(t)
			guardVersion := configureDetectorRoute(t, fixture, protocol.HarnessCodex,
				protocol.AdapterTypeGenericPTY, protocol.InputGuardProviderTmuxAttach,
				protocol.CaptureModeTmuxAttach)
			recorder, err := gateway.NewSQLRecorder(fixture.store.DB())
			require.NoError(t, err)
			analyzer := &detectorPolicyAnalyzer{
				recorder: recorder, phase: tc.phase, confidence: tc.confidence,
				probabilities: tc.probs,
			}
			producer := newTestStateProducer(t, fixture, analyzer, detectorTestProfile())
			report := detectorObservation(protocol.HarnessCodex,
				protocol.OpaqueID("observation-"+tc.name), 2, guardVersion,
				executorTestTime.Add(time.Second))
			corroboration := detectorCorroboration(report, tc.busy)
			state := produceDetectorState(t, fixture, producer, report, &corroboration)

			require.NotEqual(t, protocol.DeliverySafetySafe, state.Snapshot.DeliverySafety)
			require.False(t, state.AutomaticEligible)
		})
	}
}

func TestDetectorCorroborationDoesNotChangeProxyOrNativeGuardThreshold(t *testing.T) {
	for _, tc := range []struct {
		name     string
		adapter  protocol.AdapterType
		provider protocol.InputGuardProvider
	}{
		{
			name: "pty_proxy", adapter: protocol.AdapterTypeGenericPTY,
			provider: protocol.InputGuardProviderPTYProxy,
		},
		{
			name: "native_adapter", adapter: protocol.AdapterTypeNativeAPI,
			provider: protocol.InputGuardProviderNativeAdapter,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newExecutorFixture(t)
			guardVersion := configureDetectorRoute(t, fixture, protocol.HarnessCodex,
				tc.adapter, tc.provider, protocol.CaptureModePTYProxy)
			producer := newTestStateProducer(t, fixture, detectorIdleAnalyzer(t, fixture),
				detectorTestProfile())
			report := stateObservation(protocol.OpaqueID("observation-"+tc.name), 2,
				executorTestTime.Add(time.Second))
			report.InputGuardVersion = guardVersion
			state := produceDetectorState(t, fixture, producer, report, nil)

			require.InDelta(t, .4, state.IdleProbability, 1e-12)
			require.NotEqual(t, protocol.DeliverySafetySafe, state.Snapshot.DeliverySafety)
			require.False(t, state.AutomaticEligible)
			require.Equal(t, StateEligibilityBelowThreshold, state.EligibilityReason)
		})
	}
}

func TestDetectorCorroborationRejectsDifferentObservation(t *testing.T) {
	fixture := newExecutorFixture(t)
	guardVersion := configureDetectorRoute(t, fixture, protocol.HarnessCodex,
		protocol.AdapterTypeGenericPTY, protocol.InputGuardProviderTmuxAttach,
		protocol.CaptureModeTmuxAttach)
	producer := newTestStateProducer(t, fixture, detectorIdleAnalyzer(t, fixture),
		detectorTestProfile())
	report := detectorObservation(protocol.HarnessCodex, "observation-bound", 2,
		guardVersion, executorTestTime.Add(time.Second))
	corroboration := detectorCorroboration(report, false)
	corroboration.observationID = "observation-other"

	require.ErrorIs(t, producer.submitTmuxCorroborated(report, corroboration),
		ErrObservationUnavailable)
	require.NotContains(t, producer.pending, report.AgentID)
}

func TestDetectorCorroborationCacheCannotLendEvidenceToAnotherObservation(t *testing.T) {
	fixture := newExecutorFixture(t)
	guardVersion := configureDetectorRoute(t, fixture, protocol.HarnessCodex,
		protocol.AdapterTypeGenericPTY, protocol.InputGuardProviderTmuxAttach,
		protocol.CaptureModeTmuxAttach)
	analyzer := detectorIdleAnalyzer(t, fixture)
	producer := newTestStateProducer(t, fixture, analyzer, detectorTestProfile())
	first := detectorObservation(protocol.HarnessCodex, "observation-cache-first", 2,
		guardVersion, executorTestTime.Add(time.Second))
	corroboration := detectorCorroboration(first, false)
	firstState := produceDetectorState(t, fixture, producer, first, &corroboration)
	require.Equal(t, protocol.DeliverySafetySafe, firstState.Snapshot.DeliverySafety)
	require.Equal(t, 1, analyzer.calls)

	second := detectorObservation(protocol.HarnessCodex, "observation-cache-second", 3,
		guardVersion, first.ObservedAt.Add(stateCommitInterval))
	second.OutputGeneration, second.EventGeneration = first.OutputGeneration, first.EventGeneration
	secondState := produceDetectorState(t, fixture, producer, second, nil)

	require.Equal(t, 2, analyzer.calls, "a new observation cannot borrow the old inference/evidence pair")
	require.NotEqual(t, protocol.DeliverySafetySafe, secondState.Snapshot.DeliverySafety)
	require.False(t, secondState.AutomaticEligible)
}

func TestDetectorCorroborationCannotCrossObservationRace(t *testing.T) {
	fixture := newExecutorFixture(t)
	guardVersion := configureDetectorRoute(t, fixture, protocol.HarnessCodex,
		protocol.AdapterTypeGenericPTY, protocol.InputGuardProviderTmuxAttach,
		protocol.CaptureModeTmuxAttach)
	recorder, err := gateway.NewSQLRecorder(fixture.store.DB())
	require.NoError(t, err)
	observedAt := executorTestTime.Add(time.Second)
	record := &protocol.GlimmerCallRecord{
		CallID: "call-detector-race", Contract: protocol.ModelContractAnalyzeAgentState,
		AgentID: executorTestAgent, InferenceProfile: protocol.InferenceProfileBase,
		BaseModelRepo: stateTestBaseModelRepo, BaseModelRevision: stateTestBaseModelRevision,
		Runtime: stateTestRuntime, RedactionPolicyVersion: stateTestRedactionPolicy,
		InputSHA256: detectorTestDigest, OutputSHA256: detectorTestDigest,
		AnalysisPolicyVersion: stateAnalysisPolicy,
		Probabilities: map[string]float64{
			string(protocol.AgentPhaseIdleAtPrompt): .95,
			string(protocol.AgentPhaseThinking):     .05,
		},
		Status: protocol.GlimmerCallStatusOK, CreatedAt: observedAt,
	}
	blocker := &blockingStateAnalyzer{
		started: make(chan struct{}), release: make(chan struct{}),
		recorder: recorder, record: record,
	}
	producer := newTestStateProducer(t, fixture, blocker, detectorTestProfile())
	first := detectorObservation(protocol.HarnessCodex, "observation-race-first", 2,
		guardVersion, observedAt)
	firstCorroboration := detectorCorroboration(first, false)
	fixture.clock.set(observedAt)
	require.NoError(t, producer.Demand(first.AgentID))
	require.NoError(t, producer.submitTmuxCorroborated(first, firstCorroboration))

	done := make(chan error, 1)
	go func() { done <- producer.produceOne(t.Context(), first.AgentID) }()
	<-blocker.started
	second := detectorObservation(protocol.HarnessCodex, "observation-race-second", 3,
		guardVersion, observedAt.Add(time.Millisecond))
	second.OutputGeneration, second.EventGeneration = first.OutputGeneration, first.EventGeneration
	secondCorroboration := detectorCorroboration(second, false)
	require.NoError(t, producer.submitTmuxCorroborated(second, secondCorroboration))
	close(blocker.release)
	require.ErrorIs(t, <-done, ErrObservationSuperseded)

	var committed int64
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT count(*)
		FROM state_snapshots WHERE observation_id = ?`, first.ObservationID).Scan(&committed))
	require.Zero(t, committed, "the first observation cannot carry its corroboration across the race")
}
