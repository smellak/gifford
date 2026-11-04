package orchestrator

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/analyst"
	"github.com/smellak/gifford/internal/gateway"
	"github.com/smellak/gifford/internal/protocol"
)

// idleRecordAnalyzer records its call and reads the agent idle at its prompt
// with the confidence the recorded distribution gives that phase.
type idleRecordAnalyzer struct {
	recorder *gateway.SQLRecorder
	record   protocol.GlimmerCallRecord
	// phase is the analysis' phase; empty reads IDLE_AT_PROMPT.
	phase protocol.AgentPhase
}

func (a *idleRecordAnalyzer) Analyze(ctx context.Context, _ analyst.Observation) (analyst.Analysis, error) {
	if err := a.recorder.RecordCall(ctx, a.record, analyst.PromptVersion); err != nil {
		return analyst.Analysis{}, err
	}
	record := a.record
	phase := a.phase
	if phase == "" {
		phase = protocol.AgentPhaseIdleAtPrompt
	}
	return analyst.Analysis{
		AgentID: executorTestAgent, Route: analyst.RoutePTY,
		Phase:         phase,
		Confidence:    record.Probabilities[string(phase)],
		PromptVersion: analyst.PromptVersion, CallRecord: &record,
	}, nil
}

// The real producer marks a PTY state below its idle threshold UNCERTAIN,
// never SAFE, and the reader carries the same call's calibrated
// WAITING_APPROVAL plus ASKING_QUESTION probability that the configured
// limit then gates.
func TestProducedStateBelowThresholdCorroboratesOnlyUnderTheLimit(t *testing.T) {
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
		CallID: "call-state-corroborate", Contract: protocol.ModelContractAnalyzeAgentState,
		AgentID: executorTestAgent, InferenceProfile: protocol.InferenceProfileBase,
		BaseModelRepo: stateTestBaseModelRepo, BaseModelRevision: stateTestBaseModelRevision, Runtime: stateTestRuntime,
		RedactionPolicyVersion: stateTestRedactionPolicy, InputSHA256: digest, OutputSHA256: digest,
		Probabilities: map[string]float64{
			string(protocol.AgentPhaseIdleAtPrompt):    .7,
			string(protocol.AgentPhaseAskingQuestion):  .2,
			string(protocol.AgentPhaseWaitingApproval): .05,
			string(protocol.AgentPhaseThinking):        .05,
		},
		Status: protocol.GlimmerCallStatusOK, CreatedAt: executorTestTime.Add(time.Second),
	}
	producer := newTestStateProducer(t, fixture, &idleRecordAnalyzer{recorder: recorder, record: record}, profile)
	observed := executorTestTime.Add(time.Second)
	fixture.clock.set(observed)
	require.NoError(t, producer.Submit(stateObservation("observation-below", 2, observed)))
	require.NoError(t, producer.Demand(executorTestAgent))
	require.NoError(t, producer.produceOne(t.Context(), executorTestAgent))

	state, err := ReadLatestState(t.Context(), fixture.store.DB(), executorTestAgent,
		executorTestSession, observed.Add(time.Second), &profile)
	require.NoError(t, err)
	require.False(t, state.AutomaticEligible)
	require.Equal(t, StateEligibilityBelowThreshold, state.EligibilityReason)
	require.Equal(t, protocol.AgentPhaseIdleAtPrompt, state.Snapshot.AgentPhase)
	require.Equal(t, protocol.DeliverySafetyUncertain, state.Snapshot.DeliverySafety,
		"below the threshold the producer never derives SAFE")
	require.True(t, state.unsafePhaseValid)
	require.InDelta(t, .25, state.unsafePhaseMass, 1e-12)

	require.Equal(t, protocol.HarnessCodex, state.harness, "the harness is read from the session")
	fixture.executor.unsafePhaseLimit = .3
	require.False(t, fixture.executor.corroborable(state, false), "the session's test build has no measured profile")
	fixture.executor.corroborates = func(harness protocol.Harness, version string) bool {
		return harness == state.harness && version == state.harnessVersion
	}
	for limit, want := range map[float64]bool{0: false, .25: false, .2: false, .3: true} {
		fixture.executor.unsafePhaseLimit = limit
		require.Equal(t, want, fixture.executor.corroborable(state, false), "limit %v", limit)
	}
}

// A produced state that reads ASKING_QUESTION is NOT_IDLE, and the reader
// still carries its calibrated WAITING_APPROVAL plus ASKING_QUESTION mass:
// an answered question corroborates it, an open one never does.
func TestProducedAskingStateReadsItsMass(t *testing.T) {
	t.Parallel()

	fixture := newExecutorFixture(t)
	recorder, err := gateway.NewSQLRecorder(fixture.store.DB())
	require.NoError(t, err)
	digest := protocol.SHA256Hex("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	profile := StateAnalysisProfile{
		Revision: stateTestProfileRevision, SHA256: digest,
		CalibrationRevision: stateTestCalibrationRevision, CalibrationSHA256: digest,
		Temperature: 1, IdleThreshold: .9,
	}
	record := protocol.GlimmerCallRecord{
		CallID: "call-state-asking", Contract: protocol.ModelContractAnalyzeAgentState,
		AgentID: executorTestAgent, InferenceProfile: protocol.InferenceProfileBase,
		BaseModelRepo: stateTestBaseModelRepo, BaseModelRevision: stateTestBaseModelRevision, Runtime: stateTestRuntime,
		RedactionPolicyVersion: stateTestRedactionPolicy, InputSHA256: digest, OutputSHA256: digest,
		Probabilities: map[string]float64{
			string(protocol.AgentPhaseIdleAtPrompt):    .02,
			string(protocol.AgentPhaseAskingQuestion):  .9,
			string(protocol.AgentPhaseWaitingApproval): .03,
			string(protocol.AgentPhaseThinking):        .05,
		},
		Status: protocol.GlimmerCallStatusOK, CreatedAt: executorTestTime.Add(time.Second),
	}
	producer := newTestStateProducer(t, fixture, &idleRecordAnalyzer{
		recorder: recorder, record: record, phase: protocol.AgentPhaseAskingQuestion,
	}, profile)
	observed := executorTestTime.Add(time.Second)
	fixture.clock.set(observed)
	require.NoError(t, producer.Submit(stateObservation("observation-asking", 2, observed)))
	require.NoError(t, producer.Demand(executorTestAgent))
	require.NoError(t, producer.produceOne(t.Context(), executorTestAgent))

	state, err := ReadLatestState(t.Context(), fixture.store.DB(), executorTestAgent,
		executorTestSession, observed.Add(time.Second), &profile)
	require.NoError(t, err)
	require.False(t, state.AutomaticEligible)
	require.Equal(t, StateEligibilityNotIdle, state.EligibilityReason)
	require.Equal(t, protocol.AgentPhaseAskingQuestion, state.Snapshot.AgentPhase)
	require.True(t, state.unsafePhaseValid)
	require.InDelta(t, .93, state.unsafePhaseMass, 1e-12)

	fixture.executor.unsafePhaseLimit = .3
	fixture.executor.corroborates = func(harness protocol.Harness, version string) bool {
		return harness == state.harness && version == state.harnessVersion
	}
	require.Equal(t, protocol.DeliverySafetyUncertain, state.Snapshot.DeliverySafety)
	require.False(t, fixture.executor.corroborable(state, false), "the question is open")
	require.True(t, fixture.executor.corroborable(state, true), "the question is answered")
}
