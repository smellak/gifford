package orchestrator

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

// belowThreshold makes the fake analyst state idle and below its calibrated
// threshold, every other check passed, with the UNCERTAIN delivery safety the
// producer derives from that same probability.
func belowThreshold(s *choiceStates) {
	s.eligible, s.reason = false, StateEligibilityBelowThreshold
	s.onRead = func(state *StateRead) {
		state.Snapshot.AgentPhase = protocol.AgentPhaseIdleAtPrompt
		state.Snapshot.DeliverySafety = protocol.DeliverySafetyUncertain
		state.IdleProbability = 0.49
		state.unsafePhaseMass, state.unsafePhaseValid = 0.01, true
	}
}

// corroborationTestLimit is the fixture's configured unsafe-phase limit.
const corroborationTestLimit = 0.05

func TestCorroborableNeedsAnIdleStateBelowTheUnsafePhaseLimit(t *testing.T) {
	t.Parallel()
	e := &Executor{unsafePhaseLimit: corroborationTestLimit}
	for name, test := range map[string]struct {
		phase  protocol.AgentPhase
		safety protocol.DeliverySafety
		reason string
		mass   float64
		valid  bool
		limit  float64
		want   bool
	}{
		"uncertain below the limit": {protocol.AgentPhaseIdleAtPrompt, protocol.DeliverySafetyUncertain, StateEligibilityBelowThreshold, .01, true, corroborationTestLimit, true},
		"safe below the limit":      {protocol.AgentPhaseIdleAtPrompt, protocol.DeliverySafetySafe, StateEligibilityBelowThreshold, .01, true, corroborationTestLimit, true},
		"at the limit":              {protocol.AgentPhaseIdleAtPrompt, protocol.DeliverySafetyUncertain, StateEligibilityBelowThreshold, corroborationTestLimit, true, corroborationTestLimit, false},
		"no limit configured":       {protocol.AgentPhaseIdleAtPrompt, protocol.DeliverySafetyUncertain, StateEligibilityBelowThreshold, .01, true, 0, false},
		"mass not read":             {protocol.AgentPhaseIdleAtPrompt, protocol.DeliverySafetyUncertain, StateEligibilityBelowThreshold, 0, false, corroborationTestLimit, false},
		"unsafe":                    {protocol.AgentPhaseIdleAtPrompt, protocol.DeliverySafetyUnsafe, StateEligibilityBelowThreshold, .01, true, corroborationTestLimit, false},
		"asking a question":         {protocol.AgentPhaseAskingQuestion, protocol.DeliverySafetyUncertain, StateEligibilityBelowThreshold, .01, true, corroborationTestLimit, false},
		"another reason":            {protocol.AgentPhaseIdleAtPrompt, protocol.DeliverySafetyUncertain, StateEligibilityNotIdle, .01, true, corroborationTestLimit, false},
	} {
		e.unsafePhaseLimit = test.limit
		state := StateRead{EligibilityReason: test.reason, unsafePhaseMass: test.mass, unsafePhaseValid: test.valid}
		state.Snapshot.AgentPhase, state.Snapshot.DeliverySafety = test.phase, test.safety
		require.Equal(t, test.want, e.corroborable(state), name)
	}
}

// corroborationFixture is the wired executor on a measured build, its
// analyst state below threshold, with an eligible detector verdict.
func corroborationFixture(t *testing.T) *choiceFixture {
	t.Helper()
	f := newChoiceFixture(t)
	useHarnessVersion(t, f.executorFixture, detectorTestMeasuredVersion)
	f.states.set(belowThreshold)
	f.setDetector(eligibleDetector(f.clock.Now()))
	f.executor.unsafePhaseLimit = corroborationTestLimit
	return f
}

// An analyst state that misses only its threshold passes when the detector
// corroborates it: the model still chooses, and the decision names the
// corroborated policy and carries detector_fallback.
func TestDetectorCorroboratesAStateBelowThreshold(t *testing.T) {
	t.Parallel()
	f := corroborationFixture(t)
	candidate := f.seedDelivery(t, 1)
	require.NoError(t, f.executor.EvaluateAutomatic(t.Context(), candidate))

	require.Len(t, f.model.calls(choiceChoose), 1, "the model still chooses")
	decision := lastDecision(t, f.store.DB(), candidate.MessageID)
	require.Equal(t, choiceInject, decision.selected)
	require.Equal(t, CorroboratedPTYPolicy, decision.policy)
	require.Equal(t, protocol.GuardOutcomePass, guardOutcome(decision.guards, protocol.GuardNamePIdleThreshold))
	require.Equal(t, protocol.GuardOutcomePass, guardOutcome(decision.guards, protocol.GuardNameDetectorFallback))
	require.Equal(t, 1, attemptCount(t, f.store.DB(), candidate.MessageID))
}

// Without corroboration the state below threshold keeps its approval: an
// unmeasured build, no verdict, or a state that fails for any other reason.
func TestCorroborationNeedsAMeasuredVerdictOnAStateBelowThreshold(t *testing.T) {
	t.Parallel()
	for name, change := range map[string]func(f *choiceFixture){
		"unmeasured build": func(f *choiceFixture) {
			useHarnessVersion(t, f.executorFixture, executorTestHarnessVersion)
		},
		"verdict missing": func(f *choiceFixture) { f.setDetector(nil) },
		"not idle": func(f *choiceFixture) {
			f.states.set(func(s *choiceStates) { s.reason = StateEligibilityNotIdle })
		},
		"delivery unsafe": func(f *choiceFixture) {
			f.states.set(func(s *choiceStates) {
				s.onRead = func(state *StateRead) {
					state.Snapshot.AgentPhase = protocol.AgentPhaseIdleAtPrompt
					state.Snapshot.DeliverySafety = protocol.DeliverySafetyUnsafe
				}
			})
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := corroborationFixture(t)
			change(f)
			candidate := f.seedDelivery(t, 2)
			require.NoError(t, f.executor.EvaluateAutomatic(t.Context(), candidate))

			require.Empty(t, f.model.calls(choiceChoose))
			decision := lastDecision(t, f.store.DB(), candidate.MessageID)
			require.Equal(t, []protocol.OrchestratorAction{choiceDefer}, decision.allowed)
			require.NotEqual(t, CorroboratedPTYPolicy, decision.policy)
			require.Equal(t, protocol.GuardOutcomeFail, guardOutcome(decision.guards, protocol.GuardNamePIdleThreshold))
			require.Zero(t, attemptCount(t, f.store.DB(), candidate.MessageID))
			require.Len(t, pendingNotes(t, f.store.DB(), candidate.MessageID), 1)
		})
	}
}

// A corroboration that can still come, a busy harness or a screen not yet
// quiet, is waited out: no decision and no approval.
func TestCorroborationWaitsOutABusyScreen(t *testing.T) {
	t.Parallel()
	f := corroborationFixture(t)
	d := eligibleDetector(f.clock.Now())
	d.Verdict.Busy = true
	f.setDetector(d)
	candidate := f.seedDelivery(t, 3)
	require.NoError(t, f.executor.EvaluateAutomatic(t.Context(), candidate))
	require.Empty(t, decisionsFor(t, f.store.DB(), candidate.MessageID))
	require.Empty(t, pendingNotes(t, f.store.DB(), candidate.MessageID))
	note, waiting := f.executor.DetectorWait(executorTestAgent)
	require.True(t, waiting)
	require.Contains(t, note, string(protocol.GuardNamePIdleThreshold))

	f.setDetector(eligibleDetector(f.clock.Now()))
	require.NoError(t, f.executor.EvaluateAutomatic(t.Context(), candidate))
	require.Equal(t, CorroboratedPTYPolicy, lastDecision(t, f.store.DB(), candidate.MessageID).policy)
}
