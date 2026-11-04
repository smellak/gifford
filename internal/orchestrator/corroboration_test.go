package orchestrator

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

// belowThreshold makes the fake analyst state idle, safe and below its
// calibrated threshold: every other check passed.
func belowThreshold(s *choiceStates) {
	s.eligible, s.reason = false, StateEligibilityBelowThreshold
	s.onRead = func(state *StateRead) {
		state.Snapshot.AgentPhase = protocol.AgentPhaseIdleAtPrompt
		state.Snapshot.DeliverySafety = protocol.DeliverySafetySafe
		state.IdleProbability = 0.91
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
		"no verdict": func(f *choiceFixture) { f.setDetector(nil) },
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
