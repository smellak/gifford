package orchestrator

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/detect"
	"github.com/smellak/gifford/internal/profiles"
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
	e := &Executor{unsafePhaseLimit: corroborationTestLimit, corroborates: corroboratesEverything}
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
		"asking, not idle":          {protocol.AgentPhaseAskingQuestion, protocol.DeliverySafetyUncertain, StateEligibilityNotIdle, .01, true, corroborationTestLimit, false},
		"another reason":            {protocol.AgentPhaseIdleAtPrompt, protocol.DeliverySafetyUncertain, StateEligibilityNotIdle, .01, true, corroborationTestLimit, false},
	} {
		e.unsafePhaseLimit = test.limit
		state := StateRead{EligibilityReason: test.reason, unsafePhaseMass: test.mass, unsafePhaseValid: test.valid}
		state.Snapshot.AgentPhase, state.Snapshot.DeliverySafety = test.phase, test.safety
		require.Equal(t, test.want, e.corroborable(state, false), name)
	}
	// An answered question sets only the mass aside: the limit must still be
	// configured and the mass read.
	e.unsafePhaseLimit = corroborationTestLimit
	over := StateRead{EligibilityReason: StateEligibilityBelowThreshold, unsafePhaseMass: .4, unsafePhaseValid: true}
	over.Snapshot.AgentPhase, over.Snapshot.DeliverySafety = protocol.AgentPhaseIdleAtPrompt, protocol.DeliverySafetyUncertain
	require.False(t, e.corroborable(over, false), "over the limit")
	require.True(t, e.corroborable(over, true), "over the limit, question answered")
	unread := over
	unread.unsafePhaseValid = false
	require.False(t, e.corroborable(unread, true), "mass not read")
	e.unsafePhaseLimit = 0
	require.False(t, e.corroborable(over, true), "no limit configured")

	// An answered question read as ASKING_QUESTION counts as idle, with the
	// same limit, mass, safety and reason conditions.
	e.unsafePhaseLimit = corroborationTestLimit
	asking := over
	asking.EligibilityReason, asking.Snapshot.AgentPhase = StateEligibilityNotIdle, protocol.AgentPhaseAskingQuestion
	require.True(t, e.corroborable(asking, true), "asking, question answered")
	require.False(t, e.corroborable(asking, false), "asking, not answered")
	for name, change := range map[string]func(*StateRead){
		"mass not read":       func(s *StateRead) { s.unsafePhaseValid = false },
		"unsafe":              func(s *StateRead) { s.Snapshot.DeliverySafety = protocol.DeliverySafetyUnsafe },
		"another reason":      func(s *StateRead) { s.EligibilityReason = StateEligibilityStale },
		"another phase":       func(s *StateRead) { s.Snapshot.AgentPhase = protocol.AgentPhaseWaitingApproval },
		"eligible on its own": func(s *StateRead) { s.AutomaticEligible = true },
	} {
		changed := asking
		change(&changed)
		require.False(t, e.corroborable(changed, true), name)
	}
	e.unsafePhaseLimit = 0
	require.False(t, e.corroborable(asking, true), "asking, no limit configured")
}

// The analyst reads an answered question still on screen as ASKING mass
// over the limit; the detector, which saw the human's reply after it,
// corroborates anyway, and the guard says why. Without that verdict the
// same state goes to the human.
func TestAnsweredQuestionCorroboratesOverTheLimit(t *testing.T) {
	t.Parallel()
	overLimit := func(f *choiceFixture) {
		f.states.set(func(s *choiceStates) {
			previous := s.onRead
			s.onRead = func(state *StateRead) {
				if previous != nil {
					previous(state)
				}
				state.unsafePhaseMass = .4
			}
		})
	}
	f := corroborationFixture(t)
	overLimit(f)
	candidate := f.seedDelivery(t, 3)
	require.NoError(t, f.executor.EvaluateAutomatic(t.Context(), candidate))
	require.Len(t, pendingNotes(t, f.store.DB(), candidate.MessageID), 1, "the human decides")

	g := corroborationFixture(t)
	overLimit(g)
	d := eligibleDetector(g.clock.Now())
	d.Verdict.Detail = detect.DetailQuestionAnswered
	g.setDetector(d)
	candidate = g.seedDelivery(t, 3)
	require.NoError(t, g.executor.EvaluateAutomatic(t.Context(), candidate))
	require.Empty(t, pendingNotes(t, g.store.DB(), candidate.MessageID))
	require.Equal(t, CorroboratedPTYPolicy, lastDecision(t, g.store.DB(), candidate.MessageID).policy)
}

// The analyst keeps reading an answered question as ASKING_QUESTION, so
// the state is NOT_IDLE rather than below its threshold; the detector, which
// saw the human's reply and the agent's turn after it, corroborates anyway.
// Without that verdict the same state never delivers on its own.
func TestAnsweredQuestionCorroboratesAnAskingPhase(t *testing.T) {
	t.Parallel()
	asking := func(f *choiceFixture) {
		f.states.set(func(s *choiceStates) {
			s.eligible, s.reason = false, StateEligibilityNotIdle
			previous := s.onRead
			s.onRead = func(state *StateRead) {
				if previous != nil {
					previous(state)
				}
				state.Snapshot.AgentPhase = protocol.AgentPhaseAskingQuestion
				state.unsafePhaseMass = .4
			}
		})
	}
	f := corroborationFixture(t)
	asking(f)
	candidate := f.seedDelivery(t, 3)
	require.NoError(t, f.executor.EvaluateAutomatic(t.Context(), candidate))
	require.Zero(t, attemptCount(t, f.store.DB(), candidate.MessageID))
	for _, decision := range decisionsFor(t, f.store.DB(), candidate.MessageID) {
		require.NotEqual(t, CorroboratedPTYPolicy, decision.policy)
	}

	g := corroborationFixture(t)
	asking(g)
	d := eligibleDetector(g.clock.Now())
	d.Verdict.Detail = detect.DetailQuestionAnswered
	g.setDetector(d)
	candidate = g.seedDelivery(t, 3)
	require.NoError(t, g.executor.EvaluateAutomatic(t.Context(), candidate))
	require.Empty(t, pendingNotes(t, g.store.DB(), candidate.MessageID))
	require.Equal(t, CorroboratedPTYPolicy, lastDecision(t, g.store.DB(), candidate.MessageID).policy)
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
	f.executor.corroborates = corroboratesEverything
	return f
}

// corroboratesEverything stands in for a profile that sets corroboration.
func corroboratesEverything(protocol.Harness, string) bool { return true }

// Corroboration is per harness build: only a measured profile that sets it
// corroborates; an unmeasured build never does.
func TestCorroborationFollowsTheHarnessProfile(t *testing.T) {
	t.Parallel()
	loaded, err := profiles.Load()
	require.NoError(t, err)
	corroborates := profileCorroborates(loaded)
	require.True(t, corroborates(protocol.HarnessClaudeCode, "2.1.280 (Claude Code)"))
	require.True(t, corroborates(protocol.HarnessMuseCode, "1.4.14-R7309.1"))
	require.True(t, corroborates(protocol.HarnessCodex, "codex-cli 0.154.0"))
	require.True(t, corroborates(protocol.HarnessOpencode, "1.18.29"))
	require.False(t, corroborates(protocol.HarnessClaudeCode, "2.1.300 (Claude Code)"), "unmeasured build")
	require.False(t, corroborates(protocol.HarnessClaudeCode, ""))

	e := &Executor{unsafePhaseLimit: corroborationTestLimit, corroborates: corroborates}
	state := StateRead{
		EligibilityReason: StateEligibilityBelowThreshold, unsafePhaseMass: .01, unsafePhaseValid: true,
		harness: protocol.HarnessClaudeCode, harnessVersion: "2.1.300 (Claude Code)",
	}
	state.Snapshot.AgentPhase, state.Snapshot.DeliverySafety = protocol.AgentPhaseIdleAtPrompt, protocol.DeliverySafetyUncertain
	require.False(t, e.corroborable(state, false), "an unmeasured build stays on the threshold")
	state.harness, state.harnessVersion = protocol.HarnessClaudeCode, "2.1.280 (Claude Code)"
	require.True(t, e.corroborable(state, false))
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
