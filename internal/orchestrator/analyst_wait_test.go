package orchestrator

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

// An analyst state that refuses for a passing reason, the agent in a
// transient phase or an analysis that is not current, opens no approval:
// the pass rolls back with the hold named for the operator, a stale
// analysis is demanded again, and the next pass delivers once the agent is
// idle. A refusal that will not pass by itself still goes to the human.
func TestAnalystRouteWaitsOutATransientState(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		reason  string
		phase   protocol.AgentPhase
		waits   bool
		demands int
		hold    string
	}{
		"asking a question":  {StateEligibilityNotIdle, protocol.AgentPhaseAskingQuestion, true, 0, "NOT_IDLE_AT_PROMPT (ASKING_QUESTION)"},
		"waiting approval":   {StateEligibilityNotIdle, protocol.AgentPhaseWaitingApproval, true, 0, "(WAITING_APPROVAL)"},
		"thinking":           {StateEligibilityNotIdle, protocol.AgentPhaseThinking, true, 0, "(THINKING_STREAMING)"},
		"running a tool":     {StateEligibilityNotIdle, protocol.AgentPhaseRunningTool, true, 0, "(RUNNING_TOOL)"},
		"stale":              {StateEligibilityStale, protocol.AgentPhaseUnknown, true, 1, StateEligibilityStale},
		"expired":            {StateEligibilityExpired, protocol.AgentPhaseIdleAtPrompt, true, 1, StateEligibilityExpired},
		"exited":             {StateEligibilityNotIdle, protocol.AgentPhaseErrorOrExited, false, 0, ""},
		"unknown phase":      {StateEligibilityUnknownPhase, protocol.AgentPhaseUnknown, false, 0, ""},
		"delivery unsafe":    {StateEligibilityDeliveryUnsafe, protocol.AgentPhaseIdleAtPrompt, false, 0, ""},
		"no model evidence":  {StateEligibilityNoModelEvidence, protocol.AgentPhaseIdleAtPrompt, false, 0, ""},
		"session changed":    {StateEligibilitySessionChanged, protocol.AgentPhaseIdleAtPrompt, false, 0, ""},
		"profile changed":    {StateEligibilityProfileChanged, protocol.AgentPhaseIdleAtPrompt, false, 0, ""},
		"call invalid":       {StateEligibilityCallInvalid, protocol.AgentPhaseIdleAtPrompt, false, 0, ""},
		"native informative": {StateEligibilityNativeInformative, protocol.AgentPhaseIdleAtPrompt, false, 0, ""},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := newChoiceFixture(t)
			f.states.set(func(s *choiceStates) {
				s.eligible, s.reason = false, tc.reason
				s.onRead = func(state *StateRead) { state.Snapshot.AgentPhase = tc.phase }
			})
			candidate := f.seedDelivery(t, 3)
			// Two passes past the fresh-analysis window, so an analysis
			// reason is decided rather than waited on.
			require.NoError(t, f.executor.EvaluateAutomatic(t.Context(), candidate))
			f.clock.set(f.clock.Now().Add(automaticStateWaitWindow + time.Second))
			require.NoError(t, f.executor.EvaluateAutomatic(t.Context(), candidate))
			approvals := countRows(t, f.store.DB(),
				"SELECT count(*) FROM approval_requests WHERE object_id = ?", candidate.MessageID)
			note, waiting := f.executor.DetectorWait(executorTestAgent)
			if !tc.waits {
				require.Equal(t, 1, approvals, "the human decides")
				require.False(t, waiting)
				return
			}
			require.Zero(t, approvals, "no approval while the state passes")
			require.Empty(t, decisionsFor(t, f.store.DB(), candidate.MessageID))
			require.True(t, waiting)
			require.Contains(t, note, string(protocol.GuardNamePIdleThreshold))
			require.Contains(t, note, tc.hold)
			f.states.mu.Lock()
			demanded := len(f.states.demands)
			f.states.mu.Unlock()
			require.GreaterOrEqual(t, demanded, tc.demands)
			if automaticStateReasonNeedsAnalysis(tc.reason) {
				// Past the re-analysis bound the human decides.
				f.clock.set(f.clock.Now().Add(analystReanalysisLimit))
				require.NoError(t, f.executor.EvaluateAutomatic(t.Context(), candidate))
				require.Equal(t, 1, countRows(t, f.store.DB(),
					"SELECT count(*) FROM approval_requests WHERE object_id = ?", candidate.MessageID))
				_, waiting = f.executor.DetectorWait(executorTestAgent)
				require.False(t, waiting)
				return
			}

			f.states.set(func(s *choiceStates) { s.eligible, s.reason, s.onRead = true, StateEligibilityEligible, nil })
			require.NoError(t, f.executor.EvaluateAutomatic(t.Context(), candidate))
			_, waiting = f.executor.DetectorWait(executorTestAgent)
			require.False(t, waiting)
			require.NotEmpty(t, decisionsFor(t, f.store.DB(), candidate.MessageID), "delivered once idle")
		})
	}
}
