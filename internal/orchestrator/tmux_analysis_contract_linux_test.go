//go:build linux

package orchestrator

import (
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/daemon/agentlink"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/vt"
)

// Positive control for the doubles: this intentionally feeds the producer
// directly. The behavioral tests below do not; tmux must create their reports.
func TestTmuxAnalysisFixtureProducesEligibleState(t *testing.T) {
	f := newTmuxContractFixture(t, true)
	guards, err := agentlink.NewInputGuardStore(f.store.DB(), f.clock)
	require.NoError(t, err)
	written, err := guards.WriteInputGuard(t.Context(), protocol.InputGuardReport{
		AgentID: executorTestAgent, SessionID: executorTestSession,
		DraftState: protocol.DraftStateClosed, ActivityState: protocol.ActivityStateIdle,
		InputGuardVersion: 3, DetectionSource: protocol.InputGuardProviderTmuxAttach, Confidence: 1,
	})
	require.NoError(t, err)
	require.True(t, written)
	report := stateObservation("observation-tmux-fixture", 2, f.clock.Now())
	report.ScreenState = protocol.StateObservationScreenAvailable
	report.ScreenRows = []string{"completed fixture task", "$ "}
	report.Cols = 80
	report.Cursor = &protocol.StateObservationCursor{Row: 1, Col: 2}
	corroboration := detectorCorroboration(report, false)
	corroboration.draftGeneration = 0
	require.NoError(t, f.producer.submitTmuxCorroborated(report, corroboration))
	require.NoError(t, f.producer.Demand(report.AgentID))
	require.NoError(t, f.producer.produceOne(t.Context(), report.AgentID))
	state, err := f.producer.ReadLatestForAutomatic(t.Context(), f.store.DB(),
		executorTestAgent, executorTestSession, f.clock.Now(), &f.profile)
	require.NoError(t, err)
	require.True(t, state.AutomaticEligible, state.EligibilityReason)
	require.InDelta(t, .95, state.IdleProbability, .000001)
	require.Len(t, f.model.calls(protocol.ModelContractAnalyzeAgentState), 1)
	f.requireNoAttempt(t)
}

func TestTmuxAnalysisRefreshesExpiredStateBeforeChoosing(t *testing.T) {
	f := newTmuxContractFixture(t, true)
	candidate := f.seedDelivery(t, 401)
	f.cycles(t, 6)
	require.Len(t, f.model.calls(protocol.ModelContractAnalyzeAgentState), 1,
		"tmux must obtain one fresh ANALYZE, not bypass an expired snapshot")
	require.Len(t, f.model.calls(protocol.ModelContractChooseOrchestrationAction), 1)
	require.Equal(t, 1, attemptCount(t, f.store.DB(), candidate.MessageID))
	require.Equal(t, 2, f.writes(t), "one literal notice and one Enter")

	var stateID, observationID, analysisCallID, chooseCallID, model string
	var idle float64
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(), `SELECT od.state_id, ss.observation_id,
		ss.glimmer_call_id, od.glimmer_call_id, od.decision_model, ss.idle_probability
		FROM orchestrator_decisions od JOIN state_snapshots ss ON ss.state_id = od.state_id
		WHERE od.message_id = ? AND od.selected_action = 'INJECT_PTY_PROMPT'
		ORDER BY od.created_at DESC, od.decision_id DESC LIMIT 1`, candidate.MessageID).
		Scan(&stateID, &observationID, &analysisCallID, &chooseCallID, &model, &idle))
	require.NotEqual(t, string(f.initialState), stateID)
	require.NotEmpty(t, observationID)
	matched := false
	for _, report := range f.states.reports {
		if string(report.ObservationID) == observationID {
			matched = true
			require.True(t, report.ObservedAt.After(executorTestTime))
		}
	}
	require.True(t, matched, "the decision state comes from an actual tmux observation")
	require.NotEmpty(t, model)
	require.NotEqual(t, analysisCallID, chooseCallID)
	require.Equal(t, 1, countRows(t, f.store.DB(), `SELECT count(*) FROM glimmer_calls
		WHERE call_id = ? AND contract = 'ANALYZE_AGENT_STATE'`, analysisCallID))
	require.InDelta(t, .95, idle, .000001)
	choose := f.model.calls(protocol.ModelContractChooseOrchestrationAction)[0]
	var choice protocol.OrchestrationRequest
	require.NoError(t, json.Unmarshal([]byte(choose.Payload), &choice))
	require.InDelta(t, .95, choice.State.PIdle, .000001, "CHOOSE uses the analyzed probability")
}

func TestTmuxAnalysisUpgradesPreviousProducerVersionBeforeDelivery(t *testing.T) {
	f := newTmuxContractFixture(t, true)
	_, err := f.store.DB().ExecContext(t.Context(), `UPDATE state_snapshots
		SET producer_version = 'state-producer-5.5a'
		WHERE agent_id = ? AND session_id = ?`, executorTestAgent, executorTestSession)
	require.NoError(t, err)
	candidate := f.seedDelivery(t, 409)
	require.Equal(t, queueStateQueued, queueStateOf(t, f.store.DB(), candidate))

	f.cycles(t, 6)

	require.Equal(t, queueStateDelivered, queueStateOf(t, f.store.DB(), candidate))
	require.Equal(t, 1, attemptCount(t, f.store.DB(), candidate.MessageID))
	require.Equal(t, 2, f.writes(t))
	require.Len(t, f.model.calls(protocol.ModelContractAnalyzeAgentState), 1)
	var latestVersion string
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(), `SELECT producer_version
		FROM state_snapshots WHERE agent_id = ? AND session_id = ?
		ORDER BY observed_at DESC, state_version DESC LIMIT 1`,
		executorTestAgent, executorTestSession).Scan(&latestVersion))
	require.Equal(t, StateProducerVersion, latestVersion)
}

func TestTmuxAnalysisFailureDefersEvenWhenDetectorIsFree(t *testing.T) {
	for _, failure := range []string{tmuxContractError, tmuxContractTimeout, tmuxContractUnavailable, tmuxContractInvalid} {
		t.Run(failure, func(t *testing.T) {
			f := newTmuxContractFixture(t, true)
			f.model.failure = failure
			candidate := f.seedDelivery(t, 402)
			f.cycles(t, 6)
			f.requireNoAttempt(t)
			require.NotEmpty(t, f.model.calls(protocol.ModelContractAnalyzeAgentState), "exercise the failing analyst")
			require.Empty(t, f.model.calls(protocol.ModelContractChooseOrchestrationAction))
			require.Equal(t, protocol.OrchestratorActionDefer, lastDecision(t, f.store.DB(), candidate.MessageID).selected)
		})
	}
}

func TestTmuxAnalysisCaptureFailureHasNoCommitSent(t *testing.T) {
	f := newTmuxContractFixture(t, true)
	f.seedDelivery(t, 403)
	require.NoError(t, os.WriteFile(filepath.Join(f.dir, "capture-error"), nil, 0o600))
	for range 4 {
		// A typed capture error may propagate or be persisted as DEFER.
		// Neither disposition grants authority to write.
		_ = f.cycle(t)
	}
	f.requireNoAttempt(t)
	require.Empty(t, f.model.requests)
}

func TestTmuxAnalysisBusyToIdleRequiresFreshAnalysis(t *testing.T) {
	f := newTmuxContractFixture(t, true)
	candidate := f.seedDelivery(t, 404)
	f.setScreen(t, "working (esc to interrupt)\n$ ")
	f.cycles(t, 3)
	f.requireNoAttempt(t)
	require.Empty(t, f.model.calls(protocol.ModelContractChooseOrchestrationAction))
	require.Empty(t, f.model.calls(protocol.ModelContractAnalyzeAgentState))
	f.setScreen(t, tmuxContractIdleScreen)
	f.cycles(t, 6)
	require.Len(t, f.model.calls(protocol.ModelContractAnalyzeAgentState), 1,
		"the busy-to-idle transition must be observed and analyzed before choosing")
	require.Len(t, f.model.calls(protocol.ModelContractChooseOrchestrationAction), 1)
	require.Equal(t, 1, attemptCount(t, f.store.DB(), candidate.MessageID))
	require.Equal(t, 2, f.writes(t))
}

func TestTmuxAnalysisScreenChangeBetweenChoiceAndDeliveryDefers(t *testing.T) {
	f := newTmuxContractFixture(t, true)
	candidate := f.seedDelivery(t, 405)
	changed := false
	f.model.beforeChoose = func() {
		// Both screens have an empty prompt and pass the deterministic
		// detector: only binding the analyzed screen catches this race.
		require.NoError(t, os.WriteFile(filepath.Join(f.dir, "screen"), []byte("different result\n$ "), 0o600))
		changed = true
	}
	for range 6 {
		require.NoError(t, f.cycle(t))
		if changed {
			break
		}
	}
	f.requireNoAttempt(t)
	require.True(t, changed, "CHOOSE must run so the race is actually exercised")
	require.Equal(t, protocol.OrchestratorActionDefer, lastDecision(t, f.store.DB(), candidate.MessageID).selected)
}

func TestTmuxAnalysisNoQueuedDeliveryMakesNoModelCalls(t *testing.T) {
	f := newTmuxContractFixture(t, true)
	f.cycles(t, 20)
	f.requireNoAttempt(t)
	require.Empty(t, f.model.requests)
	require.Empty(t, f.producer.demanded)
	require.Empty(t, f.producer.pending)
	require.Empty(t, f.states.reports)
}

func TestTmuxAnalysisOpenDraftWithReplayedCursorElsewhereIsWithheld(t *testing.T) {
	f := newTmuxContractFixture(t, true)
	f.seedDelivery(t, 406)
	const draft = "UNSUBMITTED HUMAN DRAFT CANARY"
	// capture-pane is a rendered canvas, not the live cursor location.
	// Its trailing status row places the replay cursor below the draft.
	rawScreen := "$ " + draft + "\r\nstatus row\r\n"
	emulator, err := vt.New(80, 24)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, emulator.Close()) })
	_, err = emulator.Write([]byte(rawScreen))
	require.NoError(t, err)
	require.NotZero(t, emulator.Snapshot().CursorRow, "the replay cursor is not on the draft row")
	f.setScreen(t, rawScreen)
	f.cycles(t, 4)
	f.requireNoAttempt(t)
	require.Empty(t, f.model.requests, "an open tmux draft must not reach inference")
	for _, report := range f.states.reports {
		require.NotContains(t, strings.Join(report.ScreenRows, "\n"), draft,
			"even a discarded or subsequently masked observation must exclude the draft")
	}
	require.Empty(t, f.producer.pending, "no draft-bearing observation may enter the producer")
	rows, err := f.store.DB().QueryContext(t.Context(), "SELECT * FROM state_snapshots")
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()
	columns, err := rows.Columns()
	require.NoError(t, err)
	for rows.Next() {
		values := make([]any, len(columns))
		pointers := make([]any, len(columns))
		for i := range values {
			pointers[i] = &values[i]
		}
		require.NoError(t, rows.Scan(pointers...))
		raw, err := json.Marshal(values)
		require.NoError(t, err)
		require.NotContains(t, string(raw), draft)
	}
	require.NoError(t, rows.Err())
}

func TestTmuxAnalysisDisabledPreservesDeterministicDelivery(t *testing.T) {
	f := newTmuxContractFixture(t, false)
	candidate := f.seedDelivery(t, 407)
	f.cycles(t, 4)
	require.Equal(t, 1, attemptCount(t, f.store.DB(), candidate.MessageID))
	require.Equal(t, 2, f.writes(t))
	require.Empty(t, f.model.requests)
}

func TestTmuxAnalysisDecisionNeverAttributesExpiredSnapshot(t *testing.T) {
	f := newTmuxContractFixture(t, false)
	candidate := f.seedDelivery(t, 408)
	f.cycles(t, 4)
	rows, err := f.store.DB().QueryContext(t.Context(), `SELECT state_id FROM orchestrator_decisions
		WHERE message_id = ?`, candidate.MessageID)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()
	seen := 0
	for rows.Next() {
		var stateID sql.NullString
		require.NoError(t, rows.Scan(&stateID))
		require.NotEqual(t, string(f.initialState), strings.TrimSpace(stateID.String),
			"a deterministic decision cannot claim an expired, unanalyzed row as its basis")
		seen++
	}
	require.NoError(t, rows.Err())
	require.Positive(t, seen, "exercise persistEvaluation through a real delivery")
}
