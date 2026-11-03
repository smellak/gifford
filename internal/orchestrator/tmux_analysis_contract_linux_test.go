//go:build linux

package orchestrator

import (
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/daemon"
	"github.com/smellak/gifford/internal/daemon/agentlink"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy/tmuxattach"
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
	var newestID int64
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(), `SELECT rowid
		FROM delivery_queue WHERE message_id = ? AND agent_id = ?`,
		candidate.MessageID, candidate.AgentID).Scan(&newestID))
	require.Equal(t, []tmuxattach.PaneOperation{{
		Agent: string(candidate.AgentID), Unread: 1, NewestID: newestID,
	}}, f.journal.operations)
	var queueState string
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(), `SELECT state
		FROM delivery_queue WHERE message_id = ? AND agent_id = ?`,
		candidate.MessageID, candidate.AgentID).Scan(&queueState))
	require.Equal(t, queueStateDelivered, queueState)

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

func requireDetectorFallbackCommitted(
	t *testing.T,
	f *tmuxContractFixture,
	candidate daemon.QueueDelivery,
	reason tmuxDetectorFallbackReason,
) {
	t.Helper()
	require.Equal(t, queueStateDelivered, queueStateOf(t, f.store.DB(), candidate))
	require.Equal(t, 1, attemptCount(t, f.store.DB(), candidate.MessageID))
	require.Equal(t, 1, countRows(t, f.store.DB(), `SELECT count(*) FROM delivery_attempts
		WHERE message_id = ? AND state = 'COMMITTED'`, candidate.MessageID))
	require.Equal(t, 2, f.writes(t), "one literal notice and one Enter")
	guards := latestGuards(t, f.store.DB(), candidate.MessageID)
	require.Equal(t, protocol.GuardOutcomePass,
		guardOutcome(guards, protocol.GuardNameDetectorFallback))
	require.Equal(t, string(reason), guardDetail(guards, protocol.GuardNameDetectorFallback))
	require.Zero(t, countRows(t, f.store.DB(), `SELECT count(*) FROM approval_requests
		WHERE object_id = ?`, candidate.MessageID), "fallback never opens a default approval")
}

func TestTmuxAnalysisFailureFallsBackToFreeDetector(t *testing.T) {
	for index, test := range []struct {
		failure string
		reason  tmuxDetectorFallbackReason
	}{
		{tmuxContractError, tmuxFallbackAnalysisUnavailable},
		{tmuxContractTimeout, tmuxFallbackAnalysisTimeout},
		{tmuxContractUnavailable, tmuxFallbackAnalysisUnavailable},
		{tmuxContractInvalid, tmuxFallbackAnalysisUnavailable},
		{tmuxContractCircuitOpen, tmuxFallbackCircuitOpen},
	} {
		t.Run(test.failure, func(t *testing.T) {
			f := newTmuxContractFixture(t, true)
			f.model.failure = test.failure
			candidate := f.seedDelivery(t, 420+index)
			f.cycles(t, 6)
			require.NotEmpty(t, f.model.calls(protocol.ModelContractAnalyzeAgentState),
				"exercise the failing analyst")
			require.Empty(t, f.model.calls(protocol.ModelContractChooseOrchestrationAction))
			requireDetectorFallbackCommitted(t, f, candidate, test.reason)
		})
	}
}

func TestTmuxAnalysisMissingForTenSecondsFallsBackUsingExecutorClock(t *testing.T) {
	f := newTmuxContractFixture(t, true)
	candidate := f.seedDelivery(t, 430)

	_, err := f.worker.RunOnce(t.Context())
	require.NoError(t, err)
	require.True(t, f.states.HasQueuedObservation(candidate.AgentID, executorTestSession,
		f.states.reports[0].ObservationID), "leave the submitted analysis in flight")
	for range 9 {
		f.clock.set(f.clock.Now().Add(time.Second))
		_, err = f.worker.RunOnce(t.Context())
		require.NoError(t, err)
		f.requireNoAttempt(t)
	}
	// At ten seconds the same pass enters the detector path and commits using
	// the baseline already established by the preceding locked observations.
	f.clock.set(f.clock.Now().Add(time.Second))
	_, err = f.worker.RunOnce(t.Context())
	require.NoError(t, err)
	requireDetectorFallbackCommitted(t, f, candidate, tmuxFallbackAnalysisTimeout)
}

// The analyst's label never vetoes a screen that passed the detector rule. A
// phase other than idle, or none, sends the same pass to the deterministic
// route, which delivers under its own guards. The decision links the analyzed
// state, so the disagreement stays on record with the label the model gave.
func TestTmuxAnalystLabelNeverVetoesDetectorIdlePane(t *testing.T) {
	for index, test := range []struct {
		name    string
		arrange func(*tmuxContractFixture)
		phase   protocol.AgentPhase
	}{
		{"thinking", func(f *tmuxContractFixture) {
			f.model.analysisPhase = protocol.AgentPhaseThinking
		}, protocol.AgentPhaseThinking},
		{"error", func(f *tmuxContractFixture) {
			f.model.analysisPhase = protocol.AgentPhaseErrorOrExited
		}, protocol.AgentPhaseErrorOrExited},
		{"unknown", func(f *tmuxContractFixture) {
			f.model.analysisUnknown = true
		}, protocol.AgentPhaseUnknown},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newTmuxContractFixture(t, true)
			test.arrange(f)
			candidate := f.seedDelivery(t, 431+index)
			f.cycles(t, 6)
			requireDetectorFallbackCommitted(t, f, candidate, tmuxFallbackAnalystDisagrees)
			require.NotEmpty(t, f.model.calls(protocol.ModelContractAnalyzeAgentState))
			require.Empty(t, f.model.calls(protocol.ModelContractChooseOrchestrationAction),
				"a disagreeing label is not asked when to deliver")
			decision := lastDecision(t, f.store.DB(), candidate.MessageID)
			require.Equal(t, protocol.OrchestratorActionInjectPTYPrompt, decision.selected)
			var phase string
			require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
				`SELECT agent_phase FROM state_snapshots WHERE state_id = ?`,
				decision.stateID).Scan(&phase))
			require.Equal(t, string(test.phase), phase, "the decision links the disagreeing state")
		})
	}
}

// CHOOSE DEFER is Glimmer's choice of when to deliver on an idle label, so it
// holds the delivery and never falls back to the detector.
func TestTmuxChooseDeferHoldsTheDelivery(t *testing.T) {
	f := newTmuxContractFixture(t, true)
	f.model.chooseAction = protocol.OrchestratorActionDefer
	candidate := f.seedDelivery(t, 434)
	f.cycles(t, 6)
	f.requireNoAttempt(t)
	require.Equal(t, queueStateQueued, queueStateOf(t, f.store.DB(), candidate))
	require.Equal(t, protocol.OrchestratorActionDefer,
		lastDecision(t, f.store.DB(), candidate.MessageID).selected)
	require.Empty(t, guardDetail(latestGuards(t, f.store.DB(), candidate.MessageID),
		protocol.GuardNameDetectorFallback))
}

func TestTmuxChooseFailureFallsBackToDetector(t *testing.T) {
	for index, test := range []struct {
		failure string
		reason  tmuxDetectorFallbackReason
	}{
		{tmuxContractError, tmuxFallbackChooseFailed},
		{tmuxContractTimeout, tmuxFallbackChooseFailed},
		{tmuxContractCircuitOpen, tmuxFallbackCircuitOpen},
	} {
		t.Run(test.failure, func(t *testing.T) {
			f := newTmuxContractFixture(t, true)
			f.model.chooseFailure = test.failure
			candidate := f.seedDelivery(t, 440+index)
			f.cycles(t, 6)
			require.NotEmpty(t, f.model.calls(protocol.ModelContractChooseOrchestrationAction))
			requireDetectorFallbackCommitted(t, f, candidate, test.reason)
		})
	}
}

func TestTmuxModelOffFallsBackWithoutInference(t *testing.T) {
	f := newTmuxContractFixture(t, true)
	f.executor.model = nil
	candidate := f.seedDelivery(t, 450)
	f.cycles(t, 4)
	require.Empty(t, f.model.requests)
	requireDetectorFallbackCommitted(t, f, candidate, tmuxFallbackModelUnavailable)
}

func TestTmuxDetectorFallbackHonorsControlsAndPendingApproval(t *testing.T) {
	actor := channel.Actor{Type: protocol.ActorTypeHuman, ID: executorTestOperator}
	for _, test := range []struct {
		name          string
		arrange       func(*testing.T, *tmuxContractFixture, daemon.QueueDelivery)
		wantApprovals int
		wantDecision  bool
	}{
		{"manual", func(t *testing.T, f *tmuxContractFixture, candidate daemon.QueueDelivery) {
			off := false
			_, err := f.controller.SetAgent(t.Context(), protocol.SetAgentChannelParams{
				AgentID: candidate.AgentID, AutoDelivery: &off, ExpectedVersion: 0,
			}, actor)
			require.NoError(t, err)
		}, 0, true},
		{"paused", func(t *testing.T, f *tmuxContractFixture, _ daemon.QueueDelivery) {
			_, err := f.controller.SetGlobal(t.Context(), protocol.SetGlobalParams{
				Mode: protocol.ChannelModePaused, ExpectedVersion: 0,
			}, actor)
			require.NoError(t, err)
		}, 0, true},
		{"hold", func(t *testing.T, f *tmuxContractFixture, _ daemon.QueueDelivery) {
			_, err := f.controller.SetLink(t.Context(), protocol.SetLinkParams{
				FromAgent: executorTestSender, ToAgent: executorTestAgent,
				Mode: protocol.LinkModeHold, Plane: protocol.PlaneMessage,
				ExpectedVersions: protocol.ControlVersions{
					"link:" + string(executorTestSender) + "→" + string(executorTestAgent) + ":message": 0,
				},
			}, actor)
			require.NoError(t, err)
		}, 0, true},
		{"approval-required-without-default", func(t *testing.T, f *tmuxContractFixture, _ daemon.QueueDelivery) {
			_, err := f.controller.SetLink(t.Context(), protocol.SetLinkParams{
				FromAgent: executorTestSender, ToAgent: executorTestAgent,
				Mode: protocol.LinkModeRequireApproval, Plane: protocol.PlaneMessage,
				ExpectedVersions: protocol.ControlVersions{
					"link:" + string(executorTestSender) + "→" + string(executorTestAgent) + ":message": 0,
				},
			}, actor)
			require.NoError(t, err)
		}, 0, true},
		{"pending-approval", func(t *testing.T, f *tmuxContractFixture, candidate daemon.QueueDelivery) {
			tx, err := f.store.DB().BeginTx(t.Context(), nil)
			require.NoError(t, err)
			_, _, err = f.controller.EnsureOrchestratorApproval(t.Context(), tx,
				candidate.MessageID, candidate.AgentID, "human review already pending")
			require.NoError(t, err)
			require.NoError(t, tx.Commit())
		}, 1, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newTmuxContractFixture(t, true)
			f.executor.model = nil
			candidate := f.seedDelivery(t, 460)
			test.arrange(t, f, candidate)
			f.cycles(t, 4)
			f.requireNoAttempt(t)
			require.Equal(t, queueStateQueued, queueStateOf(t, f.store.DB(), candidate))
			require.Equal(t, test.wantApprovals, countRows(t, f.store.DB(),
				`SELECT count(*) FROM approval_requests WHERE object_id = ?`, candidate.MessageID))
			if test.wantDecision {
				require.Equal(t, string(tmuxFallbackModelUnavailable),
					guardDetail(latestGuards(t, f.store.DB(), candidate.MessageID),
						protocol.GuardNameDetectorFallback))
			} else {
				require.Zero(t, countRows(t, f.store.DB(), `SELECT count(*)
					FROM orchestrator_decisions WHERE message_id = ?`, candidate.MessageID),
					"the worker leaves a pending human approval untouched")
			}
		})
	}
}

func TestTmuxDetectorFallbackBusyDetectorDoesNotDeliver(t *testing.T) {
	f := newTmuxContractFixture(t, true)
	f.executor.model = nil
	f.setScreen(t, "working (esc to interrupt)\n$ ")
	candidate := f.seedDelivery(t, 470)
	f.cycles(t, 4)
	f.requireNoAttempt(t)
	guards := latestGuards(t, f.store.DB(), candidate.MessageID)
	require.Equal(t, protocol.GuardOutcomeFail,
		guardOutcome(guards, protocol.GuardNamePTYOutputQuiet))
	require.Equal(t, string(tmuxFallbackModelUnavailable),
		guardDetail(guards, protocol.GuardNameDetectorFallback))
	require.Zero(t, countRows(t, f.store.DB(),
		`SELECT count(*) FROM approval_requests WHERE object_id = ?`, candidate.MessageID))
}

func TestTmuxMappedReminderInheritsDetectorFallback(t *testing.T) {
	f := newTmuxContractFixture(t, true)
	f.executor.model = nil
	candidate := f.seedDelivery(t, 480)
	var created, expires string
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(), `SELECT created_at, expires_at
		FROM delivery_queue WHERE message_id = ? AND agent_id = ?`,
		candidate.MessageID, candidate.AgentID).Scan(&created, &expires))
	_, err := f.store.DB().ExecContext(t.Context(), `INSERT INTO bridge_reminders (
		source_node_id, expectation_id, generation, source_message_id, from_agent, to_agent,
		source_read_at, eligible_at, accepted_at, expires_at, state, reminder_id,
		gifford_message_id, created_at, updated_at
	) VALUES (?, ?, 1, 1, ?, ?, ?, ?, ?, ?, 'PENDING', ?, ?, ?, ?)`,
		"node-reminder-source", "rsp-detector-fallback", executorTestSender, executorTestAgent,
		created, created, created, expires, "reminder-detector-fallback", candidate.MessageID,
		created, created)
	require.NoError(t, err)

	f.cycles(t, 4)
	requireDetectorFallbackCommitted(t, f, candidate, tmuxFallbackModelUnavailable)
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

func TestTmuxAnalysisLongBusyPeriodStartsFreshIdleWindow(t *testing.T) {
	f := newTmuxContractFixture(t, true)
	candidate := f.seedDelivery(t, 4041)
	f.setScreen(t, "working (esc to interrupt)\n$ ")
	f.cycles(t, int(automaticStateWaitWindow/time.Second)+1)
	f.requireNoAttempt(t)
	require.Empty(t, f.model.calls(protocol.ModelContractAnalyzeAgentState))
	require.False(t, f.executor.automaticStateWaitTracked(candidate),
		"busy observations must not consume the analysis window")

	f.setScreen(t, tmuxContractIdleScreen)
	require.NoError(t, f.cycle(t))
	f.requireNoAttempt(t)
	require.Len(t, f.model.calls(protocol.ModelContractAnalyzeAgentState), 1)
	require.Empty(t, f.model.calls(protocol.ModelContractChooseOrchestrationAction),
		"the first idle pass waits for the newly submitted analysis")
	require.True(t, f.executor.automaticStateWaitTracked(candidate))

	require.NoError(t, f.cycle(t))
	require.Len(t, f.model.calls(protocol.ModelContractChooseOrchestrationAction), 1)
	require.Equal(t, 1, attemptCount(t, f.store.DB(), candidate.MessageID))
	decision := lastDecision(t, f.store.DB(), candidate.MessageID)
	require.Equal(t, protocol.OrchestratorActionInjectPTYPrompt, decision.selected)
	require.NotEmpty(t, decision.stateID, "CHOOSE must cite the fresh analyzed state")
	require.Empty(t, guardDetail(latestGuards(t, f.store.DB(), candidate.MessageID),
		protocol.GuardNameDetectorFallback))
}

func TestTmuxAnalysisChangingFreeObservationsStillTimesOut(t *testing.T) {
	f := newTmuxContractFixture(t, true)
	candidate := f.seedDelivery(t, 4042)

	f.setScreen(t, "completed fixture task\n$ ")
	_, err := f.worker.RunOnce(t.Context())
	require.NoError(t, err)
	f.requireNoAttempt(t)
	for second := 1; second < int(automaticStateWaitWindow/time.Second); second++ {
		f.clock.set(f.clock.Now().Add(time.Second))
		f.setScreen(t, "completed fixture task"+strings.Repeat(".", second)+"\n$ ")
		_, err = f.worker.RunOnce(t.Context())
		require.NoError(t, err)
		f.requireNoAttempt(t)
	}

	f.clock.set(f.clock.Now().Add(time.Second))
	f.setScreen(t, "completed fixture task after deadline\n$ ")
	_, err = f.worker.RunOnce(t.Context())
	require.NoError(t, err)
	require.Len(t, f.states.reports, int(automaticStateWaitWindow/time.Second)+1,
		"each changed free screen submits a new observation")
	require.Empty(t, f.model.calls(protocol.ModelContractAnalyzeAgentState),
		"leave every submitted observation without a state")
	requireDetectorFallbackCommitted(t, f, candidate, tmuxFallbackAnalysisTimeout)
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
