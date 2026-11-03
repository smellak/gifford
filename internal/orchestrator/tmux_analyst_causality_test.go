//go:build linux

package orchestrator

// These tests pin the analyst route's own guarantees on the tmux analysis
// runtime values: per-message freshness, the mismatch reason with its
// causality, and the pre-write abort without a physical write. They use the
// contract fixture as a black box; the fixture itself is never modified
// here.

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/tmuxlock"
)

// TestTmuxAnalysisForeignOwnerWaitsBeforeAnalyze pins the write-authority
// preflight: a pane owned by another writer remains queued without spending
// an ANALYZE call or opening an attempt on every pass. Once Gifford owns the
// same pane, the unchanged analyst route resumes normally.
func TestTmuxAnalysisForeignOwnerWaitsBeforeAnalyze(t *testing.T) {
	f := newTmuxContractFixture(t, true)
	candidate := f.seedDelivery(t, 508)
	f.journal.checkErr = &tmuxlock.GuardError{Reason: tmuxlock.GuardForeignOwner}

	f.cycles(t, 4)
	require.Positive(t, f.journal.checks, "each pass checks the pane authority")
	require.Empty(t, f.model.calls(protocol.ModelContractAnalyzeAgentState))
	require.Empty(t, f.model.calls(protocol.ModelContractChooseOrchestrationAction))
	require.Zero(t, attemptCount(t, f.store.DB(), candidate.MessageID))
	f.requireNoAttempt(t)

	// Model unavailability must not bypass the same authority barrier via
	// the deterministic fallback and create an attempt on a foreign pane.
	f.executor.model = nil
	checks := f.journal.checks
	f.cycles(t, 4)
	require.Greater(t, f.journal.checks, checks, "the unchanged guard is re-read without analysis")
	require.Empty(t, f.model.calls(protocol.ModelContractAnalyzeAgentState))
	require.Zero(t, attemptCount(t, f.store.DB(), candidate.MessageID))

	f.executor.model = f.model
	f.journal.checkErr = nil
	f.cycles(t, 6)
	require.Len(t, f.model.calls(protocol.ModelContractAnalyzeAgentState), 1,
		"Gifford ownership restores the existing analysis path")
	require.Len(t, f.model.calls(protocol.ModelContractChooseOrchestrationAction), 1)
	require.Equal(t, 1, attemptCount(t, f.store.DB(), candidate.MessageID))
}

func TestTmuxAnalysisForeignOwnerClearsAnOpenAnalysisWindow(t *testing.T) {
	f := newTmuxContractFixture(t, true)
	candidate := f.seedDelivery(t, 5081)

	// Open the wait on a valid Gifford-owned observation, then make pane
	// authority unavailable for longer than the whole analysis window.
	require.NoError(t, f.cycle(t))
	require.True(t, f.executor.automaticStateWaitTracked(candidate))
	require.Len(t, f.model.calls(protocol.ModelContractAnalyzeAgentState), 1)
	f.journal.checkErr = &tmuxlock.GuardError{Reason: tmuxlock.GuardForeignOwner}
	f.cycles(t, int(automaticStateWaitWindow/time.Second)+1)
	require.False(t, f.executor.automaticStateWaitTracked(candidate),
		"a pre-observation authority wait must discard the old window")
	f.requireNoAttempt(t)
	require.Empty(t, f.model.calls(protocol.ModelContractChooseOrchestrationAction))

	// Returning ownership to Gifford gets a new bounded wait. The expired
	// analysis is replaced, and CHOOSE cites that fresh state instead of
	// falling through to ANALYSIS_TIMEOUT.
	f.journal.checkErr = nil
	require.NoError(t, f.cycle(t))
	f.requireNoAttempt(t)
	require.Len(t, f.model.calls(protocol.ModelContractAnalyzeAgentState), 2)
	require.Empty(t, f.model.calls(protocol.ModelContractChooseOrchestrationAction))
	require.True(t, f.executor.automaticStateWaitTracked(candidate))

	require.NoError(t, f.cycle(t))
	require.Len(t, f.model.calls(protocol.ModelContractChooseOrchestrationAction), 1)
	require.Equal(t, 1, attemptCount(t, f.store.DB(), candidate.MessageID))
	decision := lastDecision(t, f.store.DB(), candidate.MessageID)
	require.Equal(t, protocol.OrchestratorActionInjectPTYPrompt, decision.selected)
	require.NotEmpty(t, decision.stateID, "CHOOSE must cite the replacement state")
	require.Empty(t, guardDetail(latestGuards(t, f.store.DB(), candidate.MessageID),
		protocol.GuardNameDetectorFallback))
}

// TestTmuxAnalysisSecondDeliveryWithUnchangedScreenRequiresFreshAnalysis is the two-message canary:
// a later message with the identical projected screen must observe and
// analyze on its own, never reuse the resolved message's observation.
func TestTmuxAnalysisSecondDeliveryWithUnchangedScreenRequiresFreshAnalysis(t *testing.T) {
	f := newTmuxContractFixture(t, true)
	first := f.seedDelivery(t, 501)
	f.cycles(t, 6)
	firstDecision := lastDecision(t, f.store.DB(), first.MessageID)
	require.Equal(t, protocol.OrchestratorActionInjectPTYPrompt, firstDecision.selected)
	require.Len(t, f.model.calls(protocol.ModelContractAnalyzeAgentState), 1)
	require.Len(t, f.model.calls(protocol.ModelContractChooseOrchestrationAction), 1)

	second := f.seedDelivery(t, 502)
	f.cycles(t, 6)
	secondDecision := lastDecision(t, f.store.DB(), second.MessageID)
	require.Equal(t, protocol.OrchestratorActionInjectPTYPrompt, secondDecision.selected)
	require.Len(t, f.model.calls(protocol.ModelContractChooseOrchestrationAction), 2)
	require.Len(t, f.states.reports, 2, "each message submits its own observation")
	require.NotEqual(t, f.states.reports[0].ObservationID, f.states.reports[1].ObservationID)
	require.NotEqual(t, firstDecision.stateID, secondDecision.stateID,
		"each message decides on its own analyzed state")
	require.NotEqual(t, firstDecision.callID, secondDecision.callID,
		"each message chooses through its own call")
}

// TestTmuxAnalysisScreenChangePersistsCausalDeferWithoutAttempt pins the pre-attempt
// mismatch trail: no attempt, no write, and the DEFER links the analyzed
// state and the valid CHOOSE call with the screen_unchanged reason.
func TestTmuxAnalysisScreenChangePersistsCausalDeferWithoutAttempt(t *testing.T) {
	f := newTmuxContractFixture(t, true)
	candidate := f.seedDelivery(t, 503)
	f.model.beforeChoose = func() { f.setScreen(t, "different result\n$ ") }
	// Two passes: the first submits and analyzes, the second chooses on the
	// analyzed screen and defers on the moved one. Later passes would heal
	// by re-analyzing the moved screen, so the canary stops here.
	require.NoError(t, f.cycle(t))
	require.NoError(t, f.cycle(t))
	f.requireNoAttempt(t)
	require.Len(t, f.model.calls(protocol.ModelContractChooseOrchestrationAction), 1)

	decided := lastDecision(t, f.store.DB(), candidate.MessageID)
	require.Equal(t, protocol.OrchestratorActionDefer, decided.selected)
	require.NotEmpty(t, decided.stateID, "the mismatch keeps the analyzed state linked")
	var snapshotID string
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
		`SELECT state_id FROM state_snapshots WHERE observation_id = ?`,
		string(f.states.reports[0].ObservationID)).Scan(&snapshotID))
	require.Equal(t, snapshotID, decided.stateID)
	var chooseCallID string
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
		`SELECT call_id FROM glimmer_calls WHERE contract = 'CHOOSE_ORCHESTRATION_ACTION'`,
	).Scan(&chooseCallID))
	require.Equal(t, chooseCallID, decided.callID, "the mismatch keeps the CHOOSE call linked")
	found := false
	for _, guardResult := range decided.guards {
		if guardResult.Guard == protocol.GuardNameScreenUnchanged {
			found = true
			require.Equal(t, protocol.GuardOutcomeFail, guardResult.Outcome)
			require.Contains(t, guardResult.Detail, "TMUX_ANALYST_SCREEN_CHANGED")
		}
	}
	require.True(t, found, "the mismatch carries its own durable reason")
	// The row keeps the real CHOOSE set, never a rewritten DEFER-only set:
	// the causal probabilities must stay inside the stored allowed set.
	require.ElementsMatch(t,
		[]protocol.OrchestratorAction{
			protocol.OrchestratorActionInjectPTYPrompt, protocol.OrchestratorActionDefer,
		}, decided.allowed, "allowed stays the CHOOSE set the model saw")
	require.Equal(t, map[protocol.OrchestratorAction]float64{
		protocol.OrchestratorActionInjectPTYPrompt: 0.8, protocol.OrchestratorActionDefer: 0.2,
	}, decided.probabilities, "the probabilities stay causal to the CHOOSE call")
	require.NotEmpty(t, decided.model, "the CHOOSE model stays linked")
	// The stored row must pass the contract validation the persisting path
	// runs before the insert: rebuild it from the row and validate.
	var allowedJSON, probsJSON, guardsJSON, callID, model, channelID string
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
		`SELECT allowed_actions_json, probabilities_json, hard_guards_json,
			glimmer_call_id, decision_model, channel_decision_id
		FROM orchestrator_decisions WHERE message_id = ? AND selected_action = 'DEFER'
		ORDER BY created_at DESC, decision_id DESC LIMIT 1`,
		candidate.MessageID).Scan(&allowedJSON, &probsJSON, &guardsJSON, &callID, &model, &channelID))
	var stored protocol.OrchestratorDecision
	require.NoError(t, json.Unmarshal([]byte(allowedJSON), &stored.AllowedActions))
	require.NoError(t, json.Unmarshal([]byte(probsJSON), &stored.Probabilities))
	require.NoError(t, json.Unmarshal([]byte(guardsJSON), &stored.HardGuards))
	stored.DecisionID, stored.AgentID = "01JB0000000000000000000013", executorTestAgent
	stored.MessageID, stored.StateID = candidate.MessageID, protocol.ULID(decided.stateID)
	stored.SelectedAction = protocol.OrchestratorActionDefer
	stored.PolicyVersion, stored.DecisionModel = decided.policy, model
	stored.GlimmerCallID = protocol.OpaqueID(callID)
	stored.Channel = protocol.OrchestrationChannel{
		DecisionID: protocol.ChannelDecisionID(channelID),
		Outcome:    protocol.DecisionOutcomeAllow,
		Global:     protocol.ChannelModeActive, Link: protocol.LinkModeAllow,
	}
	stored.CreatedAt = time.Date(2026, time.October, 26, 12, 41, 2, 0, time.UTC)
	stored.ExecutionStatus = protocol.ExecutionStatusExecuted
	require.NoError(t, stored.Validate(), "the persisted mismatch row validates")
}

// TestTmuxAnalystUnavailableFallbackLinksReadableChannel pins the causal
// channel link when ANALYZE is unavailable. The detector fallback commits in
// the same pass, and decision.get must still be able to read the decision
// through a real channel decision.
func TestTmuxAnalystUnavailableFallbackLinksReadableChannel(t *testing.T) {
	f := newTmuxContractFixture(t, true)
	candidate := f.seedDelivery(t, 505)
	f.model.failure = tmuxContractUnavailable
	f.cycles(t, 4)
	requireDetectorFallbackCommitted(t, f, candidate, tmuxFallbackAnalysisUnavailable)
	var decisionID, channelID string
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(), `SELECT decision_id,
		channel_decision_id FROM orchestrator_decisions
		WHERE message_id = ? AND selected_action = 'INJECT_PTY_PROMPT'
		ORDER BY created_at DESC, decision_id DESC LIMIT 1`, candidate.MessageID).
		Scan(&decisionID, &channelID))
	t.Logf("durable fallback=%s channel_decision_id=%q", decisionID, channelID)
	require.True(t, protocol.ChannelDecisionID(channelID).Valid(),
		"the fallback must link an actual channel decision accepted by its production reader")
	require.Equal(t, 1, countRows(t, f.store.DB(),
		"SELECT count(*) FROM channel_control_decisions WHERE decision_id = ?", channelID))
}

// TestTmuxAnalystExpiredObservationResubmitsOnUnchangedScreen is the
// delivery-level canary for the memo expiry: with an identical screen and
// an expired photo, the next passes must produce a new observation instead
// of deciding on the dead one forever. The resubmission cites the
// published guard version, never a new unpublished one. CHOOSE DEFER holds
// the delivery meanwhile; a label never would.
func TestTmuxAnalystExpiredObservationResubmitsOnUnchangedScreen(t *testing.T) {
	f := newTmuxContractFixture(t, true)
	_ = f.seedDelivery(t, 506)
	f.model.chooseAction = protocol.OrchestratorActionDefer
	f.cycles(t, 2)
	f.requireNoAttempt(t)
	require.Len(t, f.states.reports, 1, "the first photo went out once")
	// The pane never moves; the photo expires while the delivery still has
	// its whole TTL ahead.
	f.clock.set(f.clock.Now().Add(stateTTL + time.Second))
	f.cycles(t, 2)
	require.Greater(t, len(f.states.reports), 1,
		"an expired photo must resubmit on an unchanged screen")
	first, last := f.states.reports[0], f.states.reports[len(f.states.reports)-1]
	require.NotEqual(t, first.ObservationID, last.ObservationID,
		"the resubmission mints a new observation identity")
	require.Equal(t, first.InputGuardVersion, last.InputGuardVersion,
		"the resubmission reuses the published guard version")
}

// TestTmuxAnalystSlowExactObservationDoesNotResubmit pins the retry boundary:
// crossing the photo TTL does not mint a replacement while the producer still
// analyzes that exact observation. A different observation identity remains
// free to supersede through the ordinary newest-wins queue.
func TestTmuxAnalystSlowExactObservationDoesNotResubmit(t *testing.T) {
	f := newTmuxContractFixture(t, true)
	_ = f.seedDelivery(t, 507)
	_, err := f.worker.RunOnce(t.Context())
	require.NoError(t, err)
	require.Len(t, f.states.reports, 1)
	report := f.states.reports[0]
	production, claimed, err := f.producer.claimProduction(report.AgentID)
	require.NoError(t, err)
	require.True(t, claimed)
	defer f.producer.finishInFlight(production.report)
	require.True(t, f.producer.HasQueuedObservation(
		report.AgentID, report.SessionID, report.ObservationID,
	))
	require.False(t, f.producer.HasQueuedObservation(
		report.AgentID, report.SessionID, "observation-other",
	), "queue liveness is bound to the exact observation identity")

	f.clock.set(f.clock.Now().Add(stateTTL + time.Second))
	_, err = f.worker.RunOnce(t.Context())
	require.NoError(t, err)
	require.Len(t, f.states.reports, 1,
		"an aged retry must not replace its own exact in-flight observation")
	require.Equal(t, report.ObservationID, f.producer.pending[report.AgentID].report.ObservationID)
}

// TestTmuxAnalystRowCheckRejectsContradiction is the mutant for the row
// rules insertDecision enforces: a DEFER-only allowed set carrying the
// INJECT probabilities of the CHOOSE call, or an empty channel link, must
// fail before the insert instead of persisting.
func TestTmuxAnalystRowCheckRejectsContradiction(t *testing.T) {
	valid := decisionRow{
		allowed: []protocol.OrchestratorAction{
			protocol.OrchestratorActionInjectPTYPrompt, protocol.OrchestratorActionDefer,
		},
		selected: protocol.OrchestratorActionDefer,
		probabilities: map[protocol.OrchestratorAction]float64{
			protocol.OrchestratorActionInjectPTYPrompt: 0.8, protocol.OrchestratorActionDefer: 0.2,
		},
		channel: protocol.ChannelDecisionID("chd_01JB0000000000000000000010"),
	}
	require.NoError(t, checkDecisionRow(valid))
	rewritten := valid
	rewritten.allowed = []protocol.OrchestratorAction{protocol.OrchestratorActionDefer}
	err := checkDecisionRow(rewritten)
	require.Error(t, err, "a DEFER-only allowed set cannot carry INJECT probabilities")
	require.Contains(t, err.Error(), "outside allowed_actions")
	unlinked := valid
	unlinked.channel = ""
	require.Error(t, checkDecisionRow(unlinked), "an empty channel link cannot persist")
	misselected := valid
	misselected.selected = protocol.OrchestratorActionQueue
	require.Error(t, checkDecisionRow(misselected), "the selected action must stay allowed")
}

// TestTmuxAnalystMismatchAllowedRejectsRewrittenDefer is the mutant for the
// mismatch row: a rewritten DEFER-only allowed set carrying the INJECT
// probabilities of the CHOOSE call must fail OrchestratorDecision.Validate,
// so the persisting path can never store the contradiction again.
func TestTmuxAnalystMismatchAllowedRejectsRewrittenDefer(t *testing.T) {
	decision := protocol.OrchestratorDecision{
		DecisionID: "01JB0000000000000000000013", AgentID: executorTestAgent,
		MessageID: "01JB0000000000000000000014", StateID: "01JB0000000000000000000015",
		AllowedActions: []protocol.OrchestratorAction{
			protocol.OrchestratorActionInjectPTYPrompt, protocol.OrchestratorActionDefer,
		},
		SelectedAction: protocol.OrchestratorActionDefer,
		Probabilities: map[protocol.OrchestratorAction]float64{
			protocol.OrchestratorActionInjectPTYPrompt: 0.8, protocol.OrchestratorActionDefer: 0.2,
		},
		HardGuards: []protocol.GuardResult{
			{Guard: protocol.GuardNameScreenUnchanged, Outcome: protocol.GuardOutcomeFail},
		},
		PolicyVersion: "v1", DecisionModel: "example.invalid/r@v1", GlimmerCallID: "call-1",
		Channel: protocol.OrchestrationChannel{
			DecisionID: "chd_01JB0000000000000000000010", Outcome: protocol.DecisionOutcomeAllow,
			Global: protocol.ChannelModeActive, Link: protocol.LinkModeAllow,
		},
		CreatedAt:       time.Date(2026, time.October, 26, 12, 41, 2, 0, time.UTC),
		ExecutionStatus: protocol.ExecutionStatusExecuted,
	}
	require.NoError(t, decision.Validate())
	rewritten := decision
	rewritten.AllowedActions = []protocol.OrchestratorAction{protocol.OrchestratorActionDefer}
	err := rewritten.Validate()
	require.Error(t, err, "a DEFER-only allowed set cannot carry INJECT probabilities")
	require.Contains(t, err.Error(), "outside allowed_actions")
}

// TestTmuxAnalysisPreWriteScreenRaceCancelsWithoutPhysicalWrite pins the pre-write
// micro-race trail: the screen flips after the pre-commit verification but
// before the delivery-opening capture, so the attempt cancels instead of
// writing. The capture counter flips on the fourth pane read: one for the
// submit pass, one for the gate plus one for the pre-commit check, and the
// delivery-opening capture reads the moved screen.
func TestTmuxAnalysisPreWriteScreenRaceCancelsWithoutPhysicalWrite(t *testing.T) {
	f := newTmuxContractFixture(t, true)
	dir := t.TempDir()
	script := `#!/bin/sh
set -eu
counter="` + dir + `/count"
command=
literal=no
last=
for arg do
  case "$arg" in
    capture-pane|display-message|list-clients|send-keys) command="$arg" ;;
    -l) literal=yes ;;
  esac
  last="$arg"
done
case "$command" in
  capture-pane)
    n=0
    if test -f "$counter"; then n=$(cat "$counter"); fi
    n=$((n + 1))
    printf '%s' "$n" > "$counter"
    if test "$n" -le 3; then printf 'completed fixture task\n$ '; else printf 'different result\n$ '; fi
    ;;
  display-message) printf '%%7\t100\t80\t24\n' ;;
  list-clients) : ;;
  send-keys)
    printf '%s\n' "$literal" >> "$GIFFORD_TMUX_CONTRACT_DIR/writes"
    if test "$literal" = yes; then
      printf '$ %s' "$last" > "$GIFFORD_TMUX_CONTRACT_DIR/screen"
    elif test "$last" = Enter; then
      printf '$ ' > "$GIFFORD_TMUX_CONTRACT_DIR/screen"
    else
      exit 2
    fi
    ;;
  *) exit 2 ;;
esac
`
	// The pane simulator is the sudo stand-in: the transport shells out
	// through sudo, so this file shadows the fixture's own sudo earlier in
	// the PATH and counts every capture-pane read.
	require.NoError(t, os.WriteFile(filepath.Join(dir, "sudo"), []byte(script), 0o755))
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))

	candidate := f.seedDelivery(t, 504)
	require.NoError(t, f.cycle(t))
	require.NoError(t, f.cycle(t))

	require.Equal(t, 1, attemptCount(t, f.store.DB(), candidate.MessageID))
	require.Equal(t, 1, countRows(t, f.store.DB(),
		`SELECT count(*) FROM delivery_attempts WHERE message_id = ? AND commit_sent_at IS NOT NULL`,
		candidate.MessageID))
	require.Equal(t, 0, f.writes(t), "no physical write happened")
	var state, reason string
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
		`SELECT state, terminal_reason FROM delivery_attempts WHERE message_id = ?`,
		candidate.MessageID).Scan(&state, &reason))
	require.Equal(t, "CANCELLED", state)
	require.Equal(t, "TMUX_ANALYST_PREWRITE_ABORTED", reason)
	var queue string
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
		`SELECT state FROM delivery_queue WHERE message_id = ?`, candidate.MessageID).Scan(&queue))
	require.Equal(t, "QUEUED", queue, "the delivery returns to the queue for a later pass")
	decided := lastDecision(t, f.store.DB(), candidate.MessageID)
	require.Equal(t, protocol.OrchestratorActionInjectPTYPrompt, decided.selected,
		"the abort happens after the injection committed")
	require.Equal(t, protocol.ExecutionStatusAborted, decided.status,
		"the aborted injection stays visible as an aborted evaluation")
	require.Empty(t, pendingNotes(t, f.store.DB(), candidate.MessageID),
		"a proven no-write raises no uncertainty")
}
