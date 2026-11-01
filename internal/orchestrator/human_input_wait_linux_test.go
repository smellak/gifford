//go:build linux

package orchestrator

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/daemon"
	"github.com/smellak/gifford/internal/daemon/agentlink"
	"github.com/smellak/gifford/internal/protocol"
)

// mutableHumanLinks overlays only the human-input fields of a real agent
// link. Delivery still uses that link's real client and input guard versions.
type mutableHumanLinks struct {
	base     LinkProvider
	mu       sync.Mutex
	draft    protocol.DraftState
	activity protocol.ActivityState
}

func installMutableHumanLinks(t *testing.T, f *choiceFixture) *mutableHumanLinks {
	t.Helper()
	base := f.executor.links
	snapshot, linked := base.Snapshot(executorTestAgent)
	require.True(t, linked)
	links := &mutableHumanLinks{
		base: base, draft: snapshot.LastReport.DraftState,
		activity: snapshot.LastReport.ActivityState,
	}
	f.executor.links = links
	return links
}

func (l *mutableHumanLinks) Snapshot(agent protocol.AgentID) (agentlink.Snapshot, bool) {
	snapshot, linked := l.base.Snapshot(agent)
	if !linked {
		return snapshot, false
	}
	l.mu.Lock()
	snapshot.LastReport.DraftState = l.draft
	snapshot.LastReport.ActivityState = l.activity
	l.mu.Unlock()
	return snapshot, true
}

func (l *mutableHumanLinks) Client(agent protocol.AgentID) (*agentlink.Client, bool) {
	return l.base.Client(agent)
}

func (l *mutableHumanLinks) set(draft protocol.DraftState, activity protocol.ActivityState) {
	l.mu.Lock()
	l.draft, l.activity = draft, activity
	l.mu.Unlock()
}

func TestWorkerWaitsForHumanInputThenDeliversExactlyOnce(t *testing.T) {
	delivery := &integrationDelivery{}
	f := newLinkedChoice(t, delivery)
	links := installMutableHumanLinks(t, f)
	links.set(protocol.DraftStateOpen, protocol.ActivityStateActive)
	worker := newSweepWorker(t, f.executorFixture, f.executor, links)
	candidate := seedReadableDelivery(t, f.executorFixture, 230, "deliver after the human finishes")
	db := f.store.DB()

	processed, err := worker.RunOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, processed)
	require.Empty(t, f.model.calls(choiceTriage))
	require.Empty(t, f.model.calls(choiceChoose))
	require.Zero(t, f.states.demandCount())
	require.Zero(t, countRows(t, db,
		"SELECT count(*) FROM message_triage WHERE message_id = ?", candidate.MessageID))
	require.Empty(t, decisionsFor(t, db, candidate.MessageID))
	require.Zero(t, gateDecisionCount(t, db, candidate.MessageID))
	require.Zero(t, countRows(t, db,
		"SELECT count(*) FROM approval_requests WHERE object_id = ?", candidate.MessageID))
	require.Zero(t, attemptCount(t, db, candidate.MessageID))
	require.Zero(t, delivery.prepareCount())
	require.Zero(t, delivery.commitCount())
	require.Equal(t, queueQueued, queueStateOf(t, db, candidate))

	links.set(protocol.DraftStateClosed, protocol.ActivityStateIdle)
	processed, err = worker.RunOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, processed)
	require.Equal(t, "DELIVERED", queueStateOf(t, db, candidate))
	require.Equal(t, 1, delivery.prepareCount())
	require.Equal(t, 1, delivery.commitCount())
	require.Equal(t, 1, attemptCount(t, db, candidate.MessageID))
	require.Equal(t, 1, countRows(t, db, `SELECT count(*) FROM delivery_attempts
		WHERE message_id = ? AND agent_id = ? AND state = ?`, candidate.MessageID,
		candidate.AgentID, daemon.AttemptCommitted))
	require.Zero(t, countRows(t, db,
		"SELECT count(*) FROM approval_requests WHERE object_id = ?", candidate.MessageID))
}

func TestWorkerWaitsForEveryAffirmativeHumanInputState(t *testing.T) {
	tests := []struct {
		name     string
		draft    protocol.DraftState
		activity protocol.ActivityState
	}{
		{name: "open paused", draft: protocol.DraftStateOpen, activity: protocol.ActivityStatePaused},
		{name: "pasting idle", draft: protocol.DraftStatePasting, activity: protocol.ActivityStateIdle},
		{name: "submitting idle", draft: protocol.DraftStateSubmitting, activity: protocol.ActivityStateIdle},
		{name: "closed active", draft: protocol.DraftStateClosed, activity: protocol.ActivityStateActive},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := newChoiceFixture(t)
			f.setReport(func(report *protocol.InputGuardReport) {
				report.DraftState, report.ActivityState = test.draft, test.activity
			})
			worker := newSweepWorker(t, f.executorFixture, f.executor, f.links)
			candidate := seedReadableDelivery(t, f.executorFixture, 240+index, "wait for human input")
			db := f.store.DB()

			processed, err := worker.RunOnce(t.Context())
			require.NoError(t, err)
			require.Equal(t, 1, processed)
			require.Zero(t, f.triager.count())
			require.Zero(t, f.states.demandCount())
			require.Empty(t, decisionsFor(t, db, candidate.MessageID))
			require.Zero(t, gateDecisionCount(t, db, candidate.MessageID))
			require.Zero(t, countRows(t, db,
				"SELECT count(*) FROM approval_requests WHERE object_id = ?", candidate.MessageID))
			require.Equal(t, queueQueued, queueStateOf(t, db, candidate))
		})
	}
}

func TestWorkerUnknownDraftStillDefersForApproval(t *testing.T) {
	f := newChoiceFixture(t)
	f.setReport(func(report *protocol.InputGuardReport) {
		report.DraftState = protocol.DraftStateUnknown
		report.ActivityState = protocol.ActivityStateIdle
	})
	worker := newSweepWorker(t, f.executorFixture, f.executor, f.links)
	candidate := seedReadableDelivery(t, f.executorFixture, 250, "unknown draft")
	db := f.store.DB()

	processed, err := worker.RunOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, processed)
	require.Equal(t, 1, f.triager.count())
	require.Empty(t, f.model.calls(choiceChoose))
	decision := lastDecision(t, db, candidate.MessageID)
	require.Equal(t, choiceDefer, decision.selected)
	require.Equal(t, []string{
		notePrefix + "human_draft_closed: human draft is not CLOSED",
	}, pendingNotes(t, db, candidate.MessageID))
	require.Equal(t, queueQueued, queueStateOf(t, db, candidate))
}

func TestWorkerRollsBackWhenHumanInputStartsDuringTriage(t *testing.T) {
	delivery := &integrationDelivery{}
	f := newLinkedChoice(t, delivery)
	links := installMutableHumanLinks(t, f)
	worker := newSweepWorker(t, f.executorFixture, f.executor, links)
	candidate := seedReadableDelivery(t, f.executorFixture, 260, "human starts during triage")
	db := f.store.DB()
	var once sync.Once
	f.model.set(func(model *choiceModel) {
		model.during = func(request protocol.InferRequest) {
			if request.Contract != choiceTriage {
				return
			}
			once.Do(func() {
				links.set(protocol.DraftStateOpen, protocol.ActivityStateActive)
				f.states.set(func(states *choiceStates) {
					states.eligible, states.reason = false, StateEligibilityHumanInput
				})
			})
		}
	})

	processed, err := worker.RunOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, processed)
	require.Len(t, f.model.calls(choiceTriage), 1)
	require.Empty(t, f.model.calls(choiceChoose))
	require.Zero(t, f.states.demandCount())
	require.Zero(t, gateDecisionCount(t, db, candidate.MessageID), "tx A must roll back")
	require.Empty(t, decisionsFor(t, db, candidate.MessageID))
	require.Zero(t, countRows(t, db,
		"SELECT count(*) FROM approval_requests WHERE object_id = ?", candidate.MessageID))
	require.Zero(t, attemptCount(t, db, candidate.MessageID))
	require.Zero(t, delivery.prepareCount())
	require.Zero(t, delivery.commitCount())
	require.Equal(t, queueQueued, queueStateOf(t, db, candidate))

	f.model.set(func(model *choiceModel) { model.during = nil })
	links.set(protocol.DraftStateClosed, protocol.ActivityStateIdle)
	f.states.set(func(states *choiceStates) {
		states.eligible, states.reason = true, StateEligibilityEligible
	})
	processed, err = worker.RunOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, processed)
	require.Equal(t, "DELIVERED", queueStateOf(t, db, candidate))
	require.Equal(t, 1, delivery.prepareCount())
	require.Equal(t, 1, delivery.commitCount())
	require.Equal(t, 1, countRows(t, db, `SELECT count(*) FROM delivery_attempts
		WHERE message_id = ? AND agent_id = ? AND state = ?`, candidate.MessageID,
		candidate.AgentID, daemon.AttemptCommitted))
	require.Zero(t, countRows(t, db,
		"SELECT count(*) FROM approval_requests WHERE object_id = ?", candidate.MessageID))
}

func TestWorkerExpiresOpenDraftThroughExistingSweep(t *testing.T) {
	f := newChoiceFixture(t)
	f.setReport(func(report *protocol.InputGuardReport) {
		report.DraftState = protocol.DraftStateOpen
		report.ActivityState = protocol.ActivityStatePaused
	})
	worker := newSweepWorker(t, f.executorFixture, f.executor, f.links)
	candidate := seedReadableDelivery(t, f.executorFixture, 270, "expired open draft")
	expireRow(t, f.executorFixture, candidate)
	db := f.store.DB()

	processed, err := worker.RunOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, processed)
	require.Equal(t, queueStateExpired, queueStateOf(t, db, candidate))
	require.Zero(t, f.triager.count())
	require.Empty(t, decisionsFor(t, db, candidate.MessageID))
	require.Zero(t, countRows(t, db,
		"SELECT count(*) FROM approval_requests WHERE object_id = ?", candidate.MessageID))
	require.Zero(t, attemptCount(t, db, candidate.MessageID))
}

func TestWorkerForgetsOldStateWaitWhileHumanTypes(t *testing.T) {
	f := newChoiceFixture(t)
	worker := newSweepWorker(t, f.executorFixture, f.executor, f.links)
	candidate := seedReadableDelivery(t, f.executorFixture, 280, "fresh state after human input")
	db := f.store.DB()
	f.states.set(func(states *choiceStates) {
		states.eligible, states.reason = false, StateEligibilityUnknownPhase
	})

	processed, err := worker.RunOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, processed)
	require.Equal(t, 1, f.states.demandCount())
	f.executor.stateWaitMu.Lock()
	old, tracked := f.executor.stateWait[candidate]
	f.executor.stateWaitMu.Unlock()
	require.True(t, tracked)

	f.advance(automaticStateWaitWindow + 1)
	f.setReport(func(report *protocol.InputGuardReport) {
		report.DraftState = protocol.DraftStateOpen
		report.ActivityState = protocol.ActivityStateActive
	})
	processed, err = worker.RunOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, processed)
	require.Equal(t, 1, f.states.demandCount())
	require.False(t, f.executor.automaticStateWaitTracked(candidate))

	f.setReport(func(report *protocol.InputGuardReport) {
		report.DraftState = protocol.DraftStateClosed
		report.ActivityState = protocol.ActivityStateIdle
	})
	processed, err = worker.RunOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, processed)
	require.Equal(t, 2, f.states.demandCount())
	f.executor.stateWaitMu.Lock()
	fresh, tracked := f.executor.stateWait[candidate]
	f.executor.stateWaitMu.Unlock()
	require.True(t, tracked)
	require.True(t, fresh.started.After(old.started), "closing input must open a fresh analysis window")
	require.Zero(t, f.triager.count())
	require.Empty(t, decisionsFor(t, db, candidate.MessageID))
	require.Zero(t, countRows(t, db,
		"SELECT count(*) FROM approval_requests WHERE object_id = ?", candidate.MessageID))
	require.Equal(t, queueQueued, queueStateOf(t, db, candidate))
}
