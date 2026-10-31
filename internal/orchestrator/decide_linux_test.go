//go:build linux

package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/daemon"
	"github.com/smellak/gifford/internal/protocol"
)

// linkedWait bounds every wait of these tests on another goroutine. Only
// a test that fails waits that long.
const linkedWait = 10 * time.Second

// newLinkedChoice is the executor fixture over a real agent socket, with
// the automatic decision wired to the real triage, the fake state and the
// fake model.
func newLinkedChoice(t *testing.T, delivery *integrationDelivery) *choiceFixture {
	t.Helper()
	fixture := newExecutorFixture(t)
	fixture.executor = attachIntegrationLink(t, fixture, delivery, 2*time.Second)
	f := choiceFakes(t, fixture)
	useRealTriage(t, f)
	return f
}

// The whole automatic flow over a real agent socket: TRIAGE, tx A, CHOOSE
// with no transaction open, tx C, then one prepare and one commit, with
// the attempt bound to the decision that cites the call.
func TestAutomaticDecisionDeliversThroughRealAgentSocket(t *testing.T) {
	delivery := &integrationDelivery{}
	f := newLinkedChoice(t, delivery)
	db := f.store.DB()
	candidate := seedReadableDelivery(t, f.executorFixture, 70, "please review the parser change")
	require.NoError(t, f.executor.EvaluateAutomatic(t.Context(), candidate))

	require.Len(t, f.model.calls(choiceTriage), 1)
	require.Len(t, f.model.calls(choiceChoose), 1)
	for _, err := range f.model.barrierResults() {
		require.NoError(t, err, "a write transaction was open during a model call")
	}
	require.Equal(t, 1, delivery.prepareCount())
	require.Equal(t, 1, delivery.commitCount())
	generation, version := delivery.preparedGuard()
	require.Equal(t, uint64(2), generation)
	require.Equal(t, int64(3), version)
	require.Equal(t, "DELIVERED", queueStateOf(t, db, candidate))

	var attempt, selected, status, call string
	var draftGeneration, guardVersion int64
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT da.state, da.draft_generation,
		da.input_guard_version, d.selected_action, d.execution_status, d.glimmer_call_id
		FROM delivery_attempts AS da
		JOIN orchestrator_decisions AS d ON d.decision_id = da.orchestrator_decision_id
		WHERE da.message_id = ? AND da.agent_id = ?`, candidate.MessageID, candidate.AgentID).
		Scan(&attempt, &draftGeneration, &guardVersion, &selected, &status, &call))
	require.Equal(t, string(daemon.AttemptCommitted), attempt)
	require.Equal(t, int64(2), draftGeneration, "the attempt records the guard tx C saw")
	require.Equal(t, int64(3), guardVersion)
	require.Equal(t, string(choiceInject), selected)
	require.Equal(t, string(protocol.ExecutionStatusExecuted), status)
	require.Equal(t, string(choiceCallID), call)
	require.Empty(t, pendingNotes(t, db, candidate.MessageID))
}

// The first automatic selection with no usable state asks the producer for
// analysis and leaves the delivery untouched. Once that same identity has an
// eligible state, the ordinary A/CHOOSE/C/T5 path commits it exactly once.
func TestAutomaticDecisionDemandsStateBeforeDelivering(t *testing.T) {
	delivery := &integrationDelivery{}
	f := newLinkedChoice(t, delivery)
	db := f.store.DB()
	candidate := seedReadableDelivery(t, f.executorFixture, 71, "please review the parser change")
	f.states.set(func(s *choiceStates) { s.err = ErrStateObservationNotFound })

	require.NoError(t, f.executor.EvaluateAutomatic(t.Context(), candidate))
	require.Equal(t, 1, f.states.demandCount())
	require.Zero(t, f.triager.count())
	require.Zero(t, f.model.total())
	require.Zero(t, delivery.prepareCount())
	require.Zero(t, delivery.commitCount())
	require.Zero(t, attemptCount(t, db, candidate.MessageID))
	require.Empty(t, decisionsFor(t, db, candidate.MessageID))
	require.Empty(t, pendingNotes(t, db, candidate.MessageID))
	require.Equal(t, queueQueued, queueStateOf(t, db, candidate))

	f.states.set(func(s *choiceStates) {
		s.err, s.eligible, s.reason = nil, true, StateEligibilityEligible
	})
	require.NoError(t, f.executor.EvaluateAutomatic(t.Context(), candidate))
	require.Equal(t, 1, f.states.demandCount())
	require.Len(t, f.model.calls(choiceTriage), 1)
	require.Len(t, f.model.calls(choiceChoose), 1)
	require.Equal(t, 1, delivery.prepareCount())
	require.Equal(t, 1, delivery.commitCount())
	require.Equal(t, "DELIVERED", queueStateOf(t, db, candidate))
	require.Empty(t, pendingNotes(t, db, candidate.MessageID))
	var attempts, committed int
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT count(*),
		coalesce(sum(state = 'COMMITTED'), 0) FROM delivery_attempts
		WHERE message_id = ? AND agent_id = ?`, candidate.MessageID, candidate.AgentID).
		Scan(&attempts, &committed))
	require.Equal(t, 1, attempts)
	require.Equal(t, 1, committed)
}

// blockedScan is one worker scan whose model call is held open.
type blockedScan struct {
	*choiceFixture
	delivery  *integrationDelivery
	worker    *daemon.Worker
	candidate daemon.QueueDelivery
	release   func()
	done      chan error
}

// startBlockedScan runs one worker scan over a real agent socket and
// returns once its call of contract has started and waits for release.
// The scan holds the agent's in-flight mark all that time.
func startBlockedScan(t *testing.T, contract protocol.ModelContract, sequence int) *blockedScan {
	t.Helper()
	delivery := &integrationDelivery{}
	f := newLinkedChoice(t, delivery)
	worker, err := daemon.NewWorker(daemon.WorkerOptions{
		DB: f.store.DB(), Processor: f.executor, Approvals: f.executor,
		Prepared: f.executor, Proxies: f.executor.links, Clock: f.clock,
	})
	require.NoError(t, err)
	entered, held := make(chan struct{}), make(chan struct{})
	var enter, open sync.Once
	f.model.set(func(m *choiceModel) {
		m.during = func(req protocol.InferRequest) {
			if req.Contract == contract {
				enter.Do(func() { close(entered) })
				<-held
			}
		}
	})
	scan := &blockedScan{
		choiceFixture: f, delivery: delivery, worker: worker,
		candidate: seedReadableDelivery(t, f.executorFixture, sequence, "please review the parser change"),
		release:   func() { open.Do(func() { close(held) }) },
		done:      make(chan error, 1),
	}
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		processed, err := worker.RunOnce(t.Context())
		if err == nil && processed != 1 {
			err = fmt.Errorf("the scan evaluated %d deliveries, want 1", processed)
		}
		scan.done <- err
	}()
	t.Cleanup(func() {
		scan.release()
		select {
		case <-finished:
		case <-time.After(linkedWait):
			t.Error("the scan never ended")
		}
	})
	select {
	case <-entered:
	case err := <-scan.done:
		t.Fatalf("the scan ended before its %s call: %v", contract, err)
	case <-time.After(linkedWait):
		t.Fatalf("the %s call never started", contract)
	}
	return scan
}

// whileHeld runs call while the model call is still held, and fails the
// test unless call returns by itself: nothing it needs may wait for the
// model.
func (s *blockedScan) whileHeld(t *testing.T, call func(context.Context) error) {
	t.Helper()
	result := make(chan error, 1)
	go func() { result <- call(t.Context()) }()
	select {
	case err := <-result:
		require.NoError(t, err)
	case <-time.After(linkedWait):
		t.Fatal("it waited for the model call")
	}
	select {
	case err := <-s.done:
		t.Fatalf("the scan ended while its call was held: %v", err)
	default:
	}
}

// finish lets the held call answer and returns the scan's result.
func (s *blockedScan) finish(t *testing.T) error {
	t.Helper()
	s.release()
	select {
	case err := <-s.done:
		return err
	case <-time.After(linkedWait):
		t.Fatal("the scan did not end after the call answered")
		return nil
	}
}

// rescan runs the next scan and checks it reaches the model zero times.
func (s *blockedScan) rescan(t *testing.T) {
	t.Helper()
	calls := s.model.total()
	processed, err := s.worker.RunOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, processed)
	require.Equal(t, calls, s.model.total(), "a restricted delivery reaches the model zero times")
}

// A pause during a model call waits for nothing: switching the agent's
// automatic delivery off and the Proxy pause the control event then sends
// both return while the call is out, though the scan holds the agent's
// in-flight mark. What follows the call sees the pause before acting:
// nothing reaches the Proxy, and the next scan defers to the human
// without a call.
func TestPauseDuringAModelCallNeverWaitsForIt(t *testing.T) {
	actor := channel.Actor{Type: protocol.ActorTypeHuman, ID: executorTestOperator}
	for name, tc := range map[string]struct {
		contract protocol.ModelContract
		status   protocol.ExecutionStatus
	}{
		// tx C revalidates the channel decision of tx A and goes STALE.
		"during CHOOSE": {contract: choiceChoose, status: protocol.ExecutionStatusStale},
		// tx A's gate comes after the call and defers.
		"during TRIAGE": {contract: choiceTriage, status: protocol.ExecutionStatusExecuted},
	} {
		t.Run(name, func(t *testing.T) {
			scan := startBlockedScan(t, tc.contract, 80)
			off := false
			scan.whileHeld(t, func(ctx context.Context) error {
				_, err := scan.controller.SetAgent(ctx, protocol.SetAgentChannelParams{
					AgentID: executorTestAgent, AutoDelivery: &off, ExpectedVersion: 0,
				}, actor)
				return err
			})
			scan.whileHeld(t, func(ctx context.Context) error {
				return scan.worker.PauseAutomation(ctx, executorTestAgent)
			})
			require.NoError(t, scan.finish(t))

			db := scan.store.DB()
			require.Equal(t, tc.status, lastDecision(t, db, scan.candidate.MessageID).status)
			require.Zero(t, scan.delivery.prepareCount())
			require.Zero(t, scan.delivery.commitCount())
			require.Zero(t, attemptCount(t, db, scan.candidate.MessageID))
			require.Equal(t, queueQueued, queueStateOf(t, db, scan.candidate))
			if tc.contract == choiceChoose {
				scan.rescan(t)
			}
			decision := lastDecision(t, db, scan.candidate.MessageID)
			require.Equal(t, []protocol.OrchestratorAction{choiceDefer}, decision.allowed)
			require.Equal(t, protocol.ExecutionStatusExecuted, decision.status)
			notes := pendingNotes(t, db, scan.candidate.MessageID)
			require.Len(t, notes, 1)
			require.Contains(t, notes[0], notePrefix+noteChannel)
		})
	}
}

// A restrictive control during CHOOSE waits for nothing either: the
// change and the cancel the control event sends return while the call is
// out, tx C goes STALE before any Proxy byte, and the next scan defers
// without a call.
func TestControlChangeDuringChooseNeverWaitsForIt(t *testing.T) {
	actor := channel.Actor{Type: protocol.ActorTypeHuman, ID: executorTestOperator}
	scan := startBlockedScan(t, choiceChoose, 90)
	scan.whileHeld(t, func(ctx context.Context) error {
		_, err := scan.controller.SetGlobal(ctx, protocol.SetGlobalParams{
			Mode: protocol.ChannelModeOff, ExpectedVersion: 0,
		}, actor)
		return err
	})
	scan.whileHeld(t, func(ctx context.Context) error {
		return scan.worker.ControlChanged(ctx, executorTestAgent)
	})
	require.NoError(t, scan.finish(t))

	db := scan.store.DB()
	require.Equal(t, protocol.ExecutionStatusStale, lastDecision(t, db, scan.candidate.MessageID).status)
	require.Zero(t, scan.delivery.prepareCount())
	require.Zero(t, scan.delivery.cancelCount())
	require.Zero(t, attemptCount(t, db, scan.candidate.MessageID))
	require.Equal(t, queueQueued, queueStateOf(t, db, scan.candidate))
	scan.rescan(t)
	notes := pendingNotes(t, db, scan.candidate.MessageID)
	require.Len(t, notes, 1)
	require.Contains(t, notes[0], notePrefix+noteChannel)
}

// The Proxy pause alone, with the channel still allowing: tx C has no
// reason to stop, and the paused connection cancels the prepare before
// the session sees it. The pause goes to the link itself: the worker's
// pause follows the agent's confirmed control, which still allows here.
func TestProxyPauseDuringChooseCancelsThePrepare(t *testing.T) {
	scan := startBlockedScan(t, choiceChoose, 100)
	client, ok := scan.executor.links.Client(executorTestAgent)
	require.True(t, ok)
	scan.whileHeld(t, func(ctx context.Context) error {
		return client.Pause(ctx)
	})
	require.NoError(t, scan.finish(t))

	db := scan.store.DB()
	require.Equal(t, protocol.ExecutionStatusAborted, lastDecision(t, db, scan.candidate.MessageID).status)
	require.Zero(t, scan.delivery.prepareCount(), "the session never saw the prepare")
	require.Zero(t, scan.delivery.commitCount())
	var attempt, reason string
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT state, terminal_reason
		FROM delivery_attempts WHERE message_id = ? AND agent_id = ?`,
		scan.candidate.MessageID, scan.candidate.AgentID).Scan(&attempt, &reason))
	require.Equal(t, string(daemon.AttemptCancelled), attempt)
	require.Equal(t, string(protocol.CancelReasonStateChanged), reason)
	require.Equal(t, queueQueued, queueStateOf(t, db, scan.candidate))
}

// The commit of an automatic delivery needs the decision's state once
// more, after the prepare, in the transaction that marks COMMIT_SENT. If
// that state is gone, no longer valid, superseded, at another version, no
// longer eligible or unreadable, the prepared delivery is cancelled with
// STATE_CHANGED and the row goes back to QUEUED: one prepare, no commit.
func TestAutomaticCommitNeedsTheDecisionsStateAfterThePrepare(t *testing.T) {
	for name, change := range map[string]func(*choiceStates){
		"no longer eligible": func(s *choiceStates) {
			s.eligible, s.reason = false, StateEligibilityDeliveryUnsafe
		},
		"a newer state": func(s *choiceStates) {
			s.onRead = func(state *StateRead) { state.Snapshot.StateID = "01JB000000000000000000STAT" }
		},
		"another state version": func(s *choiceStates) {
			s.onRead = func(state *StateRead) { state.Snapshot.StateVersion++ }
		},
		"gone":            func(s *choiceStates) { s.err = ErrStateObservationNotFound },
		"no longer valid": func(s *choiceStates) { s.err = ErrStateObservationInvalid },
		"unreadable":      func(s *choiceStates) { s.err = errors.New("state store failed") },
	} {
		t.Run(name, func(t *testing.T) {
			delivery := &integrationDelivery{}
			f := newLinkedChoice(t, delivery)
			delivery.prepareHook = func() { f.states.set(change) }
			candidate := seedReadableDelivery(t, f.executorFixture, 110, "please review the parser change")
			require.NoError(t, f.executor.EvaluateAutomatic(t.Context(), candidate))

			require.Equal(t, 1, delivery.prepareCount())
			require.Zero(t, delivery.commitCount(), "no COMMIT without the decision's state")
			require.Equal(t, 1, delivery.cancelCount())
			require.Equal(t, 4, f.states.readCount(),
				"the read-only precheck, tx A, tx C and the commit's check")
			db := f.store.DB()
			var attempt, reason string
			require.NoError(t, db.QueryRowContext(t.Context(), `SELECT state, terminal_reason
				FROM delivery_attempts WHERE message_id = ? AND agent_id = ?`,
				candidate.MessageID, candidate.AgentID).Scan(&attempt, &reason))
			require.Equal(t, string(daemon.AttemptCancelled), attempt)
			require.Equal(t, string(protocol.CancelReasonStateChanged), reason)
			require.Equal(t, queueQueued, queueStateOf(t, db, candidate))
			require.Equal(t, protocol.ExecutionStatusAborted, lastDecision(t, db, candidate.MessageID).status)
		})
	}
}

// A human deliver_now never needs the automatic state: with the automatic
// decision wired, its commit keeps the p_idle override, and a state that
// is not eligible still lets the human's order through.
func TestManualDeliverNowKeepsItsOverrideWhenWired(t *testing.T) {
	delivery := &integrationDelivery{}
	f := newLinkedChoice(t, delivery)
	f.states.set(func(s *choiceStates) { s.eligible, s.reason = false, StateEligibilityDeliveryUnsafe })
	candidate := seedReadableDelivery(t, f.executorFixture, 120, "please review the parser change")
	require.NoError(t, f.executor.EvaluateAutomatic(t.Context(), candidate))
	require.Empty(t, f.model.calls(choiceChoose), "an ineligible state defers without CHOOSE")
	reads := f.states.readCount()

	approval := pendingApproval(t, f.executorFixture)
	resolved, err := f.executor.ResolveApproval(t.Context(), protocol.ResolveApprovalParams{
		ApprovalID: approval.ApprovalID, Option: executorTestDeliverNow,
	}, channel.Actor{Type: protocol.ActorTypeHuman, ID: executorTestOperator})
	require.NoError(t, err)
	require.Equal(t, protocol.ApprovalStateApproved, resolved.State)
	require.Equal(t, 1, delivery.prepareCount())
	require.Equal(t, 1, delivery.commitCount())
	require.Equal(t, "DELIVERED", queueStateOf(t, f.store.DB(), candidate))
	require.Equal(t, reads, f.states.readCount(), "the manual order never reads the automatic state")
}
