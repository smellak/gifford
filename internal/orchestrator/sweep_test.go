package orchestrator

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/daemon"
	"github.com/smellak/gifford/internal/daemon/agentlink"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

// newSweepWorker is the daemon's worker over executor, which it sweeps
// before every scan.
func newSweepWorker(
	t *testing.T,
	f *executorFixture,
	executor *Executor,
	proxies daemon.ProxySnapshots,
) *daemon.Worker {
	t.Helper()
	worker, err := daemon.NewWorker(daemon.WorkerOptions{
		DB: f.store.DB(), Processor: executor, Approvals: executor,
		Prepared: executor, Proxies: proxies, Clock: f.clock,
	})
	require.NoError(t, err)
	return worker
}

// approvalsIn lists the test agent's approvals in state.
func approvalsIn(t *testing.T, f *executorFixture, state protocol.ApprovalState) []protocol.ApprovalRequest {
	t.Helper()
	list, err := f.controller.Approvals(t.Context(), protocol.ListApprovalsParams{
		State: state, AgentID: executorTestAgent,
	})
	require.NoError(t, err)
	return list.Approvals
}

// cutOff takes the agent's link and session away, so the scan has nothing
// to evaluate.
func cutOff(t *testing.T, f *executorFixture) {
	t.Helper()
	f.links.mu.Lock()
	f.links.snapshot.State = agentlink.StateDown
	f.links.mu.Unlock()
	_, err := f.store.DB().ExecContext(t.Context(), `UPDATE sessions SET state = ? WHERE session_id = ?`,
		protocol.SessionStateDetached, executorTestSession)
	require.NoError(t, err)
}

func (f *executorFixture) advance(d time.Duration) {
	f.clock.mu.Lock()
	f.clock.now = f.clock.now.Add(d)
	f.clock.mu.Unlock()
}

// expireRow dates candidate's expiry a second before now.
func expireRow(t *testing.T, f *executorFixture, candidate daemon.QueueDelivery) {
	t.Helper()
	_, err := f.store.DB().ExecContext(t.Context(), `UPDATE delivery_queue SET expires_at = ?
		WHERE message_id = ? AND agent_id = ? AND delivery_generation = ?`,
		eventstore.FormatTime(f.clock.Now().Add(-time.Second)),
		candidate.MessageID, candidate.AgentID, candidate.DeliveryGeneration)
	require.NoError(t, err)
}

// insertCommittedEvidence records the Proxy's COMMITTED result for the
// attempt id of candidate.
func insertCommittedEvidence(
	t *testing.T,
	f *executorFixture,
	execer interface {
		ExecContext(context.Context, string, ...any) (sql.Result, error)
	},
	candidate daemon.QueueDelivery,
	id protocol.OpaqueID,
) {
	t.Helper()
	stamp := eventstore.FormatTime(f.clock.Now())
	_, err := execer.ExecContext(t.Context(), `INSERT INTO delivery_result_evidence (
		delivery_id, agent_id, adapter_session_id, result, frame_id, reply_to,
		proxy_observed_at, received_at, observed_draft_generation,
		observed_input_guard_version, writer_lease_id, cancel_reason, control_versions_json
	) VALUES (?, ?, ?, 'COMMITTED', 7, 3, ?, ?, 2, 3, ?, NULL, '{"global":0}')`,
		id, candidate.AgentID, executorTestSession, stamp, stamp, "lease-"+string(id))
	require.NoError(t, err)
}

// seedUncertainCommit leaves candidate as a timed-out commit does: the
// attempt id UNCERTAIN and unresolved, the row QUEUED again and a pending
// recovery approval bound to the attempt. Then the Proxy's late COMMITTED
// result arrives.
func seedUncertainCommit(t *testing.T, f *executorFixture, candidate daemon.QueueDelivery, id protocol.OpaqueID) {
	t.Helper()
	seedExecutorAttempt(t, f, candidate, id, daemon.AttemptCommitSent)
	tx, err := f.store.DB().BeginTx(t.Context(), nil)
	require.NoError(t, err)
	require.NoError(t, f.executor.attempts.MarkUncertain(t.Context(), tx, id, daemon.ReasonTimedOut))
	require.NoError(t, transitionAttemptQueue(t.Context(), tx, id, deliveryRecord{QueueDelivery: candidate},
		queueStateDelivering, queueStateQueued, f.clock.Now(), false))
	require.NoError(t, f.executor.createUncertainApproval(t.Context(), tx, id, candidate.MessageID,
		candidate.AgentID, candidate.DeliveryGeneration, "commit outcome uncertain"))
	insertCommittedEvidence(t, f, tx, candidate, id)
	require.NoError(t, tx.Commit())
}

// attemptResolution reads the attempt's state and the approval that
// resolved it, if any.
func attemptResolution(t *testing.T, db *sql.DB, id protocol.OpaqueID) (string, sql.NullString) {
	t.Helper()
	var state string
	var resolution sql.NullString
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT state, resolution_approval_id
		FROM delivery_attempts WHERE delivery_id = ?`, id).Scan(&state, &resolution))
	return state, resolution
}

// A row deferred to the human is closed once it expires, after its link
// and session went away and with the channel OFF: the sweep needs neither
// them nor the model, and the pending approval does not hold it back. It
// records no decision and creates no attempt.
func TestSweepExpiresADeferredRow(t *testing.T) {
	t.Parallel()

	f := newExecutorFixture(t)
	worker := newSweepWorker(t, f, f.executor, f.links)
	candidate := f.seedDelivery(t, 200)
	processed, err := worker.RunOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, processed, "the scan defers the fresh row")
	require.Len(t, approvalsIn(t, f, protocol.ApprovalStatePending), 1)

	cutOff(t, f)
	_, err = f.controller.SetGlobal(t.Context(), protocol.SetGlobalParams{
		Mode: protocol.ChannelModeOff, ExpectedVersion: 0,
	}, channel.Actor{Type: protocol.ActorTypeHuman, ID: executorTestOperator})
	require.NoError(t, err)
	db := f.store.DB()
	decisions := len(decisionsFor(t, db, candidate.MessageID))
	f.advance(2 * time.Hour)
	processed, err = worker.RunOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, processed)
	require.Equal(t, queueStateExpired, queueStateOf(t, db, candidate))
	require.Len(t, decisionsFor(t, db, candidate.MessageID), decisions, "the sweep decides nothing")
	require.Zero(t, attemptCount(t, db, candidate.MessageID))

	processed, err = worker.RunOnce(t.Context())
	require.NoError(t, err)
	require.Zero(t, processed, "a closed row is not swept again")
}

// A row that both expired and committed closes as EXPIRED, since the
// allowed actions put EXPIRE first. The UNCERTAIN attempt and its recovery
// approval stay for the human.
func TestSweepExpiresBeforeItDropsADuplicate(t *testing.T) {
	t.Parallel()

	f := newExecutorFixture(t)
	worker := newSweepWorker(t, f, f.executor, f.links)
	candidate := f.seedDelivery(t, 201)
	const id protocol.OpaqueID = "dlv-expired"
	seedUncertainCommit(t, f, candidate, id)
	recovery := approvalsIn(t, f, protocol.ApprovalStatePending)
	require.Len(t, recovery, 1)
	expireRow(t, f, candidate)

	processed, err := worker.RunOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, processed)
	db := f.store.DB()
	require.Equal(t, queueStateExpired, queueStateOf(t, db, candidate))
	require.Equal(t, recovery, approvalsIn(t, f, protocol.ApprovalStatePending))
	state, resolution := attemptResolution(t, db, id)
	require.Equal(t, string(daemon.AttemptUncertain), state)
	require.False(t, resolution.Valid, "the sweep resolves no UNCERTAIN attempt")
	require.Equal(t, 1, attemptCount(t, db, candidate.MessageID))
}

// Only a row exactly QUEUED that expired, or that committed at its own
// generation, is swept, oldest first: a fresh row, an expired row a human
// order holds and a row whose earlier generation committed stay as they
// are.
func TestSweepClosesOnlyWhatNeedsClosing(t *testing.T) {
	t.Parallel()

	f := newExecutorFixture(t)
	worker := newSweepWorker(t, f, f.executor, f.links)
	db := f.store.DB()
	fresh := f.seedDelivery(t, 210)
	claimed := f.seedDelivery(t, 211)
	expireRow(t, f, claimed)
	_, err := db.ExecContext(t.Context(), `UPDATE delivery_queue SET state = 'CLAIMED',
		claimed_by = 'human-order' WHERE message_id = ?`, claimed.MessageID)
	require.NoError(t, err)
	earlier := f.seedDelivery(t, 212)
	seedExecutorAttempt(t, f, earlier, "dlv-earlier", daemon.AttemptCommitSent)
	insertCommittedEvidence(t, f, db, earlier, "dlv-earlier")
	_, err = db.ExecContext(t.Context(), `UPDATE delivery_queue SET state = 'DELIVERED'
		WHERE message_id = ?`, earlier.MessageID)
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), `INSERT INTO delivery_queue
		(message_id, agent_id, delivery_generation, from_agent, thread_ref, state, priority,
		 created_at, expires_at, updated_at, hop_count, route_digest)
		SELECT message_id, agent_id, 2, from_agent, thread_ref, 'QUEUED', priority,
		 created_at, expires_at, updated_at, hop_count, route_digest
		FROM delivery_queue WHERE message_id = ? AND delivery_generation = 1`, earlier.MessageID)
	require.NoError(t, err)
	later := earlier
	later.DeliveryGeneration = 2
	expired := f.seedDelivery(t, 213)
	expireRow(t, f, expired)
	older := f.seedDelivery(t, 214)
	expireRow(t, f, older)
	_, err = db.ExecContext(t.Context(), `UPDATE delivery_queue SET created_at = ? WHERE message_id = ?`,
		eventstore.FormatTime(f.clock.Now().Add(-time.Minute)), older.MessageID)
	require.NoError(t, err)
	cutOff(t, f)

	candidates, err := f.executor.sweepCandidates(t.Context())
	require.NoError(t, err)
	require.Equal(t, []daemon.QueueDelivery{older, expired}, candidates, "only these, oldest first")
	processed, err := worker.RunOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, 2, processed)
	require.Equal(t, queueStateExpired, queueStateOf(t, db, older))
	require.Equal(t, queueStateExpired, queueStateOf(t, db, expired))
	require.Equal(t, queueStateQueued, queueStateOf(t, db, fresh))
	require.Equal(t, "CLAIMED", queueStateOf(t, db, claimed))
	require.Equal(t, queueStateDelivered, queueStateOf(t, db, earlier))
	require.Equal(t, queueStateQueued, queueStateOf(t, db, later),
		"another generation's commit is not this one's")
}

// A row expires at its expiry instant. The sweep closes it before the scan
// could take it, so no decision is recorded although the agent is live.
func TestSweepClosesAnExpiredRowBeforeTheScan(t *testing.T) {
	t.Parallel()

	f := newExecutorFixture(t)
	worker := newSweepWorker(t, f, f.executor, f.links)
	candidate := f.seedDelivery(t, 203)
	f.advance(time.Hour)
	processed, err := worker.RunOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, processed)
	db := f.store.DB()
	require.Equal(t, queueStateExpired, queueStateOf(t, db, candidate))
	require.Empty(t, decisionsFor(t, db, candidate.MessageID), "the sweep closed it, no decision did")
	require.Empty(t, approvalsIn(t, f, protocol.ApprovalStatePending))
}

// In its own transaction, reconcileOne closes nothing that is gone, that
// was taken meanwhile, or that needs no closing.
func TestReconcileOneLeavesWhatMovedOrNeedsNothing(t *testing.T) {
	t.Parallel()

	f := newExecutorFixture(t)
	db := f.store.DB()
	fresh := f.seedDelivery(t, 250)
	taken := f.seedDelivery(t, 251)
	expireRow(t, f, taken)
	_, err := db.ExecContext(t.Context(), `UPDATE delivery_queue SET state = 'CLAIMED',
		claimed_by = 'human-order' WHERE message_id = ?`, taken.MessageID)
	require.NoError(t, err)
	gone := fresh
	gone.DeliveryGeneration = 9
	for _, candidate := range []daemon.QueueDelivery{fresh, taken, gone} {
		closed, err := f.executor.reconcileOne(t.Context(), candidate)
		require.NoError(t, err)
		require.False(t, closed)
	}
	require.Equal(t, queueStateQueued, queueStateOf(t, db, fresh))
	require.Equal(t, "CLAIMED", queueStateOf(t, db, taken))
}

// A row the sweep cannot read is reported, but the sweep still closes the
// rows after it and the scan still runs.
func TestSweepGoesOnPastARowItCannotRead(t *testing.T) {
	t.Parallel()

	f := newExecutorFixture(t)
	worker := newSweepWorker(t, f, f.executor, f.links)
	db := f.store.DB()
	unreadable := f.seedDelivery(t, 240)
	fresh := f.seedDelivery(t, 241)
	expired := f.seedDelivery(t, 242)
	processed, err := worker.RunOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, processed, "the scan defers the agent's first row")
	require.Len(t, pendingNotes(t, db, unreadable.MessageID), 1)
	_, err = db.ExecContext(t.Context(), `UPDATE delivery_queue SET expires_at = '2000-01-01'
		WHERE message_id = ?`, unreadable.MessageID)
	require.NoError(t, err)
	expireRow(t, f, expired)

	processed, err = worker.RunOnce(t.Context())
	require.ErrorContains(t, err, "sweep delivery queue")
	require.ErrorContains(t, err, string(unreadable.MessageID))
	require.Equal(t, 2, processed, "the sweep closes one row and the scan evaluates another")
	require.Equal(t, queueStateQueued, queueStateOf(t, db, unreadable))
	require.Equal(t, queueStateExpired, queueStateOf(t, db, expired))
	require.Len(t, pendingNotes(t, db, fresh.MessageID), 1, "the scan deferred the fresh row")
}

// One sweep closes at most sweepBatch rows; the next one goes on.
func TestSweepClosesABoundedBatch(t *testing.T) {
	t.Parallel()

	f := newExecutorFixture(t)
	worker := newSweepWorker(t, f, f.executor, f.links)
	for sequence := range sweepBatch + 1 {
		f.seedDelivery(t, 300+sequence)
	}
	cutOff(t, f)
	f.advance(2 * time.Hour)
	processed, err := worker.RunOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, sweepBatch, processed)
	processed, err = worker.RunOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, processed)
	require.Equal(t, sweepBatch+1, countRows(t, f.store.DB(),
		"SELECT count(*) FROM delivery_queue WHERE state = 'EXPIRED'"))
}
