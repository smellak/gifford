package daemon

import (
	"bytes"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

var attemptTestTime = time.Date(2026, time.October, 30, 17, 0, 0, 0, time.UTC)

const (
	attemptTestControlKey  = "global"
	attemptTestWriterLease = "writer-lease"
)

type attemptClock struct{ now time.Time }

func (c *attemptClock) Now() time.Time { return c.now }

func openAttemptTestStore(t *testing.T, path string) (*eventstore.Store, *AttemptStore, *attemptClock) {
	t.Helper()
	clk := &attemptClock{now: attemptTestTime}
	store, err := eventstore.Open(t.Context(), path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	attempts, err := NewAttemptStore(clk)
	require.NoError(t, err)
	return store, attempts, clk
}

func attemptTestMessage(n int) protocol.ULID {
	return protocol.ULID(fmt.Sprintf("01ARZ3NDEKTSV4RRFFQ69G%04d", n))
}

func attemptTestSpec(n int) AttemptSpec {
	messageID := attemptTestMessage(n)
	return AttemptSpec{
		DeliveryID:             protocol.OpaqueID(fmt.Sprintf("delivery-%d", n)),
		MessageID:              messageID,
		AgentID:                protocol.AgentID(fmt.Sprintf("agent-%d", n)),
		DeliveryGeneration:     1,
		AdapterSessionID:       protocol.OpaqueID(fmt.Sprintf("adapter-session-%d", n)),
		OrchestratorDecisionID: messageID,
		ChannelDecisionID:      protocol.ChannelDecisionID("chd_" + messageID),
		DraftGeneration:        3,
		InputGuardVersion:      7,
		ControlVersions:        protocol.ControlVersions{attemptTestControlKey: 2},
	}
}

func seedAttemptQueue(t *testing.T, db *sql.DB, spec AttemptSpec) {
	t.Helper()
	stamp := eventstore.FormatTime(attemptTestTime)
	_, err := db.ExecContext(t.Context(), `INSERT INTO delivery_queue
		(message_id, agent_id, delivery_generation, from_agent, thread_ref,
		 state, priority, created_at, expires_at, updated_at, hop_count, route_digest)
		VALUES (?, ?, ?, 'sender', 'thread', 'QUEUED', 2, ?, ?, ?, 0, 'sha256:route')`,
		spec.MessageID, spec.AgentID, spec.DeliveryGeneration, stamp, stamp, stamp)
	require.NoError(t, err)
}

func beginAttemptTx(t *testing.T, db *sql.DB) *sql.Tx {
	t.Helper()
	tx, err := db.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	return tx
}

func transitionFor(from, to AttemptState) Transition {
	change := Transition{From: from, To: to}
	if to == AttemptPrepared {
		change.WriterLeaseID = attemptTestWriterLease
	}
	if to == AttemptAbortedHumanPriority || to == AttemptCancelled ||
		to == AttemptFailed || to == AttemptUncertain {
		change.Reason = ReasonHumanPriority
	}
	return change
}

func prepareAttemptState(
	t *testing.T,
	store *AttemptStore,
	tx *sql.Tx,
	spec AttemptSpec,
	state AttemptState,
) {
	t.Helper()
	_, err := store.Create(t.Context(), tx, spec)
	require.NoError(t, err)
	if state == AttemptPreparing {
		return
	}
	require.NoError(t, store.MarkPrepared(t.Context(), tx, spec.DeliveryID, attemptTestWriterLease))
	if state == AttemptPrepared {
		return
	}
	require.Equal(t, AttemptCommitSent, state)
	require.NoError(t, store.MarkCommitSent(t.Context(), tx, spec.DeliveryID))
}

func TestAttemptStorePermittedTransitions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "attempts.db")
	db, attempts, clk := openAttemptTestStore(t, path)
	edges := []struct {
		from AttemptState
		to   AttemptState
	}{
		{AttemptPreparing, AttemptPrepared},
		{AttemptPreparing, AttemptAbortedHumanPriority},
		{AttemptPreparing, AttemptCancelled},
		{AttemptPreparing, AttemptFailed},
		{AttemptPrepared, AttemptCommitSent},
		{AttemptPrepared, AttemptAbortedHumanPriority},
		{AttemptPrepared, AttemptCancelled},
		{AttemptPrepared, AttemptFailed},
		{AttemptCommitSent, AttemptCommitted},
		{AttemptCommitSent, AttemptCommitNotConsumed},
		{AttemptCommitSent, AttemptAbortedHumanPriority},
		{AttemptCommitSent, AttemptCancelled},
		{AttemptCommitSent, AttemptFailed},
		{AttemptCommitSent, AttemptUncertain},
	}
	for i, edge := range edges {
		t.Run(string(edge.from)+"_to_"+string(edge.to), func(t *testing.T) {
			spec := attemptTestSpec(i + 1)
			seedAttemptQueue(t, db.DB(), spec)
			tx := beginAttemptTx(t, db.DB())
			prepareAttemptState(t, attempts, tx, spec, edge.from)
			clk.now = clk.now.Add(time.Second)
			change := transitionFor(edge.from, edge.to)
			require.NoError(t, attempts.Transition(t.Context(), tx, spec.DeliveryID, change))
			got, err := attempts.Get(t.Context(), tx, spec.DeliveryID)
			require.NoError(t, err)
			require.Equal(t, edge.to, got.State)
			if edge.to == AttemptPrepared || edge.from != AttemptPreparing {
				require.False(t, got.PreparedAt.IsZero())
			}
			if edge.to == AttemptCommitSent || edge.from == AttemptCommitSent {
				require.False(t, got.CommitSentAt.IsZero())
			}
			if edge.to.terminal() {
				require.False(t, got.FinishedAt.IsZero())
			}
			if change.Reason != "" {
				require.Equal(t, change.Reason, got.TerminalReason)
			}
			require.NoError(t, tx.Commit())
		})
	}
}

func TestAttemptStoreRejectsInvalidAndStaleTransitions(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "attempts.db")
	db, attempts, _ := openAttemptTestStore(t, path)
	spec := attemptTestSpec(20)
	seedAttemptQueue(t, db.DB(), spec)
	tx := beginAttemptTx(t, db.DB())
	_, err := attempts.Create(t.Context(), tx, spec)
	require.NoError(t, err)

	err = attempts.Transition(t.Context(), tx, spec.DeliveryID,
		Transition{From: AttemptPreparing, To: AttemptCommitted})
	require.ErrorIs(t, err, ErrInvalidTransition)
	got, err := attempts.Get(t.Context(), tx, spec.DeliveryID)
	require.NoError(t, err)
	require.Equal(t, AttemptPreparing, got.State)

	require.NoError(t, attempts.MarkPrepared(t.Context(), tx, spec.DeliveryID, attemptTestWriterLease))
	err = attempts.MarkPrepared(t.Context(), tx, spec.DeliveryID, "writer-lease-2")
	var conflict *TransitionError
	require.ErrorAs(t, err, &conflict)
	require.ErrorIs(t, err, ErrInvalidTransition)
	require.Equal(t, AttemptPrepared, conflict.Actual)
	got, err = attempts.Get(t.Context(), tx, spec.DeliveryID)
	require.NoError(t, err)
	require.Equal(t, protocol.OpaqueID(attemptTestWriterLease), got.WriterLeaseID)
	require.NoError(t, tx.Commit())

	tx = beginAttemptTx(t, db.DB())
	err = attempts.Transition(t.Context(), tx, "missing-delivery",
		Transition{From: AttemptPreparing, To: AttemptPrepared, WriterLeaseID: attemptTestWriterLease})
	require.ErrorIs(t, err, ErrAttemptNotFound)
	require.NoError(t, tx.Rollback())

	reasonSpec := attemptTestSpec(21)
	seedAttemptQueue(t, db.DB(), reasonSpec)
	tx = beginAttemptTx(t, db.DB())
	prepareAttemptState(t, attempts, tx, reasonSpec, AttemptCommitSent)
	err = attempts.Transition(t.Context(), tx, reasonSpec.DeliveryID,
		Transition{From: AttemptCommitSent, To: AttemptCancelled})
	require.ErrorIs(t, err, ErrInvalidAttempt)
	got, err = attempts.Get(t.Context(), tx, reasonSpec.DeliveryID)
	require.NoError(t, err)
	require.Equal(t, AttemptCommitSent, got.State)
	require.NoError(t, tx.Rollback())

	_, err = NewAttemptStore(nil)
	require.ErrorIs(t, err, ErrInvalidOptions)
}

func TestAttemptStoreRecoveryCancelsPreCommitAttemptsAfterRestart(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "attempts.db")
	db, attempts, _ := openAttemptTestStore(t, path)
	tests := []struct {
		spec  AttemptSpec
		state AttemptState
	}{
		{spec: attemptTestSpec(31), state: AttemptPreparing},
		{spec: attemptTestSpec(32), state: AttemptPrepared},
	}
	for _, test := range tests {
		seedAttemptQueue(t, db.DB(), test.spec)
		tx := beginAttemptTx(t, db.DB())
		prepareAttemptState(t, attempts, tx, test.spec, test.state)
		require.NoError(t, tx.Commit())
	}
	require.NoError(t, db.Close())

	reopened, err := eventstore.Open(t.Context(), path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	recovered, err := NewAttemptStore(&attemptClock{now: attemptTestTime.Add(time.Minute)})
	require.NoError(t, err)
	tx := beginAttemptTx(t, reopened.DB())
	count, err := recovered.RecoverCommitSent(t.Context(), tx, ReasonProcessRestart)
	require.NoError(t, err)
	require.Equal(t, int64(len(tests)), count)
	for _, test := range tests {
		closed, err := recovered.Get(t.Context(), tx, test.spec.DeliveryID)
		require.NoError(t, err)
		require.Equal(t, AttemptCancelled, closed.State)
		require.Equal(t, ReasonProcessRestart, closed.TerminalReason)
		require.False(t, closed.FinishedAt.IsZero())

		retry := test.spec
		retry.DeliveryID += "-retry"
		retry.AdapterSessionID += "-retry"
		created, err := recovered.Create(t.Context(), tx, retry)
		require.NoError(t, err, "restart recovery must release the active-attempt index")
		require.Equal(t, AttemptPreparing, created.State)
	}
	require.NoError(t, tx.Commit())
}

func TestAttemptStoreRecoveryKeepsUncertaintyDurable(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "attempts.db")
	db, attempts, _ := openAttemptTestStore(t, path)
	spec := attemptTestSpec(30)
	seedAttemptQueue(t, db.DB(), spec)
	tx := beginAttemptTx(t, db.DB())
	prepareAttemptState(t, attempts, tx, spec, AttemptCommitSent)
	require.NoError(t, tx.Commit())
	require.NoError(t, db.Close())

	reopened, err := eventstore.Open(t.Context(), path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	clk := &attemptClock{now: attemptTestTime.Add(time.Minute)}
	recovered, err := NewAttemptStore(clk)
	require.NoError(t, err)
	tx = beginAttemptTx(t, reopened.DB())
	count, err := recovered.RecoverCommitSent(t.Context(), tx, ReasonProcessRestart)
	require.NoError(t, err)
	require.Equal(t, int64(1), count)
	require.NoError(t, tx.Commit())

	tx = beginAttemptTx(t, reopened.DB())
	blocked, err := recovered.HasUncertain(t.Context(), tx, spec.AgentID)
	require.NoError(t, err)
	require.True(t, blocked)
	uncertain, err := recovered.Get(t.Context(), tx, spec.DeliveryID)
	require.NoError(t, err)
	require.Equal(t, AttemptUncertain, uncertain.State)
	require.Equal(t, ReasonProcessRestart, uncertain.TerminalReason)

	recovery := spec
	recovery.DeliveryID = "delivery-30-recovery"
	recovery.AdapterSessionID = "adapter-session-30-recovery"
	created, err := recovered.CreateRecovery(t.Context(), tx, recovery, spec.DeliveryID)
	require.NoError(t, err)
	require.Equal(t, AttemptPreparing, created.State)
	require.Equal(t, spec.DeliveryID, created.RecoveryOfDeliveryID)
	blocked, err = recovered.HasUncertain(t.Context(), tx, spec.AgentID)
	require.NoError(t, err)
	require.True(t, blocked)
	stillUncertain, err := recovered.Get(t.Context(), tx, spec.DeliveryID)
	require.NoError(t, err)
	require.Equal(t, AttemptUncertain, stillUncertain.State)

	engine, err := channel.New(channel.Options{Clock: clock.System{}})
	require.NoError(t, err)
	controller, err := channel.NewController(channel.ControllerOptions{
		DB: reopened.DB(), Engine: engine, Clock: clk, NodeID: "node-attempts",
		Entropy: bytes.NewReader(make([]byte, 16)),
	})
	require.NoError(t, err)
	approval, err := controller.CreateOrchestratorApproval(
		t.Context(), tx, spec.MessageID, spec.AgentID, "resolve uncertain test",
	)
	require.NoError(t, err)
	require.NoError(t, recovered.BindUncertainApproval(
		t.Context(), tx, spec.DeliveryID, approval.ApprovalID))
	require.NoError(t, recovered.ResolveUncertain(t.Context(), tx, spec.DeliveryID, approval.ApprovalID))
	blocked, err = recovered.HasUncertain(t.Context(), tx, spec.AgentID)
	require.NoError(t, err)
	require.False(t, blocked)
	resolvedUncertain, err := recovered.Get(t.Context(), tx, spec.DeliveryID)
	require.NoError(t, err)
	require.Equal(t, approval.ApprovalID, resolvedUncertain.UncertainApprovalID)
	require.Equal(t, approval.ApprovalID, resolvedUncertain.ResolutionApprovalID)
	require.Equal(t, AttemptUncertain, resolvedUncertain.State)
	require.NoError(t, tx.Commit())
}

func TestAttemptStoreCreateValidationAndDatabaseConstraints(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "attempts.db")
	db, attempts, _ := openAttemptTestStore(t, path)
	spec := attemptTestSpec(40)
	tx := beginAttemptTx(t, db.DB())
	_, err := attempts.Create(t.Context(), tx, spec)
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrInvalidAttempt, "valid attempt reaches the foreign-key constraint")
	require.NoError(t, tx.Rollback())

	seedAttemptQueue(t, db.DB(), spec)
	tx = beginAttemptTx(t, db.DB())
	created, err := attempts.Create(t.Context(), tx, spec)
	require.NoError(t, err)
	spec.ControlVersions[attemptTestControlKey] = 99
	require.Equal(t, int64(2), created.ControlVersions[attemptTestControlKey])
	require.NoError(t, tx.Rollback())

	bad := spec
	bad.ControlVersions = nil
	tx = beginAttemptTx(t, db.DB())
	_, err = attempts.Create(t.Context(), tx, bad)
	require.ErrorIs(t, err, ErrInvalidAttempt)
	require.NoError(t, tx.Rollback())
}
