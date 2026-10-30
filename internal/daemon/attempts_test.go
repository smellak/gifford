package daemon

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

var attemptTestTime = time.Date(2026, time.October, 30, 17, 0, 0, 0, time.UTC)

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
		ControlVersions:        protocol.ControlVersions{"global": 2},
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
		change.WriterLeaseID = "writer-lease"
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
	require.NoError(t, store.MarkPrepared(t.Context(), tx, spec.DeliveryID, "writer-lease"))
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
			require.NoError(t, attempts.Transition(t.Context(), tx, spec.DeliveryID,
				transitionFor(edge.from, edge.to)))
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

	require.NoError(t, attempts.MarkPrepared(t.Context(), tx, spec.DeliveryID, "writer-lease"))
	err = attempts.MarkPrepared(t.Context(), tx, spec.DeliveryID, "writer-lease-2")
	var conflict *TransitionError
	require.ErrorAs(t, err, &conflict)
	require.ErrorIs(t, err, ErrInvalidTransition)
	require.Equal(t, AttemptPrepared, conflict.Actual)
	got, err = attempts.Get(t.Context(), tx, spec.DeliveryID)
	require.NoError(t, err)
	require.Equal(t, protocol.OpaqueID("writer-lease"), got.WriterLeaseID)
	require.NoError(t, tx.Commit())

	tx = beginAttemptTx(t, db.DB())
	err = attempts.Transition(t.Context(), tx, "missing-delivery",
		Transition{From: AttemptPreparing, To: AttemptPrepared, WriterLeaseID: "writer-lease"})
	require.ErrorIs(t, err, ErrAttemptNotFound)
	require.NoError(t, tx.Rollback())

	_, err = NewAttemptStore(nil)
	require.ErrorIs(t, err, ErrInvalidOptions)
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
	spec.ControlVersions["global"] = 99
	require.Equal(t, int64(2), created.ControlVersions["global"])
	require.NoError(t, tx.Rollback())

	bad := spec
	bad.ControlVersions = nil
	tx = beginAttemptTx(t, db.DB())
	_, err = attempts.Create(t.Context(), tx, bad)
	require.ErrorIs(t, err, ErrInvalidAttempt)
	require.NoError(t, tx.Rollback())
}
