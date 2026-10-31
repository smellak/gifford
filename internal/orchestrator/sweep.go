package orchestrator

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/smellak/gifford/internal/daemon"
	"github.com/smellak/gifford/internal/eventstore"
)

// This file holds the queue's maintenance sweep. A QUEUED delivery that
// expired, or whose commit already happened, needs no decision: the sweep
// closes it as EXPIRE or DROP_DUPLICATE would, whatever the agent's
// session, link, channel, approvals or other attempts, and without the
// model. Each row closes in its own short transaction, by compare-and-set
// on its exact identity and generation, and the ordinary approval left
// pending for it closes in the same transaction, through the channel.
// Rows in flight belong to whoever holds them and are left alone. The
// sweep never delivers, never resolves an UNCERTAIN attempt and never
// changes an attempt.

// sweepBatch bounds the rows one sweep closes.
const sweepBatch = 64

// The daemon's worker sweeps the queue through this interface.
var _ daemon.QueueReconciler = (*Executor)(nil)

// ReconcileQueue closes up to sweepBatch QUEUED deliveries, oldest first,
// that expired or have durable COMMITTED evidence for their exact
// identity. A row it cannot close does not stop the others: independent
// errors are joined. It returns how many rows it closed.
func (e *Executor) ReconcileQueue(ctx context.Context) (int, error) {
	candidates, err := e.sweepCandidates(ctx)
	if err != nil {
		return 0, err
	}
	closed := 0
	var joined error
	for _, candidate := range candidates {
		done, err := e.reconcileOne(ctx, candidate)
		if err != nil {
			joined = errors.Join(joined, fmt.Errorf("reconcile %s for %s: %w",
				candidate.MessageID, candidate.AgentID, err))
			continue
		}
		if done {
			closed++
		}
	}
	return closed, joined
}

// sweepCandidates reads the QUEUED rows that expired or already committed,
// oldest first.
func (e *Executor) sweepCandidates(ctx context.Context) ([]daemon.QueueDelivery, error) {
	rows, err := e.db.QueryContext(ctx, `SELECT dq.message_id, dq.agent_id, dq.delivery_generation
		FROM delivery_queue AS dq
		WHERE dq.state = 'QUEUED' AND (dq.expires_at <= ? OR EXISTS (
			SELECT 1 FROM delivery_attempts AS attempt
			JOIN delivery_result_evidence AS evidence ON evidence.delivery_id = attempt.delivery_id
			WHERE attempt.message_id = dq.message_id AND attempt.agent_id = dq.agent_id
				AND attempt.delivery_generation = dq.delivery_generation
				AND evidence.result = 'COMMITTED'))
		ORDER BY dq.created_at, dq.message_id, dq.agent_id, dq.delivery_generation
		LIMIT ?`, eventstore.FormatTime(e.clock.Now().UTC()), sweepBatch)
	if err != nil {
		return nil, fmt.Errorf("select deliveries to reconcile: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var candidates []daemon.QueueDelivery
	for rows.Next() {
		var candidate daemon.QueueDelivery
		if err := rows.Scan(&candidate.MessageID, &candidate.AgentID, &candidate.DeliveryGeneration); err != nil {
			return nil, fmt.Errorf("scan delivery to reconcile: %w", err)
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read deliveries to reconcile: %w", err)
	}
	return candidates, nil
}

// reconcileOne closes one candidate if, in this transaction, it is still
// exactly QUEUED at its generation and expired or committed. Expiry comes
// first, as in the allowed-actions rules. done is false when the row moved
// or no longer needs closing.
func (e *Executor) reconcileOne(ctx context.Context, candidate daemon.QueueDelivery) (bool, error) {
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin reconcile: %w", err)
	}
	defer rollback(tx)
	var state, expires string
	err = tx.QueryRowContext(ctx, `SELECT state, expires_at FROM delivery_queue
		WHERE message_id = ? AND agent_id = ? AND delivery_generation = ?`,
		candidate.MessageID, candidate.AgentID, candidate.DeliveryGeneration).Scan(&state, &expires)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("reload delivery to reconcile: %w", err)
	}
	if state != queueStateQueued {
		return false, nil
	}
	expiresAt, err := eventstore.ParseTime(expires)
	if err != nil {
		return false, fmt.Errorf("parse delivery expiry: %w", err)
	}
	record := deliveryRecord{QueueDelivery: candidate, expiresAt: expiresAt}
	now := e.clock.Now().UTC()
	closed, err := e.closeQueued(ctx, tx, record, now)
	if err != nil || !closed {
		return false, err
	}
	if err := e.closeStaleApproval(ctx, tx, record); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit reconcile: %w", err)
	}
	return true, nil
}

// closeQueued closes record, QUEUED in tx: EXPIRED once expired, else
// DELIVERED through the attempt whose COMMITTED evidence it reads, else
// nothing.
func (e *Executor) closeQueued(ctx context.Context, tx *sql.Tx, record deliveryRecord, now time.Time) (bool, error) {
	if !record.expiresAt.After(now) {
		return true, expireQueue(ctx, tx, record, now)
	}
	committed, err := e.attempts.HasCommittedEvidenceForIdentity(ctx, tx,
		record.MessageID, record.AgentID, record.DeliveryGeneration)
	if err != nil {
		return false, fmt.Errorf("check committed delivery evidence: %w", err)
	}
	if !committed {
		return false, nil
	}
	deliveryID, err := committedAttempt(ctx, tx, record)
	if err != nil {
		return false, err
	}
	return true, transitionAttemptQueue(ctx, tx, deliveryID, record, queueStateQueued,
		queueStateDelivered, now, false)
}

// closeStaleApproval supersedes, in tx and through the channel, the
// ordinary approval still pending for the recipient of a row the sweep
// closed: nothing is left to approve. A recovery approval stays. While the
// recipient has an unresolved UNCERTAIN attempt, the pending approval is
// that attempt's, and only the human resolves the episode.
func (e *Executor) closeStaleApproval(ctx context.Context, tx *sql.Tx, record deliveryRecord) error {
	var recovering int
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM delivery_attempts
		WHERE message_id = ? AND agent_id = ? AND state = 'UNCERTAIN'
			AND resolution_approval_id IS NULL)`, record.MessageID, record.AgentID).Scan(&recovering); err != nil {
		return fmt.Errorf("check unresolved uncertain recipient: %w", err)
	}
	if recovering == 1 {
		return nil
	}
	if _, err := e.approvals.SupersedeOrchestratorApproval(ctx, tx, record.MessageID, record.AgentID); err != nil {
		return fmt.Errorf("supersede stale approval: %w", err)
	}
	return nil
}
