package daemon

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"maps"

	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/daemon/agentlink"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

// DeliveryResultSource is the lifecycle-owned result stream of one
// authenticated agent link. Run does not start a detached goroutine: its
// caller owns both the source and this blocking consumer.
type DeliveryResultSource interface {
	DeliveryResults() <-chan agentlink.DeliveryResult
}

// DeliveryResultConsumer durably records late Proxy results. Evidence never
// mutates delivery_attempts, so a late ACK cannot rewrite UNCERTAIN or make an
// automatic retry eligible.
type DeliveryResultConsumer struct {
	db    *sql.DB
	clock clock.Clock
}

// NewDeliveryResultConsumer constructs a lifecycle consumer.
func NewDeliveryResultConsumer(db *sql.DB, clk clock.Clock) (*DeliveryResultConsumer, error) {
	if db == nil || clk == nil {
		return nil, ErrInvalidOptions
	}
	return &DeliveryResultConsumer{db: db, clock: clk}, nil
}

// Run consumes one authenticated link until its result stream closes or ctx
// ends. A validation or persistence failure is returned to the lifecycle
// owner rather than silently dropping critical evidence.
func (c *DeliveryResultConsumer) Run(
	ctx context.Context,
	agent protocol.AgentID,
	session protocol.OpaqueID,
	source DeliveryResultSource,
) error {
	if ctx == nil || !agent.Valid() || !session.Valid() || source == nil {
		return ErrInvalidOptions
	}
	results := source.DeliveryResults()
	if results == nil {
		return ErrInvalidOptions
	}
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("consume delivery results: %w", ctx.Err())
		case result, ok := <-results:
			if !ok {
				return nil
			}
			if _, err := c.Record(ctx, agent, session, result); err != nil {
				return err
			}
		}
	}
}

// Record validates exact agent/session/delivery correlation and inserts one
// semantic result kind once. The stored row deliberately omits Error and all
// message data.
func (c *DeliveryResultConsumer) Record(
	ctx context.Context,
	agent protocol.AgentID,
	session protocol.OpaqueID,
	report agentlink.DeliveryResult,
) (bool, error) {
	if ctx == nil || !agent.Valid() || !session.Valid() || report.FrameID < 1 ||
		report.ReplyTo < 1 || report.Result.AgentID != agent {
		return false, fmt.Errorf("record delivery result: %w", ErrInvalidAttempt)
	}
	if err := report.Result.Validate(); err != nil {
		return false, fmt.Errorf("validate delivery result evidence: %w", err)
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin delivery result evidence: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var controlJSON string
	var writerLease sql.NullString
	err = tx.QueryRowContext(ctx, `SELECT control_versions_json, writer_lease_id
		FROM delivery_attempts
		WHERE delivery_id = ? AND agent_id = ? AND adapter_session_id = ?`,
		report.Result.DeliveryID, agent, session).Scan(&controlJSON, &writerLease)
	if errors.Is(err, sql.ErrNoRows) {
		return false, fmt.Errorf("correlate delivery result evidence: %w", ErrAttemptNotFound)
	}
	if err != nil {
		return false, fmt.Errorf("correlate delivery result evidence: %w", err)
	}
	var controls protocol.ControlVersions
	if err := json.Unmarshal([]byte(controlJSON), &controls); err != nil {
		return false, fmt.Errorf("decode delivery result controls: %w", err)
	}
	if !maps.Equal(controls, report.Result.ControlVersions) {
		return false, fmt.Errorf("correlate delivery result controls: %w", ErrInvalidAttempt)
	}
	if report.Result.Result == protocol.DeliveryResultKindCommitted &&
		(!writerLease.Valid || report.Result.WriterLeaseID != protocol.OpaqueID(writerLease.String)) {
		return false, fmt.Errorf("correlate delivery result writer lease: %w", ErrInvalidAttempt)
	}

	encodedControls, err := json.Marshal(report.Result.ControlVersions)
	if err != nil {
		return false, fmt.Errorf("encode delivery result controls: %w", err)
	}
	var observedGuard any
	if report.Result.ObservedInputGuardVersion != nil {
		observedGuard = *report.Result.ObservedInputGuardVersion
	}
	result, err := tx.ExecContext(ctx, `INSERT INTO delivery_result_evidence (
		delivery_id, agent_id, adapter_session_id, result, frame_id, reply_to,
		proxy_observed_at, received_at, observed_draft_generation,
		observed_input_guard_version, writer_lease_id, cancel_reason,
		control_versions_json
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(delivery_id, result) DO NOTHING`,
		report.Result.DeliveryID, agent, session, report.Result.Result,
		report.FrameID, report.ReplyTo, eventstore.FormatTime(report.Result.ObservedAt.UTC()),
		eventstore.FormatTime(c.clock.Now().UTC()), report.Result.ObservedDraftGeneration,
		observedGuard, nullOpaqueID(report.Result.WriterLeaseID),
		nullCancelReason(report.Result.CancelReason), string(encodedControls))
	if err != nil {
		return false, fmt.Errorf("insert delivery result evidence: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("delivery result evidence rows affected: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit delivery result evidence: %w", err)
	}
	return changed == 1, nil
}

// HasCommittedEvidence reports whether the Proxy has durably reported that a
// delivery committed. Callers use it only as a resend veto; it never resolves
// or rewrites the source attempt.
func (s *AttemptStore) HasCommittedEvidence(
	ctx context.Context,
	tx *sql.Tx,
	deliveryID protocol.OpaqueID,
) (bool, error) {
	if tx == nil || !deliveryID.Valid() {
		return false, fmt.Errorf("%w: committed evidence query", ErrInvalidAttempt)
	}
	var found int
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(
		SELECT 1 FROM delivery_result_evidence
		WHERE delivery_id = ? AND result = 'COMMITTED'
	)`, deliveryID).Scan(&found); err != nil {
		return false, fmt.Errorf("query committed delivery evidence: %w", err)
	}
	return found == 1, nil
}

// HasCommittedEvidenceForIdentity reports whether any attempt for the exact
// message, recipient and delivery generation has durable COMMITTED evidence.
// The evidence remains a resend veto after a recovery attempt is cancelled or
// the source UNCERTAIN episode has been explicitly resolved.
func (s *AttemptStore) HasCommittedEvidenceForIdentity(
	ctx context.Context,
	tx *sql.Tx,
	messageID protocol.ULID,
	agent protocol.AgentID,
	generation int64,
) (bool, error) {
	if tx == nil || !messageID.Valid() || !agent.Valid() || generation < 1 {
		return false, fmt.Errorf("%w: committed evidence identity query", ErrInvalidAttempt)
	}
	var found int
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(
		SELECT 1
		FROM delivery_attempts AS attempt
		JOIN delivery_result_evidence AS evidence
			ON evidence.delivery_id = attempt.delivery_id
		WHERE attempt.message_id = ? AND attempt.agent_id = ?
			AND attempt.delivery_generation = ? AND evidence.result = 'COMMITTED'
	)`, messageID, agent, generation).Scan(&found); err != nil {
		return false, fmt.Errorf("query committed delivery identity evidence: %w", err)
	}
	return found == 1, nil
}

func nullCancelReason(reason protocol.CancelReason) any {
	if reason == "" {
		return nil
	}
	return reason
}
