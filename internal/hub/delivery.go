package hub

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

// ClaimMessageParams identifies the recipient delivery generation being
// fetched. A generation is explicit; the Hub never invents a default.
type ClaimMessageParams struct {
	MessageID          protocol.ULID `json:"message_id"`
	DeliveryGeneration int64         `json:"delivery_generation"`
}

// ClaimMessageResult returns the signed opaque envelope and the immutable
// recipient metadata the receiving Endpoint must compare after decryption.
type ClaimMessageResult struct {
	MessageID          protocol.ULID         `json:"message_id"`
	DeliveryGeneration int64                 `json:"delivery_generation"`
	Envelope           json.RawMessage       `json:"envelope"`
	DeliveryMode       protocol.DeliveryMode `json:"delivery_mode"`
	RequiresAck        bool                  `json:"requires_ack"`
}

func (p ClaimMessageParams) validate() error {
	if !p.MessageID.Valid() || p.DeliveryGeneration < 1 {
		return fmt.Errorf("claim params: %w", errInvalidArgument)
	}
	return nil
}

// ClaimMessage returns only a locally accepted PENDING envelope for the
// authenticated recipient. DISPLAY_ONLY is intentionally invisible here.
func (s *Service) ClaimMessage(
	ctx context.Context,
	agent protocol.AgentID,
	raw json.RawMessage,
) (any, error) {
	var params ClaimMessageParams
	if err := decodeParams(raw, &params); err != nil {
		return nil, invalidParams(err)
	}
	if err := params.validate(); err != nil {
		return nil, invalidParams(err)
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer rollback(tx)
	if err := s.requireUnblocked(ctx, tx, agent); err != nil {
		return nil, err
	}
	var state, expiresAt string
	var envelope []byte
	var mode protocol.DeliveryMode
	var requiresAck bool
	err = tx.QueryRowContext(ctx, `SELECT m.state, m.expires_at, e.envelope,
		r.delivery_mode, r.requires_ack
		FROM messages m
		JOIN message_recipients r ON r.message_id = m.message_id AND r.agent_id = ?
		JOIN message_envelopes e ON e.message_id = m.message_id
		WHERE m.message_id = ?`, agent, params.MessageID).Scan(
		&state, &expiresAt, &envelope, &mode, &requiresAck)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, domainError(protocol.ErrorCodeNotRecipient, fmt.Errorf("message is not claimable"))
	}
	if err != nil {
		return nil, fmt.Errorf("select claimable message: %w", err)
	}
	expires, err := time.Parse(eventstore.TimeLayout, expiresAt)
	if err != nil {
		return nil, fmt.Errorf("parse message expiry: %w", err)
	}
	if !s.clock.Now().Before(expires) || state == messageStateExpired {
		return nil, domainError(protocol.ErrorCodeExpired, fmt.Errorf("message expired"))
	}
	if state != messageStatePending || mode == protocol.DeliveryModeDisplayOnly {
		return nil, domainError(protocol.ErrorCodeNotFound, fmt.Errorf("message is not claimable"))
	}
	if err := commit(tx); err != nil {
		return nil, err
	}
	return ClaimMessageResult{
		MessageID: params.MessageID, DeliveryGeneration: params.DeliveryGeneration,
		Envelope: json.RawMessage(envelope), DeliveryMode: mode, RequiresAck: requiresAck,
	}, nil
}

// AckMessageStatus is the internal RPC vocabulary. The first two values map
// to protocol.AckStatus; METADATA_MISMATCH is local and never enters acks.
type AckMessageStatus string

const (
	// AckMessageProcessing reports that the Endpoint started processing.
	AckMessageProcessing AckMessageStatus = "PROCESSING"
	// AckMessageAcknowledged reports successful acknowledgement.
	AckMessageAcknowledged AckMessageStatus = "ACKNOWLEDGED"
	// AckMessageMetadataMismatch fails closed on admission metadata drift.
	AckMessageMetadataMismatch AckMessageStatus = "METADATA_MISMATCH"
)

// AckMessageParams reports one delivery generation's progress.
type AckMessageParams struct {
	MessageID          protocol.ULID    `json:"message_id"`
	DeliveryGeneration int64            `json:"delivery_generation"`
	Status             AckMessageStatus `json:"status"`
}

// AckMessageResult is stable across an idempotent retry.
type AckMessageResult struct {
	MessageID          protocol.ULID    `json:"message_id"`
	DeliveryGeneration int64            `json:"delivery_generation"`
	Status             AckMessageStatus `json:"status"`
}

func (p AckMessageParams) validate() error {
	if !p.MessageID.Valid() || p.DeliveryGeneration < 1 {
		return fmt.Errorf("ack params: %w", errInvalidArgument)
	}
	switch p.Status {
	case AckMessageProcessing, AckMessageAcknowledged, AckMessageMetadataMismatch:
		return nil
	default:
		return fmt.Errorf("ack status: %w", errInvalidArgument)
	}
}

// AckMessage records normal acknowledgements idempotently. A metadata
// mismatch never enters the frozen acks vocabulary; it fails the matching
// materialized delivery row, when present, and remains a stable local result.
func (s *Service) AckMessage(
	ctx context.Context,
	agent protocol.AgentID,
	raw json.RawMessage,
) (any, error) {
	var params AckMessageParams
	if err := decodeParams(raw, &params); err != nil {
		return nil, invalidParams(err)
	}
	if err := params.validate(); err != nil {
		return nil, invalidParams(err)
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer rollback(tx)
	if err := s.requireUnblocked(ctx, tx, agent); err != nil {
		return nil, err
	}
	var exists bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(
		SELECT 1 FROM message_recipients r
		JOIN message_envelopes e ON e.message_id = r.message_id
		WHERE r.message_id = ? AND r.agent_id = ?)`, params.MessageID, agent).Scan(&exists); err != nil {
		return nil, fmt.Errorf("select acknowledgement recipient: %w", err)
	}
	if !exists {
		return nil, domainError(protocol.ErrorCodeNotRecipient, fmt.Errorf("message recipient not found"))
	}
	now := eventstore.FormatTime(s.clock.Now())
	if params.Status == AckMessageMetadataMismatch {
		if _, err := tx.ExecContext(ctx, `UPDATE delivery_queue
			SET state = 'FAILED', updated_at = ?, claimed_by = NULL, claim_until = NULL
			WHERE message_id = ? AND agent_id = ? AND delivery_generation = ?`,
			now, params.MessageID, agent, params.DeliveryGeneration); err != nil {
			return nil, fmt.Errorf("fail mismatched delivery: %w", err)
		}
	} else {
		if _, err := tx.ExecContext(ctx, `INSERT INTO acks
			(message_id, agent_id, delivery_generation, status, acked_at)
			VALUES (?, ?, ?, ?, ?) ON CONFLICT DO NOTHING`, params.MessageID, agent,
			params.DeliveryGeneration, params.Status, now); err != nil {
			return nil, fmt.Errorf("insert acknowledgement: %w", err)
		}
	}
	if err := commit(tx); err != nil {
		return nil, err
	}
	return AckMessageResult(params), nil
}

// ExpireAdmitted performs T1 in one immediate transaction. Reservations and
// control triples remain untouched.
func (s *Service) ExpireAdmitted(ctx context.Context) (int64, error) {
	tx, err := s.begin(ctx)
	if err != nil {
		return 0, err
	}
	defer rollback(tx)
	now := eventstore.FormatTime(s.clock.Now())
	result, err := tx.ExecContext(ctx, `UPDATE messages SET state = 'EXPIRED', updated_at = ?
		WHERE state = 'ADMITTED' AND expires_at <= ?`, now, now)
	if err != nil {
		return 0, fmt.Errorf("expire admitted messages: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("count expired admissions: %w", err)
	}
	if err := commit(tx); err != nil {
		return 0, err
	}
	return count, nil
}
