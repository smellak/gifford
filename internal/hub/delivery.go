package hub

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/smellak/gifford/internal/channel"
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
	MessageID              protocol.ULID         `json:"message_id"`
	DeliveryGeneration     int64                 `json:"delivery_generation"`
	Envelope               json.RawMessage       `json:"envelope"`
	DeliveryMode           protocol.DeliveryMode `json:"delivery_mode"`
	RequiresAck            bool                  `json:"requires_ack"`
	SenderSigningKeyID     protocol.SigningKeyID `json:"sender_signing_key_id"`
	SenderSigningPublicKey []byte                `json:"sender_signing_public_key"`
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
	err = tx.QueryRowContext(ctx, `SELECT state, expires_at FROM messages WHERE message_id = ?`,
		params.MessageID).Scan(&state, &expiresAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, domainError(protocol.ErrorCodeNotFound, fmt.Errorf("message not found"))
	}
	if err != nil {
		return nil, fmt.Errorf("select claim message: %w", err)
	}
	var mode protocol.DeliveryMode
	var requiresAck bool
	err = tx.QueryRowContext(ctx, `SELECT delivery_mode, requires_ack FROM message_recipients
		WHERE message_id = ? AND agent_id = ?`, params.MessageID, agent).Scan(&mode, &requiresAck)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, domainError(protocol.ErrorCodeNotRecipient, fmt.Errorf("message recipient not found"))
	}
	if err != nil {
		return nil, fmt.Errorf("select claim recipient: %w", err)
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
	var deliveryExists, acknowledged bool
	if err := tx.QueryRowContext(ctx, `SELECT
		EXISTS(SELECT 1 FROM delivery_queue
			WHERE message_id = ? AND agent_id = ? AND delivery_generation = ?),
		EXISTS(SELECT 1 FROM acks
			WHERE message_id = ? AND agent_id = ? AND delivery_generation = ?
			AND status = 'ACKNOWLEDGED')`,
		params.MessageID, agent, params.DeliveryGeneration,
		params.MessageID, agent, params.DeliveryGeneration,
	).Scan(&deliveryExists, &acknowledged); err != nil {
		return nil, fmt.Errorf("select claim delivery: %w", err)
	}
	if !deliveryExists {
		return nil, domainError(protocol.ErrorCodeNotFound, fmt.Errorf("delivery generation not found"))
	}
	if acknowledged {
		return nil, domainError(protocol.ErrorCodeNotFound, fmt.Errorf("delivery already acknowledged"))
	}
	authorization, err := s.authorizeQueuedDelivery(ctx, tx, params.MessageID, agent, params.DeliveryGeneration)
	if err != nil {
		return nil, err
	}
	if !authorization.Allowed() {
		if err := commit(tx); err != nil {
			return nil, err
		}
		return nil, domainError(deliveryErrorCode(authorization.Reason()),
			fmt.Errorf("delivery held: %s", authorization.Reason()))
	}
	var envelope []byte
	if err := tx.QueryRowContext(ctx, "SELECT envelope FROM message_envelopes WHERE message_id = ?",
		params.MessageID).Scan(&envelope); err != nil {
		return nil, fmt.Errorf("select claim envelope: %w", err)
	}
	var wire protocol.WireEnvelope
	if err := json.Unmarshal(envelope, &wire); err != nil {
		return nil, fmt.Errorf("decode claim envelope: %w", err)
	}
	key, err := s.requireSenderKey(ctx, tx, wire.OriginAgent)
	if err != nil {
		return nil, err
	}
	if key.KeyID != string(wire.SenderSigningKeyID) {
		return nil, domainError(protocol.ErrorCodeKeyUnavailable, fmt.Errorf("sender signing key changed"))
	}
	if err := commit(tx); err != nil {
		return nil, err
	}
	return ClaimMessageResult{
		MessageID: params.MessageID, DeliveryGeneration: params.DeliveryGeneration,
		Envelope: json.RawMessage(envelope), DeliveryMode: mode, RequiresAck: requiresAck,
		SenderSigningKeyID:     wire.SenderSigningKeyID,
		SenderSigningPublicKey: append([]byte(nil), key.PublicKey...),
	}, nil
}

// PendingMessage is one exact delivery generation visible to a recipient.
type PendingMessage struct {
	MessageID          protocol.ULID    `json:"message_id"`
	DeliveryGeneration int64            `json:"delivery_generation"`
	FromAgent          protocol.AgentID `json:"from_agent"`
	CreatedAt          string           `json:"created_at"`
}

// ListPendingMessagesResult is the bounded internal pending list.
type ListPendingMessagesResult struct {
	Messages []PendingMessage `json:"messages"`
}

// ListPendingMessages returns only materialized, non-terminal WHEN_SAFE
// deliveries for the agent bound to the connection.
func (s *Service) ListPendingMessages(
	ctx context.Context,
	agent protocol.AgentID,
	raw json.RawMessage,
) (any, error) {
	var params struct{}
	if err := decodeParams(raw, &params); err != nil {
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
	rows, err := tx.QueryContext(ctx, `SELECT dq.message_id, dq.delivery_generation,
		dq.from_agent, dq.created_at FROM delivery_queue dq
		JOIN messages m ON m.message_id = dq.message_id
		JOIN message_recipients mr ON mr.message_id = dq.message_id AND mr.agent_id = dq.agent_id
		WHERE dq.agent_id = ? AND m.state = 'PENDING' AND mr.delivery_mode = 'WHEN_SAFE'
		AND dq.state NOT IN ('FAILED', 'EXPIRED', 'CANCELLED')
		AND NOT EXISTS (SELECT 1 FROM acks a WHERE a.message_id = dq.message_id
			AND a.agent_id = dq.agent_id AND a.delivery_generation = dq.delivery_generation
			AND a.status = 'ACKNOWLEDGED')
		ORDER BY dq.created_at, dq.message_id, dq.delivery_generation`, agent)
	if err != nil {
		return nil, fmt.Errorf("select pending messages: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var candidates []PendingMessage
	for rows.Next() {
		var pending PendingMessage
		if err := rows.Scan(&pending.MessageID, &pending.DeliveryGeneration,
			&pending.FromAgent, &pending.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan pending message: %w", err)
		}
		candidates = append(candidates, pending)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read pending messages: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close pending messages: %w", err)
	}
	answer := ListPendingMessagesResult{Messages: []PendingMessage{}}
	for _, pending := range candidates {
		authorization, err := s.authorizeQueuedDelivery(
			ctx, tx, pending.MessageID, agent, pending.DeliveryGeneration,
		)
		if err != nil {
			return nil, err
		}
		if authorization.Allowed() {
			answer.Messages = append(answer.Messages, pending)
		}
	}
	if err := commit(tx); err != nil {
		return nil, err
	}
	return answer, nil
}

func (s *Service) authorizeQueuedDelivery(
	ctx context.Context,
	tx *sql.Tx,
	messageID protocol.ULID,
	recipient protocol.AgentID,
	generation int64,
) (channel.DeliveryAuthorization, error) {
	message, found, err := loadStoredMessage(ctx, tx, messageID)
	if err != nil {
		return channel.DeliveryAuthorization{}, err
	}
	if !found {
		return channel.DeliveryAuthorization{}, domainError(protocol.ErrorCodeNotFound,
			fmt.Errorf("delivery message not found"))
	}
	var recipientMeta MessageRecipient
	found = false
	for _, candidate := range message.recipients {
		if candidate.Agent == recipient {
			recipientMeta = candidate
			found = true
			break
		}
	}
	if !found || recipientMeta.DeliveryMode != protocol.DeliveryModeWhenSafe {
		return channel.DeliveryAuthorization{}, domainError(protocol.ErrorCodeNotRecipient,
			fmt.Errorf("delivery recipient not found"))
	}
	gated, err := s.gateRecipients(ctx, tx, AdmitMessageParams{Recipients: []MessageRecipient{recipientMeta}})
	if err != nil {
		return channel.DeliveryAuthorization{}, err
	}
	if gated.destination != message.destinationNode {
		return channel.DeliveryAuthorization{}, domainError(protocol.ErrorCodeRecipientNotDeliverable,
			fmt.Errorf("delivery destination no longer matches recipient route"))
	}
	authorization, err := s.engine.AuthorizeDelivery(ctx, tx, channel.Message{
		ID: message.messageID, From: message.fromAgent, FromNode: message.originNode,
		To: gated.recipients, ToNode: message.destinationNode, ParentID: message.parentID,
		ThreadID: message.threadID, HopCount: message.hopCount,
	}, generation)
	if err != nil {
		return channel.DeliveryAuthorization{}, fmt.Errorf("authorize queued delivery: %w", err)
	}
	return authorization, nil
}

func deliveryErrorCode(reason protocol.DecisionReason) protocol.ErrorCode {
	code := protocol.ErrorCode(reason)
	if code.Valid() {
		return code
	}
	return protocol.ErrorCodePolicyDenied
}

// GetDeliveryStatusParams identifies a message owned by the bound sender.
type GetDeliveryStatusParams struct {
	MessageID protocol.ULID `json:"message_id"`
}

// RecipientDeliveryStatus reports one materialized generation, or generation
// zero with NOT_QUEUED when the delivery daemon has not materialized it yet.
type RecipientDeliveryStatus struct {
	Agent              protocol.AgentID `json:"agent_id"`
	DeliveryGeneration int64            `json:"delivery_generation"`
	State              string           `json:"state"`
	AckStatus          AckMessageStatus `json:"ack_status,omitempty"`
}

// GetDeliveryStatusResult is visible only to the authenticated sender.
type GetDeliveryStatusResult struct {
	MessageID  protocol.ULID             `json:"message_id"`
	State      string                    `json:"state"`
	Recipients []RecipientDeliveryStatus `json:"recipients"`
}

// GetDeliveryStatus returns per-recipient delivery generations for a message
// sent by the agent bound to the connection.
func (s *Service) GetDeliveryStatus(
	ctx context.Context,
	agent protocol.AgentID,
	raw json.RawMessage,
) (any, error) {
	var params GetDeliveryStatusParams
	if err := decodeParams(raw, &params); err != nil || !params.MessageID.Valid() {
		return nil, invalidParams(errInvalidArgument)
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer rollback(tx)
	var answer GetDeliveryStatusResult
	answer.MessageID = params.MessageID
	if err := tx.QueryRowContext(ctx, `SELECT state FROM messages
		WHERE message_id = ? AND from_agent = ?`, params.MessageID, agent).Scan(&answer.State); errors.Is(err, sql.ErrNoRows) {
		return nil, domainError(protocol.ErrorCodeNotFound, fmt.Errorf("message not found"))
	} else if err != nil {
		return nil, fmt.Errorf("select delivery status message: %w", err)
	}
	rows, err := tx.QueryContext(ctx, `SELECT mr.agent_id, dq.delivery_generation,
		COALESCE(dq.state, 'NOT_QUEUED'), CASE
		WHEN EXISTS (SELECT 1 FROM acks a WHERE a.message_id = mr.message_id AND a.agent_id = mr.agent_id
			AND a.delivery_generation = dq.delivery_generation AND a.status = 'MISMATCH') THEN 'MISMATCH'
		WHEN EXISTS (SELECT 1 FROM acks a WHERE a.message_id = mr.message_id AND a.agent_id = mr.agent_id
			AND a.delivery_generation = dq.delivery_generation AND a.status = 'ACKNOWLEDGED') THEN 'ACKNOWLEDGED'
		WHEN EXISTS (SELECT 1 FROM acks a WHERE a.message_id = mr.message_id AND a.agent_id = mr.agent_id
			AND a.delivery_generation = dq.delivery_generation AND a.status = 'PROCESSING') THEN 'PROCESSING'
		ELSE '' END
		FROM message_recipients mr LEFT JOIN delivery_queue dq ON dq.message_id = mr.message_id
			AND dq.agent_id = mr.agent_id WHERE mr.message_id = ?
		ORDER BY mr.ordinal, dq.delivery_generation`, params.MessageID)
	if err != nil {
		return nil, fmt.Errorf("select recipient delivery status: %w", err)
	}
	defer func() { _ = rows.Close() }()
	answer.Recipients = []RecipientDeliveryStatus{}
	for rows.Next() {
		var item RecipientDeliveryStatus
		var generation sql.NullInt64
		if err := rows.Scan(&item.Agent, &generation, &item.State, &item.AckStatus); err != nil {
			return nil, fmt.Errorf("scan recipient delivery status: %w", err)
		}
		item.DeliveryGeneration = generation.Int64
		answer.Recipients = append(answer.Recipients, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read recipient delivery status: %w", err)
	}
	if err := commit(tx); err != nil {
		return nil, err
	}
	return answer, nil
}

// AckMessageStatus is the internal RPC vocabulary. The first two values map
// to protocol.AckStatus; MISMATCH is local to hub.sock.
type AckMessageStatus string

const (
	// AckMessageProcessing reports that the Endpoint started processing.
	AckMessageProcessing AckMessageStatus = "PROCESSING"
	// AckMessageAcknowledged reports successful acknowledgement.
	AckMessageAcknowledged AckMessageStatus = "ACKNOWLEDGED"
	// AckMessageMismatch fails closed on admission metadata drift.
	AckMessageMismatch AckMessageStatus = "MISMATCH"
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
	Acked              bool             `json:"acked"`
}

func (p AckMessageParams) validate() error {
	if !p.MessageID.Valid() || p.DeliveryGeneration < 1 {
		return fmt.Errorf("ack params: %w", errInvalidArgument)
	}
	switch p.Status {
	case AckMessageProcessing, AckMessageAcknowledged, AckMessageMismatch:
		return nil
	default:
		return fmt.Errorf("ack status: %w", errInvalidArgument)
	}
}

// AckMessage records every internal acknowledgement idempotently. A metadata
// mismatch is durable without extending protocol.AckStatus, and it fails the
// matching materialized delivery row when present.
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
	var state string
	var originNode protocol.NodeID
	var envelopeExists bool
	if err := tx.QueryRowContext(ctx, `SELECT state, origin_node, EXISTS(SELECT 1 FROM message_envelopes
		WHERE message_id = messages.message_id) FROM messages WHERE message_id = ?`,
		params.MessageID).Scan(&state, &originNode, &envelopeExists); errors.Is(err, sql.ErrNoRows) {
		return nil, domainError(protocol.ErrorCodeNotFound, fmt.Errorf("message not found"))
	} else if err != nil {
		return nil, fmt.Errorf("select acknowledgement message: %w", err)
	}
	if state != messageStatePending || !envelopeExists {
		return nil, domainError(protocol.ErrorCodeNotFound, fmt.Errorf("message is not acknowledgeable"))
	}
	var recipientExists bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM message_recipients
		WHERE message_id = ? AND agent_id = ?)`, params.MessageID, agent).Scan(&recipientExists); err != nil {
		return nil, fmt.Errorf("select acknowledgement recipient: %w", err)
	}
	if !recipientExists {
		return nil, domainError(protocol.ErrorCodeNotRecipient, fmt.Errorf("message recipient not found"))
	}
	var deliveryExists bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM delivery_queue
		WHERE message_id = ? AND agent_id = ? AND delivery_generation = ?)`, params.MessageID,
		agent, params.DeliveryGeneration).Scan(&deliveryExists); err != nil {
		return nil, fmt.Errorf("select acknowledgement delivery: %w", err)
	}
	if !deliveryExists {
		return nil, domainError(protocol.ErrorCodeNotFound, fmt.Errorf("delivery generation not found"))
	}
	ackedAt := s.clock.Now().UTC()
	now := eventstore.FormatTime(ackedAt)
	inserted, err := tx.ExecContext(ctx, `INSERT INTO acks
		(message_id, agent_id, delivery_generation, status, acked_at)
		VALUES (?, ?, ?, ?, ?) ON CONFLICT DO NOTHING`, params.MessageID, agent,
		params.DeliveryGeneration, params.Status, now)
	if err != nil {
		return nil, fmt.Errorf("insert acknowledgement: %w", err)
	}
	insertedRows, err := inserted.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("count inserted acknowledgement: %w", err)
	}
	if params.Status == AckMessageMismatch {
		if _, err := tx.ExecContext(ctx, `UPDATE delivery_queue
			SET state = 'FAILED', updated_at = ?, claimed_by = NULL, claim_until = NULL
			WHERE message_id = ? AND agent_id = ? AND delivery_generation = ?`,
			now, params.MessageID, agent, params.DeliveryGeneration); err != nil {
			return nil, fmt.Errorf("fail mismatched delivery: %w", err)
		}
	} else if insertedRows == 1 && originNode != s.nodeID {
		if err := s.enqueueFederatedAck(
			ctx, tx, originNode, params.MessageID, agent, params.DeliveryGeneration,
			protocol.AckStatus(params.Status), ackedAt,
		); err != nil {
			return nil, err
		}
	}
	if err := commit(tx); err != nil {
		return nil, err
	}
	return AckMessageResult{
		MessageID: params.MessageID, DeliveryGeneration: params.DeliveryGeneration,
		Status: params.Status, Acked: true,
	}, nil
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
