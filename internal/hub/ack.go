package hub

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/smellak/gifford/internal/protocol"
)

// This file holds ack_message: one idempotent acknowledgement row per
// message, agent, and delivery generation. Repeats collapse on the key,
// so a second identical ack has no second effect.

// AckHandler serves ack_message behind Acker.
type AckHandler struct {
	deps Deps
}

// NewAckHandler returns the ack handler. Deps must be complete.
func NewAckHandler(deps Deps) (*AckHandler, error) {
	if !deps.valid() {
		return nil, fmt.Errorf("new ack handler: %w", errInvalidArgument)
	}
	return &AckHandler{deps: deps}, nil
}

// ackParams is the ack_message params.
type ackParams struct {
	MessageID          string `json:"message_id"`
	Status             string `json:"status"`
	DeliveryGeneration int64  `json:"delivery_generation,omitempty"`
}

// ackAnswer is the ack_message answer.
type ackAnswer struct {
	Acked bool `json:"acked"`
}

// AckMessage implements Acker.
func (h *AckHandler) AckMessage(
	ctx context.Context,
	agent protocol.AgentID,
	params json.RawMessage,
) (any, error) {
	var req ackParams
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, invalidRequest()
	}
	if !protocol.ULID(req.MessageID).Valid() {
		return nil, invalidRequest()
	}
	if !protocol.AckStatus(req.Status).Valid() {
		return nil, invalidRequest()
	}
	generation := req.DeliveryGeneration
	if generation == 0 {
		generation = 1
	}
	if generation < 1 {
		return nil, invalidRequest()
	}
	tx, err := h.deps.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, internalError()
	}
	defer func() { _ = tx.Rollback() }()
	var exists bool
	err = tx.QueryRowContext(ctx,
		`SELECT TRUE FROM messages WHERE message_id = ?`, req.MessageID).Scan(&exists)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			if err := tx.Commit(); err != nil {
				return nil, internalError()
			}
			return nil, serverError(protocol.ErrorCodeNotFound)
		}
		return nil, internalError()
	}
	var member bool
	err = tx.QueryRowContext(ctx,
		`SELECT TRUE FROM message_recipients WHERE message_id = ? AND agent_id = ?`,
		req.MessageID, string(agent)).Scan(&member)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			if err := tx.Commit(); err != nil {
				return nil, internalError()
			}
			return nil, serverError(protocol.ErrorCodeNotRecipient)
		}
		return nil, internalError()
	}
	text, err := queryText("ack_insert.sql")
	if err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, text,
		req.MessageID, string(agent), generation, req.Status, h.deps.Clock.Now().UTC().Format(timeLayout),
	); err != nil {
		return nil, fmt.Errorf("insert ack: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, internalError()
	}
	return ackAnswer{Acked: true}, nil
}
