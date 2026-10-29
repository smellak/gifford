package hub

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/smellak/gifford/internal/protocol"
)

// This file holds claim_message: the read step for the model. It returns
// the stored envelope with recipient metadata for a pending message of
// the calling agent, and refuses everything else without content: blocked
// callers, strangers, unfinished messages, and display-only rows, which
// the model never sees.

// ClaimHandler serves claim_message behind Claimer.
type ClaimHandler struct {
	deps Deps
}

// NewClaimHandler returns the claim handler. Deps must be complete.
func NewClaimHandler(deps Deps) (*ClaimHandler, error) {
	if !deps.valid() {
		return nil, fmt.Errorf("new claim handler: %w", errInvalidArgument)
	}
	return &ClaimHandler{deps: deps}, nil
}

// claimParams is the claim_message params: exactly one message.
type claimParams struct {
	MessageID string `json:"message_id"`
}

// claimAnswer is the claim_message answer: the envelope with the
// recipient metadata the model needs.
type claimAnswer struct {
	MessageID    string          `json:"message_id"`
	Envelope     json.RawMessage `json:"envelope"`
	DeliveryMode string          `json:"delivery_mode"`
	RequiresAck  bool            `json:"requires_ack"`
}

// ClaimMessage implements Claimer.
func (h *ClaimHandler) ClaimMessage(
	ctx context.Context,
	agent protocol.AgentID,
	params json.RawMessage,
) (any, error) {
	var req claimParams
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, invalidRequest()
	}
	if !protocol.ULID(req.MessageID).Valid() {
		return nil, invalidRequest()
	}
	blocked, err := h.deps.Registry.Blocked(ctx, h.deps.DB, agent)
	if err != nil {
		return nil, internalError()
	}
	if blocked {
		return nil, serverError(protocol.ErrorCodeKeyUnavailable)
	}
	tx, err := h.deps.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, internalError()
	}
	defer func() { _ = tx.Rollback() }()
	row, meta, err := selectClaimRow(ctx, tx, req.MessageID, agent)
	if err != nil {
		return nil, err
	}
	if row == nil {
		if err := tx.Commit(); err != nil {
			return nil, internalError()
		}
		return nil, serverError(protocol.ErrorCodeNotFound)
	}
	if meta == nil {
		if err := tx.Commit(); err != nil {
			return nil, internalError()
		}
		return nil, serverError(protocol.ErrorCodeNotRecipient)
	}
	if row.state != "PENDING" {
		if err := tx.Commit(); err != nil {
			return nil, internalError()
		}
		return nil, serverError(protocol.ErrorCodeNotFound)
	}
	if meta.deliveryMode == string(protocol.DeliveryModeDisplayOnly) {
		if err := tx.Commit(); err != nil {
			return nil, internalError()
		}
		return nil, internalError()
	}
	envelope, err := selectStoredEnvelope(ctx, tx, req.MessageID)
	if err != nil {
		return nil, err
	}
	if envelope == nil {
		if err := tx.Commit(); err != nil {
			return nil, internalError()
		}
		return nil, internalError()
	}
	if err := tx.Commit(); err != nil {
		return nil, internalError()
	}
	return claimAnswer{
		MessageID:    req.MessageID,
		Envelope:     envelope,
		DeliveryMode: meta.deliveryMode,
		RequiresAck:  meta.requiresAck,
	}, nil
}

// claimRow is the message state one claim works with.
type claimRow struct {
	state string
}

// claimMeta is the caller's recipient row, or nil for strangers.
type claimMeta struct {
	deliveryMode string
	requiresAck  bool
}

// selectClaimRow reads the message state with the caller's recipient
// row, if any.
func selectClaimRow(
	ctx context.Context,
	tx *sql.Tx,
	messageID string,
	agent protocol.AgentID,
) (*claimRow, *claimMeta, error) {
	var row claimRow
	err := tx.QueryRowContext(ctx,
		`SELECT state FROM messages WHERE message_id = ?`, messageID).Scan(&row.state)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil, nil
		}
		return nil, nil, fmt.Errorf("select claim message: %w", err)
	}
	var meta claimMeta
	var ack int
	err = tx.QueryRowContext(ctx,
		`SELECT delivery_mode, requires_ack FROM message_recipients WHERE message_id = ? AND agent_id = ?`,
		messageID, string(agent)).Scan(&meta.deliveryMode, &ack)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return &row, nil, nil
		}
		return nil, nil, fmt.Errorf("select claim recipient: %w", err)
	}
	meta.requiresAck = ack != 0
	return &row, &meta, nil
}
