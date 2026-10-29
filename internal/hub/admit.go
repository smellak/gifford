package hub

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

const (
	messageStateAdmitted = "ADMITTED"
	messageStatePending  = "PENDING"
	messageStateFailed   = "FAILED"
	messageStateExpired  = "EXPIRED"
)

type storedAdmission struct {
	messageID  protocol.ULID
	priority   protocol.Priority
	expiresAt  string
	state      string
	reason     protocol.DecisionReason
	result     protocol.AdmissionResult
	recipients []MessageRecipient
}

// AdmitMessage implements the sender-side A1-A8 matrix.
func (s *Service) AdmitMessage(
	ctx context.Context,
	sender protocol.AgentID,
	raw json.RawMessage,
) (any, error) {
	var params AdmitMessageParams
	if err := decodeParams(raw, &params); err != nil {
		return nil, invalidParams(err)
	}
	if err := params.validate(sender); err != nil {
		return nil, invalidParams(err)
	}
	if params.OriginNode != s.nodeID || params.DestinationNode != s.nodeID {
		return nil, domainError(protocol.ErrorCodeNotFound, fmt.Errorf("local route: %w", errInvalidArgument))
	}
	if !s.clock.Now().Before(params.ExpiresAt) {
		return nil, domainError(protocol.ErrorCodeExpired, fmt.Errorf("message expired"))
	}

	tx, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer rollback(tx)
	if _, err := s.requireSenderKey(ctx, tx, sender); err != nil {
		return nil, err
	}
	previous, found, err := loadAdmissionRetry(ctx, tx, s.nodeID, sender, params.ClientMsgID)
	if err != nil {
		return nil, err
	}
	if found {
		if err := sameAdmissionParams(params, previous); err != nil {
			return nil, invalidParams(err)
		}
		return answerAdmissionRetry(previous)
	}
	recipients, err := s.gateRecipients(ctx, tx, params)
	if err != nil {
		return nil, err
	}
	message := channel.Message{
		ID: params.MessageID, From: sender, FromNode: params.OriginNode,
		To: recipients, ToNode: params.DestinationNode,
		ParentID: params.ParentMessageID, ThreadID: params.ThreadID, HopCount: params.HopCount,
	}
	_, admitted, err := s.engine.Admit(ctx, tx, message, func(ctx context.Context, result protocol.AdmissionResult) error {
		return storeAdmission(ctx, tx, sender, params, result)
	})
	if err != nil {
		var denied *channel.DenyError
		switch {
		case errors.As(err, &denied):
			if commitErr := commit(tx); commitErr != nil {
				return nil, commitErr
			}
			return nil, domainError(denied.Code(), denied)
		case errors.Is(err, channel.ErrUnsupported), errors.Is(err, channel.ErrInvalidQuery):
			return nil, invalidParams(err)
		default:
			return nil, err
		}
	}
	if err := commit(tx); err != nil {
		return nil, err
	}
	return AdmitMessageResult{AdmissionResult: admitted, Submitted: false}, nil
}

func storeAdmission(
	ctx context.Context,
	tx *sql.Tx,
	sender protocol.AgentID,
	params AdmitMessageParams,
	result protocol.AdmissionResult,
) error {
	resultJSON, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("encode admission result: %w", err)
	}
	var budget any
	if params.RequestedBudget != nil {
		raw, marshalErr := json.Marshal(params.RequestedBudget)
		if marshalErr != nil {
			return fmt.Errorf("encode requested budget: %w", marshalErr)
		}
		budget = string(raw)
	}
	stamp := eventstore.FormatTime(params.CreatedAt)
	now := eventstore.FormatTime(params.CreatedAt)
	_, err = tx.ExecContext(ctx, `INSERT INTO messages
		(message_id, client_msg_id, origin_node, from_agent, destination_node,
		 thread_ref, thread_id, priority, state, requested_autonomy,
		 requested_budget_json, created_at, expires_at, updated_at,
		 parent_message_id, hop_count, route_digest, admission_result_json)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'ADMITTED', ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		params.MessageID, params.ClientMsgID, params.OriginNode, sender, params.DestinationNode,
		params.ThreadRef, params.ThreadID, params.Priority, nullableString(string(params.RequestedAutonomy)),
		budget, stamp, eventstore.FormatTime(params.ExpiresAt), now,
		nullableString(string(params.ParentMessageID)), params.HopCount, params.RouteDigest, string(resultJSON))
	if err != nil {
		return fmt.Errorf("insert admitted message: %w", err)
	}
	for ordinal, recipient := range params.Recipients {
		if _, err := tx.ExecContext(ctx, `INSERT INTO message_recipients
			(message_id, agent_id, ordinal, delivery_mode, requires_ack, created_at)
			VALUES (?, ?, ?, ?, ?, ?)`, params.MessageID, recipient.Agent, ordinal,
			recipient.DeliveryMode, recipient.RequiresAck, stamp); err != nil {
			return fmt.Errorf("insert message recipient: %w", err)
		}
	}
	return nil
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func loadAdmissionRetry(
	ctx context.Context,
	tx *sql.Tx,
	node protocol.NodeID,
	sender protocol.AgentID,
	clientID protocol.OpaqueID,
) (storedAdmission, bool, error) {
	var found storedAdmission
	var rawResult sql.NullString
	err := tx.QueryRowContext(ctx, `SELECT message_id, priority, expires_at, state,
		control_reason, admission_result_json FROM messages
		WHERE origin_node = ? AND from_agent = ? AND client_msg_id = ?`,
		node, sender, clientID).Scan(&found.messageID, &found.priority, &found.expiresAt,
		&found.state, &found.reason, &rawResult)
	if errors.Is(err, sql.ErrNoRows) {
		return storedAdmission{}, false, nil
	}
	if err != nil {
		return storedAdmission{}, false, fmt.Errorf("select admission retry: %w", err)
	}
	if !rawResult.Valid || json.Unmarshal([]byte(rawResult.String), &found.result) != nil {
		return storedAdmission{}, false, fmt.Errorf("stored admission result: %w", errInvalidArgument)
	}
	rows, err := tx.QueryContext(ctx, `SELECT agent_id, delivery_mode, requires_ack
		FROM message_recipients WHERE message_id = ? ORDER BY ordinal`, found.messageID)
	if err != nil {
		return storedAdmission{}, false, fmt.Errorf("select admission recipients: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var recipient MessageRecipient
		if err := rows.Scan(&recipient.Agent, &recipient.DeliveryMode, &recipient.RequiresAck); err != nil {
			return storedAdmission{}, false, fmt.Errorf("scan admission recipient: %w", err)
		}
		found.recipients = append(found.recipients, recipient)
	}
	if err := rows.Err(); err != nil {
		return storedAdmission{}, false, fmt.Errorf("read admission recipients: %w", err)
	}
	return found, true, nil
}

func sameAdmissionParams(params AdmitMessageParams, stored storedAdmission) error {
	if params.Priority != stored.priority || eventstore.FormatTime(params.ExpiresAt) != stored.expiresAt ||
		len(params.Recipients) != len(stored.recipients) {
		return fmt.Errorf("client_msg_id binds other parameters: %w", errInvalidArgument)
	}
	for i := range params.Recipients {
		if params.Recipients[i] != stored.recipients[i] {
			return fmt.Errorf("client_msg_id binds other recipients: %w", errInvalidArgument)
		}
	}
	return nil
}

func answerAdmissionRetry(stored storedAdmission) (any, error) {
	switch stored.state {
	case messageStateExpired:
		return nil, domainError(protocol.ErrorCodeExpired, fmt.Errorf("message expired"))
	case messageStateFailed:
		code := protocol.ErrorCode(stored.reason)
		if !code.Valid() {
			return nil, fmt.Errorf("stored denial reason %q: %w", stored.reason, errInvalidArgument)
		}
		return nil, domainError(code, fmt.Errorf("message denied"))
	default:
		return AdmitMessageResult{
			AdmissionResult: stored.result,
			Submitted:       stored.state != messageStateAdmitted,
		}, nil
	}
}
