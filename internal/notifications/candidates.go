package notifications

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/smellak/gifford/internal/protocol"
)

type candidate struct {
	class      Class
	objectID   protocol.OpaqueID
	sender     protocol.AgentID
	recipients []protocol.AgentID
	priority   protocol.Priority
	reason     Reason
}

// messageCandidates intentionally reads messages + message_recipients, not
// delivery_queue. HELD and APPROVAL_REQUIRED are the retained set for 5.5a;
// DENIED is final and excluded. Envelope presence is the acceptance boundary.
func messageCandidates(
	ctx context.Context,
	tx *sql.Tx,
	nodeID protocol.NodeID,
	limit int,
	skipPendingApproval bool,
) ([]candidate, error) {
	approvalClause := ""
	if skipPendingApproval {
		approvalClause = `AND NOT EXISTS (
			SELECT 1 FROM approval_requests ar
			WHERE ar.object_type = 'message' AND ar.object_id = m.message_id
			  AND (ar.state = 'PENDING' OR EXISTS (
				SELECT 1 FROM human_notifications approval_notice
				WHERE approval_notice.class = 'APPROVAL'
				  AND approval_notice.object_id = ar.approval_id
			  ))
		)`
	}
	rows, err := tx.QueryContext(ctx, `SELECT m.message_id, m.from_agent, m.priority, m.control_state,
		EXISTS (
			SELECT 1 FROM message_recipients display_recipient
			WHERE display_recipient.message_id = m.message_id
			  AND display_recipient.delivery_mode = 'DISPLAY_ONLY'
		)
		FROM messages m
		JOIN message_envelopes envelope ON envelope.message_id = m.message_id
		WHERE m.destination_node = ?
		  AND m.control_state IN ('ALLOWED', 'HELD', 'APPROVAL_REQUIRED')
		  AND NOT EXISTS (
			SELECT 1 FROM human_notifications notification
			WHERE notification.class = 'MESSAGE' AND notification.object_id = m.message_id
		  )
		  `+approvalClause+`
		ORDER BY m.message_id
		LIMIT ?`, nodeID, limit)
	if err != nil {
		return nil, fmt.Errorf("select notification message candidates: %w", err)
	}
	defer func() { _ = rows.Close() }()
	type messageRow struct {
		id          protocol.ULID
		sender      protocol.AgentID
		priority    protocol.Priority
		control     protocol.ControlState
		displayOnly bool
	}
	var found []messageRow
	for rows.Next() {
		var row messageRow
		if err := rows.Scan(&row.id, &row.sender, &row.priority, &row.control, &row.displayOnly); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("scan notification message candidate: %w", err)
		}
		if !row.id.Valid() || !row.sender.Valid() || !row.priority.Valid() {
			_ = rows.Close()
			return nil, ErrInvalidRecord
		}
		found = append(found, row)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("read notification message candidates: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close notification message candidates: %w", err)
	}

	out := make([]candidate, 0, len(found))
	for _, row := range found {
		recipients, err := messageRecipients(ctx, tx, row.id)
		if err != nil {
			return nil, err
		}
		if len(recipients) == 0 {
			return nil, ErrInvalidRecord
		}
		reason := ReasonMessageAvailable
		switch row.control {
		case protocol.ControlStateHeld:
			reason = ReasonChannelHeld
		case protocol.ControlStateApprovalRequired:
			reason = ReasonChannelApprovalRequired
		case protocol.ControlStateAllowed:
			if row.displayOnly {
				reason = ReasonDisplayOnly
			}
		default:
			return nil, ErrInvalidRecord
		}
		out = append(out, candidate{
			class: ClassMessage, objectID: protocol.OpaqueID(row.id), sender: row.sender,
			recipients: recipients, priority: row.priority, reason: reason,
		})
	}
	return out, nil
}

func messageRecipients(
	ctx context.Context,
	tx *sql.Tx,
	messageID protocol.ULID,
) ([]protocol.AgentID, error) {
	rows, err := tx.QueryContext(ctx, `SELECT agent_id, delivery_mode
		FROM message_recipients WHERE message_id = ? ORDER BY ordinal`, messageID)
	if err != nil {
		return nil, fmt.Errorf("select notification recipients: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var recipients []protocol.AgentID
	seen := make(map[protocol.AgentID]struct{})
	for rows.Next() {
		var recipient protocol.AgentID
		var mode protocol.DeliveryMode
		if err := rows.Scan(&recipient, &mode); err != nil {
			return nil, fmt.Errorf("scan notification recipient: %w", err)
		}
		if !recipient.Valid() || !mode.Valid() {
			return nil, ErrInvalidRecord
		}
		if _, duplicate := seen[recipient]; duplicate {
			return nil, ErrInvalidRecord
		}
		seen[recipient] = struct{}{}
		recipients = append(recipients, recipient)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read notification recipients: %w", err)
	}
	return recipients, nil
}

func approvalCandidates(
	ctx context.Context,
	tx *sql.Tx,
	limit int,
) ([]candidate, error) {
	rows, err := tx.QueryContext(ctx, `SELECT approval.approval_id,
		COALESCE(message.from_agent, ''), COALESCE(message.priority, ''),
		COALESCE(approval.agent_id, ''), approval.object_type,
		COALESCE(message.message_id, '')
		FROM approval_requests approval
		LEFT JOIN messages message
		  ON approval.object_type = 'message' AND message.message_id = approval.object_id
		WHERE approval.state = 'PENDING'
		  AND NOT EXISTS (
			SELECT 1 FROM human_notifications notification
			WHERE notification.class = 'APPROVAL'
			  AND notification.object_id = approval.approval_id
		  )
		ORDER BY approval.approval_id
		LIMIT ?`, limit)
	if err != nil {
		return nil, fmt.Errorf("select notification approval candidates: %w", err)
	}
	defer func() { _ = rows.Close() }()
	type approvalRow struct {
		id        protocol.ApprovalID
		sender    protocol.AgentID
		priority  protocol.Priority
		recipient protocol.AgentID
		object    string
		messageID protocol.ULID
	}
	var found []approvalRow
	for rows.Next() {
		var row approvalRow
		if err := rows.Scan(&row.id, &row.sender, &row.priority, &row.recipient,
			&row.object, &row.messageID); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("scan notification approval candidate: %w", err)
		}
		if !row.id.Valid() || row.sender != "" && !row.sender.Valid() ||
			row.priority != "" && !row.priority.Valid() ||
			row.recipient != "" && !row.recipient.Valid() {
			_ = rows.Close()
			return nil, ErrInvalidRecord
		}
		found = append(found, row)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("read notification approval candidates: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close notification approval candidates: %w", err)
	}

	out := make([]candidate, 0, len(found))
	for _, row := range found {
		var recipients []protocol.AgentID
		switch {
		case row.recipient != "":
			recipients = []protocol.AgentID{row.recipient}
		case row.object == "message" && row.messageID.Valid():
			var err error
			recipients, err = messageRecipients(ctx, tx, row.messageID)
			if err != nil {
				return nil, err
			}
		}
		out = append(out, candidate{
			class: ClassApproval, objectID: protocol.OpaqueID(row.id), sender: row.sender,
			recipients: recipients, priority: row.priority, reason: ReasonApprovalPending,
		})
	}
	return out, nil
}
