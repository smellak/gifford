package renotify

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/smellak/gifford/internal/escalate"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

// MessageAdmitter admits a message through the normal path. It is the
// hub service's AdmitMessage; the admission carries its own transaction,
// so a later mark failure retries without duplicating the notice.
type MessageAdmitter interface {
	AdmitMessage(ctx context.Context, sender protocol.AgentID, raw json.RawMessage) (any, error)
}

// AdmitOperatorNotice files the content-free escalation notice to the
// operator: only ids and the escalation key. The client message id keys
// the retry, so admitting twice after a mark failure still files one
// notice. The thread follows the original message; the notice expires a
// day after filing.
func AdmitOperatorNotice(
	ctx context.Context,
	svc MessageAdmitter,
	tx *sql.Tx,
	sender protocol.AgentID,
	operator protocol.AgentID,
	original protocol.ULID,
) error {
	if svc == nil || !sender.Valid() || !operator.Valid() || !original.Valid() {
		return fmt.Errorf("renotify: invalid operator admission")
	}
	var threadID, threadRef, priority string
	if err := tx.QueryRowContext(ctx, `SELECT
		COALESCE(thread_id, ''), thread_ref, priority FROM messages WHERE message_id = ?`,
		string(original)).Scan(&threadID, &threadRef, &priority); err != nil {
		return fmt.Errorf("renotify: read escalated message: %w", err)
	}
	if threadID == "" {
		threadID = threadRef
	}
	now := time.Now().UTC()
	params := map[string]any{
		"client_msg_id": string(escalate.ClientMsgID(original)),
		"recipients": []map[string]any{{
			"agent_id":      string(operator),
			"delivery_mode": "WHEN_SAFE",
			"requires_ack":  true,
		}},
		"thread_id":         threadID,
		"priority":          priority,
		"created_at":        eventstore.FormatTime(now),
		"expires_at":        eventstore.FormatTime(now.Add(24 * time.Hour)),
		"parent_message_id": string(original),
	}
	raw, err := json.Marshal(params)
	if err != nil {
		return fmt.Errorf("renotify: encode operator notice: %w", err)
	}
	if _, err := svc.AdmitMessage(ctx, sender, raw); err != nil {
		return fmt.Errorf("renotify: admit operator notice: %w", err)
	}
	return nil
}
