package renotify

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/smellak/gifford/internal/clock"
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

// ErrOperatorNoticeExpired reports a retry whose prior admission
// already expired: the hub would refuse it before deduplicating, so
// there is nothing left to admit. The notice did exist; callers record
// this once and still mark the escalation, and never present the
// expired notice as delivered.
var ErrOperatorNoticeExpired = errors.New("renotify: operator notice already expired")

// AdmitOperatorNotice files the content-free escalation notice to the
// operator: only ids and the escalation key. The client message id keys
// the retry, so admitting twice after a mark failure still files one
// notice. The thread follows the original message; the notice expires a
// day after filing.
// node is the hub's own id, the same origin the service stamps on every
// local admission: reading the nodes table back can return a superseded
// id after the configured id changes, and the retry would miss the
// admission the hub deduplicates against. clk stamps the notice. Nil
// means the system clock. The admission runs in the service's own
// transaction; callers must not hold an open write transaction of their
// own across it.
func AdmitOperatorNotice(
	ctx context.Context,
	svc MessageAdmitter,
	db *sql.DB,
	clk clock.Clock,
	node protocol.NodeID,
	sender protocol.AgentID,
	operator protocol.AgentID,
	original protocol.ULID,
) error {
	if svc == nil || !node.Valid() || !sender.Valid() || !operator.Valid() || !original.Valid() {
		return fmt.Errorf("renotify: invalid operator admission")
	}
	var threadID, threadRef, priority string
	if err := db.QueryRowContext(ctx, `SELECT
		COALESCE(thread_id, ''), thread_ref, priority FROM messages WHERE message_id = ?`,
		string(original)).Scan(&threadID, &threadRef, &priority); err != nil {
		return fmt.Errorf("renotify: read escalated message: %w", err)
	}
	if threadID == "" {
		threadID = threadRef
	}
	if clk == nil {
		clk = clock.System{}
	}
	// A retry must carry the first attempt's stamps: the hub keys the
	// admission on (origin_node, from_agent, client_msg_id) with a NULL
	// namespace and rejects different created_at or expires_at under
	// it. The earliest local row under that key is the admission the
	// hub deduplicates against, never a federated row sharing the key.
	// No migration: the rows already carry both stamps.
	now := clk.Now().UTC()
	createdAt, expiresAt := eventstore.FormatTime(now), eventstore.FormatTime(now.Add(24*time.Hour))
	prior := false
	var storedCreated, storedExpires string
	err := db.QueryRowContext(ctx, `SELECT created_at, expires_at FROM messages
		WHERE origin_node = ? AND from_agent = ? AND client_msg_id = ?
		AND client_msg_namespace IS NULL
		ORDER BY created_at ASC LIMIT 1`,
		string(node), string(sender), string(escalate.ClientMsgID(original))).Scan(&storedCreated, &storedExpires)
	switch {
	case err == nil:
		prior = true
		createdAt, expiresAt = storedCreated, storedExpires
	case errors.Is(err, sql.ErrNoRows):
	default:
		return fmt.Errorf("renotify: read prior operator notice: %w", err)
	}
	if prior {
		expires, perr := eventstore.ParseTime(expiresAt)
		if perr != nil {
			return fmt.Errorf("renotify: parse prior operator notice expiry: %w", perr)
		}
		if !now.Before(expires) {
			// The hub refuses an expired admission before it even
			// deduplicates, so admitting again is pointless: the
			// notice did exist, and it is reported once, expired.
			return fmt.Errorf("%w: %s", ErrOperatorNoticeExpired, createdAt)
		}
	}
	// The notice shares the original thread so it reads in context, but
	// it is not a reply: a reply must reverse the parent route back to
	// its sender, while this notice goes to the operator instead.
	params := map[string]any{
		"client_msg_id": string(escalate.ClientMsgID(original)),
		"recipients": []map[string]any{{
			"agent_id":      string(operator),
			"delivery_mode": "WHEN_SAFE",
			"requires_ack":  true,
		}},
		"thread_id":  threadID,
		"priority":   priority,
		"created_at": createdAt,
		"expires_at": expiresAt,
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
