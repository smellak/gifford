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
// there is nothing left to admit or enqueue. The notice did exist;
// callers record this once and still mark the escalation, and never
// present the expired notice as delivered.
var ErrOperatorNoticeExpired = errors.New("renotify: operator notice already expired")

// localNodeID returns the node's own id from the durable nodes table,
// the same boundary the hub stamps on every local admission. Callers
// need it to read back exactly the admission the hub deduplicates
// against, and never a federated row that happens to share the key.
func localNodeID(ctx context.Context, db *sql.DB) (string, error) {
	var node string
	if err := db.QueryRowContext(ctx, `SELECT node_id FROM nodes
		WHERE local = 1 ORDER BY node_id LIMIT 1`).Scan(&node); err != nil {
		return "", fmt.Errorf("renotify: read local node: %w", err)
	}
	return node, nil
}

// priorNotice is a previously admitted operator notice row.
type priorNotice struct {
	messageID   string
	createdAt   string
	expiresAt   string
	threadRef   string
	threadSeq   sql.NullInt64
	priority    string
	state       string
	hopCount    int64
	routeDigest string
}

// lookupPriorNotice returns the earliest local admission under the
// escalation key: the (origin_node, from_agent, client_msg_id) triple
// with a NULL namespace the hub deduplicates against, never a
// federated row sharing the key. No migration: the rows already carry
// every column read back.
func lookupPriorNotice(
	ctx context.Context,
	db *sql.DB,
	sender protocol.AgentID,
	original protocol.ULID,
) (priorNotice, bool, error) {
	var found priorNotice
	node, err := localNodeID(ctx, db)
	if err != nil {
		return found, false, err
	}
	err = db.QueryRowContext(ctx, `SELECT message_id, created_at, expires_at,
		thread_ref, thread_sequence, priority, state, hop_count, route_digest FROM messages
		WHERE origin_node = ? AND from_agent = ? AND client_msg_id = ?
		AND client_msg_namespace IS NULL
		ORDER BY created_at ASC LIMIT 1`,
		node, string(sender), string(escalate.ClientMsgID(original))).Scan(
		&found.messageID, &found.createdAt, &found.expiresAt, &found.threadRef,
		&found.threadSeq, &found.priority, &found.state, &found.hopCount, &found.routeDigest)
	switch {
	case err == nil:
		return found, true, nil
	case errors.Is(err, sql.ErrNoRows):
		return found, false, nil
	default:
		return found, false, fmt.Errorf("renotify: read prior operator notice: %w", err)
	}
}

// AdmitOperatorNotice files the content-free escalation notice to the
// operator: only ids and the escalation key. The client message id keys
// the retry, so admitting twice after a mark failure still files one
// notice. The thread follows the original message; the notice expires a
// day after filing.
// clk stamps the notice. Nil means the system clock. The admission
// runs in the service's own transaction; callers must not hold an
// open write transaction of their own across it.
func AdmitOperatorNotice(
	ctx context.Context,
	svc MessageAdmitter,
	db *sql.DB,
	clk clock.Clock,
	sender protocol.AgentID,
	operator protocol.AgentID,
	original protocol.ULID,
) error {
	if svc == nil || !sender.Valid() || !operator.Valid() || !original.Valid() {
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
	// admission on its idempotency triple and rejects different
	// created_at or expires_at under it.
	now := clk.Now().UTC()
	createdAt, expiresAt := eventstore.FormatTime(now), eventstore.FormatTime(now.Add(24*time.Hour))
	prior, found, err := lookupPriorNotice(ctx, db, sender, original)
	if err != nil {
		return err
	}
	if found {
		createdAt, expiresAt = prior.createdAt, prior.expiresAt
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

// EnqueueOperatorNotice files the first delivery of an admitted operator
// notice through the normal delivery path, exactly once per notice. The
// admission alone leaves the notice ADMITTED with no delivery row, and
// the submit path needs the sender's signature, which the loop cannot
// forge; this files the same row the submit would enqueue, with the
// channel triple the admission already evaluated. A turn that only
// admitted files it; a turn that finds any delivery row for the notice
// does nothing, so an admitted-but-never-enqueued retry still converges
// without a second notice. The check and the file run in one
// transaction, and the store opens every transaction BEGIN IMMEDIATE,
// so concurrent turns serialize on it.
func EnqueueOperatorNotice(
	ctx context.Context,
	db *sql.DB,
	engine DeliveryInitializer,
	clk clock.Clock,
	sender protocol.AgentID,
	operator protocol.AgentID,
	original protocol.ULID,
) error {
	if engine == nil || !sender.Valid() || !operator.Valid() || !original.Valid() {
		return fmt.Errorf("renotify: invalid operator enqueue")
	}
	prior, found, err := lookupPriorNotice(ctx, db, sender, original)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("renotify: no admitted operator notice to enqueue")
	}
	if prior.state != "ADMITTED" {
		// Owned elsewhere: a submitted, failed or expired notice moves
		// only through its own path.
		return nil
	}
	if clk == nil {
		clk = clock.System{}
	}
	now := clk.Now().UTC()
	expires, err := eventstore.ParseTime(prior.expiresAt)
	if err != nil {
		return fmt.Errorf("renotify: parse prior operator notice expiry: %w", err)
	}
	if !now.Before(expires) {
		return fmt.Errorf("%w: %s", ErrOperatorNoticeExpired, prior.createdAt)
	}
	noticeID := protocol.ULID(prior.messageID)
	if !noticeID.Valid() {
		return fmt.Errorf("renotify: stored operator notice id is invalid")
	}
	var queued bool
	if err := db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM delivery_queue
		WHERE message_id = ?)`, prior.messageID).Scan(&queued); err != nil {
		return fmt.Errorf("renotify: check operator notice delivery: %w", err)
	}
	if queued {
		return nil
	}
	rank, err := eventstore.PriorityRank(protocol.Priority(prior.priority))
	if err != nil {
		return fmt.Errorf("renotify: rank operator notice priority: %w", err)
	}
	stamp := eventstore.FormatTime(now)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("renotify: begin operator enqueue: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO delivery_queue
		(message_id, agent_id, delivery_generation, from_agent, thread_ref, state,
		priority, created_at, expires_at, updated_at,
		thread_sequence, hop_count, route_digest)
		VALUES(?, ?, 1, ?, ?, 'QUEUED', ?, ?, ?, ?, ?, ?, ?)`,
		prior.messageID, string(operator), string(sender), prior.threadRef,
		rank, stamp, prior.expiresAt, stamp,
		prior.threadSeq, prior.hopCount, prior.routeDigest); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("renotify: file operator notice delivery: %w", err)
	}
	if err := engine.InitializeDelivery(ctx, tx, noticeID, operator, 1); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("renotify: initialize operator notice delivery: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("renotify: commit operator enqueue: %w", err)
	}
	return nil
}
