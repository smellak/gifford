package renotify

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

// DeliveryInitializer opens the next generation for delivery. It is the
// channel engine's InitializeDelivery; tests substitute a count.
type DeliveryInitializer interface {
	InitializeDelivery(ctx context.Context, tx *sql.Tx, messageID protocol.ULID, recipient protocol.AgentID, generation int64) error
}

// Requeue files the next generation of a pending delivery so the normal
// path delivers it again, with every worker guard applied at attempt
// time. It is a no-op when a queued row already waits: the worker owns
// that delivery and the loop must not pile another generation behind it.
// clk stamps the new generation. Nil means the system clock.
func Requeue(ctx context.Context, tx *sql.Tx, clk clock.Clock, engine DeliveryInitializer, messageID protocol.ULID, agentID protocol.AgentID) error {
	if !messageID.Valid() || !agentID.Valid() {
		return fmt.Errorf("renotify: invalid requeue identity")
	}
	var live bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM delivery_queue
		WHERE message_id = ? AND agent_id = ? AND state = 'QUEUED')`,
		string(messageID), string(agentID)).Scan(&live); err != nil {
		return fmt.Errorf("renotify: check queued generation: %w", err)
	}
	if live {
		return nil
	}
	var generation int64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(delivery_generation), 0) FROM delivery_queue
		WHERE message_id = ? AND agent_id = ?`,
		string(messageID), string(agentID)).Scan(&generation); err != nil {
		return fmt.Errorf("renotify: read generations: %w", err)
	}
	if generation < 1 {
		return fmt.Errorf("renotify: nothing delivered to requeue")
	}
	if clk == nil {
		clk = clock.System{}
	}
	now := eventstore.FormatTime(clk.Now().UTC())
	res, err := tx.ExecContext(ctx, `INSERT INTO delivery_queue
		(message_id, agent_id, delivery_generation, from_agent, thread_ref, state,
		priority, created_at, expires_at, updated_at,
		parent_message_id, thread_sequence, loop_id, loop_generation, turn_index,
		hop_count, route_digest)
		SELECT message_id, agent_id, ?, from_agent, thread_ref, 'QUEUED',
		priority, ?, expires_at, ?,
		parent_message_id, thread_sequence, loop_id, loop_generation, turn_index,
		hop_count, route_digest
		FROM delivery_queue WHERE message_id = ? AND agent_id = ?
		AND delivery_generation = (SELECT MAX(delivery_generation) FROM delivery_queue
			WHERE message_id = ? AND agent_id = ?)`,
		generation+1, now, now,
		string(messageID), string(agentID), string(messageID), string(agentID))
	if err != nil {
		return fmt.Errorf("renotify: file next generation: %w", err)
	}
	if n, err := res.RowsAffected(); err != nil || n != 1 {
		return fmt.Errorf("renotify: file next generation: %w", err)
	}
	if engine == nil {
		return fmt.Errorf("renotify: nil delivery initializer")
	}
	if err := engine.InitializeDelivery(ctx, tx, messageID, agentID, generation+1); err != nil {
		return fmt.Errorf("renotify: initialize next generation: %w", err)
	}
	return nil
}
