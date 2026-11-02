package escalate

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/fallback"
	"github.com/smellak/gifford/internal/protocol"
)

// ClientMsgPrefix keys operator notices for hub deduplication: re-admitting
// the same escalation is idempotent.
const ClientMsgPrefix = "escalate:"

// ClientMsgID returns the deterministic admission key for one escalation.
func ClientMsgID(messageID protocol.ULID) protocol.OpaqueID {
	return protocol.OpaqueID(ClientMsgPrefix + string(messageID))
}

// Querier is the read half the loop needs: a transaction or a database.
type Querier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// Escalator marks escalations durable. Operator names the agent that
// receives operator notices.
type Escalator struct {
	operator protocol.AgentID
	clock    clock.Clock
}

// New validates the operator and returns an Escalator. A nil clock reads
// as the system clock.
func New(operator protocol.AgentID, clk clock.Clock) (*Escalator, error) {
	if !operator.Valid() {
		return nil, fmt.Errorf("escalate: invalid operator: %w", errInvalidOperator)
	}
	if clk == nil {
		clk = clock.System{}
	}
	return &Escalator{operator: operator, clock: clk}, nil
}

// Escalate records one escalation in the caller's transaction, idempotent:
// marking twice is a no-op. It performs no write outside the transaction;
// the operator notice itself travels normal admission afterwards.
func (e *Escalator) Escalate(
	ctx context.Context,
	tx *sql.Tx,
	messageID protocol.ULID,
	agentID protocol.AgentID,
) error {
	if tx == nil {
		return fmt.Errorf("escalate: nil transaction: %w", errInvalidInput)
	}
	if !messageID.Valid() {
		return fmt.Errorf("escalate: invalid message: %w", errInvalidInput)
	}
	if !agentID.Valid() {
		return fmt.Errorf("escalate: invalid agent: %w", errInvalidInput)
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO escalations
		(message_id, agent_id, operator_agent, created_at)
		VALUES (?, ?, ?, ?)
		ON CONFLICT (message_id) DO NOTHING`,
		string(messageID), string(agentID), string(e.operator),
		eventstore.FormatTime(e.clock.Now()))
	if err != nil {
		return fmt.Errorf("escalate: mark escalation: %w", err)
	}
	return nil
}

// Escalated reports whether the message already escalated.
func Escalated(ctx context.Context, q Querier, messageID protocol.ULID) (bool, error) {
	if !messageID.Valid() {
		return false, fmt.Errorf("escalate: invalid message: %w", errInvalidInput)
	}
	var one int
	err := q.QueryRowContext(ctx, `SELECT 1 FROM escalations WHERE message_id = ?`,
		string(messageID)).Scan(&one)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("escalate: read escalation: %w", err)
	}
	return true, nil
}

// ControlsOf maps one agent channel onto the fallback loop controls.
// Pause and manual mode arrive as Off, Hold and Manual here; message-side
// controls (rejected, expired, budget, approval) stay with the loop.
func ControlsOf(ch protocol.AgentChannel) fallback.Controls {
	return fallback.Controls{
		Off:    ch.Mode == protocol.AgentModeOff,
		Hold:   !ch.Deliverable && ch.Mode != protocol.AgentModeOff,
		Manual: !ch.AutoDelivery,
	}
}

// Operator returns the configured operator agent.
func (e *Escalator) Operator() protocol.AgentID { return e.operator }

// Deadline names how long a message may stay unread before escalation.
// It mirrors fallback.EscalateAfter so the two never disagree.
func Deadline() time.Duration { return fallback.EscalateAfter }
