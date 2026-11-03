package channel

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

// ErrBridgeRecipientBusy rejects a retire while the agent holds an
// in-flight delivery attempt.
var ErrBridgeRecipientBusy = errors.New("bridge recipient has an in-flight delivery")

// RetireBridgeRecipient cancels every non-terminal bridge delivery of one
// source node addressed to one agent, in a single transaction. Messages,
// bridge mappings, acknowledgements and source read marks are preserved.
// A COMMIT_SENT or UNCERTAIN attempt for the agent rejects the retire
// without touching anything. The call is idempotent: a retired delivery
// stays cancelled and counts as already terminal.
func (c *Controller) RetireBridgeRecipient(
	ctx context.Context,
	p protocol.BridgeRetireRecipientParams,
	actor Actor,
) (protocol.BridgeRetireRecipientResult, error) {
	if err := p.Validate(); err != nil {
		return protocol.BridgeRetireRecipientResult{}, fmt.Errorf("%w: retire bridge recipient: %w", ErrInvalidQuery, err)
	}
	if actor.Type == "" || actor.ID == "" {
		return protocol.BridgeRetireRecipientResult{}, fmt.Errorf("%w: retire bridge recipient", ErrInvalidQuery)
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return protocol.BridgeRetireRecipientResult{}, fmt.Errorf("retire bridge recipient: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	result, err := retireBridgeRecipientTx(ctx, tx, p, c.clock.Now())
	if err != nil {
		return protocol.BridgeRetireRecipientResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return protocol.BridgeRetireRecipientResult{}, fmt.Errorf("retire bridge recipient: %w", err)
	}
	return result, nil
}

// terminalQueueStates are never cancelled and never revived.
const retiredAlreadyTerminalStates = "'DELIVERED', 'FAILED', 'EXPIRED', 'CANCELLED'"

// inFlightAttemptStates reject a retire without touching anything.
const retiredInFlightStates = "'COMMIT_SENT', 'UNCERTAIN'"

func retireBridgeRecipientTx(
	ctx context.Context,
	tx *sql.Tx,
	p protocol.BridgeRetireRecipientParams,
	now time.Time,
) (protocol.BridgeRetireRecipientResult, error) {
	var inFlight int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM delivery_attempts
		WHERE agent_id = ? AND state IN (`+retiredInFlightStates+`)`,
		string(p.AgentID)).Scan(&inFlight)
	if err != nil {
		return protocol.BridgeRetireRecipientResult{}, fmt.Errorf("retire bridge recipient: %w", err)
	}
	if inFlight > 0 {
		return protocol.BridgeRetireRecipientResult{}, ErrBridgeRecipientBusy
	}
	var terminal int64
	err = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM delivery_queue
		WHERE (message_id, agent_id) IN (
			SELECT gifford_message_id, to_agent FROM bridge_message_mappings
			WHERE source_node_id = ? AND to_agent = ?
		) AND agent_id = ? AND state IN (`+retiredAlreadyTerminalStates+`)`,
		p.SourceNodeID, string(p.AgentID), string(p.AgentID)).Scan(&terminal)
	if err != nil {
		return protocol.BridgeRetireRecipientResult{}, fmt.Errorf("retire bridge recipient: %w", err)
	}
	res, err := tx.ExecContext(ctx, `UPDATE delivery_queue SET state = 'CANCELLED',
		control_reason = ?, updated_at = ?
		WHERE (message_id, agent_id) IN (
			SELECT gifford_message_id, to_agent FROM bridge_message_mappings
			WHERE source_node_id = ? AND to_agent = ?
		) AND agent_id = ? AND state NOT IN (`+retiredAlreadyTerminalStates+`)`,
		string(protocol.DecisionReasonRecipientRetired), eventstore.FormatTime(now),
		p.SourceNodeID, string(p.AgentID), string(p.AgentID))
	if err != nil {
		return protocol.BridgeRetireRecipientResult{}, fmt.Errorf("retire bridge recipient: %w", err)
	}
	cancelled, err := res.RowsAffected()
	if err != nil {
		return protocol.BridgeRetireRecipientResult{}, fmt.Errorf("retire bridge recipient: %w", err)
	}
	return protocol.BridgeRetireRecipientResult{
		Cancelled:       cancelled,
		InFlight:        false,
		AlreadyTerminal: terminal,
	}, nil
}
