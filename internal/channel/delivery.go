package channel

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/smellak/gifford/internal/protocol"
)

// DeliveryAuthorization is the recorded result of re-evaluating one queued
// delivery. It exposes no Decision and therefore confers no authority outside
// the transaction in which AuthorizeDelivery returned it.
type DeliveryAuthorization struct {
	allowed bool
	state   protocol.ControlState
	reason  protocol.DecisionReason
	version int64
}

// Allowed reports whether the queued delivery may be returned to its recipient.
func (a DeliveryAuthorization) Allowed() bool { return a.allowed }

// State returns the control state written on the delivery row.
func (a DeliveryAuthorization) State() protocol.ControlState { return a.state }

// Reason returns the governing delivery reason.
func (a DeliveryAuthorization) Reason() protocol.DecisionReason { return a.reason }

// Version returns the node control epoch written on the delivery row.
func (a DeliveryAuthorization) Version() int64 { return a.version }

// InitializeDelivery copies the message's final acceptance triple onto a new
// delivery row. Hub inserts the operational row first; only channel writes the
// protected control columns, in the same caller-owned transaction.
func (e *Engine) InitializeDelivery(
	ctx context.Context,
	tx *sql.Tx,
	messageID protocol.ULID,
	recipient protocol.AgentID,
	generation int64,
) error {
	if tx == nil || !messageID.Valid() || !recipient.Valid() || generation < 1 {
		return fmt.Errorf("%w: delivery initialization", ErrInvalidQuery)
	}
	control, found, err := selectMessageControl(ctx, tx, messageID)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("%w: delivery message", ErrInvalidQuery)
	}
	if err := validateTriple(control); err != nil {
		return err
	}
	query, err := queryText("deliveries_set_control.sql")
	if err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, query, control.state, control.reason, control.version,
		messageID, recipient, generation)
	if err != nil {
		return fmt.Errorf("initialize delivery control: %w", err)
	}
	return requireOneRow(result, "initialize delivery control")
}

// AuthorizeDelivery records a fresh delivery-stage decision and atomically
// replaces the queued row's control triple. The caller must act before its
// transaction commits; list and claim therefore cannot observe a stale allow.
func (e *Engine) AuthorizeDelivery(
	ctx context.Context,
	tx *sql.Tx,
	m Message,
	generation int64,
) (DeliveryAuthorization, error) {
	if tx == nil || generation < 1 || len(m.To) != 1 {
		return DeliveryAuthorization{}, fmt.Errorf("%w: delivery authorization", ErrInvalidQuery)
	}
	if err := m.validate(); err != nil {
		return DeliveryAuthorization{}, err
	}
	recipient := m.To[0].Agent
	stored, queueState, found, err := selectDeliveryControl(ctx, tx, m.ID, recipient, generation)
	if err != nil {
		return DeliveryAuthorization{}, err
	}
	if !found || deliveryTerminal(queueState) {
		return DeliveryAuthorization{}, fmt.Errorf("%w: queued delivery", ErrInvalidQuery)
	}
	if err := validateTriple(stored); err != nil {
		return DeliveryAuthorization{}, err
	}
	v, epoch, err := readGateView(ctx, tx)
	if err != nil {
		return DeliveryAuthorization{}, err
	}
	if epoch < stored.version {
		return DeliveryAuthorization{}, fmt.Errorf("%w: delivery epoch %d is behind stored %d",
			ErrInvalidControl, epoch, stored.version)
	}
	now := e.clock.Now()
	evaluated, err := evaluateRecipients(v, m, protocol.DecisionStageDelivery, now)
	if err != nil {
		return DeliveryAuthorization{}, err
	}
	decided, err := e.prepareDecisions(evaluated, now)
	if err != nil {
		return DeliveryAuthorization{}, err
	}
	if decided.Outcome() == Allow && (stored.state == protocol.ControlStateHeld ||
		stored.reason == protocol.DecisionReasonHoldReleased) {
		decided, err = releasedDecision(decided)
		if err != nil {
			return DeliveryAuthorization{}, err
		}
		evaluated[0].decided = decided
	}
	if err := insertDecisions(ctx, tx, evaluated); err != nil {
		return DeliveryAuthorization{}, err
	}
	next := tripleOf(decided, epoch)
	query, err := queryText("deliveries_cas_control.sql")
	if err != nil {
		return DeliveryAuthorization{}, err
	}
	result, err := tx.ExecContext(ctx, query,
		next.state, next.reason, next.version,
		m.ID, recipient, generation,
		stored.state, stored.reason, stored.version)
	if err != nil {
		return DeliveryAuthorization{}, fmt.Errorf("compare and set delivery control: %w", err)
	}
	if err := requireOneRow(result, "compare and set delivery control"); err != nil {
		return DeliveryAuthorization{}, err
	}
	return DeliveryAuthorization{
		allowed: decided.Allowed(), state: next.state, reason: next.reason, version: next.version,
	}, nil
}

func selectDeliveryControl(
	ctx context.Context,
	tx *sql.Tx,
	messageID protocol.ULID,
	recipient protocol.AgentID,
	generation int64,
) (triple, string, bool, error) {
	query, err := queryText("deliveries_select_control.sql")
	if err != nil {
		return triple{}, "", false, err
	}
	var state, reason, queueState string
	var rawVersion any
	err = tx.QueryRowContext(ctx, query, messageID, recipient, generation).
		Scan(&state, &reason, &rawVersion, &queueState)
	if errors.Is(err, sql.ErrNoRows) {
		return triple{}, "", false, nil
	}
	if err != nil {
		return triple{}, "", false, fmt.Errorf("select delivery control: %w", err)
	}
	version, ok := rawVersion.(int64)
	if !ok {
		return triple{}, "", false, fmt.Errorf("%w: delivery control version is not an integer", ErrInvalidControl)
	}
	return triple{
		state: protocol.ControlState(state), reason: protocol.DecisionReason(reason), version: version,
	}, queueState, true, nil
}

func releasedDecision(decided Decision) (Decision, error) {
	record := decided.Record()
	record.Reason = protocol.DecisionReasonHoldReleased
	record.Explanation.Reason = protocol.DecisionReasonHoldReleased
	return newDecision(record, restrictionNone, "")
}

func deliveryTerminal(state string) bool {
	switch state {
	case "FAILED", "EXPIRED", "CANCELLED":
		return true
	default:
		return false
	}
}
