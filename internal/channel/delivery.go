package channel

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"time"

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

// PreviewQueuedDelivery evaluates one queued delivery against a fresh
// transactional control view without minting a Decision or changing any
// control, queue, approval, or loop-budget row. It is a fail-closed prefilter,
// not delivery authority; callers must still use an authorization gate before
// acting.
func (e *Engine) PreviewQueuedDelivery(
	ctx context.Context,
	tx *sql.Tx,
	messageID protocol.ULID,
	recipient protocol.AgentID,
	generation int64,
) (bool, error) {
	if tx == nil || !messageID.Valid() || !recipient.Valid() || generation < 1 {
		return false, fmt.Errorf("%w: queued delivery preview", ErrInvalidQuery)
	}
	m, stored, queueState, found, err := selectDeliveryPreview(
		ctx, tx, messageID, recipient, generation,
	)
	if err != nil {
		return false, err
	}
	if !found || queueState != "QUEUED" {
		return false, nil
	}
	if err := m.validate(); err != nil {
		return false, err
	}
	evaluated, _, _, err := e.evaluateDeliveryControls(ctx, tx, m, stored)
	if err != nil {
		return false, err
	}
	if len(evaluated) != 1 {
		return false, fmt.Errorf("%w: queued delivery recipients", ErrInvalidQuery)
	}
	return evaluated[0].verdict.outcome == Allow, nil
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
	decided, next, err := e.authorizeDelivery(ctx, tx, m, generation, false, "")
	if err != nil {
		return DeliveryAuthorization{}, err
	}
	return DeliveryAuthorization{
		allowed: decided.Allowed(), state: next.state, reason: next.reason, version: next.version,
	}, nil
}

// AuthorizePTYDelivery records and returns the typed delivery-stage Decision
// needed by the PTY path. approvalID is empty for automatic delivery; a pending
// human deliver-now approval releases only AUTO_DELIVERY_OFF for this exact
// queue generation. Unlike DeliveryAuthorization, the Decision can cross the
// short gate transaction and is bound to this queue generation and epoch;
// callers still must Revalidate it immediately before commit.
func (e *Engine) AuthorizePTYDelivery(
	ctx context.Context,
	tx *sql.Tx,
	m Message,
	generation int64,
	approvalID protocol.ApprovalID,
) (Decision, error) {
	decided, _, err := e.authorizeDelivery(ctx, tx, m, generation, true, approvalID)
	return decided, err
}

func (e *Engine) authorizeDelivery(
	ctx context.Context,
	tx *sql.Tx,
	m Message,
	generation int64,
	pty bool,
	approvalID protocol.ApprovalID,
) (Decision, triple, error) {
	if tx == nil || generation < 1 || len(m.To) != 1 {
		return Decision{}, triple{}, fmt.Errorf("%w: delivery authorization", ErrInvalidQuery)
	}
	if err := m.validate(); err != nil {
		return Decision{}, triple{}, err
	}
	recipient := m.To[0].Agent
	stored, queueState, found, err := selectDeliveryControl(ctx, tx, m.ID, recipient, generation)
	if err != nil {
		return Decision{}, triple{}, err
	}
	if !found || deliveryTerminal(queueState) || pty && queueState == "DELIVERED" {
		return Decision{}, triple{}, fmt.Errorf("%w: queued delivery", ErrInvalidQuery)
	}
	if approvalID != "" {
		if !pty || !approvalID.Valid() {
			return Decision{}, triple{}, fmt.Errorf("%w: delivery approval", ErrInvalidQuery)
		}
		if err := requirePendingDeliveryApproval(
			ctx, tx, approvalID, m.ID, recipient, e.clock.Now(),
		); err != nil {
			return Decision{}, triple{}, err
		}
		m.approvedOnce = true
	}
	if stored.state == protocol.ControlStateAllowed &&
		stored.reason == protocol.DecisionReasonApprovedOnce {
		m.approvedOnce = true
	}
	evaluated, epoch, now, err := e.evaluateDeliveryControls(ctx, tx, m, stored)
	if err != nil {
		return Decision{}, triple{}, err
	}
	decided, err := e.prepareDecisions(evaluated, now)
	if err != nil {
		return Decision{}, triple{}, err
	}
	if decided.Outcome() == Allow && m.approvedOnce {
		decided, err = approvedOnceDecision(decided)
		if err != nil {
			return Decision{}, triple{}, err
		}
		evaluated[0].decided = decided
	} else if decided.Outcome() == Allow && (stored.state == protocol.ControlStateHeld ||
		stored.reason == protocol.DecisionReasonHoldReleased) {
		decided, err = releasedDecision(decided)
		if err != nil {
			return Decision{}, triple{}, err
		}
		evaluated[0].decided = decided
	}
	if err := insertDecisions(ctx, tx, evaluated); err != nil {
		return Decision{}, triple{}, err
	}
	next := tripleOf(decided, epoch)
	query, err := queryText("deliveries_cas_control.sql")
	if err != nil {
		return Decision{}, triple{}, err
	}
	result, err := tx.ExecContext(ctx, query,
		next.state, next.reason, next.version,
		m.ID, recipient, generation,
		stored.state, stored.reason, stored.version)
	if err != nil {
		return Decision{}, triple{}, fmt.Errorf("compare and set delivery control: %w", err)
	}
	if err := requireOneRow(result, "compare and set delivery control"); err != nil {
		return Decision{}, triple{}, err
	}
	return withDeliveryAuthority(decided, generation, epoch), next, nil
}

// evaluateDeliveryControls is the shared read-only half of delivery preview
// and authorization. It reads the caller's transaction and does not mint,
// persist, reserve, or consume anything.
func (e *Engine) evaluateDeliveryControls(
	ctx context.Context,
	tx *sql.Tx,
	m Message,
	stored triple,
) ([]gateEvaluation, int64, time.Time, error) {
	if err := validateTriple(stored); err != nil {
		return nil, 0, time.Time{}, err
	}
	if stored.state == protocol.ControlStateAllowed &&
		stored.reason == protocol.DecisionReasonApprovedOnce {
		m.approvedOnce = true
	}
	if m.ParentID != "" {
		if err := bindMessageLoopState(ctx, tx, &m); err != nil {
			return nil, 0, time.Time{}, err
		}
	}
	v, epoch, err := readGateView(ctx, tx)
	if err != nil {
		return nil, 0, time.Time{}, err
	}
	if epoch < stored.version {
		return nil, 0, time.Time{}, fmt.Errorf("%w: delivery epoch %d is behind stored %d",
			ErrInvalidControl, epoch, stored.version)
	}
	now := e.clock.Now()
	evaluated, err := evaluateRecipients(v, m, protocol.DecisionStageDelivery, now)
	if err != nil {
		return nil, 0, time.Time{}, err
	}
	return evaluated, epoch, now, nil
}

// Revalidate checks a PTY delivery Decision against a fresh transactional
// control view. A rejection records a revalidation-stage CONTROL_CHANGED
// decision and returns *RevalidationError; the caller commits that audit row.
// Success performs no write and never reserves or consumes a loop resource.
func (e *Engine) Revalidate(
	ctx context.Context,
	tx *sql.Tx,
	decided Decision,
	m Message,
	generation int64,
) error {
	if tx == nil || generation < 1 || len(m.To) != 1 {
		return fmt.Errorf("%w: delivery revalidation", ErrInvalidQuery)
	}
	if err := m.validate(); err != nil {
		return err
	}
	recipient := m.To[0].Agent
	stored, queueState, found, err := selectDeliveryControl(ctx, tx, m.ID, recipient, generation)
	if err != nil {
		return err
	}
	if found {
		if err := validateTriple(stored); err != nil {
			return err
		}
		if stored.state == protocol.ControlStateAllowed &&
			stored.reason == protocol.DecisionReasonApprovedOnce {
			m.approvedOnce = true
		}
	}
	if m.ParentID != "" {
		if err := bindMessageLoopState(ctx, tx, &m); err != nil {
			return err
		}
	}
	v, epoch, err := readGateView(ctx, tx)
	if err != nil {
		return err
	}
	now := e.clock.Now()
	evaluated, err := evaluateRecipients(v, m, protocol.DecisionStageDelivery, now)
	if err != nil {
		return err
	}
	current := evaluated[0]
	cause := revalidationCause(decided, m, generation, epoch, current, found, queueState)
	if cause == "" {
		return nil
	}
	revalidationID, err := e.insertRevalidationFailure(ctx, tx, current, now)
	if err != nil {
		return err
	}
	return &RevalidationError{decisionID: revalidationID, cause: cause}
}

func revalidationCause(
	decided Decision,
	m Message,
	generation, epoch int64,
	current gateEvaluation,
	found bool,
	queueState string,
) string {
	if decided.IsZero() {
		return "empty decision"
	}
	if decided.Stage() != protocol.DecisionStageDelivery {
		return "wrong decision stage"
	}
	if decided.Outcome() != Allow {
		return "decision does not allow"
	}
	objectType, objectID := decided.Object()
	if objectType != protocol.DecisionObjectTypeMessage || objectID != protocol.OpaqueID(m.ID) ||
		decided.Plane() != protocol.PlaneMessage || decided.From() != m.From ||
		decided.To() != m.To[0].Agent {
		return "delivery identity changed"
	}
	if decided.d.delivery.generation != generation {
		return "delivery generation changed"
	}
	if !found || ptyDeliveryTerminal(queueState) {
		return "queued delivery changed"
	}
	if decided.d.delivery.epoch != epoch {
		return "control epoch changed"
	}
	if !maps.Equal(decided.Versions(), current.vers) {
		return "control versions changed"
	}
	if current.verdict.outcome != Allow {
		return "fresh delivery evaluation does not allow"
	}
	return ""
}

func (e *Engine) insertRevalidationFailure(
	ctx context.Context,
	tx *sql.Tx,
	current gateEvaluation,
	now time.Time,
) (protocol.ChannelDecisionID, error) {
	id, err := e.mintDecisionID(now)
	if err != nil {
		return "", fmt.Errorf("mint revalidation decision id: %w", err)
	}
	failed := hold(protocol.DecisionReasonControlChanged)
	failed.line = protocol.ExplanationScopeObject
	failed.matched = current.verdict.matched
	current.query.Stage = protocol.DecisionStageRevalidation
	current.verdict = failed
	explanation := e.explainResult(current.query, failed, current.eff)
	if current.eff.Plane == "" {
		explanation = protocol.Explanation{
			FromAgent:  current.query.From,
			ToAgent:    current.query.To,
			Plane:      current.query.Plane,
			ObjectType: current.query.Object.Type,
			ObjectID:   current.query.Object.ID,
			Outcome:    Hold,
			Reason:     protocol.DecisionReasonControlChanged,
			Blocker:    protocol.ExplanationScopeObject,
			Lines: []protocol.ExplanationLine{{
				Scope: protocol.ExplanationScopeObject, Key: string(current.query.Object.ID),
				Value: "changed", Blocking: true,
			}},
		}
	}
	record, err := recordOf(id, current.query, failed, current.eff, current.vers, explanation, now)
	if err != nil {
		return "", err
	}
	current.decided, err = newDecision(record, restrictionHold, protocol.ExplanationScopeObject)
	if err != nil {
		return "", err
	}
	if err := insertDecisions(ctx, tx, []gateEvaluation{current}); err != nil {
		return "", err
	}
	return id, nil
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

func selectDeliveryPreview(
	ctx context.Context,
	tx *sql.Tx,
	messageID protocol.ULID,
	recipient protocol.AgentID,
	generation int64,
) (Message, triple, string, bool, error) {
	query, err := queryText("deliveries_select_preview.sql")
	if err != nil {
		return Message{}, triple{}, "", false, err
	}
	var m Message
	var threadID, parentID sql.NullString
	var rawVersion any
	var queueState, state, reason string
	var deliverable bool
	err = tx.QueryRowContext(ctx, query, messageID, recipient, generation).Scan(
		&state, &reason, &rawVersion, &queueState,
		&m.From, &m.FromNode, &m.ToNode, &threadID, &parentID, &m.HopCount,
		&deliverable,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return Message{}, triple{}, "", false, nil
	}
	if err != nil {
		return Message{}, triple{}, "", false, fmt.Errorf("select delivery preview: %w", err)
	}
	version, ok := rawVersion.(int64)
	if !ok {
		return Message{}, triple{}, "", false,
			fmt.Errorf("%w: delivery control version is not an integer", ErrInvalidControl)
	}
	m.ID = messageID
	m.To = []Recipient{{Agent: recipient, Deliverable: deliverable, Policy: PolicyAllowed}}
	m.ThreadID = protocol.ThreadID(threadID.String)
	m.ParentID = protocol.ULID(parentID.String)
	return m, triple{
		state: protocol.ControlState(state), reason: protocol.DecisionReason(reason), version: version,
	}, queueState, true, nil
}

func releasedDecision(decided Decision) (Decision, error) {
	record := decided.Record()
	record.Reason = protocol.DecisionReasonHoldReleased
	record.Explanation.Reason = protocol.DecisionReasonHoldReleased
	return newDecision(record, restrictionNone, "")
}

func approvedOnceDecision(decided Decision) (Decision, error) {
	record := decided.Record()
	record.Reason = protocol.DecisionReasonApprovedOnce
	record.Explanation.Reason = protocol.DecisionReasonApprovedOnce
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

// ptyDeliveryTerminal adds DELIVERED only for the push path. The pull path
// deliberately keeps accepting DELIVERED rows: PTY delivery marks the notice
// as delivered, not the message as claimed by its recipient.
func ptyDeliveryTerminal(state string) bool {
	return state == "DELIVERED" || deliveryTerminal(state)
}
