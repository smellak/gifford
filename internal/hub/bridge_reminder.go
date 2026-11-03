package hub

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

const (
	// MethodBridgePutReminder deposits one durable response-watch
	// reminder. Only the bridge_uid class reaches it.
	MethodBridgePutReminder = "bridge_put_response_reminder"
	// MethodBridgeGetReminder reads one reminder receipt by key.
	MethodBridgeGetReminder = "bridge_get_response_reminder"
	// MethodBridgeCancelReminder retires one reminder by key and reason.
	MethodBridgeCancelReminder = "bridge_cancel_response_reminder"

	// reminderSchema is the only accepted reminder contract version.
	reminderSchema = "capyverse.response-watch.reminder/v1"
	// reminderTTL is contract-fixed: fifteen minutes from accepted_at,
	// computed by Gifford and never supplied by the caller. Changing it
	// requires a contract change and a repeat of the boundary tests.
	reminderTTL = 15 * time.Minute
)

// Reminder states. PENDING and DELIVERING are live; the rest are terminal
// except UNCERTAIN, which freezes the panel without fallback until the
// ledger reconciles. DELIVERED and DELIVERING derive from the underlying
// delivery rows; the rest persist on the reminder row.
const (
	reminderStatePending    = "PENDING"
	reminderStateDelivering = "DELIVERING"
	reminderStateDelivered  = "DELIVERED"
	reminderStateExpired    = "EXPIRED"
	reminderStateCancelled  = "CANCELLED"
	reminderStateUncertain  = "UNCERTAIN"
)

// Reminder cancel reasons. The wire accepts exactly these.
const (
	reminderCancelResolved = "EXPECTATION_RESOLVED"
	reminderCancelMode     = "MODE_SWITCH"
	reminderCancelRollback = "ROLLBACK"
)

// Reminder dispositions. DUPLICATE proves acceptance, never a new object.
const (
	reminderDispositionCreated   = "CREATED"
	reminderDispositionDuplicate = "DUPLICATE"
)

// BridgeReminderParams is the strict PUT contract. Unknown fields refuse,
// and the caller supplies no producer, panel text, keys, command, path or
// expiry: identity, producer and TTL all derive server-side.
type BridgeReminderParams struct {
	Schema          string           `json:"schema"`
	SourceNodeID    string           `json:"source_node_id"`
	ExpectationID   string           `json:"expectation_id"`
	Generation      int64            `json:"generation"`
	SourceMessageID string           `json:"source_message_id"`
	ToAgent         protocol.AgentID `json:"to_agent"`
	SourceReadAt    string           `json:"source_read_at"`
	EligibleAt      string           `json:"eligible_at"`
}

// BridgeReminderKey addresses one reminder generation.
type BridgeReminderKey struct {
	SourceNodeID  string `json:"source_node_id"`
	ExpectationID string `json:"expectation_id"`
	Generation    int64  `json:"generation"`
}

// BridgeReminderCancelParams retires one reminder generation.
type BridgeReminderCancelParams struct {
	SourceNodeID  string `json:"source_node_id"`
	ExpectationID string `json:"expectation_id"`
	Generation    int64  `json:"generation"`
	Reason        string `json:"reason"`
}

// BridgeReminderReceipt is the durable acceptance proof. Disposition
// CREATED or DUPLICATE proves acceptance, never delivery; State carries
// the current lifecycle position.
type BridgeReminderReceipt struct {
	ReminderID        protocol.ULID `json:"reminder_id"`
	GiffordMessageID  protocol.ULID `json:"gifford_message_id"`
	Disposition       string        `json:"disposition"`
	State             string        `json:"state"`
	AcceptedAt        string        `json:"accepted_at"`
	ExpiresAt         string        `json:"expires_at"`
	DeliveryAttemptID *string       `json:"delivery_attempt_id"`
}

type storedReminder struct {
	rowID             int64
	reminderID        protocol.ULID
	messageID         protocol.ULID
	sourceMessageID   int64
	fromAgent         protocol.AgentID
	toAgent           protocol.AgentID
	sourceReadAt      string
	eligibleAt        string
	acceptedAt        string
	expiresAt         string
	state             string
	deliveryAttemptID sql.NullString
}

// validReminderExpectationID enforces the contract v1.1 grammar:
// the literal prefix "rsp-" followed by exactly 32 lowercase hex digits.
// Canonical UUIDs are no longer accepted here.
func validReminderExpectationID(value string) error {
	const prefix = "rsp-"
	if len(value) != len(prefix)+32 {
		return fmt.Errorf("expectation_id: %w", errInvalidArgument)
	}
	if value[:len(prefix)] != prefix {
		return fmt.Errorf("expectation_id: %w", errInvalidArgument)
	}
	for _, c := range value[len(prefix):] {
		if c < '0' || (c > '9' && c < 'a') || c > 'f' {
			return fmt.Errorf("expectation_id: %w", errInvalidArgument)
		}
	}
	return nil
}

func validReminderReason(reason string) bool {
	switch reason {
	case reminderCancelResolved, reminderCancelMode, reminderCancelRollback:
		return true
	default:
		return false
	}
}

func (p BridgeReminderParams) validate(nodeID protocol.NodeID, producer protocol.AgentID) (
	sourceMessageID int64, sourceReadAt, eligibleAt time.Time, err error,
) {
	if p.Schema != reminderSchema {
		return 0, time.Time{}, time.Time{}, fmt.Errorf("schema: %w", errInvalidArgument)
	}
	if p.SourceNodeID == "" || p.SourceNodeID != string(nodeID) {
		return 0, time.Time{}, time.Time{}, fmt.Errorf("source_node_id: %w", errInvalidArgument)
	}
	if err := validReminderExpectationID(p.ExpectationID); err != nil {
		return 0, time.Time{}, time.Time{}, fmt.Errorf("expectation_id: %w", err)
	}
	if p.Generation < 1 {
		return 0, time.Time{}, time.Time{}, fmt.Errorf("generation: %w", errInvalidArgument)
	}
	sourceMessageID, err = canonicalBridgeMessageID(p.SourceMessageID)
	if err != nil {
		return 0, time.Time{}, time.Time{}, err
	}
	if !p.ToAgent.Valid() {
		return 0, time.Time{}, time.Time{}, fmt.Errorf("to_agent: %w", errInvalidArgument)
	}
	if !producer.Valid() || p.ToAgent == producer {
		return 0, time.Time{}, time.Time{}, fmt.Errorf("to_agent: %w", errInvalidArgument)
	}
	sourceReadAt, err = canonicalBridgeTime(p.SourceReadAt)
	if err != nil {
		return 0, time.Time{}, time.Time{}, fmt.Errorf("source_read_at: %w", err)
	}
	eligibleAt, err = canonicalBridgeTime(p.EligibleAt)
	if err != nil {
		return 0, time.Time{}, time.Time{}, fmt.Errorf("eligible_at: %w", err)
	}
	return sourceMessageID, sourceReadAt, eligibleAt, nil
}

func (k BridgeReminderKey) validate() error {
	if k.SourceNodeID == "" {
		return fmt.Errorf("source_node_id: %w", errInvalidArgument)
	}
	if err := validReminderExpectationID(k.ExpectationID); err != nil {
		return fmt.Errorf("expectation_id: %w", err)
	}
	if k.Generation < 1 {
		return fmt.Errorf("generation: %w", errInvalidArgument)
	}
	return nil
}

// PutReminder deposits one durable response-watch reminder. The receipt
// returns only after the reminder, its bridge message, its queue row and
// its source mapping commit in one transaction. An exact replay returns
// DUPLICATE with the same ids; the same key with another tuple is a
// CONFLICT that changes nothing.
func (s *Service) PutReminder(ctx context.Context, raw json.RawMessage) (any, error) {
	var params BridgeReminderParams
	if err := decodeParams(raw, &params); err != nil {
		return nil, invalidParams(err)
	}
	sourceMessageID, _, _, err := params.validate(s.nodeID, s.responseWatchProducer())
	if err != nil {
		return nil, invalidParams(err)
	}
	producer := s.responseWatchProducer()
	if !producer.Valid() {
		return nil, fmt.Errorf("response-watch producer is not configured")
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin reminder put: %w", err)
	}
	defer rollback(tx)
	stored, found, err := loadReminder(ctx, tx, params.SourceNodeID, params.ExpectationID, params.Generation)
	if err != nil {
		return nil, err
	}
	if found {
		if !sameReminderTuple(params, sourceMessageID, producer, stored) {
			return nil, domainError(protocol.ErrorCodeControlVersionConflict,
				fmt.Errorf("reminder key is already bound to another tuple"))
		}
		state, firstAttempt, err := s.reminderDerivedState(ctx, tx, stored)
		if err != nil {
			return nil, err
		}
		return BridgeReminderReceipt{
			ReminderID: stored.reminderID, GiffordMessageID: stored.messageID,
			Disposition: reminderDispositionDuplicate, State: state,
			AcceptedAt: stored.acceptedAt, ExpiresAt: stored.expiresAt,
			DeliveryAttemptID: firstAttempt,
		}, nil
	}
	if err := s.requireBridgeAgents(ctx, tx, producer, params.ToAgent); err != nil {
		return nil, err
	}
	reminderID, err := mintULID(s.clock.Now())
	if err != nil {
		return nil, fmt.Errorf("mint reminder id: %w", err)
	}
	messageID, err := mintULID(s.clock.Now())
	if err != nil {
		return nil, fmt.Errorf("mint reminder message id: %w", err)
	}
	now := s.clock.Now().UTC()
	acceptedAt := eventstore.FormatTime(now)
	expiresAt := eventstore.FormatTime(now.Add(reminderTTL))
	threadID := protocol.ThreadID(messageID)
	threadRef := protocol.ThreadRef("thr_" + string(messageID))
	threadSequence, err := nextThreadSequence(ctx, tx, threadRef)
	if err != nil {
		return nil, fmt.Errorf("allocate reminder thread sequence: %w", err)
	}
	recipients := []MessageRecipient{{
		Agent: params.ToAgent, DeliveryMode: protocol.DeliveryModeWhenSafe, RequiresAck: true,
	}}
	routeDigest, err := localRouteDigestV1(
		"", s.nodeID, producer, s.nodeID, recipients,
	)
	if err != nil {
		return nil, fmt.Errorf("digest reminder route: %w", err)
	}
	if err := s.insertReminder(ctx, tx, params, sourceMessageID, producer,
		reminderID, messageID, acceptedAt, expiresAt); err != nil {
		if isUniqueViolation(err) {
			rollback(tx)
			return s.putReminderAfterConflict(ctx, params, sourceMessageID, producer, err)
		}
		return nil, err
	}
	if err := s.insertReminderMessage(ctx, tx, params, sourceMessageID, producer,
		messageID, threadID, threadRef, threadSequence, routeDigest, acceptedAt, expiresAt); err != nil {
		return nil, err
	}
	if err := commit(tx); err != nil {
		return nil, fmt.Errorf("commit reminder put: %w", err)
	}
	s.wakeNotifications()
	return BridgeReminderReceipt{
		ReminderID: reminderID, GiffordMessageID: messageID,
		Disposition: reminderDispositionCreated, State: reminderStatePending,
		AcceptedAt: acceptedAt, ExpiresAt: expiresAt, DeliveryAttemptID: nil,
	}, nil
}

// putReminderAfterConflict resolves a lost creation race: another writer
// committed the same key first. The same tuple replays DUPLICATE; another
// tuple is a CONFLICT. Nothing is rewritten either way.
func (s *Service) putReminderAfterConflict(
	ctx context.Context,
	params BridgeReminderParams,
	sourceMessageID int64,
	producer protocol.AgentID,
	cause error,
) (any, error) {
	tx, err := s.begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin reminder conflict read: %w", err)
	}
	defer rollback(tx)
	stored, found, err := loadReminder(ctx, tx, params.SourceNodeID, params.ExpectationID, params.Generation)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, cause
	}
	if !sameReminderTuple(params, sourceMessageID, producer, stored) {
		return nil, domainError(protocol.ErrorCodeControlVersionConflict,
			fmt.Errorf("reminder key is already bound to another tuple"))
	}
	state, firstAttempt, err := s.reminderDerivedState(ctx, tx, stored)
	if err != nil {
		return nil, err
	}
	if err := commit(tx); err != nil {
		return nil, fmt.Errorf("commit reminder conflict read: %w", err)
	}
	return BridgeReminderReceipt{
		ReminderID: stored.reminderID, GiffordMessageID: stored.messageID,
		Disposition: reminderDispositionDuplicate, State: state,
		AcceptedAt: stored.acceptedAt, ExpiresAt: stored.expiresAt,
		DeliveryAttemptID: firstAttempt,
	}, nil
}

func isUniqueViolation(err error) bool {
	return err != nil && (strings.Contains(err.Error(), "UNIQUE constraint failed") ||
		strings.Contains(err.Error(), "unique constraint"))
}

func sameReminderTuple(params BridgeReminderParams, sourceMessageID int64, producer protocol.AgentID, stored storedReminder) bool {
	return stored.sourceMessageID == sourceMessageID &&
		stored.fromAgent == producer &&
		stored.toAgent == params.ToAgent &&
		stored.sourceReadAt == params.SourceReadAt &&
		stored.eligibleAt == params.EligibleAt
}

func loadReminder(
	ctx context.Context,
	tx *sql.Tx,
	sourceNode, expectation string,
	generation int64,
) (storedReminder, bool, error) {
	var stored storedReminder
	var attempt sql.NullString
	err := tx.QueryRowContext(ctx, `SELECT rowid, reminder_id, gifford_message_id,
		source_message_id, from_agent, to_agent, source_read_at, eligible_at,
		accepted_at, expires_at, state, delivery_attempt_id
		FROM bridge_reminders
		WHERE source_node_id = ? AND expectation_id = ? AND generation = ?`,
		sourceNode, expectation, generation).Scan(
		&stored.rowID, &stored.reminderID, &stored.messageID,
		&stored.sourceMessageID, &stored.fromAgent, &stored.toAgent,
		&stored.sourceReadAt, &stored.eligibleAt, &stored.acceptedAt,
		&stored.expiresAt, &stored.state, &attempt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return storedReminder{}, false, nil
	}
	if err != nil {
		return storedReminder{}, false, fmt.Errorf("select reminder: %w", err)
	}
	stored.deliveryAttemptID = attempt
	return stored, true, nil
}

func (s *Service) insertReminder(
	ctx context.Context,
	tx *sql.Tx,
	params BridgeReminderParams,
	sourceMessageID int64,
	producer protocol.AgentID,
	reminderID, messageID protocol.ULID,
	acceptedAt, expiresAt string,
) error {
	now := eventstore.FormatTime(s.clock.Now())
	_, err := tx.ExecContext(ctx, `INSERT INTO bridge_reminders
		(source_node_id, expectation_id, generation, source_message_id, from_agent,
		 to_agent, source_read_at, eligible_at, accepted_at, expires_at, state,
		 reminder_id, gifford_message_id, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', ?, ?, ?, ?)`,
		params.SourceNodeID, params.ExpectationID, params.Generation, sourceMessageID,
		producer, params.ToAgent, params.SourceReadAt, params.EligibleAt,
		acceptedAt, expiresAt, reminderID, messageID, now, now)
	if err != nil {
		return fmt.Errorf("insert reminder: %w", err)
	}
	return nil
}

func (s *Service) insertReminderMessage(
	ctx context.Context,
	tx *sql.Tx,
	params BridgeReminderParams,
	sourceMessageID int64,
	producer protocol.AgentID,
	messageID protocol.ULID,
	threadID protocol.ThreadID,
	threadRef protocol.ThreadRef,
	threadSequence int64,
	routeDigest protocol.Digest,
	acceptedAt, expiresAt string,
) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO messages
		(message_id, client_msg_id, client_msg_namespace, origin_node, from_agent,
		 destination_node, thread_ref, thread_id, priority, state, created_at, expires_at,
		 updated_at, thread_sequence, hop_count, route_digest)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', ?, ?, ?, ?, 0, ?)`,
		messageID, params.SourceMessageID, params.SourceNodeID, s.nodeID, producer,
		s.nodeID, threadRef, threadID, protocol.PriorityNormal,
		acceptedAt, expiresAt, acceptedAt, threadSequence, routeDigest)
	if err != nil {
		return fmt.Errorf("insert reminder message: %w", err)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO message_recipients
		(message_id, agent_id, ordinal, delivery_mode, requires_ack, created_at)
		VALUES (?, ?, 0, 'WHEN_SAFE', 1, ?)`, messageID, params.ToAgent, acceptedAt)
	if err != nil {
		return fmt.Errorf("insert reminder recipient: %w", err)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO delivery_queue
		(message_id, agent_id, delivery_generation, from_agent, thread_ref, state, priority,
		 created_at, expires_at, updated_at, thread_sequence, hop_count, route_digest)
		VALUES (?, ?, 1, ?, ?, 'QUEUED', 2, ?, ?, ?, ?, 0, ?)`,
		messageID, params.ToAgent, producer, threadRef, acceptedAt, expiresAt, acceptedAt,
		threadSequence, routeDigest)
	if err != nil {
		return fmt.Errorf("enqueue reminder notification: %w", err)
	}
	if err := s.engine.InitializeDelivery(ctx, tx, messageID, params.ToAgent, 1); err != nil {
		return fmt.Errorf("initialize reminder notification: %w", err)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO bridge_message_mappings
		(source_node_id, source_message_id, gifford_message_id, from_agent, to_agent,
		 source_created_at, source_context_id, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, NULL, ?, ?)`,
		params.SourceNodeID, sourceMessageID, messageID, producer, params.ToAgent,
		params.EligibleAt, acceptedAt, acceptedAt)
	if err != nil {
		return fmt.Errorf("insert reminder mapping: %w", err)
	}
	return nil
}

// reminderDerivedState reports the live state of a stored reminder. Stored
// terminal states win; PENDING derives from the underlying queue row, and a
// QUEUED row past its expiry derives EXPIRED without writing.
func (s *Service) reminderDerivedState(
	ctx context.Context,
	tx *sql.Tx,
	stored storedReminder,
) (string, *string, error) {
	switch stored.state {
	case reminderStateDelivered, reminderStateExpired, reminderStateCancelled, reminderStateUncertain:
		return stored.state, nullStringToPtr(stored.deliveryAttemptID), nil
	}
	var queueState string
	err := tx.QueryRowContext(ctx, `SELECT state FROM delivery_queue
		WHERE message_id = ?`, stored.messageID).Scan(&queueState)
	if errors.Is(err, sql.ErrNoRows) {
		return stored.state, nullStringToPtr(stored.deliveryAttemptID), nil
	}
	if err != nil {
		return "", nil, fmt.Errorf("select reminder queue state: %w", err)
	}
	firstAttempt, err := firstDeliveryAttempt(ctx, tx, stored.messageID)
	if err != nil {
		return "", nil, err
	}
	switch queueState {
	case "DELIVERED":
		return reminderStateDelivered, firstAttempt, nil
	case "QUEUED", "RETRY_WAIT":
		if reminderExpired(s.clock.Now(), stored.expiresAt) {
			return reminderStateExpired, firstAttempt, nil
		}
		return reminderStatePending, firstAttempt, nil
	case "CLAIMED", "DELIVERING":
		if reminderExpired(s.clock.Now(), stored.expiresAt) &&
			!reminderEffectPossible(ctx, tx, stored.messageID) {
			return reminderStateExpired, firstAttempt, nil
		}
		return reminderStateDelivering, firstAttempt, nil
	case "EXPIRED":
		return reminderStateExpired, firstAttempt, nil
	default:
		return reminderStateUncertain, firstAttempt, nil
	}
}

func nullStringToPtr(value sql.NullString) *string {
	if !value.Valid {
		return nil
	}
	out := value.String
	return &out
}

func firstDeliveryAttempt(ctx context.Context, tx *sql.Tx, messageID protocol.ULID) (*string, error) {
	var deliveryID string
	err := tx.QueryRowContext(ctx, `SELECT delivery_id FROM delivery_attempts
		WHERE message_id = ? ORDER BY rowid LIMIT 1`, messageID).Scan(&deliveryID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("select reminder attempt: %w", err)
	}
	return &deliveryID, nil
}

func reminderExpired(now time.Time, expiresAt string) bool {
	expiry, err := eventstore.ParseTime(expiresAt)
	if err != nil {
		return false
	}
	return !now.UTC().Before(expiry)
}

// responseWatchProducer is the pinned service producer stamped on every
// reminder. It derives from the bridge uid and the node configuration,
// never from the request.
func (s *Service) responseWatchProducer() protocol.AgentID {
	return s.responseWatchProducerID
}

// GetReminder reads one reminder receipt by key. It never creates, reopens
// or rewrites: the receipt carries the stored ids with the current state.
func (s *Service) GetReminder(ctx context.Context, raw json.RawMessage) (any, error) {
	var key BridgeReminderKey
	if err := decodeParams(raw, &key); err != nil {
		return nil, invalidParams(err)
	}
	if key.SourceNodeID == "" || key.SourceNodeID != string(s.nodeID) {
		return nil, invalidParams(fmt.Errorf("source_node_id: %w", errInvalidArgument))
	}
	if err := key.validate(); err != nil {
		return nil, invalidParams(err)
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin reminder get: %w", err)
	}
	defer rollback(tx)
	stored, found, err := loadReminder(ctx, tx, key.SourceNodeID, key.ExpectationID, key.Generation)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, domainError(protocol.ErrorCodeNotFound,
			fmt.Errorf("reminder is not committed"))
	}
	state, firstAttempt, err := s.reminderDerivedState(ctx, tx, stored)
	if err != nil {
		return nil, err
	}
	if err := commit(tx); err != nil {
		return nil, fmt.Errorf("commit reminder get: %w", err)
	}
	return BridgeReminderReceipt{
		ReminderID: stored.reminderID, GiffordMessageID: stored.messageID,
		Disposition: reminderDispositionCreated, State: state,
		AcceptedAt: stored.acceptedAt, ExpiresAt: stored.expiresAt,
		DeliveryAttemptID: firstAttempt,
	}, nil
}

// CancelReminder retires one reminder generation. Before any effect it
// ends CANCELLED and no worker can prepare it after; with a possible
// effect it freezes UNCERTAIN; a delivered reminder reports DELIVERED and
// history is never rewritten.
func (s *Service) CancelReminder(ctx context.Context, raw json.RawMessage) (any, error) {
	var params BridgeReminderCancelParams
	if err := decodeParams(raw, &params); err != nil {
		return nil, invalidParams(err)
	}
	if params.SourceNodeID == "" || params.SourceNodeID != string(s.nodeID) {
		return nil, invalidParams(fmt.Errorf("source_node_id: %w", errInvalidArgument))
	}
	key := BridgeReminderKey{
		SourceNodeID: params.SourceNodeID, ExpectationID: params.ExpectationID,
		Generation: params.Generation,
	}
	if err := key.validate(); err != nil {
		return nil, invalidParams(err)
	}
	if !validReminderReason(params.Reason) {
		return nil, invalidParams(fmt.Errorf("reason: %w", errInvalidArgument))
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin reminder cancel: %w", err)
	}
	defer rollback(tx)
	stored, found, err := loadReminder(ctx, tx, params.SourceNodeID, params.ExpectationID, params.Generation)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, domainError(protocol.ErrorCodeNotFound,
			fmt.Errorf("reminder is not committed"))
	}
	state, firstAttempt, err := s.reminderDerivedState(ctx, tx, stored)
	if err != nil {
		return nil, err
	}
	now := eventstore.FormatTime(s.clock.Now())
	switch state {
	case reminderStateDelivered, reminderStateExpired, reminderStateCancelled, reminderStateUncertain:
		if stored.state == reminderStatePending &&
			(state == reminderStateDelivered || state == reminderStateExpired) {
			if _, err := tx.ExecContext(ctx, `UPDATE bridge_reminders
				SET state = ?, delivered_at = CASE WHEN ? = 'DELIVERED' THEN ? ELSE delivered_at END,
				terminal_at = ?, last_reconciled_at = ?, updated_at = ?
				WHERE source_node_id = ? AND expectation_id = ? AND generation = ?
				AND state = 'PENDING'`,
				state, state, now, now, now, now,
				params.SourceNodeID, params.ExpectationID, params.Generation); err != nil {
				return nil, fmt.Errorf("materialize reminder terminal: %w", err)
			}
		}
		if err := commit(tx); err != nil {
			return nil, fmt.Errorf("commit reminder cancel: %w", err)
		}
		return BridgeReminderReceipt{
			ReminderID: stored.reminderID, GiffordMessageID: stored.messageID,
			Disposition: reminderDispositionCreated, State: state,
			AcceptedAt: stored.acceptedAt, ExpiresAt: stored.expiresAt,
			DeliveryAttemptID: firstAttempt,
		}, nil
	}
	if reminderEffectPossible(ctx, tx, stored.messageID) {
		if err := s.markReminderUncertain(ctx, tx, stored); err != nil {
			return nil, err
		}
		if err := commit(tx); err != nil {
			return nil, fmt.Errorf("commit reminder cancel: %w", err)
		}
		return BridgeReminderReceipt{
			ReminderID: stored.reminderID, GiffordMessageID: stored.messageID,
			Disposition: reminderDispositionCreated, State: reminderStateUncertain,
			AcceptedAt: stored.acceptedAt, ExpiresAt: stored.expiresAt,
			DeliveryAttemptID: firstAttempt,
		}, nil
	}
	if err := s.markReminderCancelled(ctx, tx, stored, params.Reason); err != nil {
		return nil, err
	}
	if err := commit(tx); err != nil {
		return nil, fmt.Errorf("commit reminder cancel: %w", err)
	}
	return BridgeReminderReceipt{
		ReminderID: stored.reminderID, GiffordMessageID: stored.messageID,
		Disposition: reminderDispositionCreated, State: reminderStateCancelled,
		AcceptedAt: stored.acceptedAt, ExpiresAt: stored.expiresAt,
		DeliveryAttemptID: firstAttempt,
	}, nil
}

// reminderEffectPossible reports whether an attempt may already have
// produced a panel effect: any attempt at or past the commit frame.
func reminderEffectPossible(ctx context.Context, tx *sql.Tx, messageID protocol.ULID) bool {
	var count int
	_ = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM delivery_attempts
		WHERE message_id = ? AND state IN ('COMMIT_SENT', 'COMMITTED', 'COMMIT_NOT_CONSUMED')`,
		messageID).Scan(&count)
	return count > 0
}

func (s *Service) markReminderUncertain(ctx context.Context, tx *sql.Tx, stored storedReminder) error {
	now := eventstore.FormatTime(s.clock.Now())
	if _, err := tx.ExecContext(ctx, `UPDATE bridge_reminders
		SET state = 'UNCERTAIN', terminal_at = ?, last_reconciled_at = ?, updated_at = ?
		WHERE source_node_id = (SELECT source_node_id FROM bridge_reminders WHERE rowid = ?)
		AND expectation_id = (SELECT expectation_id FROM bridge_reminders WHERE rowid = ?)
		AND generation = (SELECT generation FROM bridge_reminders WHERE rowid = ?)
		AND state = 'PENDING'`,
		now, now, now, stored.rowID, stored.rowID, stored.rowID); err != nil {
		return fmt.Errorf("mark reminder uncertain: %w", err)
	}
	return nil
}

// cancelReminderAttempts retires in-flight attempts that cannot have
// produced a panel effect yet. PREPARING and PREPARED both admit the
// CANCELLED transition; anything at or past the commit frame is left
// alone and reported through reminderEffectPossible instead.
func (s *Service) cancelReminderAttempts(ctx context.Context, tx *sql.Tx, stored storedReminder, reason string) error {
	now := eventstore.FormatTime(s.clock.Now())
	if _, err := tx.ExecContext(ctx, `UPDATE delivery_attempts
		SET state = 'CANCELLED', terminal_reason = ?, finished_at = ?, updated_at = ?
		WHERE message_id = ? AND state IN ('PREPARING', 'PREPARED')`,
		reason, now, now, stored.messageID); err != nil {
		return fmt.Errorf("cancel reminder attempts: %w", err)
	}
	return nil
}

func (s *Service) markReminderCancelled(ctx context.Context, tx *sql.Tx, stored storedReminder, reason string) error {
	now := eventstore.FormatTime(s.clock.Now())
	if err := s.cancelReminderAttempts(ctx, tx, stored, "reminder_cancelled"); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE delivery_queue SET state = 'CANCELLED', updated_at = ?
		WHERE message_id = ? AND state IN ('QUEUED', 'RETRY_WAIT', 'CLAIMED', 'DELIVERING')`, now, stored.messageID); err != nil {
		return fmt.Errorf("cancel reminder queue row: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE bridge_reminders
		SET state = 'CANCELLED', cancel_reason = ?, terminal_at = ?,
		last_reconciled_at = ?, updated_at = ?
		WHERE source_node_id = (SELECT source_node_id FROM bridge_reminders WHERE rowid = ?)
		AND expectation_id = (SELECT expectation_id FROM bridge_reminders WHERE rowid = ?)
		AND generation = (SELECT generation FROM bridge_reminders WHERE rowid = ?)
		AND state = 'PENDING'`,
		reason, now, now, now, stored.rowID, stored.rowID, stored.rowID); err != nil {
		return fmt.Errorf("mark reminder cancelled: %w", err)
	}
	return nil
}

// ReconcileReminders advances durable reminder states that time or the
// queue already decided: queue-DELIVERED becomes DELIVERED, and a PENDING
// reminder past its TTL with no possible effect becomes EXPIRED with its
// queue row retired. It never creates, reopens or duplicates.
func (s *Service) ReconcileReminders(ctx context.Context) (int, error) {
	tx, err := s.begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin reminder reconcile: %w", err)
	}
	defer rollback(tx)
	type reminderKey struct {
		node, expectation string
		generation        int64
	}
	keys, err := func() ([]reminderKey, error) {
		rows, err := tx.QueryContext(ctx, `SELECT source_node_id, expectation_id, generation
			FROM bridge_reminders WHERE state = 'PENDING'`)
		if err != nil {
			return nil, fmt.Errorf("select pending reminders: %w", err)
		}
		defer func() { _ = rows.Close() }()
		var keys []reminderKey
		for rows.Next() {
			var key reminderKey
			if err := rows.Scan(&key.node, &key.expectation, &key.generation); err != nil {
				return nil, fmt.Errorf("scan pending reminders: %w", err)
			}
			keys = append(keys, key)
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("read pending reminders: %w", err)
		}
		return keys, nil
	}()
	if err != nil {
		return 0, err
	}
	now := eventstore.FormatTime(s.clock.Now())
	reconciled := 0
	for _, key := range keys {
		stored, found, err := loadReminder(ctx, tx, key.node, key.expectation, key.generation)
		if err != nil || !found || stored.state != reminderStatePending {
			if err != nil {
				return reconciled, err
			}
			continue
		}
		state, _, err := s.reminderDerivedState(ctx, tx, stored)
		if err != nil {
			return reconciled, err
		}
		switch state {
		case reminderStateDelivered:
			if _, err := tx.ExecContext(ctx, `UPDATE bridge_reminders
				SET state = 'DELIVERED', delivered_at = ?, terminal_at = ?,
				last_reconciled_at = ?, updated_at = ?
				WHERE source_node_id = ? AND expectation_id = ? AND generation = ?
				AND state = 'PENDING'`,
				now, now, now, now, key.node, key.expectation, key.generation); err != nil {
				return reconciled, fmt.Errorf("mark reminder delivered: %w", err)
			}
			reconciled++
		case reminderStateExpired:
			if reminderEffectPossible(ctx, tx, stored.messageID) {
				continue
			}
			if err := s.cancelReminderAttempts(ctx, tx, stored, "reminder_expired"); err != nil {
				return reconciled, err
			}
			if _, err := tx.ExecContext(ctx, `UPDATE delivery_queue SET state = 'EXPIRED', updated_at = ?
				WHERE message_id = ? AND state IN ('QUEUED', 'RETRY_WAIT', 'CLAIMED', 'DELIVERING')`,
				now, stored.messageID); err != nil {
				return reconciled, fmt.Errorf("expire reminder queue row: %w", err)
			}
			if _, err := tx.ExecContext(ctx, `UPDATE bridge_reminders
				SET state = 'EXPIRED', terminal_at = ?, last_reconciled_at = ?, updated_at = ?
				WHERE source_node_id = ? AND expectation_id = ? AND generation = ?
				AND state = 'PENDING'`,
				now, now, now, key.node, key.expectation, key.generation); err != nil {
				return reconciled, fmt.Errorf("mark reminder expired: %w", err)
			}
			reconciled++
		}
	}
	if err := commit(tx); err != nil {
		return reconciled, fmt.Errorf("commit reminder reconcile: %w", err)
	}
	return reconciled, nil
}
