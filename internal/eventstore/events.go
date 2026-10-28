package eventstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"

	"github.com/smellak/gifford/internal/protocol"
)

// This file holds the event writer and reader: validated batches go in
// one immediate transaction, reads come back in sequence order.

// MaxBatchEvents is the largest batch AppendEvents accepts. It bounds how
// long one process holds the write lock that the others wait for.
const MaxBatchEvents = 1000

// insertEventsSQL names the 26 events columns in table order with one
// placeholder each; one statement serves every envelope of a batch.
const insertEventsSQL = `INSERT INTO events
(event_id, schema_version, node_id, session_id, agent_id, adapter_type,
 source_quality, event_type, direction, stream, origin, wall_time,
 monotonic_ns, sequence, turn_id, message_id, correlation_id, payload_json,
 sensitivity, redaction_applied, harness, harness_version, tool_call_id,
 delivery_id, decision_id, parent_event_id)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

// selectEventsSQL reads the same columns back in table order.
const selectEventsSQL = `SELECT event_id, schema_version, node_id, session_id, agent_id,
adapter_type, source_quality, event_type, direction, stream, origin,
wall_time, monotonic_ns, sequence, turn_id, message_id, correlation_id,
payload_json, sensitivity, redaction_applied, harness, harness_version,
tool_call_id, delivery_id, decision_id, parent_event_id
FROM events WHERE session_id = ? AND sequence >= ? ORDER BY sequence LIMIT ?`

// AppendEvents validates every envelope, then appends all of them in one
// BEGIN IMMEDIATE transaction: all are stored or none is.
func (s *Store) AppendEvents(ctx context.Context, events []protocol.EventEnvelope) error {
	if len(events) == 0 {
		return nil
	}
	if len(events) > MaxBatchEvents {
		return fmt.Errorf("batch of %d events exceeds %d: %w", len(events), MaxBatchEvents, ErrInvalidArgument)
	}
	for i := range events {
		if err := events[i].Validate(); err != nil {
			return fmt.Errorf("event %d: %w", i, err)
		}
	}
	return s.withWriter(ctx, func() error { return s.appendBatch(ctx, events) })
}

// appendBatch writes validated envelopes in one transaction. The caller
// holds the writer slot, so goroutines of this process queue there.
func (s *Store) appendBatch(ctx context.Context, events []protocol.EventEnvelope) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin write transaction: %w", err)
	}
	err = func() (writeErr error) {
		stmt, err := tx.PrepareContext(ctx, insertEventsSQL)
		if err != nil {
			return fmt.Errorf("prepare event insert: %w", err)
		}
		defer func() {
			if closeErr := stmt.Close(); closeErr != nil {
				writeErr = errors.Join(writeErr, fmt.Errorf("close event insert: %w", closeErr))
			}
		}()
		for i := range events {
			if _, err := stmt.ExecContext(ctx, bindEvent(&events[i])...); err != nil {
				if dup := duplicateError(i, &events[i], err); dup != nil {
					return dup
				}
				return fmt.Errorf("event %d: %w", i, err)
			}
		}
		return nil
	}()
	if err != nil {
		return rollback(tx, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit event batch: %w", err)
	}
	return nil
}

// rollback aborts tx, joining a failed abort onto err.
func rollback(tx *sql.Tx, err error) error {
	if rbErr := tx.Rollback(); rbErr != nil {
		return errors.Join(err, rbErr)
	}
	return err
}

// duplicateError maps a primary-key or session-sequence conflict onto the
// refused envelope; any other error returns nil.
func duplicateError(index int, event *protocol.EventEnvelope, err error) *DuplicateEventError {
	var sqlErr *sqlite.Error
	if !errors.As(err, &sqlErr) {
		return nil
	}
	switch sqlErr.Code() {
	case sqlite3.SQLITE_CONSTRAINT_PRIMARYKEY, sqlite3.SQLITE_CONSTRAINT_UNIQUE:
		return &DuplicateEventError{
			Index:     index,
			EventID:   event.EventID,
			SessionID: event.SessionID,
			Sequence:  event.Sequence,
		}
	default:
		return nil
	}
}

// bindEvent binds one envelope to the 26 insert placeholders in table
// order. An empty string stays empty for the NOT NULL session columns
// and becomes NULL for the nullable ones.
func bindEvent(event *protocol.EventEnvelope) []any {
	return []any{
		string(event.EventID),
		event.SchemaVersion,
		string(event.NodeID),
		string(event.SessionID),
		string(event.AgentID),
		string(event.AdapterType),
		string(event.SourceQuality),
		string(event.EventType),
		nullString(string(event.Direction)),
		nullString(string(event.Stream)),
		nullString(string(event.Origin)),
		FormatTime(event.WallTime),
		event.MonotonicNS,
		event.Sequence,
		nullString(string(event.TurnID)),
		nullString(string(event.MessageID)),
		nullString(string(event.CorrelationID)),
		string(event.Payload),
		string(event.Sensitivity),
		boolInt(event.RedactionApplied),
		nullString(string(event.Harness)),
		nullString(event.HarnessVersion),
		nullString(string(event.ToolCallID)),
		nullString(string(event.DeliveryID)),
		nullString(string(event.DecisionID)),
		nullString(string(event.ParentEventID)),
	}
}

// nullString binds an empty string as NULL.
func nullString(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// boolInt stores a boolean as 1 or 0.
func boolInt(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

// SessionEvents returns at most limit events of one session with sequence
// at least fromSequence, in sequence order. The empty session id selects
// the Gifford-scoped events.
func (s *Store) SessionEvents(ctx context.Context, sessionID protocol.OpaqueID, fromSequence int64, limit int) ([]protocol.EventEnvelope, error) {
	if limit < 1 || limit > MaxBatchEvents {
		return nil, ErrInvalidArgument
	}
	rows, err := s.db.QueryContext(ctx, selectEventsSQL, string(sessionID), fromSequence, limit)
	if err != nil {
		return nil, fmt.Errorf("select session events: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var events []protocol.EventEnvelope
	for rows.Next() {
		event, err := scanEvent(rows)
		if err != nil {
			return nil, err
		}
		if err := event.Validate(); err != nil {
			return nil, fmt.Errorf("stored event %s: %w", event.EventID, err)
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read session events: %w", err)
	}
	return events, nil
}

// scanEvent reads one result row, failing closed on any stored value the
// writer could never have produced. NULL and empty both read back as zero.
func scanEvent(rows *sql.Rows) (protocol.EventEnvelope, error) {
	var event protocol.EventEnvelope
	var direction, stream, origin sql.NullString
	var wallTime, payload string
	var turnID, messageID, correlationID sql.NullString
	var redaction int64
	var harness, harnessVersion, toolCallID, deliveryID, decisionID, parentEventID sql.NullString
	if err := rows.Scan(
		&event.EventID, &event.SchemaVersion, &event.NodeID, &event.SessionID,
		&event.AgentID, &event.AdapterType, &event.SourceQuality, &event.EventType,
		&direction, &stream, &origin,
		&wallTime,
		&event.MonotonicNS, &event.Sequence,
		&turnID, &messageID, &correlationID,
		&payload,
		&event.Sensitivity,
		&redaction,
		&harness, &harnessVersion, &toolCallID, &deliveryID, &decisionID, &parentEventID,
	); err != nil {
		return protocol.EventEnvelope{}, fmt.Errorf("scan stored event: %w", err)
	}
	parsed, err := ParseTime(wallTime)
	if err != nil {
		return protocol.EventEnvelope{}, fmt.Errorf("stored event %s: %w", event.EventID, err)
	}
	if redaction != 0 && redaction != 1 {
		return protocol.EventEnvelope{}, fmt.Errorf("stored event %s: redaction_applied %d", event.EventID, redaction)
	}
	event.Direction = protocol.Direction(direction.String)
	event.Stream = protocol.Stream(stream.String)
	event.Origin = protocol.Origin(origin.String)
	event.WallTime = parsed
	event.TurnID = protocol.OpaqueID(turnID.String)
	event.MessageID = protocol.ULID(messageID.String)
	event.CorrelationID = protocol.OpaqueID(correlationID.String)
	event.Payload = protocol.Payload(payload)
	event.RedactionApplied = redaction == 1
	event.Harness = protocol.Harness(harness.String)
	event.HarnessVersion = harnessVersion.String
	event.ToolCallID = protocol.OpaqueID(toolCallID.String)
	event.DeliveryID = protocol.OpaqueID(deliveryID.String)
	event.DecisionID = protocol.OpaqueID(decisionID.String)
	event.ParentEventID = protocol.ULID(parentEventID.String)
	return event, nil
}
