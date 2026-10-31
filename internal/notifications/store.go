package notifications

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

const (
	baselineNotificationID = "notification-baseline"
	baselineObjectID       = "INITIAL"
)

func (s *Service) ensureBaseline(ctx context.Context) (bool, error) {
	tx, err := s.store.DB().BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin notification baseline: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var marker int
	err = tx.QueryRowContext(ctx, `SELECT 1 FROM human_notifications
		WHERE class = 'SERVICE' AND object_id = ?`, baselineObjectID).Scan(&marker)
	switch {
	case err == nil:
		if err := tx.Commit(); err != nil {
			return false, fmt.Errorf("commit notification baseline check: %w", err)
		}
		return false, nil
	case !errors.Is(err, sql.ErrNoRows):
		return false, fmt.Errorf("read notification baseline marker: %w", err)
	}
	messages, err := messageCandidates(ctx, tx, s.dbNode, -1, false)
	if err != nil {
		return false, err
	}
	approvals, err := approvalCandidates(ctx, tx, -1)
	if err != nil {
		return false, err
	}
	if s.duringBaseline != nil {
		s.duringBaseline()
	}
	now := s.clock.Now().UTC()
	for _, source := range append(messages, approvals...) {
		id, err := s.ids.notificationID(now)
		if err != nil {
			return false, err
		}
		notification := notificationFromCandidate(
			id, source, ReasonBaseline, StateBaselined, AdapterNone, now,
		)
		if err := insertNotification(ctx, tx, notification); err != nil {
			return false, err
		}
	}
	markerRow := Notification{
		ID: baselineNotificationID, Class: classService, ObjectID: baselineObjectID,
		Reason: ReasonBaseline, State: StateBaselined, Adapter: AdapterNone,
		CreatedAt: now, UpdatedAt: now, LocalRecipients: []protocol.AgentID{},
	}
	if err := insertNotification(ctx, tx, markerRow); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit notification baseline: %w", err)
	}
	return true, nil
}

func (s *Service) discoverBatch(ctx context.Context) (int, error) {
	tx, err := s.store.DB().BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin notification discovery: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	messageLimit := (s.batchSize + 1) / 2
	messages, err := messageCandidates(ctx, tx, s.dbNode, messageLimit, true)
	if err != nil {
		return 0, err
	}
	approvalLimit := s.batchSize - len(messages)
	approvals, err := approvalCandidates(ctx, tx, approvalLimit)
	if err != nil {
		return 0, err
	}
	candidates := make([]candidate, 0, len(messages)+len(approvals))
	candidates = append(candidates, messages...)
	candidates = append(candidates, approvals...)
	if len(candidates) == 0 {
		return 0, nil
	}
	now := s.clock.Now().UTC()
	sequence, err := nextGiffordSequence(ctx, tx)
	if err != nil {
		return 0, err
	}
	events := make([]protocol.EventEnvelope, 0, len(candidates))
	for _, source := range candidates {
		id, err := s.ids.notificationID(now)
		if err != nil {
			return 0, err
		}
		notification := notificationFromCandidate(
			id, source, source.reason, StatePending, s.adapter.Name(), now,
		)
		if err := insertNotification(ctx, tx, notification); err != nil {
			return 0, err
		}
		event, err := s.notificationEvent(
			now, sequence, protocol.EventTypeHumanNotificationCreated, notification,
		)
		if err != nil {
			return 0, err
		}
		events = append(events, event)
		sequence++
	}
	if err := s.store.AppendEventsTx(ctx, tx, events); err != nil {
		return 0, fmt.Errorf("append created notification events: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit notification discovery: %w", err)
	}
	return len(candidates), nil
}

func notificationFromCandidate(
	id protocol.OpaqueID,
	source candidate,
	reason Reason,
	state State,
	adapter AdapterKind,
	now time.Time,
) Notification {
	return Notification{
		ID: id, Class: source.class, ObjectID: source.objectID, Sender: source.sender,
		LocalRecipients: append([]protocol.AgentID(nil), source.recipients...),
		Priority:        source.priority, Reason: reason, State: state, Adapter: adapter,
		CreatedAt: now, UpdatedAt: now,
	}
}

func insertNotification(ctx context.Context, tx *sql.Tx, notification Notification) error {
	if err := notification.validate(); err != nil {
		return err
	}
	recipients := notification.LocalRecipients
	if recipients == nil {
		recipients = []protocol.AgentID{}
	}
	recipientsJSON, err := json.Marshal(recipients)
	if err != nil {
		return fmt.Errorf("marshal notification recipients: %w", err)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO human_notifications
		(notification_id, class, object_id, sender_agent, recipient_agents_json,
		 priority, reason, state, adapter, attempts, last_error_reason,
		 created_at, updated_at, last_attempt_at, next_attempt_at, rendered_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		notification.ID, notification.Class, notification.ObjectID,
		nullText(string(notification.Sender)), string(recipientsJSON),
		nullText(string(notification.Priority)), notification.Reason,
		notification.State, notification.Adapter, notification.Attempts,
		nullText(string(notification.LastError)),
		eventstore.FormatTime(notification.CreatedAt), eventstore.FormatTime(notification.UpdatedAt),
		nullTime(notification.LastAttemptAt), nullTime(notification.NextAttemptAt),
		nullTime(notification.RenderedAt))
	if err != nil {
		return fmt.Errorf("insert human notification: %w", err)
	}
	return nil
}

func nullText(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func nullTime(value *time.Time) any {
	if value == nil {
		return nil
	}
	return eventstore.FormatTime(*value)
}

func nextGiffordSequence(ctx context.Context, tx *sql.Tx) (int64, error) {
	var sequence int64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(sequence), -1) + 1
		FROM events WHERE session_id = ''`).Scan(&sequence); err != nil {
		return 0, fmt.Errorf("read notification event sequence: %w", err)
	}
	if sequence < 0 {
		return 0, ErrInvalidRecord
	}
	return sequence, nil
}

func (s *Service) notificationEvent(
	now time.Time,
	sequence int64,
	eventType protocol.EventType,
	notification Notification,
) (protocol.EventEnvelope, error) {
	payloadState := notification.State
	payload, err := marshalEventPayload(eventType, EventPayload{
		NotificationID: notification.ID, Class: notification.Class,
		ObjectID: notification.ObjectID, Reason: notification.Reason,
		State: payloadState, Adapter: notification.Adapter,
	})
	if err != nil {
		return protocol.EventEnvelope{}, err
	}
	eventID, err := s.ids.ulid(now)
	if err != nil {
		return protocol.EventEnvelope{}, err
	}
	event := protocol.EventEnvelope{
		EventID: eventID, SchemaVersion: protocol.SchemaVersion, NodeID: s.dbNode,
		SourceQuality: protocol.SourceQualityInternal, EventType: eventType,
		Origin: protocol.OriginSystem, WallTime: now, Sequence: sequence,
		CorrelationID: notification.ID, Payload: payload,
		Sensitivity: protocol.SensitivityNormal,
	}
	if notification.Class == ClassMessage {
		event.MessageID = protocol.ULID(notification.ObjectID)
	}
	if err := event.Validate(); err != nil {
		return protocol.EventEnvelope{}, fmt.Errorf("validate notification event: %w", err)
	}
	return event, nil
}

func (s *Service) renderPendingBatch(ctx context.Context) (int, error) {
	pending, err := s.pendingNotifications(ctx)
	if err != nil {
		return 0, err
	}
	for _, notification := range pending {
		if err := s.renderOne(ctx, notification); err != nil {
			return 0, err
		}
	}
	return len(pending), nil
}

func (s *Service) pendingNotifications(ctx context.Context) ([]Notification, error) {
	rows, err := s.store.DB().QueryContext(ctx, `SELECT notification_id, class, object_id,
		COALESCE(sender_agent, ''), recipient_agents_json, COALESCE(priority, ''),
		reason, state, adapter, attempts, COALESCE(last_error_reason, ''),
		created_at, updated_at, last_attempt_at, next_attempt_at, rendered_at
		FROM human_notifications
		WHERE state = 'PENDING' AND attempts < ?
		  AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
		ORDER BY created_at, notification_id LIMIT ?`,
		s.maxAttempts, eventstore.FormatTime(s.clock.Now().UTC()), s.batchSize)
	if err != nil {
		return nil, fmt.Errorf("select pending human notifications: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var pending []Notification
	for rows.Next() {
		notification, err := scanNotification(rows)
		if err != nil {
			return nil, err
		}
		if notification.State != StatePending {
			return nil, ErrInvalidRecord
		}
		pending = append(pending, notification)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read pending human notifications: %w", err)
	}
	return pending, nil
}

func scanNotification(rows *sql.Rows) (Notification, error) {
	var notification Notification
	var recipientsJSON, createdAt, updatedAt string
	var lastAttemptAt, nextAttemptAt, renderedAt sql.NullString
	if err := rows.Scan(
		&notification.ID, &notification.Class, &notification.ObjectID,
		&notification.Sender, &recipientsJSON, &notification.Priority,
		&notification.Reason, &notification.State, &notification.Adapter,
		&notification.Attempts, &notification.LastError, &createdAt, &updatedAt,
		&lastAttemptAt, &nextAttemptAt, &renderedAt,
	); err != nil {
		return Notification{}, fmt.Errorf("scan human notification: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewBufferString(recipientsJSON))
	if err := decoder.Decode(&notification.LocalRecipients); err != nil {
		return Notification{}, ErrInvalidRecord
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return Notification{}, ErrInvalidRecord
	}
	var err error
	if notification.CreatedAt, err = eventstore.ParseTime(createdAt); err != nil {
		return Notification{}, ErrInvalidRecord
	}
	if notification.UpdatedAt, err = eventstore.ParseTime(updatedAt); err != nil {
		return Notification{}, ErrInvalidRecord
	}
	for _, field := range []struct {
		raw    sql.NullString
		target **time.Time
	}{
		{raw: lastAttemptAt, target: &notification.LastAttemptAt},
		{raw: nextAttemptAt, target: &notification.NextAttemptAt},
		{raw: renderedAt, target: &notification.RenderedAt},
	} {
		if !field.raw.Valid {
			continue
		}
		parsed, parseErr := eventstore.ParseTime(field.raw.String)
		if parseErr != nil {
			return Notification{}, ErrInvalidRecord
		}
		*field.target = &parsed
	}
	if err := notification.validate(); err != nil {
		return Notification{}, ErrInvalidRecord
	}
	return notification, nil
}

func (s *Service) renderOne(ctx context.Context, notification Notification) error {
	notification.Adapter = s.adapter.Name()
	err := s.adapter.Render(ctx, notification)
	if ctx.Err() != nil {
		return fmt.Errorf("render notification adapter: %w", ctx.Err())
	}
	if err != nil {
		var adapterErr *AdapterError
		if !errors.As(err, &adapterErr) || !adapterErr.Reason.valid() {
			adapterErr = &AdapterError{Reason: FailureAdapterFailed, Retryable: true}
		}
		return s.markFailure(ctx, notification, adapterErr)
	}
	if s.afterAdapter != nil {
		if err := s.afterAdapter(notification); err != nil {
			return err
		}
	}
	return s.markRendered(ctx, notification)
}

func (s *Service) markFailure(
	ctx context.Context,
	notification Notification,
	failure *AdapterError,
) error {
	now := s.clock.Now().UTC()
	attempts := notification.Attempts + 1
	if !failure.Retryable {
		attempts = s.maxAttempts
	}
	var next any
	if failure.Retryable && attempts < s.maxAttempts {
		next = eventstore.FormatTime(now.Add(s.retryDelay))
	}
	result, err := s.store.DB().ExecContext(ctx, `UPDATE human_notifications SET
		adapter = ?, attempts = ?, last_error_reason = ?, updated_at = ?,
		last_attempt_at = ?, next_attempt_at = ?
		WHERE notification_id = ? AND state = 'PENDING'`,
		notification.Adapter, attempts, failure.Reason, eventstore.FormatTime(now),
		eventstore.FormatTime(now), next, notification.ID)
	if err != nil {
		return fmt.Errorf("record notification adapter failure: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read notification adapter failure result: %w", err)
	}
	if changed != 1 {
		return ErrInvalidRecord
	}
	return nil
}

func (s *Service) markRendered(ctx context.Context, notification Notification) error {
	tx, err := s.store.DB().BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin rendered notification: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	now := s.clock.Now().UTC()
	result, err := tx.ExecContext(ctx, `UPDATE human_notifications SET
		state = 'RENDERED', adapter = ?, attempts = attempts + 1,
		last_error_reason = NULL, updated_at = ?, last_attempt_at = ?,
		next_attempt_at = NULL, rendered_at = ?
		WHERE notification_id = ? AND state = 'PENDING'`,
		notification.Adapter, eventstore.FormatTime(now), eventstore.FormatTime(now),
		eventstore.FormatTime(now), notification.ID)
	if err != nil {
		return fmt.Errorf("mark human notification rendered: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read rendered notification result: %w", err)
	}
	if changed == 0 {
		return nil
	}
	if changed != 1 {
		return ErrInvalidRecord
	}
	sequence, err := nextGiffordSequence(ctx, tx)
	if err != nil {
		return err
	}
	notification.State = StateRendered
	notification.UpdatedAt = now
	notification.RenderedAt = &now
	event, err := s.notificationEvent(
		now, sequence, protocol.EventTypeHumanNotificationRendered, notification,
	)
	if err != nil {
		return err
	}
	if err := s.store.AppendEventsTx(ctx, tx, []protocol.EventEnvelope{event}); err != nil {
		return fmt.Errorf("append rendered notification event: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit rendered notification: %w", err)
	}
	return nil
}
