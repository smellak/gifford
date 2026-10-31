package notifications

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

const notificationListSQL = `
SELECT notification_id, class, object_id,
       COALESCE(sender_agent, ''), recipient_agents_json, COALESCE(priority, ''),
       reason, state, adapter, attempts, COALESCE(last_error_reason, ''),
       created_at, updated_at, last_attempt_at, next_attempt_at, rendered_at
FROM human_notifications
WHERE class <> 'SERVICE'
  AND (? = '' OR state = ?)
  AND (? = 0 OR (created_at, notification_id) < (?, ?))
ORDER BY created_at DESC, notification_id DESC
LIMIT ?`

const pendingNotificationCountSQL = `
SELECT COUNT(*)
FROM human_notifications
WHERE class <> 'SERVICE' AND state = 'PENDING'`

// ListQuery is the validated storage query supplied by the Control API.
// Limit may be one larger than the public page bound so the caller can
// determine whether a next cursor is needed.
type ListQuery struct {
	State           State
	BeforeCreatedAt *time.Time
	BeforeID        protocol.OpaqueID
	Limit           int64
}

// ListPage is a metadata-only registry snapshot. PendingCount is independent
// of State so a single Control API read can render the panel header.
type ListPage struct {
	Notifications []Notification
	PendingCount  int64
}

// List reads public notification rows through the caller-owned read
// transaction. It never acknowledges or mutates a row.
func List(ctx context.Context, tx *sql.Tx, query ListQuery) (ListPage, error) {
	page := ListPage{Notifications: []Notification{}}
	if tx == nil || query.Limit < 1 || query.Limit > protocol.MaxListLimit+1 ||
		query.State != "" && !query.State.valid() ||
		(query.BeforeCreatedAt == nil) != (query.BeforeID == "") ||
		query.BeforeCreatedAt != nil && (!query.BeforeID.Valid() || query.BeforeCreatedAt.IsZero()) {
		return page, ErrInvalidOptions
	}
	if err := tx.QueryRowContext(ctx, pendingNotificationCountSQL).Scan(&page.PendingCount); err != nil {
		return page, fmt.Errorf("count pending human notifications: %w", err)
	}
	if page.PendingCount < 0 {
		return page, ErrInvalidRecord
	}
	hasCursor := 0
	createdAt := ""
	if query.BeforeCreatedAt != nil {
		hasCursor = 1
		createdAt = eventstore.FormatTime(query.BeforeCreatedAt.UTC())
	}
	rows, err := tx.QueryContext(ctx, notificationListSQL,
		string(query.State), string(query.State), hasCursor, createdAt, query.BeforeID, query.Limit)
	if err != nil {
		return page, fmt.Errorf("select human notifications: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		notification, scanErr := scanNotification(rows)
		if scanErr != nil {
			return page, scanErr
		}
		if notification.Class == classService {
			return page, ErrInvalidRecord
		}
		page.Notifications = append(page.Notifications, notification)
	}
	if err := rows.Close(); err != nil {
		return page, fmt.Errorf("close human notification list: %w", err)
	}
	if err := rows.Err(); err != nil {
		return page, fmt.Errorf("read human notification list: %w", err)
	}
	return page, nil
}
