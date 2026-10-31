package notifications

import (
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

const notificationListFirstID protocol.OpaqueID = "notification-a"

func TestListReturnsPublicMetadataInStableOrder(t *testing.T) {
	fixture := newNotificationFixture(t)
	when := notificationTestTime
	equalAttempt := when.Add(time.Minute)
	rows := []Notification{
		{
			ID: notificationListFirstID, Class: ClassMessage,
			ObjectID: protocol.OpaqueID(notificationMessageID(11)), Sender: notificationTestSender,
			LocalRecipients: []protocol.AgentID{notificationTestRecipient}, Priority: protocol.PriorityHigh,
			Reason: ReasonMessageAvailable, State: StatePending, Adapter: AdapterTMUX,
			CreatedAt: when, UpdatedAt: when,
		},
		{
			ID: "notification-z", Class: ClassMessage,
			ObjectID: protocol.OpaqueID(notificationMessageID(12)), Sender: notificationTestSender,
			LocalRecipients: []protocol.AgentID{notificationTestRecipient}, Priority: protocol.PriorityNormal,
			Reason: ReasonDisplayOnly, State: StateRendered, Adapter: AdapterTMUX,
			Attempts: 1, CreatedAt: when, UpdatedAt: equalAttempt,
			LastAttemptAt: &equalAttempt, RenderedAt: &equalAttempt,
		},
		{
			ID: "notification-baselined", Class: ClassApproval,
			ObjectID:        "apr_01JB0000000000000000000013",
			LocalRecipients: []protocol.AgentID{}, Reason: ReasonBaseline,
			State: StateBaselined, Adapter: AdapterNone,
			CreatedAt: when.Add(-time.Minute), UpdatedAt: when.Add(-time.Minute),
		},
		{
			ID: "notification-service", Class: classService, ObjectID: "INITIAL",
			LocalRecipients: []protocol.AgentID{}, Reason: ReasonBaseline,
			State: StateBaselined, Adapter: AdapterNone,
			CreatedAt: when.Add(time.Hour), UpdatedAt: when.Add(time.Hour),
		},
	}
	insertListNotifications(t, fixture.store.DB(), rows...)

	tx, err := fixture.store.DB().BeginTx(t.Context(), &sql.TxOptions{ReadOnly: true})
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	page, err := List(t.Context(), tx, ListQuery{Limit: protocol.MaxListLimit + 1})
	require.NoError(t, err)
	require.Equal(t, int64(1), page.PendingCount)
	require.Len(t, page.Notifications, 3)
	require.Equal(t, []protocol.OpaqueID{
		"notification-z", notificationListFirstID, "notification-baselined",
	}, notificationIDs(page.Notifications))
	require.Equal(t, StateRendered, page.Notifications[0].State)
	require.NotNil(t, page.Notifications[0].LastAttemptAt)
	require.NotNil(t, page.Notifications[0].RenderedAt)
	require.Equal(t, *page.Notifications[0].LastAttemptAt, *page.Notifications[0].RenderedAt,
		"equal non-null timestamps must decode into both independent fields")
	require.Nil(t, page.Notifications[0].NextAttemptAt)
	for _, notification := range page.Notifications {
		require.NotEqual(t, classService, notification.Class)
	}

	before := page.Notifications[0]
	second, err := List(t.Context(), tx, ListQuery{
		BeforeCreatedAt: &before.CreatedAt, BeforeID: before.ID, Limit: 1,
	})
	require.NoError(t, err)
	require.Equal(t, []protocol.OpaqueID{notificationListFirstID}, notificationIDs(second.Notifications),
		"notification_id breaks identical creation timestamps deterministically")

	rendered, err := List(t.Context(), tx, ListQuery{State: StateRendered, Limit: 2})
	require.NoError(t, err)
	require.Equal(t, int64(1), rendered.PendingCount, "pending count is independent of the page filter")
	require.Len(t, rendered.Notifications, 1)
	require.Equal(t, StateRendered, rendered.Notifications[0].State)
}

func TestListRejectsInvalidStoredNotification(t *testing.T) {
	fixture := newNotificationFixture(t)
	row := Notification{
		ID: "notification-invalid", Class: ClassMessage,
		ObjectID: protocol.OpaqueID(notificationMessageID(21)), Sender: notificationTestSender,
		LocalRecipients: []protocol.AgentID{notificationTestRecipient}, Priority: protocol.PriorityHigh,
		Reason: ReasonMessageAvailable, State: StatePending, Adapter: AdapterTMUX,
		CreatedAt: notificationTestTime, UpdatedAt: notificationTestTime,
	}
	insertListNotifications(t, fixture.store.DB(), row)
	_, err := fixture.store.DB().ExecContext(t.Context(), `UPDATE human_notifications
		SET recipient_agents_json = '["Bad Agent"]' WHERE notification_id = ?`, row.ID)
	require.NoError(t, err)

	tx, err := fixture.store.DB().BeginTx(t.Context(), &sql.TxOptions{ReadOnly: true})
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	_, err = List(t.Context(), tx, ListQuery{Limit: 1})
	require.ErrorIs(t, err, ErrInvalidRecord)
	require.NotContains(t, err.Error(), "Bad Agent", "stored values never enter diagnostics")
}

func TestNotificationListSQLIsMetadataOnly(t *testing.T) {
	t.Parallel()

	for _, query := range []string{notificationListSQL, pendingNotificationCountSQL} {
		lower := strings.ToLower(query)
		require.Contains(t, lower, "human_notifications")
		for _, forbidden := range []string{
			"messages", "message_envelopes", "approval_requests", "body", "text",
			"subject", "request_note", "payload", "select *",
		} {
			require.NotContains(t, lower, forbidden)
		}
	}
}

func insertListNotifications(t *testing.T, db *sql.DB, notifications ...Notification) {
	t.Helper()
	tx, err := db.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	for _, notification := range notifications {
		require.NoError(t, insertNotification(t.Context(), tx, notification))
	}
	require.NoError(t, tx.Commit())
}

func notificationIDs(notifications []Notification) []protocol.OpaqueID {
	ids := make([]protocol.OpaqueID, 0, len(notifications))
	for _, notification := range notifications {
		ids = append(ids, notification.ID)
	}
	return ids
}
