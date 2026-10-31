package eventstore

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestHumanNotificationsMigrationUpDownUp keeps the notification migration
// reversible beneath every migration that follows it.
func TestHumanNotificationsMigrationUpDownUp(t *testing.T) {
	t.Parallel()

	store := testStore(t)
	ctx := t.Context()
	requireHumanNotificationsTable(t, store, true)
	require.NoError(t, store.MigrateDown(ctx,
		migrationStepsFrom(t, "20261030233216_human_notifications.sql")))
	requireHumanNotificationsTable(t, store, false)
	require.NoError(t, store.MigrateUp(ctx))
	requireHumanNotificationsTable(t, store, true)
}

func requireHumanNotificationsTable(t *testing.T, store *Store, want bool) {
	t.Helper()
	var count int
	require.NoError(t, store.DB().QueryRowContext(t.Context(),
		"SELECT count(*) FROM sqlite_schema WHERE type = 'table' AND name = ?",
		tableHumanNotifications).Scan(&count))
	if want {
		require.Equal(t, 1, count)
		return
	}
	require.Zero(t, count)
}
