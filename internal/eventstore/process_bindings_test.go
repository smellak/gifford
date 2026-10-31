package eventstore

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProcessBindingMigrationUpDownUp(t *testing.T) {
	t.Parallel()

	store := testStore(t)
	ctx := t.Context()
	requireProcessBindingTable(t, store.DB(), true)
	// session_boot_id, human_notifications, state_producer and message_triage
	// follow this migration.
	require.NoError(t, store.MigrateDown(ctx, 5))
	requireProcessBindingTable(t, store.DB(), false)
	require.NoError(t, store.MigrateUp(ctx))
	requireProcessBindingTable(t, store.DB(), true)
}

func requireProcessBindingTable(t *testing.T, db *sql.DB, want bool) {
	t.Helper()
	var count int
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT count(*)
		FROM sqlite_schema WHERE type = 'table'
		AND name = 'orchestrator_process_bindings'`).Scan(&count))
	if want {
		require.Equal(t, 1, count)
	} else {
		require.Zero(t, count)
	}
}
