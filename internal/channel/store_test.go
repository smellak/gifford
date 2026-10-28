package channel

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/eventstore"
)

// testChannelStore opens an event store database, which applies every
// embedded migration including the control plane tables.
func testChannelStore(t *testing.T) *eventstore.Store {
	t.Helper()
	store, err := eventstore.Open(t.Context(), filepath.Join(t.TempDir(), "gifford.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return store
}

// testPragmaObjects lists the main-schema tables and, for each table, its
// indexes except the automatic ones, through PRAGMA introspection.
func testPragmaObjects(t *testing.T, ctx context.Context, db *sql.DB) (tables, indexes map[string]bool) {
	t.Helper()
	tables = make(map[string]bool)
	rows, err := db.QueryContext(ctx, "PRAGMA table_list")
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var schema, name, typ string
		var ncol, withoutRowID, strict int64
		require.NoError(t, rows.Scan(&schema, &name, &typ, &ncol, &withoutRowID, &strict))
		if schema == "main" && typ == "table" {
			tables[name] = true
		}
	}
	require.NoError(t, rows.Err())
	indexes = make(map[string]bool)
	for table := range tables {
		idxRows, err := db.QueryContext(ctx, "PRAGMA index_list("+quoteIdentifier(table)+")")
		require.NoError(t, err)
		for idxRows.Next() {
			var seq int64
			var name, origin string
			var unique, partial int64
			require.NoError(t, idxRows.Scan(&seq, &name, &unique, &origin, &partial))
			if len(name) >= 7 && name[:7] == "sqlite_" {
				continue
			}
			indexes[name] = true
		}
		require.NoError(t, idxRows.Err())
		require.NoError(t, idxRows.Close())
	}
	return tables, indexes
}

// quoteIdentifier double-quotes an SQLite identifier.
func quoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// channelTables holds the four table names of the control plane migration.
var channelTables = []string{
	"channel_controls",
	"communication_loops",
	"channel_control_decisions",
	"approval_requests",
}

// channelIndexes holds the six index names of the control plane migration.
var channelIndexes = []string{
	"channel_controls_scope",
	"channel_controls_link",
	"channel_controls_expiry",
	"channel_control_decisions_object",
	"approval_requests_pending",
	"approval_requests_object",
}

// TestMigrationControlTables applies the control plane migration up, down
// and up again: the ten objects come and go while the 1.9 tables stay.
func TestMigrationControlTables(t *testing.T) {
	t.Parallel()

	store := testChannelStore(t)
	ctx := t.Context()
	tables, indexes := testPragmaObjects(t, ctx, store.DB())
	for _, name := range channelTables {
		require.True(t, tables[name], "table %s", name)
	}
	for _, name := range channelIndexes {
		require.True(t, indexes[name], "index %s", name)
	}

	require.NoError(t, store.MigrateDown(ctx, 1))
	downTables, downIndexes := testPragmaObjects(t, ctx, store.DB())
	for _, name := range channelTables {
		require.False(t, downTables[name], "table %s removed", name)
	}
	for _, name := range channelIndexes {
		require.False(t, downIndexes[name], "index %s removed", name)
	}
	for _, name := range []string{"agents", "sessions", "events", "input_guards", "capture_sessions", "capture_segments"} {
		require.True(t, downTables[name], "1.9 table %s kept", name)
	}

	require.NoError(t, store.MigrateUp(ctx))
	upTables, upIndexes := testPragmaObjects(t, ctx, store.DB())
	require.Equal(t, tables, upTables)
	require.Equal(t, indexes, upIndexes)
}
