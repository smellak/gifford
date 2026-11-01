package eventstore

import (
	"database/sql"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProcessBindingMigrationUpDownUp(t *testing.T) {
	t.Parallel()

	store := testStore(t)
	ctx := t.Context()
	requireProcessBindingTable(t, store.DB(), true)
	require.NoError(t, store.MigrateDown(ctx,
		migrationStepsFrom(t, "20261030204257_orchestrator_process_bindings.sql")))
	requireProcessBindingTable(t, store.DB(), false)
	require.NoError(t, store.MigrateUp(ctx))
	requireProcessBindingTable(t, store.DB(), true)
}

func TestProcessBindingExecutablesMigrationUpDownUp(t *testing.T) {
	t.Parallel()

	store := testStore(t)
	ctx := t.Context()
	requireProcessBindingExecutableColumns(t, store.DB(), true)
	require.NoError(t, store.MigrateDown(ctx,
		migrationStepsFrom(t, "20261101034917_process_binding_executables.sql")))
	requireProcessBindingExecutableColumns(t, store.DB(), false)
	require.NoError(t, store.MigrateUp(ctx))
	requireProcessBindingExecutableColumns(t, store.DB(), true)
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

func requireProcessBindingExecutableColumns(t *testing.T, db *sql.DB, want bool) {
	t.Helper()
	columns := readTableColumns(t, db, "orchestrator_process_bindings")
	names := make([]string, len(columns))
	for index := range columns {
		names[index] = columns[index].name
	}
	for _, name := range []string{
		"layout_version", "child_executable", "auxiliary_pid", "auxiliary_start_time", "auxiliary_executable",
	} {
		require.Equal(t, want, slices.Contains(names, name), name)
	}
}
