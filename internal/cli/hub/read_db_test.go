package hub

import (
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/eventstore"
)

func TestReadDBAppliesQueryOnlyToEveryConnection(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "read db #.sqlite")
	store, err := eventstore.Open(t.Context(), path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	_, err = store.DB().ExecContext(t.Context(), "CREATE TABLE read_db_probe (value TEXT)")
	require.NoError(t, err)

	readDB, err := openReadDB(t.Context(), path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, readDB.Close()) })
	connections := make([]*sql.Conn, 0, 4)
	for range 4 {
		conn, connErr := readDB.Conn(t.Context())
		require.NoError(t, connErr)
		connections = append(connections, conn)
		var queryOnly int
		require.NoError(t, conn.QueryRowContext(t.Context(), "PRAGMA query_only").Scan(&queryOnly))
		require.Equal(t, 1, queryOnly)
	}
	for _, conn := range connections {
		_, err = conn.ExecContext(t.Context(), "PRAGMA query_only = 0")
		require.NoError(t, err)
		var queryOnly int
		require.NoError(t, conn.QueryRowContext(t.Context(), "PRAGMA query_only").Scan(&queryOnly))
		require.Zero(t, queryOnly)
		_, writeErr := conn.ExecContext(t.Context(), "INSERT INTO read_db_probe (value) VALUES ('read-pool')")
		require.Error(t, writeErr, "mode=ro must reject writes even if query_only is disabled")
		require.NoError(t, conn.Close())
	}

	_, err = store.DB().ExecContext(t.Context(), "INSERT INTO read_db_probe (value) VALUES ('writer-pool')")
	require.NoError(t, err, "query_only must not affect the event-store writer pool")
	var count int
	require.NoError(t, readDB.QueryRowContext(t.Context(), "SELECT count(*) FROM read_db_probe").Scan(&count))
	require.Equal(t, 1, count)
}

func TestReadDBRefusesDSNsAndMissingFiles(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	for _, path := range []string{
		"", ":memory:", "file:test.db", filepath.Join(dir, "test.db") + "?mode=ro",
		filepath.Join(dir, "missing.db"),
	} {
		db, err := openReadDB(t.Context(), path)
		require.Error(t, err)
		require.Nil(t, db)
	}
}
