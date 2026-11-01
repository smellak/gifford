package dataset

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/eventstore"
)

func TestOpenNativeReadDBCannotWrite(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "gifford.db")
	store, err := eventstore.Open(t.Context(), path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	db, err := openNativeReadDB(t.Context(), path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	conn, err := db.Conn(t.Context())
	require.NoError(t, err)
	defer func() { require.NoError(t, conn.Close()) }()
	var queryOnly int
	require.NoError(t, conn.QueryRowContext(t.Context(), "PRAGMA query_only").Scan(&queryOnly))
	require.Equal(t, 1, queryOnly)

	// Even if one connection turns off the advisory query-only pragma, the
	// SQLite mode=ro URI remains the hard boundary.
	_, err = conn.ExecContext(t.Context(), "PRAGMA query_only = 0")
	require.NoError(t, err)
	_, err = conn.ExecContext(t.Context(), "CREATE TABLE forbidden_native_write (id INTEGER)")
	require.Error(t, err)
}

func TestOpenNativeReadDBRejectsUnsafeOrMissingPaths(t *testing.T) {
	t.Parallel()
	for _, path := range []string{"", ":memory:", "file:test.db", "test.db?mode=ro"} {
		_, err := openNativeReadDB(t.Context(), path)
		require.Error(t, err, path)
	}
	_, err := openNativeReadDB(t.Context(), filepath.Join(t.TempDir(), "missing.db"))
	require.Error(t, err)
}
