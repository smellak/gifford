package daemon

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/eventstore"
)

var publicationTestTime = time.Date(2026, time.November, 3, 9, 30, 0, 0, time.UTC)

func openPublicationTestStore(t *testing.T) *eventstore.Store {
	t.Helper()
	store, err := eventstore.Open(t.Context(), filepath.Join(t.TempDir(), "gifford.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return store
}

// TestIndexPublicationRecord requires the store to persist one publication
// row in the caller transaction with the provision timestamp, and to reject
// invalid rows before touching the database.
func TestIndexPublicationRecord(t *testing.T) {
	store := openPublicationTestStore(t)
	var publications IndexPublicationStore

	tx, err := store.DB().BeginTx(t.Context(), nil)
	require.NoError(t, err)
	require.NoError(t, publications.Record(t.Context(), tx, "1113/panelhash/12.lock", publicationTestTime))
	require.NoError(t, tx.Commit())

	var key, stamp string
	require.NoError(t, store.DB().QueryRowContext(t.Context(), `SELECT panel_key, published_at
		FROM pane_index_publications`).Scan(&key, &stamp))
	require.Equal(t, "1113/panelhash/12.lock", key)
	require.Equal(t, eventstore.FormatTime(publicationTestTime), stamp)

	second, err := store.DB().BeginTx(t.Context(), nil)
	require.NoError(t, err)
	require.NoError(t, second.Rollback())
	for _, invalid := range []struct {
		name string
		tx   *sql.Tx
		key  string
		when time.Time
	}{
		{"nil tx", nil, "1113/panelhash/12.lock", publicationTestTime},
		{"empty key", second, "", publicationTestTime},
		{"zero time", second, "1113/panelhash/12.lock", time.Time{}},
	} {
		t.Run(invalid.name, func(t *testing.T) {
			err := publications.Record(t.Context(), invalid.tx, invalid.key, invalid.when)
			require.ErrorIs(t, err, ErrInvalidPublication)
		})
	}
	var count int
	require.NoError(t, store.DB().QueryRowContext(t.Context(), `SELECT COUNT(*)
		FROM pane_index_publications`).Scan(&count))
	require.Equal(t, 1, count)
}
