package eventstore

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSessionBootIDMigrationUpDownUp(t *testing.T) {
	t.Parallel()

	store := testStore(t)
	ctx := t.Context()
	requireSessionBootID := func(want bool) {
		t.Helper()
		columns := readTableColumns(t, store.DB(), tableSessions)
		found := false
		for _, column := range columns {
			if column.name == columnBootID {
				found = true
				require.Equal(t, "TEXT", column.typeName)
				require.False(t, column.notNull, "legacy sessions must retain a NULL boot id")
			}
		}
		require.Equal(t, want, found)
	}

	requireSessionBootID(true)
	// human_notifications, state_producer and message_triage follow this
	// migration.
	require.NoError(t, store.MigrateDown(ctx, 4))
	requireSessionBootID(false)
	require.NoError(t, store.MigrateUp(ctx))
	requireSessionBootID(true)
}
