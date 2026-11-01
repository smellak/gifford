package eventstore

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFederationArtifactAckMigrationUpDownUp(t *testing.T) {
	t.Parallel()
	store := testStore(t)
	ctx := t.Context()
	before := schemaSnapshot(ctx, t, store.DB())
	requireFederationArtifactAckTables(t, store, true)
	require.NoError(t, store.MigrateDown(ctx,
		migrationStepsFrom(t, "20261101000004_federation_artifact_ack.sql")))
	requireFederationArtifactAckTables(t, store, false)
	require.NoError(t, store.MigrateUp(ctx))
	requireFederationArtifactAckTables(t, store, true)
	require.Equal(t, before, schemaSnapshot(ctx, t, store.DB()))
}

func requireFederationArtifactAckTables(t *testing.T, store *Store, want bool) {
	t.Helper()
	var count int
	require.NoError(t, store.DB().QueryRowContext(t.Context(), `SELECT COUNT(*)
		FROM sqlite_schema WHERE type = 'table' AND name IN (?, ?, ?)`,
		tableArtifactAckInbox, tableArtifactAckOutbox, tableArtifactAckReceipts).Scan(&count))
	if want {
		require.Equal(t, 3, count)
		return
	}
	require.Zero(t, count)
}
