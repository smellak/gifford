package eventstore

import (
	"database/sql"
	"io/fs"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/migrations"
)

func migrationStepsFrom(t *testing.T, name string) int {
	t.Helper()
	entries, err := fs.ReadDir(migrations.FS(), ".")
	require.NoError(t, err)
	found := false
	steps := 0
	for _, entry := range entries {
		if entry.Name() == name {
			found = true
		}
		if found && !entry.IsDir() && strings.HasSuffix(entry.Name(), ".sql") {
			steps++
		}
	}
	require.True(t, found, "embedded migration %s", name)
	return steps
}

func TestStateProducerMigrationUpDownUp(t *testing.T) {
	t.Parallel()

	store := testStore(t)
	ctx := t.Context()
	before := schemaSnapshot(ctx, t, store.DB())
	requireStateProducerColumn(t, store.DB(), true)

	require.NoError(t, store.MigrateDown(ctx,
		migrationStepsFrom(t, "20261031022426_state_producer.sql")))
	requireStateProducerColumn(t, store.DB(), false)
	require.NoError(t, store.MigrateUp(ctx))
	requireStateProducerColumn(t, store.DB(), true)
	require.Equal(t, before, schemaSnapshot(ctx, t, store.DB()))
}

func TestLegacyStateSnapshotIsNeverProducerEligible(t *testing.T) {
	t.Parallel()

	store := testStore(t)
	_, err := store.DB().ExecContext(t.Context(), `INSERT INTO state_snapshots (
		state_id, agent_id, observed_at, session_state, human_draft_state,
		human_activity_state, draft_generation, input_guard_version, agent_phase,
		attention_state, health_state, delivery_safety, confidence_json,
		evidence_json, analyst_version, stale
	) VALUES ('01JBC000000000000000000001', 'agent-a', ?, 'CONNECTED',
		'UNKNOWN', 'UNKNOWN', 0, 0, 'UNKNOWN', 'NONE', 'HEALTHY',
		'UNCERTAIN', '{}', '[]', 'legacy', 1)`, FormatTime(testEventTime))
	require.NoError(t, err)

	var producer, session, expires sql.NullString
	require.NoError(t, store.DB().QueryRowContext(t.Context(), `SELECT
		producer_version, session_id, expires_at FROM state_snapshots
		WHERE state_id = '01JBC000000000000000000001'`).Scan(&producer, &session, &expires))
	require.False(t, producer.Valid)
	require.False(t, session.Valid)
	require.False(t, expires.Valid)
}

func requireStateProducerColumn(t *testing.T, db *sql.DB, want bool) {
	t.Helper()
	found := false
	for _, column := range readTableColumns(t, db, tableStateSnapshots) {
		if column.name == "producer_version" {
			found = true
			require.False(t, column.notNull, "legacy rows must remain nullable")
		}
	}
	require.Equal(t, want, found)
}
