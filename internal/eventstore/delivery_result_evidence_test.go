package eventstore

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDeliveryResultEvidenceMigrationUpDownUp(t *testing.T) {
	t.Parallel()
	store := testStore(t)
	ctx := t.Context()
	before := schemaSnapshot(ctx, t, store.DB())

	var recipientIndex string
	require.NoError(t, store.DB().QueryRowContext(ctx, `SELECT sql FROM sqlite_schema
		WHERE type = 'index' AND name = 'approval_requests_orchestrator_recipient'`).Scan(&recipientIndex))
	require.Contains(t, recipientIndex, "COALESCE(agent_id, '')")
	require.Contains(t, recipientIndex, "source = 'orchestrator'")
	var objectIndex string
	require.NoError(t, store.DB().QueryRowContext(ctx, `SELECT sql FROM sqlite_schema
		WHERE type = 'index' AND name = 'approval_requests_object'`).Scan(&objectIndex))
	require.Contains(t, objectIndex, "source <> 'orchestrator'")

	// The process-binding, session-boot and notification migrations follow
	// this one, so revert all four to inspect the evidence migration's Down.
	require.NoError(t, store.MigrateDown(ctx, 4))
	var count int
	require.NoError(t, store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_schema
		WHERE name IN ('delivery_result_evidence', 'approval_requests_orchestrator_recipient')`).Scan(&count))
	require.Zero(t, count)
	var legacyIndex string
	require.NoError(t, store.DB().QueryRowContext(ctx, `SELECT sql FROM sqlite_schema
		WHERE type = 'index' AND name = 'approval_requests_object'`).Scan(&legacyIndex))
	require.NotContains(t, legacyIndex, "orchestrator")

	require.NoError(t, store.MigrateUp(ctx))
	require.Equal(t, before, schemaSnapshot(ctx, t, store.DB()))
}

func TestDeliveryResultEvidenceSchemaOmitsPayloadAndErrorText(t *testing.T) {
	t.Parallel()
	store := testStore(t)
	columns := readTableColumns(t, store.DB(), "delivery_result_evidence")
	for _, column := range columns {
		name := strings.ToLower(column.name)
		require.NotContains(t, name, "message")
		require.NotContains(t, name, "payload")
		require.NotEqual(t, "error", name)
	}
}
