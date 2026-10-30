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

	require.NoError(t, store.MigrateDown(ctx, 1))
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

func TestOrchestratorApprovalRecipientIndexClosesNullHole(t *testing.T) {
	t.Parallel()
	store := testStore(t)
	insert := func(id string) error {
		_, err := store.DB().ExecContext(t.Context(), `INSERT INTO approval_requests
			(approval_id, source, object_type, object_id, agent_id, options_json,
			 state, created_at) VALUES (?, 'orchestrator', 'message', 'message-null',
			 NULL, '["deliver_now","reject"]', 'PENDING', ?)`, id, FormatTime(testEventTime))
		return err
	}
	require.NoError(t, insert("apr_01JB0000000000000000000001"))
	require.Error(t, insert("apr_01JB0000000000000000000002"))

	insertLink := func(id string) error {
		_, err := store.DB().ExecContext(t.Context(), `INSERT INTO approval_requests
			(approval_id, source, object_type, object_id, options_json, state, created_at)
			VALUES (?, 'link', 'message', 'message-link', '["approve_once","reject"]',
			'PENDING', ?)`, id, FormatTime(testEventTime))
		return err
	}
	require.NoError(t, insertLink("apr_01JB0000000000000000000003"))
	require.Error(t, insertLink("apr_01JB0000000000000000000004"),
		"non-orchestrator sources retain object/source deduplication")
}
