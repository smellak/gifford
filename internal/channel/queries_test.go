package channel

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// testQueryText reads one embedded query or fails the test.
func testQueryText(t *testing.T, name string) string {
	t.Helper()
	text, err := queryText(name)
	require.NoError(t, err)
	return text
}

// TestQueriesEmbedded requires exactly the query files used by channel,
// none empty.
func TestQueriesEmbedded(t *testing.T) {
	t.Parallel()

	entries, err := queryFiles.ReadDir("queries")
	require.NoError(t, err)
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		require.False(t, entry.IsDir())
		names = append(names, entry.Name())
	}
	require.ElementsMatch(t, []string{
		"approvals_insert.sql",
		"approvals_resolve.sql",
		"approvals_select_all.sql",
		"approvals_select_by_id.sql",
		"approvals_select_pending_orchestrator.sql",
		"approvals_select_pending_object.sql",
		"approvals_supersede_pending_loop.sql",
		"approvals_supersede_pending_loop_extension.sql",
		"approvals_supersede_pending_loop_mode.sql",
		"approvals_supersede_pending_orchestrator.sql",
		"artifacts_select_control.sql",
		"artifacts_set_control.sql",
		"controls_select_all.sql",
		"controls_insert.sql",
		"controls_update_cas.sql",
		"controls_delete_cas.sql",
		"decisions_insert.sql",
		"decisions_select_by_object.sql",
		"deliveries_cas_control.sql",
		"deliveries_select_control.sql",
		"deliveries_select_preview.sql",
		"deliveries_set_control.sql",
		"envelopes_count.sql",
		"envelopes_set_control.sql",
		"epoch_select.sql",
		"loops_insert.sql",
		"loops_select_active_by_thread.sql",
		"loops_select_all.sql",
		"loops_select_by_id.sql",
		"loops_select_by_root_message.sql",
		"loops_select_closed_by_sequence.sql",
		"loops_select_closed_sequence.sql",
		"loops_select_latest_by_thread.sql",
		"loops_select_latest_terminal_by_thread.sql",
		"loops_update_cas.sql",
		"messages_cas_control.sql",
		"messages_select_loop_binding.sql",
		"messages_select_reply_parent.sql",
		"messages_select_control.sql",
		"messages_set_control.sql",
		"state_approvals_count.sql",
		"state_loops_select.sql",
	}, names)
	for _, name := range names {
		text, err := queryText(name)
		require.NoError(t, err)
		require.NotEmpty(t, text, "query %s", name)
	}
	_, err = queryText("controls_missing.sql")
	require.ErrorIs(t, err, ErrInvalidQuery)
}

// TestQueriesPrepare prepares every embedded query against the migrated
// schema.
func TestQueriesPrepare(t *testing.T) {
	t.Parallel()

	store := testChannelStore(t)
	ctx := t.Context()
	entries, err := queryFiles.ReadDir("queries")
	require.NoError(t, err)
	for _, entry := range entries {
		name := entry.Name()
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			stmt, err := store.DB().PrepareContext(ctx, testQueryText(t, name))
			require.NoError(t, err)
			defer func() { _ = stmt.Close() }()
		})
	}
}
