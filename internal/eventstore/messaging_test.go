package eventstore

import (
	"database/sql"
	"io/fs"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/migrations"
)

func stepsFromMessagingMigration(t *testing.T) int {
	t.Helper()

	entries, err := fs.ReadDir(migrations.FS(), ".")
	require.NoError(t, err)
	const messagingMigration = "20261029172034_local_messaging_tables.sql"
	found := false
	steps := 0
	for _, entry := range entries {
		if entry.Name() == messagingMigration {
			found = true
		}
		if found && !entry.IsDir() && strings.HasSuffix(entry.Name(), ".sql") {
			steps++
		}
	}
	require.True(t, found)
	return steps
}

func insertMessagingTestMessage(t *testing.T, db *sql.DB, id string) {
	t.Helper()
	stamp := FormatTime(testEventTime)
	_, err := db.ExecContext(t.Context(), `INSERT INTO messages
		(message_id, client_msg_id, origin_node, from_agent, destination_node,
		 thread_ref, priority, state, created_at, expires_at, updated_at,
		 hop_count, route_digest)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id, "client-"+id, "node-a", "sender", "node-b", "thread-1",
		"NORMAL", "PENDING", stamp, stamp, stamp, 0, "sha256:route")
	require.NoError(t, err)
}

func TestMessagingMigrationUpDownUp(t *testing.T) {
	t.Parallel()

	store := testStore(t)
	before := schemaSnapshot(t.Context(), t, store.DB())
	require.NoError(t, store.MigrateDown(t.Context(), stepsFromMessagingMigration(t)))

	for _, name := range []string{
		tableAcks,
		tableMessageRecipients,
		"message_recipients_ordinal",
		"message_recipients_immutable",
		"messages_admission_result_once",
	} {
		var count int
		require.NoError(t, store.DB().QueryRowContext(t.Context(),
			"SELECT count(*) FROM sqlite_schema WHERE name = ?", name).Scan(&count))
		require.Zero(t, count, name)
	}
	for _, column := range readTableColumns(t, store.DB(), tableMessages) {
		require.NotEqual(t, columnAdmissionResultJSON, column.name)
	}

	require.NoError(t, store.MigrateUp(t.Context()))
	require.Equal(t, before, schemaSnapshot(t.Context(), t, store.DB()))
}

func TestMessagingTablesAndConstraints(t *testing.T) {
	t.Parallel()

	store := testStore(t)
	db := store.DB()
	insertMessagingTestMessage(t, db, "message-1")
	stamp := FormatTime(testEventTime)

	_, err := db.ExecContext(t.Context(), `INSERT INTO message_recipients
		(message_id, agent_id, ordinal, delivery_mode, requires_ack, created_at)
		VALUES (?, ?, ?, ?, ?, ?)`, "message-1", "agent-a", 0, "WHEN_SAFE", 1, stamp)
	require.NoError(t, err)
	var ordinal, requiresAck int64
	var deliveryMode, createdAt string
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT ordinal, delivery_mode,
		requires_ack, created_at FROM message_recipients
		WHERE message_id = ? AND agent_id = ?`, "message-1", "agent-a").
		Scan(&ordinal, &deliveryMode, &requiresAck, &createdAt))
	require.Equal(t, int64(0), ordinal)
	require.Equal(t, "WHEN_SAFE", deliveryMode)
	require.Equal(t, int64(1), requiresAck)
	require.Equal(t, stamp, createdAt)

	_, err = db.ExecContext(t.Context(), `INSERT INTO message_recipients
		(message_id, agent_id, ordinal, delivery_mode, requires_ack, created_at)
		VALUES (?, ?, ?, ?, ?, ?)`, "message-1", "agent-b", 0, "DISPLAY_ONLY", 0, stamp)
	require.Error(t, err)
	require.Contains(t, err.Error(), "message_recipients.message_id, message_recipients.ordinal")

	_, err = db.ExecContext(t.Context(), `INSERT INTO message_recipients
		(message_id, agent_id, ordinal, delivery_mode, requires_ack, created_at)
		VALUES (?, ?, ?, ?, ?, ?)`, "message-1", "agent-b", 1, "UNKNOWN", 0, stamp)
	require.Error(t, err)
	require.Contains(t, err.Error(), "CHECK constraint failed")

	_, err = db.ExecContext(t.Context(), `UPDATE message_recipients
		SET delivery_mode = 'DISPLAY_ONLY' WHERE message_id = ? AND agent_id = ?`,
		"message-1", "agent-a")
	require.Error(t, err)
	require.Contains(t, err.Error(), "message_recipients is immutable")

	_, err = db.ExecContext(t.Context(),
		"UPDATE messages SET admission_result_json = ? WHERE message_id = ?",
		`{"decision":"allow"}`, "message-1")
	require.NoError(t, err)
	var admissionResult string
	require.NoError(t, db.QueryRowContext(t.Context(),
		"SELECT admission_result_json FROM messages WHERE message_id = ?", "message-1").
		Scan(&admissionResult))
	require.JSONEq(t, `{"decision":"allow"}`, admissionResult)
	_, err = db.ExecContext(t.Context(),
		"UPDATE messages SET admission_result_json = ? WHERE message_id = ?",
		`{"decision":"deny"}`, "message-1")
	require.Error(t, err)
	require.Contains(t, err.Error(), "messages.admission_result_json is written once")

	ackSQL := `INSERT INTO acks
		(message_id, agent_id, delivery_generation, status, acked_at)
		VALUES (?, ?, ?, ?, ?) ON CONFLICT DO NOTHING`
	result, err := db.ExecContext(t.Context(), ackSQL,
		"message-1", "agent-a", 1, "ACKNOWLEDGED", stamp)
	require.NoError(t, err)
	affected, err := result.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), affected)
	result, err = db.ExecContext(t.Context(), ackSQL,
		"message-1", "agent-a", 1, "ACKNOWLEDGED", stamp)
	require.NoError(t, err)
	affected, err = result.RowsAffected()
	require.NoError(t, err)
	require.Zero(t, affected)
	var ackedAt string
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT acked_at FROM acks
		WHERE message_id = ? AND agent_id = ? AND delivery_generation = ? AND status = ?`,
		"message-1", "agent-a", 1, "ACKNOWLEDGED").Scan(&ackedAt))
	require.Equal(t, stamp, ackedAt)
}
