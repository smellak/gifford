package eventstore

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	testAttemptAgent   = "attempt-agent"
	testAttemptMessage = "attempt-message-1"
	testAttemptState   = "PREPARING"
)

func insertAttemptQueue(t *testing.T, db *sql.DB, messageID string, generation int64) {
	t.Helper()
	stamp := FormatTime(testEventTime)
	_, err := db.ExecContext(t.Context(), `INSERT INTO delivery_queue
		(message_id, agent_id, delivery_generation, from_agent, thread_ref,
		 state, priority, created_at, expires_at, updated_at, hop_count, route_digest)
		VALUES (?, ?, ?, ?, ?, 'QUEUED', 2, ?, ?, ?, 0, ?)`,
		messageID, testAttemptAgent, generation, "attempt-sender", "attempt-thread",
		stamp, stamp, stamp, "sha256:attempt-route")
	require.NoError(t, err)
}

func insertAttemptRow(
	t *testing.T,
	db *sql.DB,
	deliveryID, messageID, state string,
) error {
	t.Helper()
	stamp := FormatTime(testEventTime)
	_, err := db.ExecContext(t.Context(), `INSERT INTO delivery_attempts
		(delivery_id, message_id, agent_id, delivery_generation,
		 adapter_session_id, orchestrator_decision_id, channel_decision_id,
		 draft_generation, input_guard_version, control_versions_json,
		 state, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		deliveryID, messageID, testAttemptAgent, 1, "adapter-session-1",
		"01ARZ3NDEKTSV4RRFFQ69G5FAV", "chd_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		3, 7, `{"global":2}`, state, stamp, stamp)
	return err
}

func TestDeliveryAttemptsTableAndConstraints(t *testing.T) {
	t.Parallel()

	store := testStore(t)
	db := store.DB()
	insertAttemptQueue(t, db, testAttemptMessage, 1)
	require.NoError(t, insertAttemptRow(t, db, "delivery-1", testAttemptMessage, testAttemptState))

	var messageID, agentID, state, versions, createdAt string
	var generation, draftGeneration, guardVersion int64
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT message_id, agent_id,
		delivery_generation, draft_generation, input_guard_version,
		control_versions_json, state, created_at
		FROM delivery_attempts WHERE delivery_id = ?`, "delivery-1").Scan(
		&messageID, &agentID, &generation, &draftGeneration, &guardVersion,
		&versions, &state, &createdAt))
	require.Equal(t, testAttemptMessage, messageID)
	require.Equal(t, testAttemptAgent, agentID)
	require.Equal(t, int64(1), generation)
	require.Equal(t, int64(3), draftGeneration)
	require.Equal(t, int64(7), guardVersion)
	require.JSONEq(t, `{"global":2}`, versions)
	require.Equal(t, testAttemptState, state)
	require.Equal(t, FormatTime(testEventTime), createdAt)

	const secondMessage = "attempt-message-2"
	insertAttemptQueue(t, db, secondMessage, 1)
	err := insertAttemptRow(t, db, "delivery-1", secondMessage, "COMMITTED")
	require.Error(t, err)
	require.Contains(t, err.Error(), "delivery_attempts.delivery_id")

	err = insertAttemptRow(t, db, "delivery-2", testAttemptMessage, "PREPARED")
	require.Error(t, err)
	require.Contains(t, err.Error(), "delivery_attempts.message_id")

	// UNCERTAIN is terminal for the per-delivery uniqueness rule. The row is
	// retained while an explicitly initiated recovery gets its own identity.
	_, err = db.ExecContext(t.Context(), `UPDATE delivery_attempts
		SET state = 'UNCERTAIN' WHERE delivery_id = 'delivery-1'`)
	require.NoError(t, err)
	require.NoError(t, insertAttemptRow(t, db, "delivery-2", testAttemptMessage, testAttemptState))
	_, err = db.ExecContext(t.Context(), `UPDATE delivery_attempts
		SET state = 'COMMIT_SENT' WHERE delivery_id = 'delivery-2'`)
	require.NoError(t, err)

	err = insertAttemptRow(t, db, "delivery-3", secondMessage, "COMMIT_SENT")
	require.Error(t, err)
	require.Contains(t, err.Error(), "delivery_attempts.agent_id")

	err = insertAttemptRow(t, db, "delivery-invalid", secondMessage, "UNKNOWN")
	require.Error(t, err)
	require.Contains(t, err.Error(), "CHECK constraint failed")
}
