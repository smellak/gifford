package eventstore

import (
	"testing"

	"github.com/stretchr/testify/require"
)

const bridgeMessagesMigration = "20261102205956_bridge_messages.sql"

func TestBridgeMessagesMigrationRemovesContentlessRowsOnDown(t *testing.T) {
	t.Parallel()

	store := testStore(t)
	db := store.DB()
	ctx := t.Context()
	before := schemaSnapshot(ctx, t, db)
	stamp := FormatTime(testEventTime)

	insertMessagingTestMessage(t, db, "ordinary-message")
	_, err := db.ExecContext(ctx, `UPDATE messages SET client_msg_id = 'shared-client-id'
		WHERE message_id = 'ordinary-message'`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `INSERT INTO messages
		(message_id, client_msg_id, client_msg_namespace, origin_node, from_agent,
		 destination_node, thread_ref, priority, state, created_at, expires_at, updated_at,
		 hop_count, route_digest)
		VALUES ('bridge-message', 'shared-client-id', 'source-node-a', 'node-a', 'sender',
		 'node-a', 'bridge-thread', 'NORMAL', 'PENDING', ?, ?, ?, 0, 'sha256:route')`,
		stamp, stamp, stamp)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `INSERT INTO message_recipients
		(message_id, agent_id, ordinal, delivery_mode, requires_ack, created_at)
		VALUES ('bridge-message', 'recipient', 0, 'WHEN_SAFE', 1, ?)`, stamp)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `INSERT INTO delivery_queue
		(message_id, agent_id, delivery_generation, from_agent, thread_ref, state, priority,
		 created_at, expires_at, updated_at, hop_count, route_digest)
		VALUES ('bridge-message', 'recipient', 1, 'sender', 'bridge-thread', 'QUEUED', 2,
		 ?, ?, ?, 0, 'sha256:route')`, stamp, stamp, stamp)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `INSERT INTO delivery_attempts
		(delivery_id, message_id, agent_id, delivery_generation, adapter_session_id,
		 orchestrator_decision_id, channel_decision_id, draft_generation, input_guard_version,
		 control_versions_json, state, created_at, updated_at)
		VALUES ('bridge-delivery', 'bridge-message', 'recipient', 1, 'adapter-session',
		 'orchestrator-decision', 'channel-decision', 0, 0, '{}', 'PREPARING', ?, ?)`,
		stamp, stamp)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `INSERT INTO delivery_result_evidence
		(delivery_id, agent_id, adapter_session_id, result, frame_id, reply_to,
		 proxy_observed_at, received_at, observed_draft_generation,
		 observed_input_guard_version, control_versions_json)
		VALUES ('bridge-delivery', 'recipient', 'adapter-session', 'PREPARED', 1, 1,
		 ?, ?, 0, 0, '{}')`, stamp, stamp)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `INSERT INTO bridge_message_mappings
		(source_node_id, source_message_id, gifford_message_id, from_agent, to_agent,
		 source_created_at, created_at, updated_at)
		VALUES ('source-node-a', 42, 'bridge-message', 'sender', 'recipient', ?, ?, ?)`,
		stamp, stamp, stamp)
	require.NoError(t, err)

	require.NoError(t, store.MigrateDown(ctx, migrationStepsFrom(t, bridgeMessagesMigration)))
	var ordinary, bridge int
	require.NoError(t, db.QueryRowContext(ctx,
		"SELECT count(*) FROM messages WHERE message_id = 'ordinary-message'").Scan(&ordinary))
	require.NoError(t, db.QueryRowContext(ctx,
		"SELECT count(*) FROM messages WHERE message_id = 'bridge-message'").Scan(&bridge))
	require.Equal(t, 1, ordinary)
	require.Zero(t, bridge)
	for _, column := range readTableColumns(t, db, tableMessages) {
		require.NotEqual(t, columnClientNamespace, column.name)
	}

	require.NoError(t, store.MigrateUp(ctx))
	require.Equal(t, before, schemaSnapshot(ctx, t, db))
}
