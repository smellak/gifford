package eventstore

import (
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	bridgeSourcePersistenceMigration = "20261103161034_bridge_source_persistence.sql"
	bridgeSourceOutboxMigration      = "20261103161231_bridge_source_outbox.sql"
)

func bridgeSourceDigest(fill byte) string {
	return "sha256:" + strings.Repeat(string(fill), 64)
}

func bridgeSourceID(prefix string, fill byte) string {
	return prefix + strings.Repeat(string(fill), 32)
}

func insertBridgeSourceMessage(t *testing.T, db *sql.DB, messageID string) {
	t.Helper()
	insertMessagingTestMessage(t, db, messageID)
	_, err := db.ExecContext(t.Context(), `UPDATE messages
		SET thread_id = 'gifford-thread', thread_ref = 'gifford-thread-ref'
		WHERE message_id = ?`, messageID)
	require.NoError(t, err)
}

func insertBridgeLegacyMapping(
	t *testing.T,
	db *sql.DB,
	messageID string,
	hubMessageID int64,
	threadSequence int64,
	parentHubID any,
	parentGiffordID any,
	manifestDigest string,
	mappingReceiptID string,
	stamp string,
) {
	t.Helper()
	var parentHubStore any
	if parentHubID != nil {
		parentHubStore = "messages"
	}
	_, err := db.ExecContext(t.Context(), `INSERT INTO bridge_message_mappings
		(source_node_id, source_message_id, gifford_message_id, from_agent, to_agent,
		 source_created_at, source_context_id, source_parent_message_id, gifford_parent_message_id,
		 created_at, updated_at, gifford_thread_id, gifford_thread_ref,
		 thread_sequence, recipient_manifest_root, recipient_count, envelope_digest,
		 projection_digest, mapping_receipt_id, mapping_receipt_digest,
		 hub_store, hub_parent_store)
		VALUES ('hub-a', ?, ?, 'sender', 'recipient', ?, 'context-a', ?, ?, ?, ?,
		 'gifford-thread', 'gifford-thread-ref', ?, ?, 1, ?, ?, ?, ?, 'messages', ?)`,
		hubMessageID, messageID, stamp, parentHubID, parentGiffordID, stamp, stamp,
		threadSequence, manifestDigest, bridgeSourceDigest('e'), bridgeSourceDigest('p'),
		mappingReceiptID, bridgeSourceDigest('r'), parentHubStore)
	require.NoError(t, err)
}

func insertBridgeV112Projection(
	t *testing.T,
	db *sql.DB,
	messageID string,
	hubStore any,
	hubMessageID any,
	hubContextID any,
	threadSequence int64,
	parentHubID any,
	parentHubStore any,
	parentGiffordID any,
	messageOrigin string,
	signatureAuthority string,
	signingKeyID string,
	manifestDigest string,
	stamp string,
) {
	t.Helper()
	_, err := db.ExecContext(t.Context(), `INSERT INTO bridge_v112_message_projection
		(gifford_message_id, hub_node_id, hub_store, hub_message_id, message_origin, message_class,
		 signature_authority, signing_key_id, hub_context_id,
		 hub_parent_store, hub_parent_message_id, gifford_parent_message_id,
		 gifford_thread_id, gifford_thread_ref, thread_sequence, envelope_json,
		 envelope_digest, projection_digest, recipient_manifest_root, recipient_count,
		 response_mode, sin_acuse, response_basis, projection_complete,
		 created_at, completed_at, updated_at)
		VALUES (?, 'hub-a', ?, ?, ?, 'USER', ?, ?, ?, ?, ?, ?,
		 'gifford-thread', 'gifford-thread-ref', ?, '{}', ?, ?, ?, 1,
		 'NONE', 0, 'NONE', 1, ?, ?, ?)`,
		messageID, hubStore, hubMessageID, messageOrigin, signatureAuthority, signingKeyID,
		hubContextID, parentHubStore, parentHubID, parentGiffordID, threadSequence,
		bridgeSourceDigest('e'), bridgeSourceDigest('p'), manifestDigest,
		stamp, stamp, stamp)
	require.NoError(t, err)
}

func insertBridgeV112Mapping(
	t *testing.T,
	db *sql.DB,
	messageID string,
	hubMessageID int64,
	threadSequence int64,
	parentHubID any,
	parentGiffordID any,
	manifestDigest string,
	mappingReceiptID string,
	stamp string,
) {
	t.Helper()
	insertBridgeLegacyMapping(t, db, messageID, hubMessageID, threadSequence, parentHubID,
		parentGiffordID, manifestDigest, mappingReceiptID, stamp)
	var parentHubStore any
	if parentHubID != nil {
		parentHubStore = "messages"
	}
	insertBridgeV112Projection(t, db, messageID, "messages", hubMessageID, "context-a", threadSequence,
		parentHubID, parentHubStore, parentGiffordID, "LEGACY_BACKFILL", "AGENT_ENDPOINT",
		"ed25519:sender:v1", manifestDigest, stamp)
}

// TestBridgeSourcePersistenceSurvivesRestart pins every durable family and,
// in particular, proves retained terminal history needs no delivery row.
func TestBridgeSourcePersistenceSurvivesRestart(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	path := testStorePath(t)
	store, err := Open(ctx, path)
	require.NoError(t, err)
	db := store.DB()
	stamp := FormatTime(testEventTime)
	later := FormatTime(testEventTime.Add(time.Second))
	laterTwo := FormatTime(testEventTime.Add(2 * time.Second))
	laterThree := FormatTime(testEventTime.Add(3 * time.Second))
	modeLater := "2026-11-04T00:00:00.000000000Z"

	var initialFence string
	var initialModeVersion int64
	require.NoError(t, db.QueryRowContext(ctx, `SELECT source_fence, mode_version
		FROM bridge_source_mode WHERE singleton = 1`).Scan(&initialFence, &initialModeVersion))
	require.True(t, strings.HasPrefix(initialFence, "fence-"))
	require.Equal(t, int64(1), initialModeVersion)

	cutID := bridgeSourceID("cut-", 'a')
	_, err = db.ExecContext(ctx, `INSERT INTO bridge_source_cuts
		(cut_id, expected_mode, target_mode, expected_mode_version, hub_scope_id,
		 coordinator_node_id, participant_pairs_json, participants_digest, phase,
		 initial_watermarks_json, coordinator_receipt_id, coordinator_receipt_digest,
		 barrier_fence, begin_request_digest, created_at, updated_at)
		VALUES (?, 'CAPYCOM_SOURCE', 'GIFFORD_SOURCE', 1, 'scope-a', 'node-a',
		 '[{"gifford_node_id":"node-a","hub_node_id":"hub-a"}]', ?,
		 'PREPARING', '{}', ?, ?, 'barrier-a', ?, ?, ?)`,
		cutID, bridgeSourceDigest('a'), bridgeSourceID("rcp-", 'a'),
		bridgeSourceDigest('b'), bridgeSourceDigest('c'), stamp, stamp)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `UPDATE bridge_source_mode
		SET last_cut_id = ?, updated_at = ? WHERE singleton = 1`, cutID, modeLater)
	require.NoError(t, err)

	_, err = db.ExecContext(ctx, `INSERT INTO bridge_context_mappings
		(source_node_id, source_context_id, gifford_thread_id, gifford_thread_ref, created_at)
		VALUES ('hub-a', 'context-a', 'gifford-thread', 'gifford-thread-ref', ?)`, stamp)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `INSERT INTO bridge_thread_mappings
		(hub_node_id, hub_context_id, gifford_thread_id, gifford_thread_ref, created_at)
		VALUES ('hub-a', 'context-a', 'gifford-thread', 'gifford-thread-ref', ?)`, stamp)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `INSERT INTO agents
		(agent_id, node_id, harness, created_at, updated_at)
		VALUES ('sender', 'node-a', 'test', ?, ?)`, stamp, stamp)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `INSERT INTO bridge_system_principal_mappings
		(hub_node_id, hub_principal_id, gifford_agent_id, roster_digest, created_at)
		VALUES ('hub-a', 'hub-system', 'sender', ?, ?)`, bridgeSourceDigest('i'), stamp)
	require.NoError(t, err)

	insertBridgeSourceMessage(t, db, "message-history")
	insertBridgeSourceMessage(t, db, "message-live")
	insertBridgeV112Mapping(t, db, "message-history", 42, 1, nil, nil,
		bridgeSourceDigest('h'), bridgeSourceID("rcp-", 'h'), stamp)
	insertBridgeV112Projection(t, db, "message-live", nil, nil, "context-a", 2,
		int64(42), "messages", "message-history",
		"GIFFORD_FORWARD", "NODE_FORWARD", "ed25519:sender:v900000000",
		bridgeSourceDigest('l'), stamp)

	for _, row := range []struct {
		messageID string
		ordinal   int64
	}{
		{messageID: "message-history", ordinal: 0},
		{messageID: "message-live", ordinal: 0},
	} {
		_, err = db.ExecContext(ctx, `INSERT INTO message_recipients
			(message_id, agent_id, ordinal, delivery_mode, requires_ack, created_at)
			VALUES (?, 'recipient', ?, 'WHEN_SAFE', 1, ?)`, row.messageID, row.ordinal, stamp)
		require.NoError(t, err)
	}

	_, err = db.ExecContext(ctx, `INSERT INTO bridge_recipient_projection
		(gifford_message_id, hub_node_id, ordinal, recipient_agent_id,
		 delivery_mode, requires_ack, generation, merkle_proof_json, state, read_at,
		 ack_status, acked_at, state_version, delivery_materialized, created_at, updated_at)
		VALUES ('message-history', 'hub-a', 0, 'recipient', 'WHEN_SAFE', 1, 1,
		 '[]', 'ACKNOWLEDGED', ?, 'ACKNOWLEDGED', ?, 2, 0, ?, ?)`,
		stamp, stamp, stamp, stamp)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `INSERT INTO bridge_recipient_projection
		(gifford_message_id, hub_node_id, ordinal, recipient_agent_id,
		 delivery_mode, requires_ack, generation, merkle_proof_json, state,
		 state_version, delivery_materialized, created_at, updated_at)
		VALUES ('message-live', 'hub-a', 0, 'recipient', 'WHEN_SAFE', 1, 1,
		 '[]', 'PENDING', 1, 1, ?, ?)`, stamp, stamp)
	require.NoError(t, err)

	_, err = db.ExecContext(ctx, `INSERT INTO delivery_queue
		(message_id, agent_id, delivery_generation, from_agent, thread_ref, state,
		 priority, created_at, expires_at, updated_at, hop_count, route_digest)
		VALUES ('message-live', 'recipient', 1, 'sender', 'gifford-thread-ref', 'QUEUED',
		 2, ?, ?, ?, 0, 'sha256:route')`, stamp, later, stamp)
	require.NoError(t, err)

	streamID := bridgeSourceID("stm-", 'a')
	_, err = db.ExecContext(ctx, `INSERT INTO bridge_ingress_versions
		(hub_node_id, protocol_state, stream_id, start_seq, snapshot_at,
		 snapshot_watermarks_json, bootstrap_expected_count, bootstrap_manifest_digest,
		 inventory_criteria_json, backfill_applied_count, first_event_id, first_receipt_id,
		 first_receipt_digest, last_source_event_seq, legacy_retired_at, created_at, updated_at)
		VALUES ('hub-a', 'V1_12_BOOTSTRAP', ?, 1, ?, '{}', 1, ?, '{}', 1, ?, ?, ?, 2, ?, ?, ?)`,
		streamID, stamp, bridgeSourceDigest('m'), bridgeSourceID("evt-", 'a'),
		bridgeSourceID("rcp-", 's'), bridgeSourceDigest('s'), stamp, stamp, stamp)
	require.NoError(t, err)

	for _, event := range []struct {
		sequence int64
		idFill   byte
		typeName string
		receipt  byte
	}{
		{sequence: 1, idFill: 'a', typeName: "MIRROR_STREAM_START", receipt: 's'},
		{sequence: 2, idFill: 'b', typeName: "MESSAGE_COMMITTED", receipt: 't'},
	} {
		_, err = db.ExecContext(ctx, `INSERT INTO bridge_ingress_ledger
			(hub_node_id, stream_id, source_event_seq, event_id, event_type, payload_json,
			 payload_digest, projection_digest, watermarks_after_json,
			 application_receipt_id, application_receipt_digest, disposition, applied_at)
			VALUES ('hub-a', ?, ?, ?, ?, '{}', ?, ?, '{}', ?, ?, 'CREATED', ?)`,
			streamID, event.sequence, bridgeSourceID("evt-", event.idFill), event.typeName,
			bridgeSourceDigest(event.idFill), bridgeSourceDigest('p'),
			bridgeSourceID("rcp-", event.receipt), bridgeSourceDigest(event.receipt), stamp)
		require.NoError(t, err)
	}

	_, err = db.ExecContext(ctx, `INSERT INTO bridge_hub_reference_quarantine
		(hub_node_id, stream_id, source_event_seq, event_id, reference_role,
		 hub_message_id, resolution_state, reference_digest, created_at)
		VALUES ('hub-a', ?, 3, ?, 'MESSAGE', 99, 'AMBIGUOUS', ?, ?)`,
		streamID, bridgeSourceID("evt-", 'q'), bridgeSourceDigest('q'), stamp)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `UPDATE bridge_ingress_versions
		SET protocol_state = 'V1_12_READY', updated_at = ? WHERE hub_node_id = 'hub-a'`, later)
	require.ErrorContains(t, err, "ambiguous hub reference")

	_, err = db.ExecContext(ctx, `INSERT INTO bridge_watermarks
		(gifford_node_id, hub_node_id, direction,
		 admissions_produced, admissions_applied, admissions_acked,
		 reads_produced, reads_applied, reads_acked, responses_produced, responses_applied,
		 responses_acked, recipient_states_produced, recipient_states_applied,
		 recipient_states_acked, updated_at)
		VALUES ('node-a', 'hub-a', 'CAPYCOM_TO_GIFFORD',
		 1, 1, 1, 1, 1, 1, 0, 0, 0, 1, 1, 1, ?)`, stamp)
	require.NoError(t, err)

	_, err = db.ExecContext(ctx, `INSERT INTO bridge_forward_requests
		(hub_node_id, authenticated_agent_id, request_id, auth_context_id,
		 source_fence, expected_mode_version, hub_scope_id, scope_version,
		 origin_gifford_node_id, destination_gifford_node_id, admission_digest,
		 envelope_json, envelope_digest, signature_authority, signing_key_id,
		 admission_result_json, admission_receipt_id, admission_receipt_digest,
		 gifford_message_id, hub_store, hub_message_id, state, created_at, updated_at)
		VALUES ('hub-a', 'sender', ?, 'auth-a', ?, 1, 'scope-a', 1,
		 'node-a', 'node-a', ?, '{}', ?, 'NODE_FORWARD', 'ed25519:sender:v900000000',
		 '{}', ?, ?, 'message-live', NULL, NULL, 'ADMITTED', ?, ?)`,
		bridgeSourceID("req-", 'a'), initialFence, bridgeSourceDigest('f'), bridgeSourceDigest('e'),
		bridgeSourceID("rcp-", 'f'), bridgeSourceDigest('g'), stamp, stamp)
	require.NoError(t, err)

	_, err = db.ExecContext(ctx, `INSERT INTO bridge_outbox_message_bases
		(gifford_message_id, hub_node_id, hub_store, hub_message_id,
		 hub_parent_store, hub_parent_message_id, gifford_parent_message_id,
		 hub_context_id, gifford_thread_id, gifford_thread_ref,
		 thread_sequence, sender_agent_id, message_class, envelope_json, envelope_digest,
		 projection_digest, signature_authority, signing_key_id,
		 response_mode, sin_acuse, response_basis, recipient_manifest_root,
		 recipient_count, canonical_base_json, base_digest, created_at, updated_at)
		VALUES ('message-live', 'hub-a', NULL, NULL, 'messages', 42, 'message-history', 'context-a',
		 'gifford-thread', 'gifford-thread-ref', 2, 'sender', 'USER', '{}', ?, ?,
		 'NODE_FORWARD', 'ed25519:sender:v900000000', 'NONE', 0, 'NONE', ?, 1, '{}', ?, ?, ?)`,
		bridgeSourceDigest('e'), bridgeSourceDigest('p'), bridgeSourceDigest('l'),
		bridgeSourceDigest('b'), stamp, stamp)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `INSERT INTO bridge_outbox_partitions
		(hub_node_id, recipient_agent_id, next_sequence, acked_through,
		 produced_watermarks_json, acked_watermarks_json, created_at, updated_at)
		VALUES ('hub-a', 'recipient', 2, 0, '{}', '{}', ?, ?)`, stamp, stamp)
	require.NoError(t, err)

	outboxEventID := bridgeSourceID("evt-", 'o')
	_, err = db.ExecContext(ctx, `INSERT INTO bridge_outbox
		(event_id, logical_event_id, event_type, gifford_message_id, hub_node_id,
		 hub_store, hub_message_id, recipient_agent_id, recipient_ordinal, recipient_generation,
		 partition_seq, recipient_projection_json, merkle_proof_json,
		 canonical_payload_json, payload_digest, projection_digest,
		 watermarks_after_json, state, attempt_count, created_at, updated_at)
		VALUES (?, ?, 'MESSAGE_COMMITTED', 'message-live', 'hub-a', NULL, NULL, 'recipient',
		 0, 1, 1, '{}', '[]', '{}', ?, ?, '{}', 'READY', 0, ?, ?)`,
		outboxEventID, bridgeSourceID("evt-", 'l'), bridgeSourceDigest('o'),
		bridgeSourceDigest('p'), stamp, stamp)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `UPDATE bridge_recipient_projection
		SET message_committed_event_id = ?, updated_at = ?
		WHERE gifford_message_id = 'message-live' AND ordinal = 0 AND generation = 1`,
		outboxEventID, later)
	require.NoError(t, err)

	// A crash before the hub ACK must retain the exact signed envelope while
	// every hub-assigned id remains absent.
	require.NoError(t, store.Close())
	store, err = Open(ctx, path)
	require.NoError(t, err)
	db = store.DB()

	var projectionHubID, forwardHubID, baseHubID, outboxHubID sql.NullInt64
	var projectionStore, forwardStore, baseStore, outboxStore sql.NullString
	var projectionContext, baseContext, projectionParentStore, baseParentStore sql.NullString
	var projectionParentID, baseParentID sql.NullInt64
	require.NoError(t, db.QueryRowContext(ctx, `SELECT hub_store, hub_message_id, hub_context_id,
		hub_parent_store, hub_parent_message_id
		FROM bridge_v112_message_projection WHERE gifford_message_id = 'message-live'`).
		Scan(&projectionStore, &projectionHubID, &projectionContext,
			&projectionParentStore, &projectionParentID))
	require.NoError(t, db.QueryRowContext(ctx, `SELECT hub_store, hub_message_id
		FROM bridge_forward_requests WHERE gifford_message_id = 'message-live'`).
		Scan(&forwardStore, &forwardHubID))
	require.NoError(t, db.QueryRowContext(ctx, `SELECT hub_store, hub_message_id, hub_context_id,
		hub_parent_store, hub_parent_message_id
		FROM bridge_outbox_message_bases WHERE gifford_message_id = 'message-live'`).
		Scan(&baseStore, &baseHubID, &baseContext, &baseParentStore, &baseParentID))
	require.NoError(t, db.QueryRowContext(ctx, `SELECT hub_store, hub_message_id FROM bridge_outbox
		WHERE event_id = ?`, outboxEventID).Scan(&outboxStore, &outboxHubID))
	require.False(t, projectionStore.Valid)
	require.False(t, projectionHubID.Valid)
	require.Equal(t, "context-a", projectionContext.String)
	require.Equal(t, "messages", projectionParentStore.String)
	require.Equal(t, int64(42), projectionParentID.Int64)
	require.False(t, forwardStore.Valid)
	require.False(t, forwardHubID.Valid)
	require.False(t, baseStore.Valid)
	require.False(t, baseHubID.Valid)
	require.Equal(t, "context-a", baseContext.String)
	require.Equal(t, "messages", baseParentStore.String)
	require.Equal(t, int64(42), baseParentID.Int64)
	require.False(t, outboxStore.Valid)
	require.False(t, outboxHubID.Valid)

	// The closed NACK table rejects an otherwise well-shaped class/code pair.
	_, err = db.ExecContext(ctx, `INSERT INTO bridge_outbox_receipts
		(receipt_id, event_id, receipt_type, attempt_id, nack_code, request_digest,
		 disposition, previous_state, resulting_state, watermarks_after_json, created_at)
		VALUES (?, ?, 'NACK_TERMINAL', 'attempt-invalid', 'NOT_FOUND', ?, 'CREATED',
		 'READY', 'BLOCKED', '{}', ?)`,
		bridgeSourceID("rcp-", 'x'), outboxEventID, bridgeSourceDigest('x'), later)
	require.Error(t, err)

	_, err = db.ExecContext(ctx, `INSERT INTO bridge_outbox_receipts
		(receipt_id, event_id, receipt_type, attempt_id, nack_code, request_digest,
		 disposition, previous_state, resulting_state, watermarks_after_json, created_at)
		VALUES (?, ?, 'NACK_RETRYABLE', 'attempt-1', 'NOT_FOUND', ?, 'CREATED',
		 'READY', 'RETRY_WAIT', '{}', ?)`,
		bridgeSourceID("rcp-", 'n'), outboxEventID, bridgeSourceDigest('n'), later)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `UPDATE bridge_outbox
		SET state = 'RETRY_WAIT', attempt_count = 1, next_attempt_at = ?,
		 last_error_class = 'RETRYABLE', last_error_code = 'NOT_FOUND',
		 last_diagnostic_digest = ?, updated_at = ? WHERE event_id = ?`,
		laterTwo, bridgeSourceDigest('n'), later, outboxEventID)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `UPDATE bridge_outbox
		SET state = 'READY', next_attempt_at = NULL, last_error_class = NULL,
		 last_error_code = NULL, last_diagnostic_digest = NULL, updated_at = ?
		WHERE event_id = ?`, laterTwo, outboxEventID)
	require.NoError(t, err)

	_, err = db.ExecContext(ctx, `INSERT INTO bridge_outbox_receipts
		(receipt_id, event_id, receipt_type, hub_apply_receipt_id,
		 hub_apply_receipt_json, hub_apply_receipt_digest,
		 assigned_hub_store, assigned_hub_message_id, assigned_hub_context_id,
		 request_digest, disposition,
		 previous_state, resulting_state, acked_through, watermarks_after_json, created_at)
		VALUES (?, ?, 'ACK', ?, '{}', ?, 'cv2_outbound', 43, 'context-a', ?, 'CREATED',
		 'READY', 'ACKED', 1, '{}', ?)`,
		bridgeSourceID("rcp-", 'o'), outboxEventID, bridgeSourceID("rcp-", 'u'),
		bridgeSourceDigest('u'), bridgeSourceDigest('v'), laterThree)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `UPDATE bridge_outbox
		SET state = 'ACKED', updated_at = ? WHERE event_id = ?`, laterThree, outboxEventID)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `UPDATE bridge_outbox_partitions
		SET acked_through = 1, updated_at = ?
		WHERE hub_node_id = 'hub-a' AND recipient_agent_id = 'recipient'`, laterThree)
	require.NoError(t, err)

	_, err = db.ExecContext(ctx, `UPDATE bridge_v112_message_projection
		SET hub_store = 'cv2_outbound', hub_message_id = 43,
		 updated_at = ? WHERE gifford_message_id = 'message-live'`, laterThree)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `UPDATE bridge_forward_requests
		SET hub_store = 'cv2_outbound', hub_message_id = 43, updated_at = ?
		WHERE gifford_message_id = 'message-live'`, laterThree)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `UPDATE bridge_outbox_message_bases
		SET hub_store = 'cv2_outbound', hub_message_id = 43,
		 updated_at = ? WHERE gifford_message_id = 'message-live'`, laterThree)
	require.NoError(t, err)

	require.NoError(t, store.Close())
	store, err = Open(ctx, path)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
	db = store.DB()

	var reopenedFence, lastCutID, phase, ingressState, parentID string
	var assignedStore, assignedContext, projectedStore, projectedContext string
	var forwardAssignedStore, baseAssignedStore, baseAssignedContext string
	var reopenedModeVersion, historyMaterialized, historyQueue, historyOutbox int64
	var ambiguousLegacyMappings, ambiguousV112Mappings int64
	var assignedHubID, projectedHubID, forwardAssignedHubID, baseAssignedHubID int64
	var immutableEventHubID sql.NullInt64
	var immutableEventStore sql.NullString
	require.NoError(t, db.QueryRowContext(ctx, `SELECT source_fence, mode_version
		FROM bridge_source_mode WHERE singleton = 1`).Scan(&reopenedFence, &reopenedModeVersion))
	require.Equal(t, initialFence, reopenedFence)
	require.Equal(t, initialModeVersion, reopenedModeVersion)
	require.NoError(t, db.QueryRowContext(ctx, `SELECT last_cut_id
		FROM bridge_source_mode WHERE singleton = 1`).Scan(&lastCutID))
	require.Equal(t, cutID, lastCutID)
	require.NoError(t, db.QueryRowContext(ctx,
		"SELECT phase FROM bridge_source_cuts WHERE cut_id = ?", cutID).Scan(&phase))
	require.Equal(t, "PREPARING", phase)
	require.NoError(t, db.QueryRowContext(ctx, `SELECT protocol_state
		FROM bridge_ingress_versions WHERE hub_node_id = 'hub-a'`).Scan(&ingressState))
	require.Equal(t, "V1_12_BOOTSTRAP", ingressState)
	require.NoError(t, db.QueryRowContext(ctx, `SELECT gifford_parent_message_id
		FROM bridge_v112_message_projection WHERE gifford_message_id = 'message-live'`).Scan(&parentID))
	require.Equal(t, "message-history", parentID)
	require.NoError(t, db.QueryRowContext(ctx, `SELECT assigned_hub_store, assigned_hub_message_id,
		assigned_hub_context_id FROM bridge_outbox_receipts
		WHERE event_id = ? AND receipt_type = 'ACK'`, outboxEventID).
		Scan(&assignedStore, &assignedHubID, &assignedContext))
	require.Equal(t, "cv2_outbound", assignedStore)
	require.Equal(t, int64(43), assignedHubID)
	require.Equal(t, "context-a", assignedContext)
	require.NoError(t, db.QueryRowContext(ctx, `SELECT hub_store, hub_message_id, hub_context_id
		FROM bridge_v112_message_projection WHERE gifford_message_id = 'message-live'`).
		Scan(&projectedStore, &projectedHubID, &projectedContext))
	require.Equal(t, assignedStore, projectedStore)
	require.Equal(t, assignedHubID, projectedHubID)
	require.Equal(t, assignedContext, projectedContext)
	require.NoError(t, db.QueryRowContext(ctx, `SELECT hub_store, hub_message_id
		FROM bridge_forward_requests WHERE gifford_message_id = 'message-live'`).
		Scan(&forwardAssignedStore, &forwardAssignedHubID))
	require.Equal(t, assignedStore, forwardAssignedStore)
	require.Equal(t, assignedHubID, forwardAssignedHubID)
	require.NoError(t, db.QueryRowContext(ctx, `SELECT hub_store, hub_message_id, hub_context_id
		FROM bridge_outbox_message_bases WHERE gifford_message_id = 'message-live'`).
		Scan(&baseAssignedStore, &baseAssignedHubID, &baseAssignedContext))
	require.Equal(t, assignedStore, baseAssignedStore)
	require.Equal(t, assignedHubID, baseAssignedHubID)
	require.Equal(t, assignedContext, baseAssignedContext)
	require.NoError(t, db.QueryRowContext(ctx, `SELECT hub_store, hub_message_id FROM bridge_outbox
		WHERE event_id = ?`, outboxEventID).Scan(&immutableEventStore, &immutableEventHubID))
	require.False(t, immutableEventStore.Valid)
	require.False(t, immutableEventHubID.Valid)
	require.NoError(t, db.QueryRowContext(ctx, `SELECT delivery_materialized
		FROM bridge_recipient_projection WHERE gifford_message_id = 'message-history'`).
		Scan(&historyMaterialized))
	require.Zero(t, historyMaterialized)
	require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM delivery_queue
		WHERE message_id = 'message-history'`).Scan(&historyQueue))
	require.Zero(t, historyQueue)
	require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM bridge_outbox
		WHERE gifford_message_id = 'message-history'`).Scan(&historyOutbox))
	require.Zero(t, historyOutbox)
	require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM bridge_message_mappings
		WHERE source_node_id = 'hub-a' AND source_message_id = 99`).Scan(&ambiguousLegacyMappings))
	require.Zero(t, ambiguousLegacyMappings)
	require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM bridge_v112_message_projection
		WHERE hub_node_id = 'hub-a' AND hub_message_id = 99`).Scan(&ambiguousV112Mappings))
	require.Zero(t, ambiguousV112Mappings)

	for table, want := range map[string]int64{
		tableBridgeForwardRequests: 1,
		tableBridgeIngressLedger:   2,
		tableBridgeIngressVersions: 1,
		tableBridgeQuarantine:      1,
		tableBridgeMessages:        1,
		tableBridgeOutbox:          1,
		tableBridgeOutboxBases:     1,
		tableBridgeOutboxParts:     1,
		tableBridgeOutboxReceipts:  2,
		tableBridgeRecipients:      2,
		tableBridgeSourceCuts:      1,
		tableBridgeSourceMode:      1,
		tableBridgePrincipalMap:    1,
		tableBridgeThreads:         1,
		tableBridgeV112Messages:    2,
		tableBridgeWatermarks:      1,
	} {
		var count int64
		require.NoError(t, db.QueryRowContext(ctx, "SELECT count(*) FROM "+table).Scan(&count))
		require.Equal(t, want, count, table)
	}

	_, err = db.ExecContext(ctx, `UPDATE bridge_message_mappings
		SET recipient_manifest_root = ? WHERE gifford_message_id = 'message-history'`,
		bridgeSourceDigest('x'))
	require.ErrorContains(t, err, "written once")
	_, err = db.ExecContext(ctx, `UPDATE bridge_recipient_projection SET read_at = ?
		WHERE gifford_message_id = 'message-history'`, later)
	require.ErrorContains(t, err, "written once")
	_, err = db.ExecContext(ctx, `UPDATE bridge_ingress_ledger SET payload_digest = ?
		WHERE hub_node_id = 'hub-a' AND source_event_seq = 2`, bridgeSourceDigest('x'))
	require.ErrorContains(t, err, "immutable")
	_, err = db.ExecContext(ctx, `UPDATE bridge_outbox SET payload_digest = ?
		WHERE event_id = ?`, bridgeSourceDigest('x'), outboxEventID)
	require.ErrorContains(t, err, "immutable")
	_, err = db.ExecContext(ctx, `UPDATE bridge_source_cuts SET participants_digest = ?
		WHERE cut_id = ?`, bridgeSourceDigest('x'), cutID)
	require.ErrorContains(t, err, "identity is immutable")
	_, err = db.ExecContext(ctx, `UPDATE bridge_v112_message_projection SET hub_message_id = 44
		WHERE gifford_message_id = 'message-live'`)
	require.ErrorContains(t, err, "immutable")
	_, err = db.ExecContext(ctx, `DELETE FROM bridge_hub_reference_quarantine
		WHERE hub_node_id = 'hub-a' AND hub_message_id = 99`)
	require.ErrorContains(t, err, "durable")
	_, err = db.ExecContext(ctx, `UPDATE bridge_system_principal_mappings
		SET gifford_agent_id = 'recipient'
		WHERE hub_node_id = 'hub-a' AND hub_principal_id = 'hub-system'`)
	require.ErrorContains(t, err, "immutable")
	_, err = db.ExecContext(ctx, `UPDATE bridge_watermarks SET reads_acked = 0
		WHERE gifford_node_id = 'node-a' AND hub_node_id = 'hub-a'
		AND direction = 'CAPYCOM_TO_GIFFORD'`)
	require.ErrorContains(t, err, "regressed")
	_, err = db.ExecContext(ctx, `UPDATE bridge_ingress_versions SET protocol_state = 'LEGACY'
		WHERE hub_node_id = 'hub-a'`)
	require.ErrorContains(t, err, "regressed")
	_, err = db.ExecContext(ctx, `DELETE FROM bridge_outbox_receipts WHERE event_id = ?`, outboxEventID)
	require.ErrorContains(t, err, "durable")
}

func TestBridgeHubMessageIdentityIncludesStore(t *testing.T) {
	t.Parallel()

	store := testStore(t)
	db := store.DB()
	stamp := FormatTime(testEventTime)
	_, err := db.ExecContext(t.Context(), `INSERT INTO bridge_thread_mappings
		(hub_node_id, hub_context_id, gifford_thread_id, gifford_thread_ref, created_at)
		VALUES ('hub-a', 'context-a', 'gifford-thread', 'gifford-thread-ref', ?)`, stamp)
	require.NoError(t, err)
	insertBridgeSourceMessage(t, db, "message-store-messages")
	insertBridgeSourceMessage(t, db, "message-store-outbound")
	insertBridgeV112Projection(t, db, "message-store-messages", "messages", int64(55),
		"context-a", 1, nil, nil, nil, "LEGACY_BACKFILL", "AGENT_ENDPOINT",
		"ed25519:sender:v1", bridgeSourceDigest('m'), stamp)
	insertBridgeV112Projection(t, db, "message-store-outbound", "cv2_outbound", int64(55),
		"context-a", 2, nil, nil, nil, "GIFFORD_FORWARD", "NODE_FORWARD",
		"ed25519:sender:v900000000", bridgeSourceDigest('o'), stamp)

	var count int64
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT count(*)
		FROM bridge_v112_message_projection
		WHERE hub_node_id = 'hub-a' AND hub_message_id = 55`).Scan(&count))
	require.Equal(t, int64(2), count)
}

func TestBridgeSourceMigrationsUpDownUp(t *testing.T) {
	t.Parallel()

	store := testStore(t)
	ctx := t.Context()
	before := schemaSnapshot(ctx, t, store.DB())
	require.NoError(t, store.MigrateDown(ctx,
		migrationStepsFrom(t, bridgeSourcePersistenceMigration)))

	for _, name := range []string{
		tableBridgeForwardRequests,
		tableBridgeIngressLedger,
		tableBridgeQuarantine,
		tableBridgeOutbox,
		tableBridgeSourceMode,
		tableBridgeV112Messages,
	} {
		var count int64
		require.NoError(t, store.DB().QueryRowContext(ctx,
			"SELECT count(*) FROM sqlite_schema WHERE name = ?", name).Scan(&count))
		require.Zero(t, count, name)
	}
	for _, column := range readTableColumns(t, store.DB(), tableBridgeMessages) {
		require.NotEqual(t, "recipient_manifest_root", column.name)
	}

	require.NoError(t, store.MigrateUp(ctx))
	require.Equal(t, before, schemaSnapshot(ctx, t, store.DB()))
	var outboxMigrationCount int64
	require.NoError(t, store.DB().QueryRowContext(ctx, `SELECT count(*) FROM schema_migrations
		WHERE name = ?`, bridgeSourceOutboxMigration).Scan(&outboxMigrationCount))
	require.Equal(t, int64(1), outboxMigrationCount)
}
