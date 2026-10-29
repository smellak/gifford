package eventstore

import (
	"database/sql"
	"io/fs"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/migrations"
)

const (
	tableAgentKeys             = "agent_keys"
	tableArtifactRecipients    = "artifact_recipients"
	tableArtifactTransfers     = "artifact_transfers"
	tableDeliveryQueue         = "delivery_queue"
	tableEvaluationRuns        = "evaluation_runs"
	tableFederationOutbox      = "federation_outbox"
	tableGlimmerCalls          = "glimmer_calls"
	tableMessageEnvelopes      = "message_envelopes"
	tableMessages              = "messages"
	tableModelAdapters         = "model_adapters"
	tableOrchestratorDecisions = "orchestrator_decisions"
	tableStateSnapshots        = "state_snapshots"
	tableTrainingRuns          = "training_runs"
	columnAdapterRevision      = "adapter_revision"
	columnAgentID              = "agent_id"
	columnArtifactID           = "artifact_id"
	columnAttempts             = "attempts"
	columnCiphertextSHA256     = "ciphertext_sha256"
	columnCiphertextSize       = "ciphertext_size"
	columnClientMessageID      = "client_msg_id"
	columnContract             = "contract"
	columnControlReason        = "control_reason"
	columnControlVersion       = "control_version"
	columnCreatedAt            = "created_at"
	columnDeliveryGeneration   = "delivery_generation"
	columnDestinationNode      = "destination_node"
	columnEnvelope             = "envelope"
	columnEncryptionProfile    = "encryption_profile"
	columnExpiresAt            = "expires_at"
	columnFromAgent            = "from_agent"
	columnInferenceProfile     = "inference_profile"
	columnHopCount             = "hop_count"
	columnLoopGeneration       = "loop_generation"
	columnLoopID               = "loop_id"
	columnMessageID            = "message_id"
	columnOriginNode           = "origin_node"
	columnParentMessageID      = "parent_message_id"
	columnPriority             = "priority"
	columnRouteDigest          = "route_digest"
	columnState                = "state"
	columnThreadRef            = "thread_ref"
	columnThreadSequence       = "thread_sequence"
	columnTurnIndex            = "turn_index"
	columnUpdatedAt            = "updated_at"
	columnDatasetVersion       = "dataset_version"
	columnControlState         = "control_state"
	columnNextAttemptAt        = "next_attempt_at"
	columnSignature            = "signature"
	columnStatus               = "status"
	columnTrainingRunID        = "training_run_id"
	columnVersion              = "version"
	columnWireControlVersion   = "wire_control_version"
	testAgentA                 = "agent-a"
	testCreatedEarlier         = "2026-10-29T00:00:01.000000000Z"
	testCreatedLater           = "2026-10-29T00:00:02.000000000Z"
)

var completeSchemaTables = []string{
	tableAgentKeys,
	"agents",
	"approval_requests",
	"artifact_chunks",
	tableArtifactRecipients,
	tableArtifactTransfers,
	"capture_segments",
	"capture_sessions",
	"channel_control_decisions",
	"channel_controls",
	"communication_loops",
	"dataset_sources",
	"dataset_versions",
	tableDeliveryQueue,
	tableEvaluationRuns,
	"events",
	tableFederationOutbox,
	tableGlimmerCalls,
	"input_guards",
	tableMessageEnvelopes,
	tableMessages,
	tableModelAdapters,
	"nodes",
	tableOrchestratorDecisions,
	"schema_migrations",
	"sessions",
	tableStateSnapshots,
	tableTrainingRuns,
}

// TestSchemaTables pins the complete schema table inventory.
func TestSchemaTables(t *testing.T) {
	t.Parallel()

	store := testStore(t)
	rows, err := store.DB().QueryContext(t.Context(), `SELECT name FROM sqlite_schema
		WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name`)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	var names []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		names = append(names, name)
	}
	require.NoError(t, rows.Err())
	require.Equal(t, completeSchemaTables, names)
}

func insertControlRows(t *testing.T, db *sql.DB, envelope []byte, wireVersion int64) {
	t.Helper()
	stamp := FormatTime(testEventTime)
	_, err := db.ExecContext(t.Context(), `INSERT INTO messages
		(message_id, client_msg_id, origin_node, from_agent, destination_node,
		 thread_ref, priority, state, created_at, expires_at, updated_at,
		 hop_count, route_digest)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"message-1", "client-1", "node-a", testAgentA, "node-b", "thr-1",
		"NORMAL", "PENDING", stamp, stamp, stamp, 0, "sha256:route")
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), `INSERT INTO message_envelopes
		(message_id, envelope, protocol, version, client_msg_id, origin_node,
		 origin_agent, destination_node, recipient_agents_json, created_at,
		 expires_at, priority, thread_ref, encryption_profile,
		 sender_signing_key_id, ciphertext_sha256, stored_at, hop_count,
		 route_digest, wire_control_version)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"message-1", envelope, "gifford-message", 1, "client-1", "node-a",
		testAgentA, "node-b", `["agent-b"]`, stamp, stamp, "NORMAL", "thr-1",
		"none", "ed25519:agent-a:v1", "sha256:ciphertext", stamp, 0,
		"sha256:route", wireVersion)
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), `INSERT INTO delivery_queue
		(message_id, agent_id, from_agent, thread_ref, state, priority,
		 created_at, expires_at, updated_at, hop_count, route_digest)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"message-1", "agent-b", testAgentA, "thr-1", "QUEUED", 2,
		stamp, stamp, stamp, 0, "sha256:route")
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), `INSERT INTO artifact_transfers
		(artifact_id, client_msg_id, origin_node, sender_agent, destination_node,
		 state, encryption_profile, sender_key_id, signature, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"artifact-1", "client-artifact-1", "node-a", testAgentA, "node-b",
		"CREATED", "none", "ed25519:agent-a:v1", []byte("test-signature"), stamp, stamp)
	require.NoError(t, err)
}

func requireControlDefaults(t *testing.T, db *sql.DB, table, where string) {
	t.Helper()
	var state, reason string
	var version int64
	err := db.QueryRowContext(t.Context(), "SELECT control_state, control_reason, control_version FROM "+table+" WHERE "+where).
		Scan(&state, &reason, &version)
	require.NoError(t, err)
	require.Equal(t, "HELD", state)
	require.Equal(t, "CONTROL_UNAVAILABLE", reason)
	require.Equal(t, int64(0), version)
}

// TestControlDefaults proves rows fail closed until internal/channel writes
// their local control evaluation.
func TestControlDefaults(t *testing.T) {
	t.Parallel()

	store := testStore(t)
	insertControlRows(t, store.DB(), []byte("signed-envelope"), 7)
	requireControlDefaults(t, store.DB(), tableMessages, "message_id = 'message-1'")
	requireControlDefaults(t, store.DB(), tableMessageEnvelopes, "message_id = 'message-1'")
	requireControlDefaults(t, store.DB(), tableDeliveryQueue, "message_id = 'message-1'")
	requireControlDefaults(t, store.DB(), tableArtifactTransfers, "artifact_id = 'artifact-1'")

	var generation, wireVersion int64
	require.NoError(t, store.DB().QueryRowContext(t.Context(),
		"SELECT delivery_generation FROM delivery_queue WHERE message_id = 'message-1'").Scan(&generation))
	require.Equal(t, int64(1), generation)
	require.NoError(t, store.DB().QueryRowContext(t.Context(),
		"SELECT wire_control_version FROM message_envelopes WHERE message_id = 'message-1'").Scan(&wireVersion))
	require.Equal(t, int64(7), wireVersion)
}

// TestEnvelopeControlVersions keeps the signed header projection separate
// from the mutable local channel evaluation.
func TestEnvelopeControlVersions(t *testing.T) {
	t.Parallel()

	store := testStore(t)
	envelope := []byte{0x00, 0xff, 'B', '\n'}
	insertControlRows(t, store.DB(), envelope, 7)
	_, err := store.DB().ExecContext(t.Context(), `UPDATE message_envelopes
		SET control_state = ?, control_reason = ?, control_version = ?
		WHERE message_id = ?`, protocol.ControlStateAllowed,
		protocol.DecisionReasonAllowed, 9, "message-1")
	require.NoError(t, err)

	var gotEnvelope []byte
	var wireVersion, localVersion int64
	var state, reason string
	require.NoError(t, store.DB().QueryRowContext(t.Context(), `SELECT envelope,
		wire_control_version, control_state, control_reason, control_version
		FROM message_envelopes WHERE message_id = 'message-1'`).Scan(
		&gotEnvelope, &wireVersion, &state, &reason, &localVersion))
	require.Equal(t, envelope, gotEnvelope)
	require.Equal(t, int64(7), wireVersion)
	require.Equal(t, string(protocol.ControlStateAllowed), state)
	require.Equal(t, string(protocol.DecisionReasonAllowed), reason)
	require.Equal(t, int64(9), localVersion)
}

var newTableColumns = map[string][]string{
	tableStateSnapshots:        {"state_id", columnAgentID, "observed_at", "session_state", "human_draft_state", "human_activity_state", "draft_generation", "input_guard_version", "agent_phase", "attention_state", "health_state", "delivery_safety", "confidence_json", "evidence_json", "analyst_version", "stale"},
	tableOrchestratorDecisions: {"decision_id", columnAgentID, columnMessageID, columnArtifactID, "state_id", "allowed_actions_json", "selected_action", "probabilities_json", "hard_guards_json", "policy_version", "decision_model", "glimmer_call_id", "channel_decision_id", columnCreatedAt, "execution_status"},
	tableGlimmerCalls:          {"call_id", columnContract, columnAgentID, columnInferenceProfile, "base_model_repo", "base_model_revision", "adapter_name", columnAdapterRevision, "adapter_sha256", columnDatasetVersion, "dataset_sha256", columnTrainingRunID, "training_code_revision", "quantization", "runtime", "runtime_version", "gpu", "request_id", "latency_ms", "redaction_policy_version", "redactions_count", "input_sha256", "output_sha256", "probabilities_json", columnStatus, columnCreatedAt},
	"dataset_versions":         {columnDatasetVersion, "ml_contract_version", "manifest_path", "dataset_sha256", "window_count", "counts_by_label_json", "counts_by_harness_json", "split_strategy", "redaction_policy_version", "readiness_status", "readiness_json", columnCreatedAt},
	"dataset_sources":          {columnDatasetVersion, "source_type", "source_ref", "harness", "harness_version", "window_count", "split"},
	tableTrainingRuns:          {columnTrainingRunID, columnDatasetVersion, "base_model_repo", "base_model_revision", "method", "hyperparameters_json", "seed", "training_code_revision", "gpu", "readiness_status", "started_at", "ended_at", columnStatus, "log_path"},
	tableModelAdapters:         {"adapter_name", columnAdapterRevision, "adapter_sha256", columnTrainingRunID, columnContract, "gate_status", "active", "activated_at", "deactivated_at"},
	tableEvaluationRuns:        {"evaluation_run_id", columnDatasetVersion, columnInferenceProfile, columnAdapterRevision, "split", "metrics_json", "gate_verdict_json", columnCreatedAt},
	tableMessages:              {columnMessageID, columnClientMessageID, columnOriginNode, columnFromAgent, columnDestinationNode, columnThreadRef, "thread_id", columnPriority, columnState, "human_notified_at", "requested_autonomy", "requested_budget_json", columnCreatedAt, columnExpiresAt, columnUpdatedAt, columnParentMessageID, columnThreadSequence, columnLoopID, columnLoopGeneration, columnTurnIndex, columnHopCount, columnRouteDigest, columnControlState, columnControlReason, columnControlVersion},
	tableMessageEnvelopes:      {columnMessageID, columnEnvelope, "protocol", columnVersion, columnClientMessageID, columnOriginNode, "origin_agent", columnDestinationNode, "recipient_agents_json", columnCreatedAt, columnExpiresAt, columnPriority, columnThreadRef, columnEncryptionProfile, "sender_signing_key_id", columnCiphertextSHA256, "stored_at", columnParentMessageID, columnThreadSequence, columnLoopID, columnLoopGeneration, columnTurnIndex, columnHopCount, columnRouteDigest, columnWireControlVersion, columnControlState, columnControlReason, columnControlVersion},
	tableDeliveryQueue:         {columnMessageID, columnAgentID, columnDeliveryGeneration, columnFromAgent, columnThreadRef, columnState, columnPriority, columnAttempts, columnNextAttemptAt, "claimed_by", "claim_until", columnCreatedAt, columnExpiresAt, columnUpdatedAt, columnParentMessageID, columnThreadSequence, columnLoopID, columnLoopGeneration, columnTurnIndex, columnHopCount, columnRouteDigest, columnControlState, columnControlReason, columnControlVersion},
	"nodes":                    {"node_id", "organization_id", "local", columnStatus, "federation_endpoint", "certificate_fingerprint", "policy_version", "last_seen_at", columnCreatedAt},
	tableAgentKeys:             {"key_id", columnAgentID, "node_id", "purpose", "algorithm", "public_key", columnStatus, "valid_from", "valid_until", "issuer", "record_signature"},
	tableFederationOutbox:      {"outbox_id", "object_type", "object_id", columnDestinationNode, columnState, columnAttempts, columnNextAttemptAt, "last_error", "idempotency_key", "lease_owner", "lease_until", columnCreatedAt, columnUpdatedAt},
	tableArtifactTransfers:     {columnArtifactID, columnClientMessageID, columnOriginNode, "sender_agent", columnDestinationNode, columnState, columnEncryptionProfile, columnCiphertextSize, "chunk_count", columnCiphertextSHA256, "encrypted_manifest", "sender_key_id", columnSignature, columnExpiresAt, columnCreatedAt, columnUpdatedAt, columnThreadRef, columnLoopID, columnLoopGeneration, columnHopCount, columnRouteDigest, columnControlState, columnControlReason, columnControlVersion},
	tableArtifactRecipients:    {columnArtifactID, "recipient_agent", "recipient_node", "key_envelope", columnState, "acknowledged_at"},
	"artifact_chunks":          {columnArtifactID, "chunk_index", columnCiphertextSize, columnCiphertextSHA256, "storage_path", "fsynced", "replicated", columnCreatedAt},
}

type tableColumn struct {
	name     string
	typeName string
	notNull  bool
}

func readTableColumns(t *testing.T, db *sql.DB, table string) []tableColumn {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), "PRAGMA table_info("+table+")")
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	var columns []tableColumn
	for rows.Next() {
		var cid, notNull, primaryKey int64
		var name, typeName string
		var defaultValue sql.NullString
		require.NoError(t, rows.Scan(&cid, &name, &typeName, &notNull, &defaultValue, &primaryKey))
		columns = append(columns, tableColumn{name: name, typeName: typeName, notNull: notNull == 1})
	}
	require.NoError(t, rows.Err())
	return columns
}

type columnShape struct {
	integers []string
	blobs    []string
	required []string
}

var mandatedColumnShapes = map[string]columnShape{
	tableMessages: {
		integers: []string{columnThreadSequence, columnLoopGeneration, columnTurnIndex, columnHopCount, columnControlVersion},
		required: []string{columnClientMessageID, columnOriginNode, columnFromAgent, columnDestinationNode, columnThreadRef, columnPriority, columnState, columnCreatedAt, columnExpiresAt, columnUpdatedAt, columnHopCount, columnRouteDigest, columnControlState, columnControlReason, columnControlVersion},
	},
	tableMessageEnvelopes: {
		integers: []string{columnVersion, columnThreadSequence, columnLoopGeneration, columnTurnIndex, columnHopCount, columnWireControlVersion, columnControlVersion},
		blobs:    []string{columnEnvelope},
		required: []string{columnEnvelope, "protocol", columnVersion, columnClientMessageID, columnOriginNode, "origin_agent", columnDestinationNode, "recipient_agents_json", columnCreatedAt, columnExpiresAt, columnPriority, columnThreadRef, columnEncryptionProfile, "sender_signing_key_id", columnCiphertextSHA256, "stored_at", columnHopCount, columnRouteDigest, columnWireControlVersion, columnControlState, columnControlReason, columnControlVersion},
	},
	tableDeliveryQueue: {
		integers: []string{columnDeliveryGeneration, columnPriority, columnAttempts, columnThreadSequence, columnLoopGeneration, columnTurnIndex, columnHopCount, columnControlVersion},
		required: []string{columnMessageID, columnAgentID, columnDeliveryGeneration, columnFromAgent, columnThreadRef, columnState, columnPriority, columnAttempts, columnCreatedAt, columnExpiresAt, columnUpdatedAt, columnHopCount, columnRouteDigest, columnControlState, columnControlReason, columnControlVersion},
	},
	tableArtifactTransfers: {
		integers: []string{columnCiphertextSize, "chunk_count", columnLoopGeneration, columnHopCount, columnControlVersion},
		blobs:    []string{"encrypted_manifest", columnSignature},
		required: []string{columnClientMessageID, columnOriginNode, "sender_agent", columnDestinationNode, columnState, columnEncryptionProfile, "sender_key_id", columnSignature, columnCreatedAt, columnUpdatedAt, columnHopCount, columnControlState, columnControlReason, columnControlVersion},
	},
}

// TestSchemaColumns pins every new table's column order and the declared
// shape of the derived messaging and artifact-control columns.
func TestSchemaColumns(t *testing.T) {
	t.Parallel()

	store := testStore(t)
	for table, wantNames := range newTableColumns {
		t.Run(table, func(t *testing.T) {
			t.Parallel()
			columns := readTableColumns(t, store.DB(), table)
			gotNames := make([]string, len(columns))
			for i, column := range columns {
				gotNames[i] = column.name
			}
			require.Equal(t, wantNames, gotNames)
			shape, checked := mandatedColumnShapes[table]
			if !checked {
				return
			}
			for _, column := range columns {
				wantType := "TEXT"
				if slices.Contains(shape.integers, column.name) {
					wantType = "INTEGER"
				} else if slices.Contains(shape.blobs, column.name) {
					wantType = "BLOB"
				}
				require.Equal(t, wantType, column.typeName, column.name)
				require.Equal(t, slices.Contains(shape.required, column.name), column.notNull, column.name)
			}
		})
	}
}

type schemaIndex struct {
	name    string
	table   string
	columns []string
	where   string
}

var newSchemaIndexes = []schemaIndex{
	{"glimmer_calls_contract_profile", tableGlimmerCalls, []string{columnContract, columnInferenceProfile, columnCreatedAt}, ""},
	{"state_snapshots_agent", tableStateSnapshots, []string{columnAgentID, "observed_at"}, ""},
	{"orchestrator_decisions_agent", tableOrchestratorDecisions, []string{columnAgentID, columnCreatedAt}, ""},
	{"orchestrator_decisions_message", tableOrchestratorDecisions, []string{columnMessageID}, "WHERE message_id IS NOT NULL"},
	{"training_runs_dataset", tableTrainingRuns, []string{columnDatasetVersion, "started_at"}, ""},
	{"evaluation_runs_dataset", tableEvaluationRuns, []string{columnDatasetVersion, columnInferenceProfile, columnCreatedAt}, ""},
	{"model_adapters_active", tableModelAdapters, []string{columnContract}, "WHERE active = 1"},
	{"messages_client_msg", tableMessages, []string{"origin_node", columnFromAgent, columnClientMessageID}, ""},
	{"messages_thread", tableMessages, []string{columnThreadRef, columnThreadSequence}, ""},
	{"messages_loop", tableMessages, []string{columnLoopID, columnLoopGeneration}, "WHERE loop_id IS NOT NULL"},
	{"delivery_queue_pending", tableDeliveryQueue, []string{columnAgentID, columnState, columnPriority, columnCreatedAt}, ""},
	{"delivery_queue_control", tableDeliveryQueue, []string{columnAgentID, columnControlState, columnState}, ""},
	{"federation_outbox_idempotency", tableFederationOutbox, []string{columnDestinationNode, "idempotency_key"}, ""},
	{"agent_keys_agent", tableAgentKeys, []string{columnAgentID, "purpose", "valid_from"}, ""},
	{"federation_outbox_pending", tableFederationOutbox, []string{columnState, columnNextAttemptAt}, ""},
	{"artifact_transfers_control", tableArtifactTransfers, []string{columnControlState, columnState}, ""},
	{"artifact_recipients_agent", tableArtifactRecipients, []string{"recipient_agent", columnState}, ""},
}

// TestSchemaIndexes pins every new index, its table, column order and partial
// predicate.
func TestSchemaIndexes(t *testing.T) {
	t.Parallel()

	store := testStore(t)
	for _, index := range newSchemaIndexes {
		t.Run(index.name, func(t *testing.T) {
			t.Parallel()
			var table, statement string
			require.NoError(t, store.DB().QueryRowContext(t.Context(),
				"SELECT tbl_name, sql FROM sqlite_schema WHERE type = 'index' AND name = ?", index.name).
				Scan(&table, &statement))
			require.Equal(t, index.table, table)
			rows, err := store.DB().QueryContext(t.Context(), "PRAGMA index_info("+index.name+")")
			require.NoError(t, err)
			defer func() { _ = rows.Close() }()
			var columns []string
			for rows.Next() {
				var sequence, cid int64
				var name string
				require.NoError(t, rows.Scan(&sequence, &cid, &name))
				columns = append(columns, name)
			}
			require.NoError(t, rows.Err())
			require.Equal(t, index.columns, columns)
			if index.where == "" {
				require.NotContains(t, statement, " WHERE ")
			} else {
				require.Contains(t, statement, index.where)
			}
		})
	}
}

func explainQueryPlan(t *testing.T, db *sql.DB, query string, args ...any) string {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), "EXPLAIN QUERY PLAN "+query, args...)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	var details []string
	for rows.Next() {
		var id, parent, unused int64
		var detail string
		require.NoError(t, rows.Scan(&id, &parent, &unused, &detail))
		details = append(details, detail)
	}
	require.NoError(t, rows.Err())
	return strings.Join(details, "\n")
}

// TestQueueIndexes proves the canonical queue queries use both required
// indexes.
func TestQueueIndexes(t *testing.T) {
	t.Parallel()

	store := testStore(t)
	cases := []struct {
		name  string
		query string
		args  []any
	}{
		{"delivery_queue_pending", "SELECT message_id FROM delivery_queue WHERE agent_id = ? AND state = ? ORDER BY priority, created_at LIMIT 1", []any{testAgentA, "QUEUED"}},
		{"delivery_queue_control", "SELECT count(*) FROM delivery_queue WHERE agent_id = ? AND control_state = ? AND state = ?", []any{testAgentA, "HELD", "QUEUED"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			plan := explainQueryPlan(t, store.DB(), tc.query, tc.args...)
			t.Logf("%s: %s", tc.name, plan)
			require.Contains(t, plan, tc.name)
			require.NotContains(t, plan, "USE TEMP B-TREE")
		})
	}
}

// TestPriorityRank checks both validated priority conversions.
func TestPriorityRank(t *testing.T) {
	t.Parallel()

	cases := []struct {
		priority protocol.Priority
		rank     int64
	}{
		{protocol.PriorityUrgent, 0},
		{protocol.PriorityHigh, 1},
		{protocol.PriorityNormal, 2},
		{protocol.PriorityLow, 3},
	}
	for _, tc := range cases {
		rank, err := PriorityRank(tc.priority)
		require.NoError(t, err)
		require.Equal(t, tc.rank, rank)
		priority, err := PriorityFromRank(tc.rank)
		require.NoError(t, err)
		require.Equal(t, tc.priority, priority)
	}
	for _, priority := range []protocol.Priority{"", "low", "CRITICAL"} {
		_, err := PriorityRank(priority)
		require.ErrorIs(t, err, ErrInvalidArgument)
	}
	for _, rank := range []int64{-1, 4} {
		_, err := PriorityFromRank(rank)
		require.ErrorIs(t, err, ErrInvalidArgument)
	}
}

// TestPriorityRankOrder proves the pending queue orders by urgency, then age,
// without selecting another agent's row.
func TestPriorityRankOrder(t *testing.T) {
	t.Parallel()

	store := testStore(t)
	stamp := FormatTime(testEventTime)
	type queued struct {
		id       string
		agent    string
		priority protocol.Priority
		created  string
	}
	rows := []queued{
		{"low-new", testAgentA, protocol.PriorityLow, testCreatedLater},
		{"urgent-new", testAgentA, protocol.PriorityUrgent, testCreatedLater},
		{"normal-old", testAgentA, protocol.PriorityNormal, testCreatedEarlier},
		{"high-new", testAgentA, protocol.PriorityHigh, testCreatedLater},
		{"urgent-old", testAgentA, protocol.PriorityUrgent, testCreatedEarlier},
		{"low-old", testAgentA, protocol.PriorityLow, testCreatedEarlier},
		{"high-old", testAgentA, protocol.PriorityHigh, testCreatedEarlier},
		{"normal-new", testAgentA, protocol.PriorityNormal, testCreatedLater},
		{"other-agent", "agent-b", protocol.PriorityUrgent, "2026-10-28T00:00:00.000000000Z"},
	}
	for _, row := range rows {
		rank, err := PriorityRank(row.priority)
		require.NoError(t, err)
		_, err = store.DB().ExecContext(t.Context(), `INSERT INTO delivery_queue
			(message_id, agent_id, from_agent, thread_ref, state, priority,
			 created_at, expires_at, updated_at, hop_count, route_digest)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, row.id, row.agent,
			"sender", "thr-1", "QUEUED", rank, row.created, stamp, stamp, 0, "sha256:route")
		require.NoError(t, err)
	}

	wantIDs := []string{"urgent-old", "urgent-new", "high-old", "high-new", "normal-old", "normal-new", "low-old", "low-new"}
	wantPriorities := []protocol.Priority{protocol.PriorityUrgent, protocol.PriorityUrgent, protocol.PriorityHigh, protocol.PriorityHigh, protocol.PriorityNormal, protocol.PriorityNormal, protocol.PriorityLow, protocol.PriorityLow}
	for i := range wantIDs {
		var id string
		var rank int64
		require.NoError(t, store.DB().QueryRowContext(t.Context(), `SELECT message_id, priority
			FROM delivery_queue WHERE agent_id = ? AND state = ?
			ORDER BY priority, created_at LIMIT 1`, testAgentA, "QUEUED").Scan(&id, &rank))
		require.Equal(t, wantIDs[i], id)
		priority, err := PriorityFromRank(rank)
		require.NoError(t, err)
		require.Equal(t, wantPriorities[i], priority)
		_, err = store.DB().ExecContext(t.Context(), "UPDATE delivery_queue SET state = 'DELIVERED' WHERE message_id = ? AND agent_id = ?", id, testAgentA)
		require.NoError(t, err)
	}
	var remaining int
	require.NoError(t, store.DB().QueryRowContext(t.Context(),
		"SELECT count(*) FROM delivery_queue WHERE agent_id = 'agent-a' AND state = 'QUEUED'").Scan(&remaining))
	require.Zero(t, remaining)
	require.NoError(t, store.DB().QueryRowContext(t.Context(),
		"SELECT count(*) FROM delivery_queue WHERE message_id = 'other-agent' AND state = 'QUEUED'").Scan(&remaining))
	require.Equal(t, 1, remaining)
}

// TestEnvelopeStoredWhole keeps arbitrary signed envelope bytes intact and
// stored with SQLite's blob type.
func TestEnvelopeStoredWhole(t *testing.T) {
	t.Parallel()

	store := testStore(t)
	want := []byte{0x00, 0xff, 0x80, 'x', '\n'}
	insertControlRows(t, store.DB(), want, 7)
	var got []byte
	var storageType string
	require.NoError(t, store.DB().QueryRowContext(t.Context(),
		"SELECT envelope, typeof(envelope) FROM message_envelopes WHERE message_id = 'message-1'").
		Scan(&got, &storageType))
	require.Equal(t, want, got)
	require.Equal(t, "blob", storageType)
}

// TestExistingTablesUnchanged compares every object created by the two
// integrated migrations with the same objects in the complete schema.
func TestExistingTablesUnchanged(t *testing.T) {
	t.Parallel()

	const eventStoreMigration = "20261028153524_event_store.sql"
	const channelMigration = "20261028155622_channel_control_plane.sql"
	files := make(fstest.MapFS, 2)
	for _, name := range []string{eventStoreMigration, channelMigration} {
		contents, err := fs.ReadFile(migrations.FS(), name)
		require.NoError(t, err)
		files[name] = &fstest.MapFile{Data: contents}
	}
	baseline := testOpenDB(t, filepath.Join(t.TempDir(), "baseline.db"))
	require.NoError(t, migrateUp(t.Context(), baseline, files))
	store := testStore(t)

	complete := make(map[string]schemaObject)
	for _, object := range schemaSnapshot(t.Context(), t, store.DB()) {
		complete[object.Name] = object
	}
	for _, object := range schemaSnapshot(t.Context(), t, baseline) {
		require.Equal(t, object, complete[object.Name], object.Name)
	}
}
