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
	tableAcks                  = "acks"
	tableAgentKeys             = "agent_keys"
	tableArtifactRecipients    = "artifact_recipients"
	tableArtifactTransfers     = "artifact_transfers"
	tableBridgeContexts        = "bridge_context_mappings"
	tableBridgeForwardRequests = "bridge_forward_requests"
	tableBridgeIngressLedger   = "bridge_ingress_ledger"
	tableBridgeIngressVersions = "bridge_ingress_versions"
	tableBridgeQuarantine      = "bridge_hub_reference_quarantine"
	tableBridgeMessages        = "bridge_message_mappings"
	tableBridgeOutbox          = "bridge_outbox"
	tableBridgeOutboxBases     = "bridge_outbox_message_bases"
	tableBridgeOutboxParts     = "bridge_outbox_partitions"
	tableBridgeOutboxReceipts  = "bridge_outbox_receipts"
	tableBridgeRecipients      = "bridge_recipient_projection"
	tableBridgeReminders       = "bridge_reminders"
	tableBridgeTombstones      = "bridge_reminder_tombstones"
	tableBridgeSourceCuts      = "bridge_source_cuts"
	tableBridgeSourceMode      = "bridge_source_mode"
	tableBridgePrincipalMap    = "bridge_system_principal_mappings"
	tableBridgeThreads         = "bridge_thread_mappings"
	tableBridgeV112Messages    = "bridge_v112_message_projection"
	tableBridgeWatermarks      = "bridge_watermarks"
	tableChannelEpoch          = "channel_epoch"
	tableDeliveryAttempts      = "delivery_attempts"
	tableDeliveryIncidentLinks = "delivery_incident_messages"
	tableIncidentSenderNotices = "delivery_incident_sender_notices"
	tableDeliveryIncidents     = "delivery_incidents"
	tableDeliveryQueue         = "delivery_queue"
	tableResultEvidence        = "delivery_result_evidence"
	tableEnrollmentTokens      = "enrollment_tokens"
	tableEscalations           = "escalations"
	tableEvaluationRuns        = "evaluation_runs"
	tableFederationAckInbox    = "federation_ack_inbox"
	tableFederationAckReceipts = "federation_ack_receipts"
	tableArtifactAckInbox      = "federation_artifact_ack_inbox"
	tableArtifactAckOutbox     = "federation_artifact_ack_outbox"
	tableArtifactAckReceipts   = "federation_artifact_ack_receipts"
	tableArtifactFedInbox      = "federation_artifact_inbox"
	tableArtifactFedOutbox     = "federation_artifact_outbox"
	tableArtifactFedReceipts   = "federation_artifact_receipts"
	tableFederationInbox       = "federation_inbox"
	tableFederationOutbox      = "federation_outbox"
	tableFederationOutboxAcks  = "federation_outbox_acks"
	tableFederationRequests    = "federation_outbox_requests"
	tableFederationReceipts    = "federation_receipts"
	tableGlimmerCalls          = "glimmer_calls"
	tableHumanNotifications    = "human_notifications"
	tableLoopClosures          = "communication_loop_closures"
	tableMessageEnvelopes      = "message_envelopes"
	tableMessageRecipients     = "message_recipients"
	tableMessageTriage         = "message_triage"
	tableMessages              = "messages"
	tableModelOverrides        = "model_activation_overrides"
	tableModelAdapters         = "model_adapters"
	tableModelRegistry         = "model_profile_registry"
	tableModelSelections       = "model_profile_selections"
	tableNodes                 = "nodes"
	tableOrchestratorDecisions = "orchestrator_decisions"
	tableProcessBindings       = "orchestrator_process_bindings"
	tablePaneIndexPublications = "pane_index_publications"
	tablePolicyVersions        = "policy_versions"
	tableSessions              = "sessions"
	tableStateSnapshots        = "state_snapshots"
	tableTmuxInstances         = "tmux_instances"
	tableTmuxMonitorChains     = "tmux_monitor_chains"
	tableTmuxRefusals          = "tmux_instance_refusals"
	tableTrainingRuns          = "training_runs"
	tableTrainingRunEvidence   = "training_run_evidence"
	columnAdapterRevision      = "adapter_revision"
	columnAdapterSHA256        = "adapter_sha256"
	columnAdmissionResultJSON  = "admission_result_json"
	columnAckedAt              = "acked_at"
	columnAcceptedAt           = "accepted_at"
	columnAgentID              = "agent_id"
	columnArtifactID           = "artifact_id"
	columnAttemptCount         = "attempt_count"
	columnAttempts             = "attempts"
	columnBaseModelRevision    = "base_model_revision"
	columnBootID               = "boot_id"
	columnCiphertextSHA256     = "ciphertext_sha256"
	columnCiphertextSize       = "ciphertext_size"
	columnClientMessageID      = "client_msg_id"
	columnClientNamespace      = "client_msg_namespace"
	columnContract             = "contract"
	columnControlReason        = "control_reason"
	columnControlVersion       = "control_version"
	columnCreatedAt            = "created_at"
	columnDeliveryID           = "delivery_id"
	columnDeliveryGeneration   = "delivery_generation"
	columnDeliveryAttemptID    = "delivery_attempt_id"
	columnDestinationNode      = "destination_node"
	columnDeliveryMode         = "delivery_mode"
	columnEnvelope             = "envelope"
	columnEncryptionProfile    = "encryption_profile"
	columnExpiresAt            = "expires_at"
	columnExpectationID        = "expectation_id"
	columnFiledAt              = "filed_at"
	columnFromAgent            = "from_agent"
	columnToAgent              = "to_agent"
	columnSourceReadAt         = "source_read_at"
	columnGeneration           = "generation"
	columnGiffordMessageID     = "gifford_message_id"
	columnGlimmerCallID        = "glimmer_call_id"
	columnInferenceProfile     = "inference_profile"
	columnKind                 = "kind"
	columnHopCount             = "hop_count"
	columnLoopGeneration       = "loop_generation"
	columnLoopID               = "loop_id"
	columnLoopClosedSequence   = "closed_thread_sequence"
	columnLocal                = "local"
	columnMessageID            = "message_id"
	columnMinutes              = "minutes"
	columnMetadataPath         = "metadata_path"
	columnMetadataSHA256       = "metadata_sha256"
	columnOriginNode           = "origin_node"
	columnOrdinal              = "ordinal"
	columnOutboxID             = "outbox_id"
	columnParentMessageID      = "parent_message_id"
	columnPriority             = "priority"
	columnQueuedAt             = "queued_at"
	columnRecipientAgent       = "recipient_agent"
	columnReason               = "reason"
	columnReminderID           = "reminder_id"
	columnPolicyVersion        = "policy_version"
	columnRouteDigest          = "route_digest"
	columnSequence             = "sequence"
	columnSenderAgent          = "sender_agent"
	columnSessionID            = "session_id"
	columnRequiresAck          = "requires_ack"
	columnRequest              = "request"
	columnState                = "state"
	columnStartedAt            = "started_at"
	columnStateVersion         = "state_version"
	columnEndedAt              = "ended_at"
	columnEvaluationSHA256     = "evaluation_sha256"
	columnThreadRef            = "thread_ref"
	columnThreadSequence       = "thread_sequence"
	columnTurnIndex            = "turn_index"
	columnUpdatedAt            = "updated_at"
	columnDatasetVersion       = "dataset_version"
	columnControlState         = "control_state"
	columnNextAttemptAt        = "next_attempt_at"
	columnNonce                = "nonce"
	columnNormalizedReason     = "normalized_reason"
	columnObservedAt           = "observed_at"
	columnStateID              = "state_id"
	columnSourceContextID      = "source_context_id"
	columnSourceMessageID      = "source_message_id"
	columnSourceNodeID         = "source_node_id"
	columnReceipt              = "receipt"
	columnReceivedAt           = "received_at"
	columnSignature            = "signature"
	columnStatus               = "status"
	columnRequestSHA256        = "request_sha256"
	columnTrainingRunID        = "training_run_id"
	columnVersion              = "version"
	columnWireControlVersion   = "wire_control_version"
	testAgentA                 = "agent-a"
	testCreatedEarlier         = "2026-10-29T00:00:01.000000000Z"
	testCreatedLater           = "2026-10-29T00:00:02.000000000Z"
	stateProducerIndexWhere    = "WHERE producer_version IS NOT NULL"
)

const (
	columnAdapterName            = "adapter_name"
	columnAnalysisPolicyVersion  = "analysis_policy_version"
	columnBaseModelRepo          = "base_model_repo"
	columnCalibrationRevision    = "calibration_revision"
	columnCalibrationSHA256      = "calibration_sha256"
	columnCalibrationTemperature = "calibration_temperature"
	columnDatasetSHA256          = "dataset_sha256"
	columnEvaluationRunID        = "evaluation_run_id"
	columnEvaluationReportJSON   = "evaluation_report_json"
	columnIdleThreshold          = "idle_threshold"
	columnProfileRevision        = "profile_revision"
	columnProfileSHA256          = "profile_sha256"
	columnPromptVersion          = "prompt_version"
	columnTrainingCodeRevision   = "training_code_revision"
)

var completeSchemaTables = []string{
	tableAcks,
	tableAgentKeys,
	"agents",
	"approval_requests",
	"artifact_chunks",
	tableArtifactRecipients,
	tableArtifactTransfers,
	tableBridgeContexts,
	tableBridgeForwardRequests,
	tableBridgeQuarantine,
	tableBridgeIngressLedger,
	tableBridgeIngressVersions,
	tableBridgeMessages,
	tableBridgeOutbox,
	tableBridgeOutboxBases,
	tableBridgeOutboxParts,
	tableBridgeOutboxReceipts,
	tableBridgeRecipients,
	tableBridgeTombstones,
	tableBridgeReminders,
	tableBridgeSourceCuts,
	tableBridgeSourceMode,
	tableBridgePrincipalMap,
	tableBridgeThreads,
	tableBridgeV112Messages,
	tableBridgeWatermarks,
	"capture_segments",
	"capture_sessions",
	"channel_control_decisions",
	"channel_controls",
	tableChannelEpoch,
	tableLoopClosures,
	"communication_loops",
	"dataset_sources",
	"dataset_versions",
	tableDeliveryAttempts,
	tableDeliveryIncidentLinks,
	tableIncidentSenderNotices,
	tableDeliveryIncidents,
	tableDeliveryQueue,
	tableResultEvidence,
	tableEnrollmentTokens,
	tableEscalations,
	tableEvaluationRuns,
	"events",
	tableFederationAckInbox,
	tableFederationAckReceipts,
	tableArtifactAckInbox,
	tableArtifactAckOutbox,
	tableArtifactAckReceipts,
	tableArtifactFedInbox,
	tableArtifactFedOutbox,
	tableArtifactFedReceipts,
	tableFederationInbox,
	tableFederationOutbox,
	tableFederationOutboxAcks,
	tableFederationRequests,
	tableFederationReceipts,
	tableGlimmerCalls,
	tableHumanNotifications,
	"input_guards",
	tableMessageEnvelopes,
	tableMessageRecipients,
	tableMessageTriage,
	tableMessages,
	tableModelOverrides,
	tableModelAdapters,
	tableModelRegistry,
	tableModelSelections,
	tableNodes,
	tableOrchestratorDecisions,
	tableProcessBindings,
	tablePaneIndexPublications,
	tablePolicyVersions,
	"schema_migrations",
	"sessions",
	tableStateSnapshots,
	tableTmuxRefusals,
	tableTmuxInstances,
	tableTmuxMonitorChains,
	tableTrainingRunEvidence,
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
	tableSessions:              {columnSessionID, columnAgentID, columnState, "capture_mode", "deliverable", "manifest_json", "process_json", columnStartedAt, columnEndedAt, columnUpdatedAt, columnBootID},
	tableStateSnapshots:        {columnStateID, columnAgentID, columnObservedAt, "session_state", "human_draft_state", "human_activity_state", "draft_generation", "input_guard_version", "agent_phase", "attention_state", "health_state", "delivery_safety", "confidence_json", "evidence_json", "analyst_version", "stale", columnStateVersion, "last_human_key_at", "background_activity", "channel_effective_json", "active_loops_json", "pending_approvals", columnSessionID, "session_updated_at", "manifest_sha256", "observation_id", "observation_generation", "output_generation", "event_generation", "committed_at", columnExpiresAt, "producer_version", "analysis_route", columnGlimmerCallID, columnInferenceProfile, columnBaseModelRevision, columnAdapterRevision, columnAdapterSHA256, columnProfileRevision, columnProfileSHA256, columnCalibrationRevision, columnCalibrationSHA256, columnCalibrationTemperature, columnIdleThreshold, "idle_probability", columnPromptVersion, columnAnalysisPolicyVersion, "selection_generation"},
	tableOrchestratorDecisions: {"decision_id", columnAgentID, columnMessageID, columnArtifactID, columnStateID, "allowed_actions_json", "selected_action", "probabilities_json", "hard_guards_json", columnPolicyVersion, "decision_model", columnGlimmerCallID, "channel_decision_id", columnCreatedAt, "execution_status"},
	tableGlimmerCalls:          {"call_id", columnContract, columnAgentID, columnInferenceProfile, columnBaseModelRepo, columnBaseModelRevision, columnAdapterName, columnAdapterRevision, columnAdapterSHA256, columnDatasetVersion, columnDatasetSHA256, columnTrainingRunID, columnTrainingCodeRevision, "quantization", "runtime", "runtime_version", "gpu", "request_id", "latency_ms", "redaction_policy_version", "redactions_count", "input_sha256", "output_sha256", "probabilities_json", columnStatus, columnCreatedAt, columnPromptVersion, "selection_generation", columnEvaluationRunID, columnEvaluationSHA256, "projection_policy_version", "projection_policy_sha256", columnProfileRevision, columnProfileSHA256, columnCalibrationRevision, columnCalibrationSHA256, columnCalibrationTemperature, columnIdleThreshold, columnAnalysisPolicyVersion},
	"dataset_versions":         {columnDatasetVersion, "ml_contract_version", "manifest_path", columnDatasetSHA256, "window_count", "counts_by_label_json", "counts_by_harness_json", "split_strategy", "redaction_policy_version", "readiness_status", "readiness_json", columnCreatedAt, "manifest_sha256"},
	"dataset_sources":          {columnDatasetVersion, "source_type", "source_ref", "harness", "harness_version", "window_count", "split"},
	tableTrainingRuns:          {columnTrainingRunID, columnDatasetVersion, columnBaseModelRepo, columnBaseModelRevision, "method", "hyperparameters_json", "seed", columnTrainingCodeRevision, "gpu", "readiness_status", columnStartedAt, columnEndedAt, columnStatus, "log_path", columnMetadataPath, columnMetadataSHA256, "training_dataset_sha256", "training_manifest_path", "training_manifest_sha256"},
	tableTmuxInstances:         {"instance_id", columnAgentID, "machine_id", "boot_id", "owner_user", "owner_uid", "socket", "pane_id", "pane_pid", "pane_start", "pane_comm", "harness", "harness_version", "harness_pid", "harness_start", "harness_comm", "agent_uid", "agent_gid", "source", "registered_by", columnSessionID, columnState, "invalid_reason", columnCreatedAt, columnUpdatedAt, "harness_argv0", "harness_argv1", "bank", "monitor_profile"},
	tableTmuxRefusals:          {columnAgentID, "source", "code", "refused_at"},
	tableTrainingRunEvidence:   {columnTrainingRunID, columnMetadataSHA256, columnMetadataPath, columnStatus, columnEndedAt, "imported_at"},
	tableModelAdapters:         {columnAdapterName, columnAdapterRevision, columnAdapterSHA256, columnTrainingRunID, columnContract, "gate_status", "active", "activated_at", "deactivated_at", columnMetadataPath, columnMetadataSHA256},
	tableModelRegistry:         {columnAdapterName, columnAdapterRevision, columnAdapterSHA256, columnContract, columnBaseModelRepo, columnBaseModelRevision, "runtime", "quantization", "ml_contract_version", columnDatasetVersion, columnDatasetSHA256, "dataset_readiness_status", "dataset_readiness_sha256", columnTrainingRunID, columnTrainingCodeRevision, "training_readiness_status", "training_status", columnEvaluationRunID, columnEvaluationSHA256, columnEvaluationReportJSON, "gate_verdict_json", columnPromptVersion, "projection_policy_version", "projection_policy_sha256", columnProfileRevision, columnProfileSHA256, columnCalibrationRevision, columnCalibrationSHA256, columnCalibrationTemperature, columnIdleThreshold, columnAnalysisPolicyVersion, "metadata_json", "registered_at"},
	tableModelSelections:       {columnContract, columnGeneration, columnInferenceProfile, columnAdapterName, columnAdapterRevision, columnReason, "gate_status", "operational_state", "event_id", "selected_at", "selection_json"},
	tableModelOverrides:        {columnEvaluationRunID, columnContract, columnGeneration, columnAdapterName, columnAdapterRevision, columnAdapterSHA256, columnTrainingRunID, "adapter_metadata_sha256", "training_run_metadata_sha256", "evaluation_report_sha256", columnReason, "actor_type", "actor_id", columnCreatedAt, "selection_json", "adapter_metadata_json", "training_run_metadata_json", columnEvaluationReportJSON},
	tableEvaluationRuns:        {columnEvaluationRunID, columnDatasetVersion, columnInferenceProfile, columnAdapterRevision, "split", "metrics_json", "gate_verdict_json", columnCreatedAt, columnEvaluationSHA256, columnEvaluationReportJSON, "evidence_json"},
	tableLoopClosures:          {columnLoopID, columnLoopClosedSequence},
	tableMessages:              {columnMessageID, columnClientMessageID, columnOriginNode, columnFromAgent, columnDestinationNode, columnThreadRef, "thread_id", columnPriority, columnState, "human_notified_at", "requested_autonomy", "requested_budget_json", columnCreatedAt, columnExpiresAt, columnUpdatedAt, columnParentMessageID, columnThreadSequence, columnLoopID, columnLoopGeneration, columnTurnIndex, columnHopCount, columnRouteDigest, columnControlState, columnControlReason, columnControlVersion, columnAdmissionResultJSON, columnClientNamespace},
	tableBridgeContexts:        {columnSourceNodeID, columnSourceContextID, "gifford_thread_id", "gifford_thread_ref", columnCreatedAt},
	tableBridgeReminders:       {columnSourceNodeID, columnExpectationID, columnGeneration, columnSourceMessageID, columnFromAgent, columnToAgent, columnSourceReadAt, "eligible_at", columnAcceptedAt, columnExpiresAt, columnState, columnReminderID, columnGiffordMessageID, columnDeliveryAttemptID, "delivered_at", "terminal_at", "last_reconciled_at", "cancel_reason", columnCreatedAt, columnUpdatedAt},
	tableBridgeTombstones:      {columnSourceNodeID, columnExpectationID, columnGeneration, columnReason, columnReminderID, columnGiffordMessageID, "disposition", columnState, columnAcceptedAt, columnExpiresAt, columnDeliveryAttemptID, columnCreatedAt},
	tableBridgeForwardRequests: {
		"hub_node_id", "authenticated_agent_id", "request_id", "auth_context_id",
		"source_fence", "expected_mode_version", "hub_scope_id",
		"scope_version", "origin_gifford_node_id", "destination_gifford_node_id",
		"admission_digest", "envelope_json", "envelope_digest", "signature_authority",
		"signing_key_id", "admission_result_json", "admission_receipt_id",
		"admission_receipt_digest", columnGiffordMessageID, "hub_store", "hub_message_id", columnState,
		columnCreatedAt, columnUpdatedAt,
	},
	tableBridgeIngressLedger: {
		"hub_node_id", "stream_id", "source_event_seq", "event_id", "event_type",
		"payload_json", "payload_digest", "projection_digest", "watermarks_after_json",
		"application_receipt_id", "application_receipt_digest", "disposition", "applied_at",
	},
	tableBridgeIngressVersions: {
		"hub_node_id", "protocol_state", "stream_id", "start_seq", "snapshot_at",
		"snapshot_watermarks_json", "bootstrap_expected_count", "bootstrap_manifest_digest",
		"inventory_criteria_json", "backfill_applied_count", "first_event_id", "first_receipt_id",
		"first_receipt_digest", "last_source_event_seq", "ready_event_id", "ready_receipt_id",
		"ready_receipt_digest", "legacy_retired_at", "ready_at", columnCreatedAt, columnUpdatedAt,
	},
	tableBridgeQuarantine: {
		"hub_node_id", "stream_id", "source_event_seq", "event_id", "reference_role",
		"hub_message_id", "resolution_state", "reference_digest", columnCreatedAt,
	},
	tableBridgeMessages: {
		columnSourceNodeID, columnSourceMessageID, columnGiffordMessageID, columnFromAgent,
		columnToAgent, "source_created_at", columnSourceContextID, "source_parent_message_id",
		"gifford_parent_message_id", columnSourceReadAt, columnCreatedAt, columnUpdatedAt,
		"hub_store", "hub_parent_store", "gifford_thread_id", "gifford_thread_ref", columnThreadSequence,
		"recipient_manifest_root", "recipient_count", "envelope_digest", "projection_digest",
		"mapping_receipt_id", "mapping_receipt_digest",
	},
	tableBridgeOutbox: {
		"event_id", "logical_event_id", "event_type", columnGiffordMessageID, "hub_node_id",
		"hub_store", "hub_message_id", "recipient_agent_id", "recipient_ordinal", "recipient_generation",
		"partition_seq", "recipient_projection_json", "merkle_proof_json",
		"canonical_payload_json", "payload_digest", "projection_digest",
		"watermarks_after_json", "depends_on_event_id", columnState, columnAttemptCount,
		columnNextAttemptAt, "last_error_class", "last_error_code", "last_diagnostic_digest", columnCreatedAt,
		columnUpdatedAt,
	},
	tableBridgeOutboxBases: {
		columnGiffordMessageID, "hub_node_id", "hub_store", "hub_message_id",
		"hub_parent_store", "hub_parent_message_id",
		"gifford_parent_message_id", "hub_context_id", "gifford_thread_id",
		"gifford_thread_ref", columnThreadSequence, "sender_agent_id", "message_class",
		"signature_authority", "signing_key_id", "envelope_json", "envelope_digest", "projection_digest", "response_mode",
		"expectation_id", "sin_acuse", "response_basis", "recipient_manifest_root",
		"recipient_count", "canonical_base_json", "base_digest", columnCreatedAt, columnUpdatedAt,
	},
	tableBridgeOutboxParts: {
		"hub_node_id", "recipient_agent_id", "next_sequence", "acked_through",
		"produced_watermarks_json", "acked_watermarks_json", columnCreatedAt, columnUpdatedAt,
	},
	tableBridgeOutboxReceipts: {
		"receipt_id", "event_id", "receipt_type", "attempt_id", "hub_apply_receipt_id",
		"hub_apply_receipt_json", "hub_apply_receipt_digest", "assigned_hub_store", "assigned_hub_message_id",
		"assigned_hub_context_id", "nack_code", "request_digest", "disposition",
		"previous_state", "resulting_state", "acked_through", "watermarks_after_json", columnCreatedAt,
	},
	tableBridgeRecipients: {
		columnGiffordMessageID, "hub_node_id", columnOrdinal,
		"recipient_agent_id", columnDeliveryMode, columnRequiresAck, columnGeneration,
		"merkle_proof_json", columnState, "rejection_reason", "read_at", "ack_status",
		"acked_at", columnStateVersion, "message_committed_event_id", "delivery_materialized",
		columnCreatedAt, columnUpdatedAt,
	},
	tableBridgeSourceCuts: {
		"cut_id", "expected_mode", "target_mode", "expected_mode_version", "hub_scope_id",
		"coordinator_node_id", "participant_pairs_json", "participants_digest", "phase",
		"initial_watermarks_json", "final_watermarks_json", "coordinator_receipt_id",
		"coordinator_receipt_digest", "barrier_fence", "begin_request_digest",
		"seal_request_digest", "coordinator_seal_receipt_id", "coordinator_seal_receipt_digest",
		"verification_request_digest", "participant_verification_digest",
		"all_participants_verified_digest", "verification_digest", "verification_receipt_id",
		"verification_receipt_digest", "cas_request_digest", "coordinator_router_fence_digest",
		"cas_receipt_id", "cas_receipt_digest", "abort_request_digest", "abort_reason",
		"coordinator_abort_receipt_digest", "abort_receipt_id", "abort_receipt_digest",
		columnCreatedAt, "sealed_at", "verified_at", "committed_at", "aborted_at", columnUpdatedAt,
	},
	tableBridgeSourceMode: {"singleton", "mode", "mode_version", "source_fence", "last_cut_id", columnUpdatedAt},
	tableBridgePrincipalMap: {
		"hub_node_id", "hub_principal_id", "gifford_agent_id", "roster_digest", columnCreatedAt,
	},
	tableBridgeThreads: {
		"hub_node_id", "hub_context_id", "gifford_thread_id",
		"gifford_thread_ref", columnCreatedAt,
	},
	tableBridgeV112Messages: {
		columnGiffordMessageID, "hub_node_id", "hub_store", "hub_message_id", "message_origin",
		"message_class", "signature_authority", "signing_key_id", "hub_context_id", "hub_parent_store",
		"hub_parent_message_id", "gifford_parent_message_id", "gifford_thread_id", "gifford_thread_ref",
		columnThreadSequence, "envelope_json", "envelope_digest", "projection_digest",
		"recipient_manifest_root", "recipient_count", "response_mode", "expectation_id",
		"sin_acuse", "response_basis", "projection_complete", columnCreatedAt, "completed_at",
		columnUpdatedAt,
	},
	tableBridgeWatermarks: {
		"gifford_node_id", "hub_node_id", "direction", "admissions_produced", "admissions_applied",
		"admissions_acked", "reads_produced", "reads_applied", "reads_acked",
		"responses_produced", "responses_applied", "responses_acked",
		"recipient_states_produced", "recipient_states_applied", "recipient_states_acked",
		columnUpdatedAt,
	},
	tableDeliveryIncidents:     {columnRecipientAgent, columnNormalizedReason, columnAttemptCount, "first_seen", "last_seen", "alerted_at", "last_alerted_at", "recovered_at", "recovery_notified_at", "pending_kind", "pending_at", "pending_minutes", "pending_attempt_count", columnCreatedAt, columnUpdatedAt},
	tableDeliveryIncidentLinks: {columnRecipientAgent, columnNormalizedReason, columnDeliveryID, columnMessageID, columnObservedAt},
	tableIncidentSenderNotices: {columnRecipientAgent, columnNormalizedReason, columnSenderAgent, columnKind, columnQueuedAt, columnMinutes, columnAttemptCount, columnFiledAt, columnCreatedAt, columnUpdatedAt},
	tableMessageRecipients:     {columnMessageID, columnAgentID, columnOrdinal, columnDeliveryMode, columnRequiresAck, columnCreatedAt},
	tableMessageTriage:         {columnMessageID, "label", "outcome", columnGlimmerCallID, columnCreatedAt},
	tableAcks:                  {columnMessageID, columnAgentID, columnDeliveryGeneration, columnStatus, columnAckedAt},
	tableMessageEnvelopes:      {columnMessageID, columnEnvelope, "protocol", columnVersion, columnClientMessageID, columnOriginNode, "origin_agent", columnDestinationNode, "recipient_agents_json", columnCreatedAt, columnExpiresAt, columnPriority, columnThreadRef, columnEncryptionProfile, "sender_signing_key_id", columnCiphertextSHA256, "stored_at", columnParentMessageID, columnThreadSequence, columnLoopID, columnLoopGeneration, columnTurnIndex, columnHopCount, columnRouteDigest, columnWireControlVersion, columnControlState, columnControlReason, columnControlVersion},
	tableDeliveryQueue:         {columnMessageID, columnAgentID, columnDeliveryGeneration, columnFromAgent, columnThreadRef, columnState, columnPriority, columnAttempts, columnNextAttemptAt, "claimed_by", "claim_until", columnCreatedAt, columnExpiresAt, columnUpdatedAt, columnParentMessageID, columnThreadSequence, columnLoopID, columnLoopGeneration, columnTurnIndex, columnHopCount, columnRouteDigest, columnControlState, columnControlReason, columnControlVersion},
	tableResultEvidence:        {columnDeliveryID, columnAgentID, "adapter_session_id", "result", "frame_id", "reply_to", "proxy_observed_at", "received_at", "observed_draft_generation", "observed_input_guard_version", "writer_lease_id", "cancel_reason", "control_versions_json"},
	tableEnrollmentTokens:      {"hash", "org", columnCreatedAt, columnExpiresAt, "used_at", "used_by"},
	tableNodes:                 {"node_id", "organization_id", columnLocal, columnStatus, "federation_endpoint", "certificate_fingerprint", columnPolicyVersion, "last_seen_at", columnCreatedAt, "signing_public_key"},
	tableAgentKeys:             {"key_id", columnAgentID, "node_id", "purpose", "algorithm", "public_key", columnStatus, "valid_from", "valid_until", "issuer", "record_signature"},
	tableFederationOutbox:      {columnOutboxID, "object_type", "object_id", columnDestinationNode, columnState, columnAttempts, columnNextAttemptAt, "last_error", "idempotency_key", "lease_owner", "lease_until", columnCreatedAt, columnUpdatedAt},
	tableFederationOutboxAcks:  {columnOutboxID, columnDestinationNode, columnRequest, columnRequestSHA256, columnSequence, columnNonce},
	tableFederationRequests:    {columnOutboxID, columnDestinationNode, columnRequest, columnRequestSHA256, columnSequence, columnNonce},
	tableFederationReceipts:    {columnOutboxID, columnReceipt, columnState, columnReceivedAt},
	tableFederationAckInbox:    {columnOriginNode, columnSequence, columnNonce, columnMessageID, columnAgentID, columnDeliveryGeneration, columnStatus, columnRequestSHA256, columnReceipt, columnReceivedAt},
	tableFederationAckReceipts: {columnOutboxID, columnReceipt, columnReceivedAt},
	tableArtifactAckOutbox:     {columnOutboxID, columnDestinationNode, columnRequest, columnRequestSHA256, columnSequence, columnNonce},
	tableArtifactAckInbox:      {columnDestinationNode, columnSequence, columnNonce, columnArtifactID, columnRecipientAgent, columnRequestSHA256, columnReceipt, columnReceivedAt},
	tableArtifactAckReceipts:   {columnOutboxID, columnReceipt, columnReceivedAt},
	tablePolicyVersions:        {columnVersion, "digest", "compiled_json", columnCreatedAt},
	tablePaneIndexPublications: {"id", "panel_key", "published_at"},
	tableArtifactTransfers:     {columnArtifactID, columnClientMessageID, columnOriginNode, columnSenderAgent, columnDestinationNode, columnState, columnEncryptionProfile, columnCiphertextSize, "chunk_count", columnCiphertextSHA256, "encrypted_manifest", "sender_key_id", columnSignature, columnExpiresAt, columnCreatedAt, columnUpdatedAt, columnThreadRef, columnLoopID, columnLoopGeneration, columnHopCount, columnRouteDigest, columnControlState, columnControlReason, columnControlVersion},
	tableArtifactRecipients:    {columnArtifactID, columnRecipientAgent, "recipient_node", "key_envelope", columnState, "acknowledged_at", "recipient_key_id"},
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
	tableEnrollmentTokens: {
		blobs:    []string{"hash"},
		required: []string{"org", columnCreatedAt, columnExpiresAt},
	},
	tableNodes: {
		integers: []string{columnLocal},
		blobs:    []string{"signing_public_key"},
		required: []string{"organization_id", columnLocal, columnStatus, columnPolicyVersion, columnCreatedAt},
	},
	tableLoopClosures: {
		integers: []string{columnLoopClosedSequence},
		required: []string{columnLoopID, columnLoopClosedSequence},
	},
	tableMessages: {
		integers: []string{columnThreadSequence, columnLoopGeneration, columnTurnIndex, columnHopCount, columnControlVersion},
		required: []string{columnClientMessageID, columnOriginNode, columnFromAgent, columnDestinationNode, columnThreadRef, columnPriority, columnState, columnCreatedAt, columnExpiresAt, columnUpdatedAt, columnHopCount, columnRouteDigest, columnControlState, columnControlReason, columnControlVersion},
	},
	tableBridgeContexts: {
		required: []string{columnSourceNodeID, columnSourceContextID, "gifford_thread_id", "gifford_thread_ref", columnCreatedAt},
	},
	tableBridgeForwardRequests: {
		integers: []string{"expected_mode_version", "scope_version", "hub_message_id"},
		required: []string{"hub_node_id", "authenticated_agent_id", "request_id", "auth_context_id", "source_fence", "expected_mode_version", "hub_scope_id", "scope_version", "origin_gifford_node_id", "destination_gifford_node_id", "admission_digest", "envelope_json", "envelope_digest", "signature_authority", "signing_key_id", "admission_result_json", "admission_receipt_id", "admission_receipt_digest", columnState, columnCreatedAt, columnUpdatedAt},
	},
	tableBridgeIngressLedger: {
		integers: []string{"source_event_seq"},
		required: []string{"hub_node_id", "stream_id", "source_event_seq", "event_id", "event_type", "payload_json", "payload_digest", "watermarks_after_json", "application_receipt_id", "application_receipt_digest", "disposition", "applied_at"},
	},
	tableBridgeIngressVersions: {
		integers: []string{"start_seq", "bootstrap_expected_count", "backfill_applied_count", "last_source_event_seq"},
		required: []string{"hub_node_id", "protocol_state", "backfill_applied_count", "last_source_event_seq", columnCreatedAt, columnUpdatedAt},
	},
	tableBridgeQuarantine: {
		integers: []string{"source_event_seq", "hub_message_id"},
		required: []string{"hub_node_id", "stream_id", "source_event_seq", "event_id", "reference_role", "hub_message_id", "resolution_state", "reference_digest", columnCreatedAt},
	},
	tableBridgeMessages: {
		integers: []string{columnSourceMessageID, "source_parent_message_id", columnThreadSequence, "recipient_count"},
		required: []string{columnSourceNodeID, columnSourceMessageID, columnGiffordMessageID, columnFromAgent, columnToAgent, "source_created_at", columnCreatedAt, columnUpdatedAt, "hub_store"},
	},
	tableBridgeOutbox: {
		integers: []string{"hub_message_id", "recipient_ordinal", "recipient_generation", "partition_seq", columnAttemptCount},
		required: []string{"event_id", "logical_event_id", "event_type", columnGiffordMessageID, "hub_node_id", "recipient_agent_id", "recipient_ordinal", "recipient_generation", "partition_seq", "recipient_projection_json", "merkle_proof_json", "canonical_payload_json", "payload_digest", "projection_digest", "watermarks_after_json", columnState, columnAttemptCount, columnCreatedAt, columnUpdatedAt},
	},
	tableBridgeOutboxBases: {
		integers: []string{"hub_message_id", "hub_parent_message_id", columnThreadSequence, "sin_acuse", "recipient_count"},
		required: []string{columnGiffordMessageID, "hub_node_id", "gifford_thread_id", "gifford_thread_ref", columnThreadSequence, "sender_agent_id", "message_class", "signature_authority", "signing_key_id", "envelope_json", "envelope_digest", "projection_digest", "response_mode", "sin_acuse", "response_basis", "recipient_manifest_root", "recipient_count", "canonical_base_json", "base_digest", columnCreatedAt, columnUpdatedAt},
	},
	tableBridgeOutboxParts: {
		integers: []string{"next_sequence", "acked_through"},
		required: []string{"hub_node_id", "recipient_agent_id", "next_sequence", "acked_through", "produced_watermarks_json", "acked_watermarks_json", columnCreatedAt, columnUpdatedAt},
	},
	tableBridgeOutboxReceipts: {
		integers: []string{"assigned_hub_message_id", "acked_through"},
		required: []string{"receipt_id", "event_id", "receipt_type", "request_digest", "disposition", "previous_state", "resulting_state", "watermarks_after_json", columnCreatedAt},
	},
	tableBridgeRecipients: {
		integers: []string{columnOrdinal, columnRequiresAck, columnGeneration, columnStateVersion, "delivery_materialized"},
		required: []string{columnGiffordMessageID, "hub_node_id", columnOrdinal, "recipient_agent_id", columnDeliveryMode, columnRequiresAck, columnGeneration, "merkle_proof_json", columnState, columnStateVersion, "delivery_materialized", columnCreatedAt, columnUpdatedAt},
	},
	tableBridgeReminders: {
		integers: []string{columnGeneration, columnSourceMessageID},
		required: []string{columnSourceNodeID, columnExpectationID, columnGeneration, columnSourceMessageID, columnFromAgent, columnToAgent, columnSourceReadAt, "eligible_at", columnAcceptedAt, columnExpiresAt, columnState, columnReminderID, columnGiffordMessageID, columnCreatedAt, columnUpdatedAt},
	},
	tableBridgeTombstones: {
		integers: []string{columnGeneration},
		required: []string{columnSourceNodeID, columnExpectationID, columnGeneration, columnReason, columnReminderID, columnGiffordMessageID, "disposition", columnState, columnAcceptedAt, columnExpiresAt, columnCreatedAt},
	},
	tableBridgeSourceCuts: {
		integers: []string{"expected_mode_version"},
		required: []string{"cut_id", "expected_mode", "target_mode", "expected_mode_version", "hub_scope_id", "coordinator_node_id", "participant_pairs_json", "participants_digest", "phase", "initial_watermarks_json", "coordinator_receipt_id", "coordinator_receipt_digest", "barrier_fence", "begin_request_digest", columnCreatedAt, columnUpdatedAt},
	},
	tableBridgeSourceMode: {
		integers: []string{"singleton", "mode_version"},
		required: []string{"mode", "mode_version", "source_fence", columnUpdatedAt},
	},
	tableBridgePrincipalMap: {
		required: []string{"hub_node_id", "hub_principal_id", "gifford_agent_id", "roster_digest", columnCreatedAt},
	},
	tableBridgeThreads: {
		required: []string{"hub_node_id", "hub_context_id", "gifford_thread_id", "gifford_thread_ref", columnCreatedAt},
	},
	tableBridgeV112Messages: {
		integers: []string{"hub_message_id", "hub_parent_message_id", columnThreadSequence, "recipient_count", "sin_acuse", "projection_complete"},
		required: []string{columnGiffordMessageID, "hub_node_id", "message_origin", "message_class", "signature_authority", "signing_key_id", "gifford_thread_id", "gifford_thread_ref", columnThreadSequence, "recipient_manifest_root", "recipient_count", "response_mode", "sin_acuse", "response_basis", "projection_complete", columnCreatedAt, columnUpdatedAt},
	},
	tableBridgeWatermarks: {
		integers: []string{"admissions_produced", "admissions_applied", "admissions_acked", "reads_produced", "reads_applied", "reads_acked", "responses_produced", "responses_applied", "responses_acked", "recipient_states_produced", "recipient_states_applied", "recipient_states_acked"},
		required: []string{"gifford_node_id", "hub_node_id", "direction", "admissions_produced", "admissions_applied", "admissions_acked", "reads_produced", "reads_applied", "reads_acked", "responses_produced", "responses_applied", "responses_acked", "recipient_states_produced", "recipient_states_applied", "recipient_states_acked", columnUpdatedAt},
	},
	tableDeliveryIncidents: {
		integers: []string{columnAttemptCount, "pending_minutes", "pending_attempt_count"},
		required: []string{columnRecipientAgent, columnNormalizedReason, columnAttemptCount, "first_seen", "last_seen", columnCreatedAt, columnUpdatedAt},
	},
	tableDeliveryIncidentLinks: {
		required: []string{columnRecipientAgent, columnNormalizedReason, columnDeliveryID, columnMessageID, columnObservedAt},
	},
	tableIncidentSenderNotices: {
		integers: []string{columnMinutes, columnAttemptCount},
		required: []string{columnRecipientAgent, columnNormalizedReason, columnSenderAgent, columnKind, columnQueuedAt, columnMinutes, columnAttemptCount, columnCreatedAt, columnUpdatedAt},
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
	tableMessageTriage: {
		required: []string{columnMessageID, "outcome", columnCreatedAt},
	},
	tableFederationRequests: {
		integers: []string{columnSequence},
		blobs:    []string{columnRequest, columnNonce},
		required: []string{columnDestinationNode, columnRequest, columnRequestSHA256, columnSequence, columnNonce},
	},
	tableFederationOutboxAcks: {
		integers: []string{columnSequence},
		blobs:    []string{columnRequest, columnNonce},
		required: []string{columnDestinationNode, columnRequest, columnRequestSHA256, columnSequence, columnNonce},
	},
	tableFederationReceipts: {
		blobs:    []string{columnReceipt},
		required: []string{columnReceipt, columnState, columnReceivedAt},
	},
	tableFederationAckInbox: {
		integers: []string{columnSequence, columnDeliveryGeneration},
		blobs:    []string{columnNonce, columnReceipt},
		required: []string{columnOriginNode, columnSequence, columnNonce, columnMessageID, columnAgentID, columnDeliveryGeneration, columnStatus, columnRequestSHA256, columnReceipt, columnReceivedAt},
	},
	tableFederationAckReceipts: {
		blobs:    []string{columnReceipt},
		required: []string{columnReceipt, columnReceivedAt},
	},
	tableArtifactAckOutbox: {
		integers: []string{columnSequence},
		blobs:    []string{columnRequest, columnNonce},
		required: []string{columnDestinationNode, columnRequest, columnRequestSHA256, columnSequence, columnNonce},
	},
	tableArtifactAckInbox: {
		integers: []string{columnSequence},
		blobs:    []string{columnNonce, columnReceipt},
		required: []string{columnDestinationNode, columnSequence, columnNonce, columnArtifactID, columnRecipientAgent, columnRequestSHA256, columnReceipt, columnReceivedAt},
	},
	tableArtifactAckReceipts: {
		blobs:    []string{columnReceipt},
		required: []string{columnReceipt, columnReceivedAt},
	},
	tableArtifactTransfers: {
		integers: []string{columnCiphertextSize, "chunk_count", columnLoopGeneration, columnHopCount, columnControlVersion},
		blobs:    []string{"encrypted_manifest", columnSignature},
		required: []string{columnClientMessageID, columnOriginNode, columnSenderAgent, columnDestinationNode, columnState, columnEncryptionProfile, "sender_key_id", columnSignature, columnCreatedAt, columnUpdatedAt, columnHopCount, columnControlState, columnControlReason, columnControlVersion},
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
	{"model_profile_selection_generation", tableModelSelections, []string{columnContract, columnGeneration}, ""},
	{"tmux_instances_active_agent", tableTmuxInstances, []string{columnAgentID}, "WHERE state = 'ACTIVE'"},
	{"tmux_instances_active_pane", tableTmuxInstances, []string{"socket", "pane_id"}, "WHERE state = 'ACTIVE'"},
	{"state_snapshots_agent", tableStateSnapshots, []string{columnAgentID, columnObservedAt}, ""},
	{"state_snapshots_version", tableStateSnapshots, []string{columnAgentID, columnSessionID, columnStateVersion}, stateProducerIndexWhere},
	{"state_snapshots_observation", tableStateSnapshots, []string{"observation_id"}, stateProducerIndexWhere},
	{"state_snapshots_latest", tableStateSnapshots, []string{columnAgentID, columnSessionID, columnObservedAt, columnStateVersion}, stateProducerIndexWhere},
	{"state_snapshots_glimmer_call", tableStateSnapshots, []string{columnGlimmerCallID, columnObservedAt, columnStateID}, ""},
	{"orchestrator_decisions_agent", tableOrchestratorDecisions, []string{columnAgentID, columnCreatedAt}, ""},
	{"orchestrator_decisions_message", tableOrchestratorDecisions, []string{columnMessageID}, "WHERE message_id IS NOT NULL"},
	{"orchestrator_decisions_register", tableOrchestratorDecisions, []string{columnCreatedAt, "decision_id"}, "WHERE message_id IS NOT NULL"},
	{"delivery_attempts_decision", tableDeliveryAttempts, []string{"orchestrator_decision_id", columnCreatedAt, columnDeliveryID}, ""},
	{"delivery_incident_messages_message", tableDeliveryIncidentLinks, []string{columnMessageID}, ""},
	{"delivery_incident_sender_notices_pending", tableIncidentSenderNotices, []string{columnQueuedAt, columnRecipientAgent, columnNormalizedReason, columnSenderAgent}, "WHERE filed_at IS NULL"},
	{"delivery_incident_sender_notices_cooldown", tableIncidentSenderNotices, []string{columnSenderAgent, columnRecipientAgent, columnKind, columnFiledAt}, ""},
	{"training_runs_dataset", tableTrainingRuns, []string{columnDatasetVersion, columnStartedAt}, ""},
	{"evaluation_runs_dataset", tableEvaluationRuns, []string{columnDatasetVersion, columnInferenceProfile, columnCreatedAt}, ""},
	{"model_adapters_active", tableModelAdapters, []string{columnContract}, "WHERE active = 1"},
	{"bridge_source_cuts_active_scope", tableBridgeSourceCuts, []string{"hub_scope_id"}, "WHERE phase IN ('PREPARING', 'SEALED', 'VERIFIED')"},
	{"bridge_message_mapping_identity", tableBridgeMessages, []string{columnSourceNodeID, "hub_store", columnSourceMessageID, columnGiffordMessageID}, ""},
	{"bridge_message_mapping_thread_sequence", tableBridgeMessages, []string{columnSourceNodeID, columnSourceContextID, columnThreadSequence}, "WHERE source_context_id IS NOT NULL AND thread_sequence IS NOT NULL"},
	{"bridge_v112_message_hub_id", tableBridgeV112Messages, []string{"hub_node_id", "hub_store", "hub_message_id"}, "WHERE hub_message_id IS NOT NULL"},
	{"bridge_forward_requests_hub_message", tableBridgeForwardRequests, []string{"hub_node_id", "hub_store", "hub_message_id"}, "WHERE hub_message_id IS NOT NULL"},
	{"bridge_hub_reference_quarantine_unresolved", tableBridgeQuarantine, []string{"hub_node_id", "resolution_state", "hub_message_id"}, ""},
	{"bridge_recipient_projection_pending", tableBridgeRecipients, []string{"recipient_agent_id", columnState, columnGiffordMessageID, columnGeneration}, ""},
	{"bridge_outbox_base_hub_id", tableBridgeOutboxBases, []string{"hub_node_id", "hub_store", "hub_message_id"}, "WHERE hub_message_id IS NOT NULL"},
	{"bridge_outbox_ready", tableBridgeOutbox, []string{columnState, columnNextAttemptAt, "hub_node_id", "recipient_agent_id", "partition_seq"}, ""},
	{"bridge_outbox_dependency", tableBridgeOutbox, []string{"depends_on_event_id"}, "WHERE depends_on_event_id IS NOT NULL"},
	{"bridge_outbox_receipts_attempt", tableBridgeOutboxReceipts, []string{"event_id", "attempt_id"}, "WHERE attempt_id IS NOT NULL"},
	{"messages_client_msg", tableMessages, []string{"origin_node", columnFromAgent, columnClientMessageID}, "WHERE client_msg_namespace IS NULL"},
	{"messages_namespaced_client_msg", tableMessages, []string{columnClientNamespace, columnFromAgent, columnClientMessageID}, "WHERE client_msg_namespace IS NOT NULL"},
	{"messages_thread", tableMessages, []string{columnThreadRef, columnThreadSequence}, ""},
	{"messages_loop", tableMessages, []string{columnLoopID, columnLoopGeneration}, "WHERE loop_id IS NOT NULL"},
	{"message_recipients_ordinal", tableMessageRecipients, []string{columnMessageID, columnOrdinal}, ""},
	{"delivery_queue_pending", tableDeliveryQueue, []string{columnAgentID, columnState, columnPriority, columnCreatedAt}, ""},
	{"delivery_queue_control", tableDeliveryQueue, []string{columnAgentID, columnControlState, columnState}, ""},
	{"delivery_result_evidence_committed", tableResultEvidence, []string{columnDeliveryID}, "WHERE result = 'COMMITTED'"},
	{"delivery_result_evidence_delivery", tableResultEvidence, []string{columnDeliveryID, "received_at", "result"}, ""},
	{"federation_outbox_idempotency", tableFederationOutbox, []string{columnDestinationNode, "idempotency_key"}, ""},
	{"agent_keys_agent", tableAgentKeys, []string{columnAgentID, "purpose", "valid_from"}, ""},
	{"federation_outbox_pending", tableFederationOutbox, []string{columnState, columnNextAttemptAt}, ""},
	{"federation_outbox_requests_sequence", tableFederationRequests, []string{columnDestinationNode, columnSequence}, ""},
	{"federation_outbox_requests_nonce", tableFederationRequests, []string{columnDestinationNode, columnNonce}, ""},
	{"federation_outbox_acks_sequence", tableFederationOutboxAcks, []string{columnDestinationNode, columnSequence}, ""},
	{"federation_outbox_acks_nonce", tableFederationOutboxAcks, []string{columnDestinationNode, columnNonce}, ""},
	{"federation_ack_inbox_sequence", tableFederationAckInbox, []string{columnOriginNode, columnSequence}, ""},
	{"federation_ack_inbox_nonce", tableFederationAckInbox, []string{columnOriginNode, columnNonce}, ""},
	{"federation_artifact_ack_outbox_sequence", tableArtifactAckOutbox, []string{columnDestinationNode, columnSequence}, ""},
	{"federation_artifact_ack_outbox_nonce", tableArtifactAckOutbox, []string{columnDestinationNode, columnNonce}, ""},
	{"federation_artifact_ack_inbox_sequence", tableArtifactAckInbox, []string{columnDestinationNode, columnSequence}, ""},
	{"federation_artifact_ack_inbox_nonce", tableArtifactAckInbox, []string{columnDestinationNode, columnNonce}, ""},
	{"federation_artifact_receipts_outbox", tableArtifactFedReceipts, []string{"outbox_id", "receipt_id"}, ""},
	{"artifact_transfers_control", tableArtifactTransfers, []string{columnControlState, columnState}, ""},
	{"artifact_recipients_agent", tableArtifactRecipients, []string{columnRecipientAgent, columnState}, ""},
	{"artifact_transfers_sender_client", tableArtifactTransfers, []string{columnSenderAgent, columnClientMessageID}, ""},
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
		if object.Name == tableSessions {
			// Session resume deliberately adds the nullable boot_id column;
			// the exact resulting inventory is pinned by TestSchemaColumns.
			continue
		}
		if object.Name == "approval_requests_object" {
			// The latest migration deliberately narrows this legacy index;
			// recipient-scoped orchestrator uniqueness is pinned separately.
			continue
		}
		require.Equal(t, object, complete[object.Name], object.Name)
	}
}
