-- +goose Up
-- Durable source authority. The singleton starts on the legacy authority with
-- a node-local fence generated once by SQLite and survives process restarts.
CREATE TABLE bridge_source_mode (
  singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
  mode TEXT NOT NULL CHECK (mode IN ('CAPYCOM_SOURCE', 'GIFFORD_SOURCE')),
  mode_version INTEGER NOT NULL CHECK (mode_version >= 1),
  source_fence TEXT NOT NULL CHECK (length(source_fence) BETWEEN 1 AND 128),
  last_cut_id TEXT CHECK (
    last_cut_id IS NULL OR (
      length(last_cut_id) = 36
      AND substr(last_cut_id, 1, 4) = 'cut-'
      AND substr(last_cut_id, 5) NOT GLOB '*[^0-9a-f]*'
    )
  ),
  updated_at TEXT NOT NULL
);

INSERT INTO bridge_source_mode
  (singleton, mode, mode_version, source_fence, last_cut_id, updated_at)
VALUES
  (1, 'CAPYCOM_SOURCE', 1, 'fence-' || lower(hex(randomblob(16))), NULL,
   '2026-11-03T16:10:34.000000000Z');

-- +goose StatementBegin
CREATE TRIGGER bridge_source_mode_cas
BEFORE UPDATE ON bridge_source_mode
WHEN NEW.singleton IS NOT OLD.singleton
  OR NEW.updated_at <= OLD.updated_at
  OR NOT (
    (NEW.mode IS OLD.mode
     AND NEW.mode_version = OLD.mode_version
     AND NEW.source_fence IS OLD.source_fence
     AND NEW.last_cut_id IS NOT NULL
     AND NEW.last_cut_id IS NOT OLD.last_cut_id)
    OR
    (NEW.mode IS NOT OLD.mode
     AND NEW.mode_version = OLD.mode_version + 1
     AND NEW.source_fence IS NOT OLD.source_fence
     AND NEW.last_cut_id IS NOT NULL)
  )
BEGIN
  SELECT RAISE(ABORT, 'bridge source mode requires cut tracking or one fenced CAS');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER bridge_source_mode_no_delete
BEFORE DELETE ON bridge_source_mode
BEGIN
  SELECT RAISE(ABORT, 'bridge source mode is durable');
END;
-- +goose StatementEnd

CREATE TABLE bridge_source_cuts (
  cut_id TEXT NOT NULL PRIMARY KEY CHECK (
    length(cut_id) = 36
    AND substr(cut_id, 1, 4) = 'cut-'
    AND substr(cut_id, 5) NOT GLOB '*[^0-9a-f]*'
  ),
  expected_mode TEXT NOT NULL CHECK (expected_mode IN ('CAPYCOM_SOURCE', 'GIFFORD_SOURCE')),
  target_mode TEXT NOT NULL CHECK (target_mode IN ('CAPYCOM_SOURCE', 'GIFFORD_SOURCE')),
  expected_mode_version INTEGER NOT NULL CHECK (expected_mode_version >= 1),
  hub_scope_id TEXT NOT NULL CHECK (length(hub_scope_id) BETWEEN 1 AND 128),
  coordinator_node_id TEXT NOT NULL CHECK (length(coordinator_node_id) BETWEEN 1 AND 128),
  participant_pairs_json TEXT NOT NULL CHECK (length(participant_pairs_json) >= 3),
  participants_digest TEXT NOT NULL CHECK (length(participants_digest) = 71),
  phase TEXT NOT NULL CHECK (phase IN ('PREPARING', 'SEALED', 'VERIFIED', 'COMMITTED', 'ABORTED')),
  initial_watermarks_json TEXT NOT NULL CHECK (length(initial_watermarks_json) >= 2),
  final_watermarks_json TEXT,
  coordinator_receipt_id TEXT NOT NULL CHECK (
    length(coordinator_receipt_id) = 36 AND substr(coordinator_receipt_id, 1, 4) = 'rcp-'
  ),
  coordinator_receipt_digest TEXT NOT NULL CHECK (length(coordinator_receipt_digest) = 71),
  barrier_fence TEXT NOT NULL CHECK (length(barrier_fence) BETWEEN 1 AND 128),
  begin_request_digest TEXT NOT NULL CHECK (length(begin_request_digest) = 71),
  seal_request_digest TEXT CHECK (seal_request_digest IS NULL OR length(seal_request_digest) = 71),
  coordinator_seal_receipt_id TEXT CHECK (
    coordinator_seal_receipt_id IS NULL OR (
      length(coordinator_seal_receipt_id) = 36
      AND substr(coordinator_seal_receipt_id, 1, 4) = 'rcp-'
    )
  ),
  coordinator_seal_receipt_digest TEXT CHECK (
    coordinator_seal_receipt_digest IS NULL OR length(coordinator_seal_receipt_digest) = 71
  ),
  verification_request_digest TEXT CHECK (
    verification_request_digest IS NULL OR length(verification_request_digest) = 71
  ),
  participant_verification_digest TEXT CHECK (
    participant_verification_digest IS NULL OR length(participant_verification_digest) = 71
  ),
  all_participants_verified_digest TEXT CHECK (
    all_participants_verified_digest IS NULL OR length(all_participants_verified_digest) = 71
  ),
  verification_digest TEXT CHECK (verification_digest IS NULL OR length(verification_digest) = 71),
  verification_receipt_id TEXT CHECK (
    verification_receipt_id IS NULL OR (
      length(verification_receipt_id) = 36
      AND substr(verification_receipt_id, 1, 4) = 'rcp-'
    )
  ),
  verification_receipt_digest TEXT CHECK (
    verification_receipt_digest IS NULL OR length(verification_receipt_digest) = 71
  ),
  cas_request_digest TEXT CHECK (cas_request_digest IS NULL OR length(cas_request_digest) = 71),
  coordinator_router_fence_digest TEXT CHECK (
    coordinator_router_fence_digest IS NULL OR length(coordinator_router_fence_digest) = 71
  ),
  cas_receipt_id TEXT CHECK (
    cas_receipt_id IS NULL OR (
      length(cas_receipt_id) = 36 AND substr(cas_receipt_id, 1, 4) = 'rcp-'
    )
  ),
  cas_receipt_digest TEXT CHECK (cas_receipt_digest IS NULL OR length(cas_receipt_digest) = 71),
  abort_request_digest TEXT CHECK (abort_request_digest IS NULL OR length(abort_request_digest) = 71),
  abort_reason TEXT CHECK (
    abort_reason IS NULL OR abort_reason IN (
      'VALIDATION_FAILED', 'OPERATOR_CANCELLED', 'TIME_BUDGET_EXCEEDED', 'EMERGENCY'
    )
  ),
  coordinator_abort_receipt_digest TEXT CHECK (
    coordinator_abort_receipt_digest IS NULL OR length(coordinator_abort_receipt_digest) = 71
  ),
  abort_receipt_id TEXT CHECK (
    abort_receipt_id IS NULL OR (
      length(abort_receipt_id) = 36 AND substr(abort_receipt_id, 1, 4) = 'rcp-'
    )
  ),
  abort_receipt_digest TEXT CHECK (
    abort_receipt_digest IS NULL OR length(abort_receipt_digest) = 71
  ),
  created_at TEXT NOT NULL,
  sealed_at TEXT,
  verified_at TEXT,
  committed_at TEXT,
  aborted_at TEXT,
  updated_at TEXT NOT NULL,
  CHECK (expected_mode != target_mode),
  CHECK ((final_watermarks_json IS NULL) = (sealed_at IS NULL)),
  CHECK ((coordinator_seal_receipt_id IS NULL) = (coordinator_seal_receipt_digest IS NULL)),
  CHECK ((verification_receipt_id IS NULL) = (verification_receipt_digest IS NULL)),
  CHECK ((cas_receipt_id IS NULL) = (cas_receipt_digest IS NULL)),
  CHECK ((abort_receipt_id IS NULL) = (abort_receipt_digest IS NULL)),
  CHECK (phase != 'ABORTED' OR abort_reason IS NOT NULL)
);

CREATE UNIQUE INDEX bridge_source_cuts_active_scope
  ON bridge_source_cuts (hub_scope_id)
  WHERE phase IN ('PREPARING', 'SEALED', 'VERIFIED');

-- +goose StatementBegin
CREATE TRIGGER bridge_source_cuts_identity_immutable
BEFORE UPDATE ON bridge_source_cuts
WHEN NEW.cut_id IS NOT OLD.cut_id
  OR NEW.expected_mode IS NOT OLD.expected_mode
  OR NEW.target_mode IS NOT OLD.target_mode
  OR NEW.expected_mode_version IS NOT OLD.expected_mode_version
  OR NEW.hub_scope_id IS NOT OLD.hub_scope_id
  OR NEW.coordinator_node_id IS NOT OLD.coordinator_node_id
  OR NEW.participant_pairs_json IS NOT OLD.participant_pairs_json
  OR NEW.participants_digest IS NOT OLD.participants_digest
  OR NEW.initial_watermarks_json IS NOT OLD.initial_watermarks_json
  OR NEW.coordinator_receipt_id IS NOT OLD.coordinator_receipt_id
  OR NEW.coordinator_receipt_digest IS NOT OLD.coordinator_receipt_digest
  OR NEW.barrier_fence IS NOT OLD.barrier_fence
  OR NEW.begin_request_digest IS NOT OLD.begin_request_digest
  OR NEW.created_at IS NOT OLD.created_at
BEGIN
  SELECT RAISE(ABORT, 'bridge source cut identity is immutable');
END;
-- +goose StatementEnd

-- Every phase payload is write-once. Retries read the stored receipt rather
-- than rewriting a cut with equivalent JSON.
-- +goose StatementBegin
CREATE TRIGGER bridge_source_cuts_receipts_write_once
BEFORE UPDATE ON bridge_source_cuts
WHEN (OLD.final_watermarks_json IS NOT NULL AND NEW.final_watermarks_json IS NOT OLD.final_watermarks_json)
  OR (OLD.seal_request_digest IS NOT NULL AND NEW.seal_request_digest IS NOT OLD.seal_request_digest)
  OR (OLD.coordinator_seal_receipt_id IS NOT NULL AND NEW.coordinator_seal_receipt_id IS NOT OLD.coordinator_seal_receipt_id)
  OR (OLD.coordinator_seal_receipt_digest IS NOT NULL AND NEW.coordinator_seal_receipt_digest IS NOT OLD.coordinator_seal_receipt_digest)
  OR (OLD.verification_request_digest IS NOT NULL AND NEW.verification_request_digest IS NOT OLD.verification_request_digest)
  OR (OLD.participant_verification_digest IS NOT NULL AND NEW.participant_verification_digest IS NOT OLD.participant_verification_digest)
  OR (OLD.all_participants_verified_digest IS NOT NULL AND NEW.all_participants_verified_digest IS NOT OLD.all_participants_verified_digest)
  OR (OLD.verification_digest IS NOT NULL AND NEW.verification_digest IS NOT OLD.verification_digest)
  OR (OLD.verification_receipt_id IS NOT NULL AND NEW.verification_receipt_id IS NOT OLD.verification_receipt_id)
  OR (OLD.verification_receipt_digest IS NOT NULL AND NEW.verification_receipt_digest IS NOT OLD.verification_receipt_digest)
  OR (OLD.cas_request_digest IS NOT NULL AND NEW.cas_request_digest IS NOT OLD.cas_request_digest)
  OR (OLD.coordinator_router_fence_digest IS NOT NULL AND NEW.coordinator_router_fence_digest IS NOT OLD.coordinator_router_fence_digest)
  OR (OLD.cas_receipt_id IS NOT NULL AND NEW.cas_receipt_id IS NOT OLD.cas_receipt_id)
  OR (OLD.cas_receipt_digest IS NOT NULL AND NEW.cas_receipt_digest IS NOT OLD.cas_receipt_digest)
  OR (OLD.abort_request_digest IS NOT NULL AND NEW.abort_request_digest IS NOT OLD.abort_request_digest)
  OR (OLD.abort_reason IS NOT NULL AND NEW.abort_reason IS NOT OLD.abort_reason)
  OR (OLD.coordinator_abort_receipt_digest IS NOT NULL AND NEW.coordinator_abort_receipt_digest IS NOT OLD.coordinator_abort_receipt_digest)
  OR (OLD.abort_receipt_id IS NOT NULL AND NEW.abort_receipt_id IS NOT OLD.abort_receipt_id)
  OR (OLD.abort_receipt_digest IS NOT NULL AND NEW.abort_receipt_digest IS NOT OLD.abort_receipt_digest)
  OR (OLD.sealed_at IS NOT NULL AND NEW.sealed_at IS NOT OLD.sealed_at)
  OR (OLD.verified_at IS NOT NULL AND NEW.verified_at IS NOT OLD.verified_at)
  OR (OLD.committed_at IS NOT NULL AND NEW.committed_at IS NOT OLD.committed_at)
  OR (OLD.aborted_at IS NOT NULL AND NEW.aborted_at IS NOT OLD.aborted_at)
BEGIN
  SELECT RAISE(ABORT, 'bridge source cut receipt is written once');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER bridge_source_cuts_phase_transition
BEFORE UPDATE OF phase ON bridge_source_cuts
WHEN NOT (
  NEW.phase IS OLD.phase
  OR (OLD.phase = 'PREPARING' AND NEW.phase IN ('SEALED', 'ABORTED'))
  OR (OLD.phase = 'SEALED' AND NEW.phase IN ('VERIFIED', 'ABORTED'))
  OR (OLD.phase = 'VERIFIED' AND NEW.phase IN ('COMMITTED', 'ABORTED'))
)
BEGIN
  SELECT RAISE(ABORT, 'invalid bridge source cut transition');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER bridge_source_cuts_no_delete
BEFORE DELETE ON bridge_source_cuts
BEGIN
  SELECT RAISE(ABORT, 'bridge source cuts are durable');
END;
-- +goose StatementEnd

-- The v1.12 relation is separate from the legacy context table: legacy rows
-- can be upgraded without changing the key or any legacy column.
CREATE TABLE bridge_thread_mappings (
  hub_node_id TEXT NOT NULL CHECK (length(hub_node_id) BETWEEN 1 AND 255),
  hub_context_id TEXT NOT NULL CHECK (length(hub_context_id) BETWEEN 1 AND 512),
  gifford_thread_id TEXT NOT NULL,
  gifford_thread_ref TEXT NOT NULL,
  created_at TEXT NOT NULL,
  PRIMARY KEY (hub_node_id, hub_context_id),
  UNIQUE (hub_node_id, gifford_thread_id),
  UNIQUE (hub_node_id, gifford_thread_ref)
);

-- +goose StatementBegin
CREATE TRIGGER bridge_thread_mappings_immutable
BEFORE UPDATE ON bridge_thread_mappings
BEGIN
  SELECT RAISE(ABORT, 'bridge thread mapping is immutable');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER bridge_thread_mappings_no_delete
BEFORE DELETE ON bridge_thread_mappings
BEGIN
  SELECT RAISE(ABORT, 'bridge thread mapping is durable');
END;
-- +goose StatementEnd

-- Extend the existing one-to-one mapping in place. All old columns retain
-- their order, names, constraints and legacy writer semantics.
-- Legacy bridge_mirror_message rows always name the hub's messages store.
ALTER TABLE bridge_message_mappings ADD COLUMN hub_store TEXT NOT NULL DEFAULT 'messages'
  CHECK (hub_store = 'messages');
ALTER TABLE bridge_message_mappings ADD COLUMN hub_parent_store TEXT
  CHECK (hub_parent_store IS NULL OR hub_parent_store = 'messages');
ALTER TABLE bridge_message_mappings ADD COLUMN gifford_thread_id TEXT;
ALTER TABLE bridge_message_mappings ADD COLUMN gifford_thread_ref TEXT;
ALTER TABLE bridge_message_mappings ADD COLUMN thread_sequence INTEGER
  CHECK (thread_sequence IS NULL OR thread_sequence >= 1);
ALTER TABLE bridge_message_mappings ADD COLUMN recipient_manifest_root TEXT
  CHECK (recipient_manifest_root IS NULL OR length(recipient_manifest_root) = 71);
ALTER TABLE bridge_message_mappings ADD COLUMN recipient_count INTEGER
  CHECK (recipient_count IS NULL OR recipient_count >= 1);
ALTER TABLE bridge_message_mappings ADD COLUMN envelope_digest TEXT
  CHECK (envelope_digest IS NULL OR length(envelope_digest) = 71);
ALTER TABLE bridge_message_mappings ADD COLUMN projection_digest TEXT
  CHECK (projection_digest IS NULL OR length(projection_digest) = 71);
ALTER TABLE bridge_message_mappings ADD COLUMN mapping_receipt_id TEXT
  CHECK (
    mapping_receipt_id IS NULL OR (
      length(mapping_receipt_id) = 36 AND substr(mapping_receipt_id, 1, 4) = 'rcp-'
    )
  );
ALTER TABLE bridge_message_mappings ADD COLUMN mapping_receipt_digest TEXT
  CHECK (mapping_receipt_digest IS NULL OR length(mapping_receipt_digest) = 71);

UPDATE bridge_message_mappings
SET hub_parent_store = CASE
      WHEN source_parent_message_id IS NOT NULL THEN 'messages'
      ELSE NULL
    END,
    gifford_thread_id = (
      SELECT messages.thread_id FROM messages
      WHERE messages.message_id = bridge_message_mappings.gifford_message_id
    ),
    gifford_thread_ref = (
      SELECT messages.thread_ref FROM messages
      WHERE messages.message_id = bridge_message_mappings.gifford_message_id
    ),
    thread_sequence = (
      SELECT messages.thread_sequence FROM messages
      WHERE messages.message_id = bridge_message_mappings.gifford_message_id
    );

CREATE UNIQUE INDEX bridge_message_mapping_identity
  ON bridge_message_mappings
    (source_node_id, hub_store, source_message_id, gifford_message_id);
CREATE UNIQUE INDEX bridge_message_mapping_thread_sequence
  ON bridge_message_mappings (source_node_id, source_context_id, thread_sequence)
  WHERE source_context_id IS NOT NULL AND thread_sequence IS NOT NULL;

-- +goose StatementBegin
CREATE TRIGGER bridge_message_v112_fields_consistent_insert
BEFORE INSERT ON bridge_message_mappings
WHEN (NEW.recipient_manifest_root IS NULL) != (NEW.recipient_count IS NULL)
  OR (NEW.mapping_receipt_id IS NULL) != (NEW.mapping_receipt_digest IS NULL)
  OR (NEW.source_parent_message_id IS NULL) != (NEW.hub_parent_store IS NULL)
BEGIN
  SELECT RAISE(ABORT, 'bridge v1.12 mapping fields are incomplete');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER bridge_message_v112_fields_consistent_update
BEFORE UPDATE ON bridge_message_mappings
WHEN (NEW.recipient_manifest_root IS NULL) != (NEW.recipient_count IS NULL)
  OR (NEW.mapping_receipt_id IS NULL) != (NEW.mapping_receipt_digest IS NULL)
  OR (NEW.source_parent_message_id IS NULL) != (NEW.hub_parent_store IS NULL)
BEGIN
  SELECT RAISE(ABORT, 'bridge v1.12 mapping fields are incomplete');
END;
-- +goose StatementEnd

-- Legacy NULL values may be filled exactly once by backfill. Once known,
-- thread, manifest, payload and receipt identity cannot be rewritten.
-- +goose StatementBegin
CREATE TRIGGER bridge_message_v112_fields_write_once
BEFORE UPDATE ON bridge_message_mappings
WHEN NEW.hub_store IS NOT OLD.hub_store
  OR NEW.hub_parent_store IS NOT OLD.hub_parent_store
  OR (OLD.gifford_thread_id IS NOT NULL AND NEW.gifford_thread_id IS NOT OLD.gifford_thread_id)
  OR (OLD.gifford_thread_ref IS NOT NULL AND NEW.gifford_thread_ref IS NOT OLD.gifford_thread_ref)
  OR (OLD.thread_sequence IS NOT NULL AND NEW.thread_sequence IS NOT OLD.thread_sequence)
  OR (OLD.recipient_manifest_root IS NOT NULL AND NEW.recipient_manifest_root IS NOT OLD.recipient_manifest_root)
  OR (OLD.recipient_count IS NOT NULL AND NEW.recipient_count IS NOT OLD.recipient_count)
  OR (OLD.envelope_digest IS NOT NULL AND NEW.envelope_digest IS NOT OLD.envelope_digest)
  OR (OLD.projection_digest IS NOT NULL AND NEW.projection_digest IS NOT OLD.projection_digest)
  OR (OLD.mapping_receipt_id IS NOT NULL AND NEW.mapping_receipt_id IS NOT OLD.mapping_receipt_id)
  OR (OLD.mapping_receipt_digest IS NOT NULL AND NEW.mapping_receipt_digest IS NOT OLD.mapping_receipt_digest)
BEGIN
  SELECT RAISE(ABORT, 'bridge v1.12 mapping fields are written once');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER bridge_message_mappings_no_delete
BEFORE DELETE ON bridge_message_mappings
BEGIN
  SELECT RAISE(ABORT, 'bridge message mappings are durable');
END;
-- +goose StatementEnd

-- Admission may create an incomplete projection before the exact envelope is
-- submitted. Mirror and backfill insert the same record complete in one tx.
CREATE TABLE bridge_v112_message_projection (
  gifford_message_id TEXT NOT NULL PRIMARY KEY,
  hub_node_id TEXT NOT NULL CHECK (length(hub_node_id) BETWEEN 1 AND 255),
  hub_store TEXT CHECK (hub_store IS NULL OR hub_store IN ('messages', 'cv2_outbound')),
  hub_message_id INTEGER CHECK (hub_message_id IS NULL OR hub_message_id > 0),
  message_origin TEXT NOT NULL CHECK (length(message_origin) BETWEEN 1 AND 64),
  message_class TEXT NOT NULL CHECK (message_class IN ('USER', 'SYSTEM', 'RESPONSE_WATCH_NOTICE')),
  signature_authority TEXT NOT NULL CHECK (
    signature_authority IN ('AGENT_ENDPOINT', 'NODE_FORWARD', 'NODE_EMITTER')
  ),
  signing_key_id TEXT NOT NULL CHECK (length(signing_key_id) BETWEEN 1 AND 128),
  hub_context_id TEXT CHECK (hub_context_id IS NULL OR length(hub_context_id) BETWEEN 1 AND 512),
  hub_parent_store TEXT CHECK (
    hub_parent_store IS NULL OR hub_parent_store IN ('messages', 'cv2_outbound')
  ),
  hub_parent_message_id INTEGER CHECK (hub_parent_message_id > 0),
  gifford_parent_message_id TEXT REFERENCES messages(message_id),
  gifford_thread_id TEXT NOT NULL,
  gifford_thread_ref TEXT NOT NULL,
  thread_sequence INTEGER NOT NULL CHECK (thread_sequence >= 1),
  envelope_json TEXT,
  envelope_digest TEXT CHECK (envelope_digest IS NULL OR length(envelope_digest) = 71),
  projection_digest TEXT CHECK (projection_digest IS NULL OR length(projection_digest) = 71),
  recipient_manifest_root TEXT NOT NULL CHECK (length(recipient_manifest_root) = 71),
  recipient_count INTEGER NOT NULL CHECK (recipient_count >= 1),
  response_mode TEXT NOT NULL CHECK (response_mode IN ('NONE', 'REQUIRED', 'WAIVED')),
  expectation_id TEXT CHECK (
    expectation_id IS NULL OR (
      length(expectation_id) = 36 AND substr(expectation_id, 1, 4) = 'rsp-'
    )
  ),
  sin_acuse INTEGER NOT NULL CHECK (sin_acuse IN (0, 1)),
  response_basis TEXT NOT NULL CHECK (response_basis IN ('NONE', 'AUTHENTICATED_HIERARCHY')),
  projection_complete INTEGER NOT NULL CHECK (projection_complete IN (0, 1)),
  created_at TEXT NOT NULL,
  completed_at TEXT,
  updated_at TEXT NOT NULL,
  FOREIGN KEY (gifford_message_id) REFERENCES messages(message_id),
  FOREIGN KEY (hub_node_id, hub_context_id)
    REFERENCES bridge_thread_mappings (hub_node_id, hub_context_id),
  CHECK ((hub_store IS NULL) = (hub_message_id IS NULL)),
  CHECK ((hub_parent_store IS NULL) = (hub_parent_message_id IS NULL)),
  CHECK (hub_parent_message_id IS NULL OR gifford_parent_message_id IS NOT NULL),
  CHECK ((envelope_json IS NULL) = (envelope_digest IS NULL)),
  CHECK (
    (response_mode = 'NONE' AND expectation_id IS NULL AND sin_acuse = 0 AND response_basis = 'NONE')
    OR (response_mode = 'REQUIRED' AND expectation_id IS NOT NULL AND sin_acuse = 0
        AND response_basis = 'AUTHENTICATED_HIERARCHY')
    OR (response_mode = 'WAIVED' AND expectation_id IS NULL AND sin_acuse = 1
        AND response_basis = 'AUTHENTICATED_HIERARCHY')
  ),
  CHECK (
    projection_complete = 0
    OR (envelope_json IS NOT NULL AND envelope_digest IS NOT NULL
        AND projection_digest IS NOT NULL AND completed_at IS NOT NULL)
  )
);

CREATE UNIQUE INDEX bridge_v112_message_hub_id
  ON bridge_v112_message_projection (hub_node_id, hub_store, hub_message_id)
  WHERE hub_message_id IS NOT NULL;

-- +goose StatementBegin
CREATE TRIGGER bridge_v112_message_projection_write_once
BEFORE UPDATE ON bridge_v112_message_projection
WHEN NEW.gifford_message_id IS NOT OLD.gifford_message_id
  OR NEW.hub_node_id IS NOT OLD.hub_node_id
  OR (OLD.hub_store IS NOT NULL AND NEW.hub_store IS NOT OLD.hub_store)
  OR (OLD.hub_message_id IS NOT NULL AND NEW.hub_message_id IS NOT OLD.hub_message_id)
  OR NEW.message_origin IS NOT OLD.message_origin
  OR NEW.message_class IS NOT OLD.message_class
  OR NEW.signature_authority IS NOT OLD.signature_authority
  OR NEW.signing_key_id IS NOT OLD.signing_key_id
  OR (OLD.hub_context_id IS NOT NULL AND NEW.hub_context_id IS NOT OLD.hub_context_id)
  OR (OLD.hub_parent_store IS NOT NULL
      AND NEW.hub_parent_store IS NOT OLD.hub_parent_store)
  OR (OLD.hub_parent_message_id IS NOT NULL AND NEW.hub_parent_message_id IS NOT OLD.hub_parent_message_id)
  OR NEW.gifford_parent_message_id IS NOT OLD.gifford_parent_message_id
  OR NEW.gifford_thread_id IS NOT OLD.gifford_thread_id
  OR NEW.gifford_thread_ref IS NOT OLD.gifford_thread_ref
  OR NEW.thread_sequence IS NOT OLD.thread_sequence
  OR NEW.recipient_manifest_root IS NOT OLD.recipient_manifest_root
  OR NEW.recipient_count IS NOT OLD.recipient_count
  OR NEW.response_mode IS NOT OLD.response_mode
  OR NEW.expectation_id IS NOT OLD.expectation_id
  OR NEW.sin_acuse IS NOT OLD.sin_acuse
  OR NEW.response_basis IS NOT OLD.response_basis
  OR NEW.created_at IS NOT OLD.created_at
  OR (OLD.envelope_json IS NOT NULL AND NEW.envelope_json IS NOT OLD.envelope_json)
  OR (OLD.envelope_digest IS NOT NULL AND NEW.envelope_digest IS NOT OLD.envelope_digest)
  OR (OLD.projection_digest IS NOT NULL AND NEW.projection_digest IS NOT OLD.projection_digest)
  OR NEW.projection_complete < OLD.projection_complete
  OR (OLD.completed_at IS NOT NULL AND NEW.completed_at IS NOT OLD.completed_at)
BEGIN
  SELECT RAISE(ABORT, 'bridge v1.12 message projection is immutable');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER bridge_v112_message_projection_no_delete
BEFORE DELETE ON bridge_v112_message_projection
BEGIN
  SELECT RAISE(ABORT, 'bridge v1.12 message projection is durable');
END;
-- +goose StatementEnd

-- Recipient state is independent from delivery_queue. In particular, a
-- terminal history row can be retained with delivery_materialized = 0 and
-- cannot create a notice merely by existing in this projection.
CREATE TABLE bridge_recipient_projection (
  gifford_message_id TEXT NOT NULL,
  hub_node_id TEXT NOT NULL CHECK (length(hub_node_id) BETWEEN 1 AND 255),
  ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
  recipient_agent_id TEXT NOT NULL CHECK (length(recipient_agent_id) BETWEEN 1 AND 64),
  delivery_mode TEXT NOT NULL,
  requires_ack INTEGER NOT NULL CHECK (requires_ack IN (0, 1)),
  generation INTEGER NOT NULL CHECK (generation >= 1),
  merkle_proof_json TEXT NOT NULL CHECK (length(merkle_proof_json) >= 2),
  state TEXT NOT NULL CHECK (
    state IN ('PENDING', 'PROCESSING', 'ACKNOWLEDGED', 'REJECTED', 'MISMATCH',
              'EXPIRED', 'CANCELLED')
  ),
  rejection_reason TEXT CHECK (
    rejection_reason IS NULL OR rejection_reason IN ('RECIPIENT_DECLINED', 'POLICY', 'UNDELIVERABLE')
  ),
  read_at TEXT,
  ack_status TEXT CHECK (
    ack_status IS NULL OR ack_status IN ('PROCESSING', 'ACKNOWLEDGED', 'REJECTED', 'MISMATCH')
  ),
  acked_at TEXT,
  state_version INTEGER NOT NULL CHECK (state_version >= 1),
  message_committed_event_id TEXT CHECK (
    message_committed_event_id IS NULL OR (
      length(message_committed_event_id) = 36
      AND substr(message_committed_event_id, 1, 4) = 'evt-'
    )
  ),
  delivery_materialized INTEGER NOT NULL CHECK (delivery_materialized IN (0, 1)),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  PRIMARY KEY (gifford_message_id, ordinal, generation),
  UNIQUE (gifford_message_id, recipient_agent_id, generation),
  FOREIGN KEY (gifford_message_id)
    REFERENCES bridge_v112_message_projection (gifford_message_id),
  CHECK (
    (state = 'REJECTED' AND rejection_reason IS NOT NULL)
    OR (state != 'REJECTED' AND rejection_reason IS NULL)
  )
);

CREATE INDEX bridge_recipient_projection_pending
  ON bridge_recipient_projection (recipient_agent_id, state, gifford_message_id, generation);

-- +goose StatementBegin
CREATE TRIGGER bridge_recipient_projection_identity_immutable
BEFORE UPDATE ON bridge_recipient_projection
WHEN NEW.gifford_message_id IS NOT OLD.gifford_message_id
  OR NEW.hub_node_id IS NOT OLD.hub_node_id
  OR NEW.ordinal IS NOT OLD.ordinal
  OR NEW.recipient_agent_id IS NOT OLD.recipient_agent_id
  OR NEW.delivery_mode IS NOT OLD.delivery_mode
  OR NEW.requires_ack IS NOT OLD.requires_ack
  OR NEW.generation IS NOT OLD.generation
  OR NEW.merkle_proof_json IS NOT OLD.merkle_proof_json
  OR NEW.delivery_materialized IS NOT OLD.delivery_materialized
  OR NEW.created_at IS NOT OLD.created_at
  OR (OLD.message_committed_event_id IS NOT NULL
      AND NEW.message_committed_event_id IS NOT OLD.message_committed_event_id)
BEGIN
  SELECT RAISE(ABORT, 'bridge recipient identity is immutable');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER bridge_recipient_projection_read_once
BEFORE UPDATE OF read_at ON bridge_recipient_projection
WHEN OLD.read_at IS NOT NULL AND NEW.read_at IS NOT OLD.read_at
BEGIN
  SELECT RAISE(ABORT, 'bridge recipient read_at is written once');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER bridge_recipient_projection_monotonic
BEFORE UPDATE ON bridge_recipient_projection
WHEN NEW.state_version < OLD.state_version
  OR (OLD.state IN ('ACKNOWLEDGED', 'REJECTED', 'MISMATCH', 'EXPIRED', 'CANCELLED')
      AND NEW.state IS NOT OLD.state)
BEGIN
  SELECT RAISE(ABORT, 'bridge recipient state regressed');
END;
-- +goose StatementEnd

-- Per-hub stream state is the durable gate used by both legacy methods. A
-- missing row is LEGACY; existing legacy hubs are materialized on migration.
CREATE TABLE bridge_ingress_versions (
  hub_node_id TEXT NOT NULL PRIMARY KEY CHECK (length(hub_node_id) BETWEEN 1 AND 255),
  protocol_state TEXT NOT NULL CHECK (protocol_state IN ('LEGACY', 'V1_12_BOOTSTRAP', 'V1_12_READY')),
  stream_id TEXT UNIQUE CHECK (
    stream_id IS NULL OR (
      length(stream_id) = 36
      AND substr(stream_id, 1, 4) = 'stm-'
      AND substr(stream_id, 5) NOT GLOB '*[^0-9a-f]*'
    )
  ),
  start_seq INTEGER CHECK (start_seq IS NULL OR start_seq = 1),
  snapshot_at TEXT,
  snapshot_watermarks_json TEXT,
  bootstrap_expected_count INTEGER CHECK (bootstrap_expected_count IS NULL OR bootstrap_expected_count >= 0),
  bootstrap_manifest_digest TEXT CHECK (
    bootstrap_manifest_digest IS NULL OR length(bootstrap_manifest_digest) = 71
  ),
  inventory_criteria_json TEXT,
  backfill_applied_count INTEGER NOT NULL DEFAULT 0 CHECK (backfill_applied_count >= 0),
  first_event_id TEXT CHECK (
    first_event_id IS NULL OR (
      length(first_event_id) = 36 AND substr(first_event_id, 1, 4) = 'evt-'
    )
  ),
  first_receipt_id TEXT CHECK (
    first_receipt_id IS NULL OR (
      length(first_receipt_id) = 36 AND substr(first_receipt_id, 1, 4) = 'rcp-'
    )
  ),
  first_receipt_digest TEXT CHECK (
    first_receipt_digest IS NULL OR length(first_receipt_digest) = 71
  ),
  last_source_event_seq INTEGER NOT NULL DEFAULT 0 CHECK (last_source_event_seq >= 0),
  ready_event_id TEXT CHECK (
    ready_event_id IS NULL OR (
      length(ready_event_id) = 36 AND substr(ready_event_id, 1, 4) = 'evt-'
    )
  ),
  ready_receipt_id TEXT CHECK (
    ready_receipt_id IS NULL OR (
      length(ready_receipt_id) = 36 AND substr(ready_receipt_id, 1, 4) = 'rcp-'
    )
  ),
  ready_receipt_digest TEXT CHECK (
    ready_receipt_digest IS NULL OR length(ready_receipt_digest) = 71
  ),
  legacy_retired_at TEXT,
  ready_at TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE (hub_node_id, stream_id),
  CHECK (backfill_applied_count <= COALESCE(bootstrap_expected_count, backfill_applied_count)),
  CHECK ((first_receipt_id IS NULL) = (first_receipt_digest IS NULL)),
  CHECK ((ready_receipt_id IS NULL) = (ready_receipt_digest IS NULL)),
  CHECK (
    protocol_state = 'LEGACY'
    OR (stream_id IS NOT NULL AND start_seq = 1 AND snapshot_at IS NOT NULL
        AND snapshot_watermarks_json IS NOT NULL AND bootstrap_expected_count IS NOT NULL
        AND bootstrap_manifest_digest IS NOT NULL AND inventory_criteria_json IS NOT NULL
        AND first_event_id IS NOT NULL AND first_receipt_id IS NOT NULL
        AND legacy_retired_at IS NOT NULL)
  ),
  CHECK (
    protocol_state != 'V1_12_READY'
    OR (ready_event_id IS NOT NULL AND ready_receipt_id IS NOT NULL AND ready_at IS NOT NULL)
  )
);

INSERT INTO bridge_ingress_versions
  (hub_node_id, protocol_state, created_at, updated_at)
SELECT DISTINCT source_node_id, 'LEGACY',
  '2026-11-03T16:10:34.000000000Z', '2026-11-03T16:10:34.000000000Z'
FROM bridge_message_mappings;

-- +goose StatementBegin
CREATE TRIGGER bridge_ingress_versions_stream_write_once
BEFORE UPDATE ON bridge_ingress_versions
WHEN NEW.hub_node_id IS NOT OLD.hub_node_id
  OR (OLD.stream_id IS NOT NULL AND NEW.stream_id IS NOT OLD.stream_id)
  OR (OLD.start_seq IS NOT NULL AND NEW.start_seq IS NOT OLD.start_seq)
  OR (OLD.snapshot_at IS NOT NULL AND NEW.snapshot_at IS NOT OLD.snapshot_at)
  OR (OLD.snapshot_watermarks_json IS NOT NULL
      AND NEW.snapshot_watermarks_json IS NOT OLD.snapshot_watermarks_json)
  OR (OLD.bootstrap_expected_count IS NOT NULL
      AND NEW.bootstrap_expected_count IS NOT OLD.bootstrap_expected_count)
  OR (OLD.bootstrap_manifest_digest IS NOT NULL
      AND NEW.bootstrap_manifest_digest IS NOT OLD.bootstrap_manifest_digest)
  OR (OLD.inventory_criteria_json IS NOT NULL
      AND NEW.inventory_criteria_json IS NOT OLD.inventory_criteria_json)
  OR (OLD.first_event_id IS NOT NULL AND NEW.first_event_id IS NOT OLD.first_event_id)
  OR (OLD.first_receipt_id IS NOT NULL AND NEW.first_receipt_id IS NOT OLD.first_receipt_id)
  OR (OLD.first_receipt_digest IS NOT NULL
      AND NEW.first_receipt_digest IS NOT OLD.first_receipt_digest)
  OR (OLD.ready_event_id IS NOT NULL AND NEW.ready_event_id IS NOT OLD.ready_event_id)
  OR (OLD.ready_receipt_id IS NOT NULL AND NEW.ready_receipt_id IS NOT OLD.ready_receipt_id)
  OR (OLD.ready_receipt_digest IS NOT NULL
      AND NEW.ready_receipt_digest IS NOT OLD.ready_receipt_digest)
  OR (OLD.legacy_retired_at IS NOT NULL AND NEW.legacy_retired_at IS NOT OLD.legacy_retired_at)
  OR (OLD.ready_at IS NOT NULL AND NEW.ready_at IS NOT OLD.ready_at)
  OR NEW.created_at IS NOT OLD.created_at
BEGIN
  SELECT RAISE(ABORT, 'bridge ingress stream identity is written once');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER bridge_ingress_versions_transition
BEFORE UPDATE ON bridge_ingress_versions
WHEN NOT (
  NEW.protocol_state IS OLD.protocol_state
  OR (OLD.protocol_state = 'LEGACY' AND NEW.protocol_state = 'V1_12_BOOTSTRAP')
  OR (OLD.protocol_state = 'V1_12_BOOTSTRAP' AND NEW.protocol_state = 'V1_12_READY')
)
  OR NEW.backfill_applied_count < OLD.backfill_applied_count
  OR NEW.last_source_event_seq < OLD.last_source_event_seq
  OR NEW.updated_at < OLD.updated_at
BEGIN
  SELECT RAISE(ABORT, 'bridge ingress state regressed');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER bridge_ingress_versions_no_delete
BEFORE DELETE ON bridge_ingress_versions
BEGIN
  SELECT RAISE(ABORT, 'bridge ingress version is durable');
END;
-- +goose StatementEnd

-- A legacy integer that exists in both hub stores is not guessed. The event
-- and reference are retained without creating a message, mapping or delivery.
CREATE TABLE bridge_hub_reference_quarantine (
  hub_node_id TEXT NOT NULL CHECK (length(hub_node_id) BETWEEN 1 AND 255),
  stream_id TEXT NOT NULL CHECK (
    length(stream_id) = 36 AND substr(stream_id, 1, 4) = 'stm-'
  ),
  source_event_seq INTEGER NOT NULL CHECK (source_event_seq >= 1),
  event_id TEXT NOT NULL CHECK (
    length(event_id) = 36 AND substr(event_id, 1, 4) = 'evt-'
  ),
  reference_role TEXT NOT NULL CHECK (reference_role IN ('MESSAGE', 'PARENT')),
  hub_message_id INTEGER NOT NULL CHECK (hub_message_id > 0),
  resolution_state TEXT NOT NULL CHECK (resolution_state = 'AMBIGUOUS'),
  reference_digest TEXT NOT NULL CHECK (length(reference_digest) = 71),
  created_at TEXT NOT NULL,
  PRIMARY KEY (hub_node_id, stream_id, source_event_seq, reference_role),
  UNIQUE (hub_node_id, event_id, reference_role),
  FOREIGN KEY (hub_node_id, stream_id)
    REFERENCES bridge_ingress_versions (hub_node_id, stream_id)
);

CREATE INDEX bridge_hub_reference_quarantine_unresolved
  ON bridge_hub_reference_quarantine
    (hub_node_id, resolution_state, hub_message_id);

-- +goose StatementBegin
CREATE TRIGGER bridge_hub_reference_quarantine_immutable
BEFORE UPDATE ON bridge_hub_reference_quarantine
BEGIN
  SELECT RAISE(ABORT, 'ambiguous hub reference is immutable');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER bridge_hub_reference_quarantine_no_delete
BEFORE DELETE ON bridge_hub_reference_quarantine
BEGIN
  SELECT RAISE(ABORT, 'ambiguous hub reference is durable');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER bridge_ingress_versions_no_ambiguous_ready
BEFORE UPDATE OF protocol_state ON bridge_ingress_versions
WHEN NEW.protocol_state = 'V1_12_READY'
  AND EXISTS (
    SELECT 1 FROM bridge_hub_reference_quarantine
    WHERE hub_node_id = NEW.hub_node_id AND resolution_state = 'AMBIGUOUS'
  )
BEGIN
  SELECT RAISE(ABORT, 'ambiguous hub reference blocks v1.12 readiness');
END;
-- +goose StatementEnd

CREATE TABLE bridge_ingress_ledger (
  hub_node_id TEXT NOT NULL CHECK (length(hub_node_id) BETWEEN 1 AND 255),
  stream_id TEXT NOT NULL CHECK (
    length(stream_id) = 36 AND substr(stream_id, 1, 4) = 'stm-'
  ),
  source_event_seq INTEGER NOT NULL CHECK (source_event_seq >= 1),
  event_id TEXT NOT NULL CHECK (
    length(event_id) = 36 AND substr(event_id, 1, 4) = 'evt-'
  ),
  event_type TEXT NOT NULL CHECK (
    event_type IN ('MIRROR_STREAM_START', 'MESSAGE_COMMITTED', 'RECIPIENT_READ',
                   'RECIPIENT_STATE', 'MIRROR_STREAM_READY')
  ),
  payload_json TEXT NOT NULL,
  payload_digest TEXT NOT NULL CHECK (length(payload_digest) = 71),
  projection_digest TEXT CHECK (projection_digest IS NULL OR length(projection_digest) = 71),
  watermarks_after_json TEXT NOT NULL,
  application_receipt_id TEXT NOT NULL CHECK (
    length(application_receipt_id) = 36 AND substr(application_receipt_id, 1, 4) = 'rcp-'
  ),
  application_receipt_digest TEXT NOT NULL CHECK (length(application_receipt_digest) = 71),
  disposition TEXT NOT NULL CHECK (disposition IN ('CREATED', 'DUPLICATE', 'UPGRADED_LEGACY')),
  applied_at TEXT NOT NULL,
  PRIMARY KEY (hub_node_id, stream_id, source_event_seq),
  UNIQUE (hub_node_id, event_id),
  UNIQUE (application_receipt_id),
  FOREIGN KEY (hub_node_id, stream_id)
    REFERENCES bridge_ingress_versions (hub_node_id, stream_id)
);

-- +goose StatementBegin
CREATE TRIGGER bridge_ingress_ledger_immutable
BEFORE UPDATE ON bridge_ingress_ledger
BEGIN
  SELECT RAISE(ABORT, 'bridge ingress ledger is immutable');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER bridge_ingress_ledger_no_delete
BEFORE DELETE ON bridge_ingress_ledger
BEGIN
  SELECT RAISE(ABORT, 'bridge ingress ledger is durable');
END;
-- +goose StatementEnd

CREATE TABLE bridge_watermarks (
  gifford_node_id TEXT NOT NULL CHECK (length(gifford_node_id) BETWEEN 1 AND 128),
  hub_node_id TEXT NOT NULL CHECK (length(hub_node_id) BETWEEN 1 AND 255),
  direction TEXT NOT NULL CHECK (direction IN ('CAPYCOM_TO_GIFFORD', 'GIFFORD_TO_CAPYCOM')),
  admissions_produced INTEGER NOT NULL DEFAULT 0 CHECK (admissions_produced >= 0),
  admissions_applied INTEGER NOT NULL DEFAULT 0 CHECK (admissions_applied >= 0),
  admissions_acked INTEGER NOT NULL DEFAULT 0 CHECK (admissions_acked >= 0),
  reads_produced INTEGER NOT NULL DEFAULT 0 CHECK (reads_produced >= 0),
  reads_applied INTEGER NOT NULL DEFAULT 0 CHECK (reads_applied >= 0),
  reads_acked INTEGER NOT NULL DEFAULT 0 CHECK (reads_acked >= 0),
  responses_produced INTEGER NOT NULL DEFAULT 0 CHECK (responses_produced >= 0),
  responses_applied INTEGER NOT NULL DEFAULT 0 CHECK (responses_applied >= 0),
  responses_acked INTEGER NOT NULL DEFAULT 0 CHECK (responses_acked >= 0),
  recipient_states_produced INTEGER NOT NULL DEFAULT 0 CHECK (recipient_states_produced >= 0),
  recipient_states_applied INTEGER NOT NULL DEFAULT 0 CHECK (recipient_states_applied >= 0),
  recipient_states_acked INTEGER NOT NULL DEFAULT 0 CHECK (recipient_states_acked >= 0),
  updated_at TEXT NOT NULL,
  PRIMARY KEY (gifford_node_id, hub_node_id, direction),
  CHECK (admissions_acked <= admissions_applied AND admissions_applied <= admissions_produced),
  CHECK (reads_acked <= reads_applied AND reads_applied <= reads_produced),
  CHECK (responses_acked <= responses_applied AND responses_applied <= responses_produced),
  CHECK (recipient_states_acked <= recipient_states_applied
         AND recipient_states_applied <= recipient_states_produced)
);

-- +goose StatementBegin
CREATE TRIGGER bridge_watermarks_monotonic
BEFORE UPDATE ON bridge_watermarks
WHEN NEW.gifford_node_id IS NOT OLD.gifford_node_id
  OR NEW.hub_node_id IS NOT OLD.hub_node_id
  OR NEW.direction IS NOT OLD.direction
  OR NEW.admissions_produced < OLD.admissions_produced
  OR NEW.admissions_applied < OLD.admissions_applied
  OR NEW.admissions_acked < OLD.admissions_acked
  OR NEW.reads_produced < OLD.reads_produced
  OR NEW.reads_applied < OLD.reads_applied
  OR NEW.reads_acked < OLD.reads_acked
  OR NEW.responses_produced < OLD.responses_produced
  OR NEW.responses_applied < OLD.responses_applied
  OR NEW.responses_acked < OLD.responses_acked
  OR NEW.recipient_states_produced < OLD.recipient_states_produced
  OR NEW.recipient_states_applied < OLD.recipient_states_applied
  OR NEW.recipient_states_acked < OLD.recipient_states_acked
  OR NEW.updated_at < OLD.updated_at
BEGIN
  SELECT RAISE(ABORT, 'bridge watermark regressed');
END;
-- +goose StatementEnd

-- System principals asserted by a hub are bound to one registered Gifford
-- agent under the roster digest that authorized the relation.
CREATE TABLE bridge_system_principal_mappings (
  hub_node_id TEXT NOT NULL CHECK (length(hub_node_id) BETWEEN 1 AND 255),
  hub_principal_id TEXT NOT NULL CHECK (length(hub_principal_id) BETWEEN 1 AND 128),
  gifford_agent_id TEXT NOT NULL REFERENCES agents(agent_id),
  roster_digest TEXT NOT NULL CHECK (length(roster_digest) = 71),
  created_at TEXT NOT NULL,
  PRIMARY KEY (hub_node_id, hub_principal_id)
);

-- +goose StatementBegin
CREATE TRIGGER bridge_system_principal_mappings_immutable
BEFORE UPDATE ON bridge_system_principal_mappings
BEGIN
  SELECT RAISE(ABORT, 'bridge system principal mapping is immutable');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER bridge_system_principal_mappings_no_delete
BEFORE DELETE ON bridge_system_principal_mappings
BEGIN
  SELECT RAISE(ABORT, 'bridge system principal mapping is durable');
END;
-- +goose StatementEnd

-- Admission composes and signs the exact envelope in one operation. The hub
-- id is absent until the mirror apply receipt assigns it, then is written once.
CREATE TABLE bridge_forward_requests (
  hub_node_id TEXT NOT NULL CHECK (length(hub_node_id) BETWEEN 1 AND 255),
  authenticated_agent_id TEXT NOT NULL CHECK (length(authenticated_agent_id) BETWEEN 1 AND 64),
  request_id TEXT NOT NULL CHECK (
    length(request_id) = 36 AND substr(request_id, 1, 4) = 'req-'
  ),
  auth_context_id TEXT NOT NULL CHECK (length(auth_context_id) BETWEEN 1 AND 128),
  source_fence TEXT NOT NULL CHECK (length(source_fence) BETWEEN 1 AND 128),
  expected_mode_version INTEGER NOT NULL CHECK (expected_mode_version >= 1),
  hub_scope_id TEXT NOT NULL CHECK (length(hub_scope_id) BETWEEN 1 AND 128),
  scope_version INTEGER NOT NULL CHECK (scope_version >= 1),
  origin_gifford_node_id TEXT NOT NULL CHECK (length(origin_gifford_node_id) BETWEEN 1 AND 128),
  destination_gifford_node_id TEXT NOT NULL CHECK (length(destination_gifford_node_id) BETWEEN 1 AND 128),
  admission_digest TEXT NOT NULL CHECK (length(admission_digest) = 71),
  envelope_json TEXT NOT NULL,
  envelope_digest TEXT NOT NULL CHECK (length(envelope_digest) = 71),
  signature_authority TEXT NOT NULL CHECK (
    signature_authority IN ('AGENT_ENDPOINT', 'NODE_FORWARD', 'NODE_EMITTER')
  ),
  signing_key_id TEXT NOT NULL CHECK (length(signing_key_id) BETWEEN 1 AND 128),
  admission_result_json TEXT NOT NULL,
  admission_receipt_id TEXT NOT NULL CHECK (
    length(admission_receipt_id) = 36 AND substr(admission_receipt_id, 1, 4) = 'rcp-'
  ),
  admission_receipt_digest TEXT NOT NULL CHECK (length(admission_receipt_digest) = 71),
  gifford_message_id TEXT REFERENCES messages(message_id),
  hub_store TEXT CHECK (hub_store IS NULL OR hub_store IN ('messages', 'cv2_outbound')),
  hub_message_id INTEGER CHECK (hub_message_id IS NULL OR hub_message_id > 0),
  state TEXT NOT NULL CHECK (state IN ('ADMITTED', 'REJECTED')),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  PRIMARY KEY (hub_node_id, authenticated_agent_id, request_id),
  UNIQUE (gifford_message_id),
  UNIQUE (admission_receipt_id),
  CHECK ((hub_store IS NULL) = (hub_message_id IS NULL)),
  CHECK (state != 'ADMITTED' OR gifford_message_id IS NOT NULL)
);

CREATE UNIQUE INDEX bridge_forward_requests_hub_message
  ON bridge_forward_requests (hub_node_id, hub_store, hub_message_id)
  WHERE hub_message_id IS NOT NULL;

-- +goose StatementBegin
CREATE TRIGGER bridge_forward_requests_identity_immutable
BEFORE UPDATE ON bridge_forward_requests
WHEN NEW.hub_node_id IS NOT OLD.hub_node_id
  OR NEW.authenticated_agent_id IS NOT OLD.authenticated_agent_id
  OR NEW.request_id IS NOT OLD.request_id
  OR NEW.auth_context_id IS NOT OLD.auth_context_id
  OR NEW.source_fence IS NOT OLD.source_fence
  OR NEW.expected_mode_version IS NOT OLD.expected_mode_version
  OR NEW.hub_scope_id IS NOT OLD.hub_scope_id
  OR NEW.scope_version IS NOT OLD.scope_version
  OR NEW.origin_gifford_node_id IS NOT OLD.origin_gifford_node_id
  OR NEW.destination_gifford_node_id IS NOT OLD.destination_gifford_node_id
  OR NEW.admission_digest IS NOT OLD.admission_digest
  OR NEW.envelope_json IS NOT OLD.envelope_json
  OR NEW.envelope_digest IS NOT OLD.envelope_digest
  OR NEW.signature_authority IS NOT OLD.signature_authority
  OR NEW.signing_key_id IS NOT OLD.signing_key_id
  OR NEW.admission_result_json IS NOT OLD.admission_result_json
  OR NEW.admission_receipt_id IS NOT OLD.admission_receipt_id
  OR NEW.admission_receipt_digest IS NOT OLD.admission_receipt_digest
  OR NEW.gifford_message_id IS NOT OLD.gifford_message_id
  OR NEW.state IS NOT OLD.state
  OR NEW.created_at IS NOT OLD.created_at
  OR (OLD.hub_store IS NOT NULL AND NEW.hub_store IS NOT OLD.hub_store)
  OR (OLD.hub_message_id IS NOT NULL AND NEW.hub_message_id IS NOT OLD.hub_message_id)
BEGIN
  SELECT RAISE(ABORT, 'bridge forward request is immutable');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER bridge_forward_requests_no_delete
BEFORE DELETE ON bridge_forward_requests
BEGIN
  SELECT RAISE(ABORT, 'bridge forward requests are durable');
END;
-- +goose StatementEnd

-- +goose Down
DROP TRIGGER bridge_forward_requests_no_delete;
DROP TRIGGER bridge_forward_requests_identity_immutable;
DROP INDEX bridge_forward_requests_hub_message;
DROP TABLE bridge_forward_requests;
DROP TRIGGER bridge_system_principal_mappings_no_delete;
DROP TRIGGER bridge_system_principal_mappings_immutable;
DROP TABLE bridge_system_principal_mappings;
DROP TRIGGER bridge_watermarks_monotonic;
DROP TABLE bridge_watermarks;
DROP TRIGGER bridge_ingress_ledger_no_delete;
DROP TRIGGER bridge_ingress_ledger_immutable;
DROP TABLE bridge_ingress_ledger;
DROP TRIGGER bridge_ingress_versions_no_ambiguous_ready;
DROP TRIGGER bridge_hub_reference_quarantine_no_delete;
DROP TRIGGER bridge_hub_reference_quarantine_immutable;
DROP INDEX bridge_hub_reference_quarantine_unresolved;
DROP TABLE bridge_hub_reference_quarantine;
DROP TRIGGER bridge_ingress_versions_no_delete;
DROP TRIGGER bridge_ingress_versions_transition;
DROP TRIGGER bridge_ingress_versions_stream_write_once;
DROP TABLE bridge_ingress_versions;
DROP TRIGGER bridge_recipient_projection_monotonic;
DROP TRIGGER bridge_recipient_projection_read_once;
DROP TRIGGER bridge_recipient_projection_identity_immutable;
DROP INDEX bridge_recipient_projection_pending;
DROP TABLE bridge_recipient_projection;
DROP TRIGGER bridge_v112_message_projection_no_delete;
DROP TRIGGER bridge_v112_message_projection_write_once;
DROP INDEX bridge_v112_message_hub_id;
DROP TABLE bridge_v112_message_projection;
DROP TRIGGER bridge_message_mappings_no_delete;
DROP TRIGGER bridge_message_v112_fields_write_once;
DROP TRIGGER bridge_message_v112_fields_consistent_update;
DROP TRIGGER bridge_message_v112_fields_consistent_insert;
DROP INDEX bridge_message_mapping_thread_sequence;
DROP INDEX bridge_message_mapping_identity;
ALTER TABLE bridge_message_mappings DROP COLUMN mapping_receipt_digest;
ALTER TABLE bridge_message_mappings DROP COLUMN mapping_receipt_id;
ALTER TABLE bridge_message_mappings DROP COLUMN projection_digest;
ALTER TABLE bridge_message_mappings DROP COLUMN envelope_digest;
ALTER TABLE bridge_message_mappings DROP COLUMN recipient_count;
ALTER TABLE bridge_message_mappings DROP COLUMN recipient_manifest_root;
ALTER TABLE bridge_message_mappings DROP COLUMN thread_sequence;
ALTER TABLE bridge_message_mappings DROP COLUMN gifford_thread_ref;
ALTER TABLE bridge_message_mappings DROP COLUMN gifford_thread_id;
ALTER TABLE bridge_message_mappings DROP COLUMN hub_parent_store;
ALTER TABLE bridge_message_mappings DROP COLUMN hub_store;
DROP TRIGGER bridge_thread_mappings_no_delete;
DROP TRIGGER bridge_thread_mappings_immutable;
DROP TABLE bridge_thread_mappings;
DROP TRIGGER bridge_source_cuts_no_delete;
DROP TRIGGER bridge_source_cuts_phase_transition;
DROP TRIGGER bridge_source_cuts_receipts_write_once;
DROP TRIGGER bridge_source_cuts_identity_immutable;
DROP INDEX bridge_source_cuts_active_scope;
DROP TABLE bridge_source_cuts;
DROP TRIGGER bridge_source_mode_no_delete;
DROP TRIGGER bridge_source_mode_cas;
DROP TABLE bridge_source_mode;
