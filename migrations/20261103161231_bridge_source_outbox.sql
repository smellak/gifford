-- +goose Up
-- One immutable message base is shared by every recipient shard. It is not an
-- event queue row: inbound mirrors and history-only projections never acquire
-- an outbox base and therefore cannot echo back to their source.
CREATE TABLE bridge_outbox_message_bases (
  gifford_message_id TEXT NOT NULL PRIMARY KEY,
  hub_node_id TEXT NOT NULL CHECK (length(hub_node_id) BETWEEN 1 AND 255),
  hub_store TEXT CHECK (hub_store IS NULL OR hub_store IN ('messages', 'cv2_outbound')),
  hub_message_id INTEGER CHECK (hub_message_id IS NULL OR hub_message_id > 0),
  hub_parent_store TEXT CHECK (
    hub_parent_store IS NULL OR hub_parent_store IN ('messages', 'cv2_outbound')
  ),
  hub_parent_message_id INTEGER CHECK (hub_parent_message_id > 0),
  gifford_parent_message_id TEXT REFERENCES messages(message_id),
  hub_context_id TEXT CHECK (hub_context_id IS NULL OR length(hub_context_id) BETWEEN 1 AND 512),
  gifford_thread_id TEXT NOT NULL,
  gifford_thread_ref TEXT NOT NULL,
  thread_sequence INTEGER NOT NULL CHECK (thread_sequence >= 1),
  sender_agent_id TEXT NOT NULL CHECK (length(sender_agent_id) BETWEEN 1 AND 64),
  message_class TEXT NOT NULL CHECK (message_class IN ('USER', 'SYSTEM', 'RESPONSE_WATCH_NOTICE')),
  signature_authority TEXT NOT NULL CHECK (
    signature_authority IN ('AGENT_ENDPOINT', 'NODE_FORWARD', 'NODE_EMITTER')
  ),
  signing_key_id TEXT NOT NULL CHECK (length(signing_key_id) BETWEEN 1 AND 128),
  envelope_json TEXT NOT NULL,
  envelope_digest TEXT NOT NULL CHECK (length(envelope_digest) = 71),
  projection_digest TEXT NOT NULL CHECK (length(projection_digest) = 71),
  response_mode TEXT NOT NULL CHECK (response_mode IN ('NONE', 'REQUIRED', 'WAIVED')),
  expectation_id TEXT CHECK (
    expectation_id IS NULL OR (
      length(expectation_id) = 36 AND substr(expectation_id, 1, 4) = 'rsp-'
    )
  ),
  sin_acuse INTEGER NOT NULL CHECK (sin_acuse IN (0, 1)),
  response_basis TEXT NOT NULL CHECK (response_basis IN ('NONE', 'AUTHENTICATED_HIERARCHY')),
  recipient_manifest_root TEXT NOT NULL CHECK (length(recipient_manifest_root) = 71),
  recipient_count INTEGER NOT NULL CHECK (recipient_count >= 1),
  canonical_base_json TEXT NOT NULL,
  base_digest TEXT NOT NULL CHECK (length(base_digest) = 71),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY (gifford_message_id)
    REFERENCES bridge_v112_message_projection (gifford_message_id),
  CHECK ((hub_store IS NULL) = (hub_message_id IS NULL)),
  CHECK ((hub_parent_store IS NULL) = (hub_parent_message_id IS NULL)),
  CHECK (hub_parent_message_id IS NULL OR gifford_parent_message_id IS NOT NULL),
  CHECK (
    (response_mode = 'NONE' AND expectation_id IS NULL AND sin_acuse = 0 AND response_basis = 'NONE')
    OR (response_mode = 'REQUIRED' AND expectation_id IS NOT NULL AND sin_acuse = 0
        AND response_basis = 'AUTHENTICATED_HIERARCHY')
    OR (response_mode = 'WAIVED' AND expectation_id IS NULL AND sin_acuse = 1
        AND response_basis = 'AUTHENTICATED_HIERARCHY')
  )
);

CREATE UNIQUE INDEX bridge_outbox_base_hub_id
  ON bridge_outbox_message_bases (hub_node_id, hub_store, hub_message_id)
  WHERE hub_message_id IS NOT NULL;

-- +goose StatementBegin
CREATE TRIGGER bridge_outbox_message_bases_immutable
BEFORE UPDATE ON bridge_outbox_message_bases
WHEN NEW.gifford_message_id IS NOT OLD.gifford_message_id
  OR NEW.hub_node_id IS NOT OLD.hub_node_id
  OR (OLD.hub_store IS NOT NULL AND NEW.hub_store IS NOT OLD.hub_store)
  OR (OLD.hub_message_id IS NOT NULL AND NEW.hub_message_id IS NOT OLD.hub_message_id)
  OR (OLD.hub_parent_store IS NOT NULL
      AND NEW.hub_parent_store IS NOT OLD.hub_parent_store)
  OR (OLD.hub_parent_message_id IS NOT NULL
      AND NEW.hub_parent_message_id IS NOT OLD.hub_parent_message_id)
  OR NEW.gifford_parent_message_id IS NOT OLD.gifford_parent_message_id
  OR (OLD.hub_context_id IS NOT NULL AND NEW.hub_context_id IS NOT OLD.hub_context_id)
  OR NEW.gifford_thread_id IS NOT OLD.gifford_thread_id
  OR NEW.gifford_thread_ref IS NOT OLD.gifford_thread_ref
  OR NEW.thread_sequence IS NOT OLD.thread_sequence
  OR NEW.sender_agent_id IS NOT OLD.sender_agent_id
  OR NEW.message_class IS NOT OLD.message_class
  OR NEW.signature_authority IS NOT OLD.signature_authority
  OR NEW.signing_key_id IS NOT OLD.signing_key_id
  OR NEW.envelope_json IS NOT OLD.envelope_json
  OR NEW.envelope_digest IS NOT OLD.envelope_digest
  OR NEW.projection_digest IS NOT OLD.projection_digest
  OR NEW.response_mode IS NOT OLD.response_mode
  OR NEW.expectation_id IS NOT OLD.expectation_id
  OR NEW.sin_acuse IS NOT OLD.sin_acuse
  OR NEW.response_basis IS NOT OLD.response_basis
  OR NEW.recipient_manifest_root IS NOT OLD.recipient_manifest_root
  OR NEW.recipient_count IS NOT OLD.recipient_count
  OR NEW.canonical_base_json IS NOT OLD.canonical_base_json
  OR NEW.base_digest IS NOT OLD.base_digest
  OR NEW.created_at IS NOT OLD.created_at
  OR NEW.updated_at < OLD.updated_at
BEGIN
  SELECT RAISE(ABORT, 'bridge outbox message base is immutable');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER bridge_outbox_message_bases_no_delete
BEFORE DELETE ON bridge_outbox_message_bases
BEGIN
  SELECT RAISE(ABORT, 'bridge outbox message base is durable');
END;
-- +goose StatementEnd

-- Sequence allocation and acknowledgement are isolated by recipient. A poison
-- shard can stop this row only; no global outbox head exists.
CREATE TABLE bridge_outbox_partitions (
  hub_node_id TEXT NOT NULL CHECK (length(hub_node_id) BETWEEN 1 AND 255),
  recipient_agent_id TEXT NOT NULL CHECK (length(recipient_agent_id) BETWEEN 1 AND 64),
  next_sequence INTEGER NOT NULL DEFAULT 1 CHECK (next_sequence >= 1),
  acked_through INTEGER NOT NULL DEFAULT 0 CHECK (acked_through >= 0),
  produced_watermarks_json TEXT NOT NULL,
  acked_watermarks_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  PRIMARY KEY (hub_node_id, recipient_agent_id),
  CHECK (acked_through < next_sequence)
);

-- +goose StatementBegin
CREATE TRIGGER bridge_outbox_partitions_monotonic
BEFORE UPDATE ON bridge_outbox_partitions
WHEN NEW.hub_node_id IS NOT OLD.hub_node_id
  OR NEW.recipient_agent_id IS NOT OLD.recipient_agent_id
  OR NEW.next_sequence < OLD.next_sequence
  OR NEW.acked_through < OLD.acked_through
  OR NEW.updated_at < OLD.updated_at
BEGIN
  SELECT RAISE(ABORT, 'bridge outbox partition regressed');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER bridge_outbox_partitions_no_delete
BEFORE DELETE ON bridge_outbox_partitions
BEGIN
  SELECT RAISE(ABORT, 'bridge outbox partition is durable');
END;
-- +goose StatementEnd

CREATE TABLE bridge_outbox (
  event_id TEXT NOT NULL PRIMARY KEY CHECK (
    length(event_id) = 36 AND substr(event_id, 1, 4) = 'evt-'
  ),
  logical_event_id TEXT NOT NULL CHECK (
    length(logical_event_id) = 36 AND substr(logical_event_id, 1, 4) = 'evt-'
  ),
  event_type TEXT NOT NULL CHECK (
    event_type IN ('MESSAGE_COMMITTED', 'RECIPIENT_READ', 'RECIPIENT_STATE')
  ),
  gifford_message_id TEXT NOT NULL,
  hub_node_id TEXT NOT NULL CHECK (length(hub_node_id) BETWEEN 1 AND 255),
  hub_store TEXT CHECK (hub_store IS NULL OR hub_store IN ('messages', 'cv2_outbound')),
  hub_message_id INTEGER CHECK (hub_message_id IS NULL OR hub_message_id > 0),
  recipient_agent_id TEXT NOT NULL CHECK (length(recipient_agent_id) BETWEEN 1 AND 64),
  recipient_ordinal INTEGER NOT NULL CHECK (recipient_ordinal >= 0),
  recipient_generation INTEGER NOT NULL CHECK (recipient_generation >= 1),
  partition_seq INTEGER NOT NULL CHECK (partition_seq >= 1),
  recipient_projection_json TEXT NOT NULL,
  merkle_proof_json TEXT NOT NULL,
  canonical_payload_json TEXT NOT NULL,
  payload_digest TEXT NOT NULL CHECK (length(payload_digest) = 71),
  projection_digest TEXT NOT NULL CHECK (length(projection_digest) = 71),
  watermarks_after_json TEXT NOT NULL,
  depends_on_event_id TEXT REFERENCES bridge_outbox(event_id),
  state TEXT NOT NULL CHECK (state IN ('READY', 'RETRY_WAIT', 'BLOCKED', 'UNCERTAIN', 'ACKED')),
  attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
  next_attempt_at TEXT,
  last_error_class TEXT CHECK (
    last_error_class IS NULL OR last_error_class IN ('RETRYABLE', 'TERMINAL', 'UNCERTAIN')
  ),
  last_error_code TEXT CHECK (
    last_error_code IS NULL OR last_error_code IN (
      'HUB_STORAGE_BUSY', 'NOT_FOUND', 'HUB_APPLY_OUTCOME_UNKNOWN',
      'PROJECTION_CONFLICT', 'CIPHERTEXT_DIGEST_MISMATCH',
      'PARENT_MAPPING_NOT_FOUND', 'READ_BEFORE_MIRROR', 'IDENTITY_ASSERTION_INVALID'
    )
  ),
  last_diagnostic_digest TEXT CHECK (
    last_diagnostic_digest IS NULL OR length(last_diagnostic_digest) = 71
  ),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE (hub_node_id, recipient_agent_id, partition_seq),
  UNIQUE (logical_event_id, recipient_ordinal),
  FOREIGN KEY (hub_node_id, recipient_agent_id)
    REFERENCES bridge_outbox_partitions (hub_node_id, recipient_agent_id),
  FOREIGN KEY (gifford_message_id)
    REFERENCES bridge_outbox_message_bases (gifford_message_id),
  FOREIGN KEY (gifford_message_id, recipient_ordinal, recipient_generation)
    REFERENCES bridge_recipient_projection (gifford_message_id, ordinal, generation),
  CHECK ((hub_store IS NULL) = (hub_message_id IS NULL)),
  CHECK (
    (state = 'RETRY_WAIT' AND next_attempt_at IS NOT NULL)
    OR (state != 'RETRY_WAIT' AND next_attempt_at IS NULL)
  ),
  CHECK ((last_error_class IS NULL) = (last_error_code IS NULL)),
  CHECK ((last_error_code IS NULL) = (last_diagnostic_digest IS NULL)),
  CHECK (
    last_error_code IS NULL
    OR (last_error_class = 'RETRYABLE'
        AND last_error_code IN ('HUB_STORAGE_BUSY', 'NOT_FOUND'))
    OR (last_error_class = 'UNCERTAIN' AND last_error_code = 'HUB_APPLY_OUTCOME_UNKNOWN')
    OR (last_error_class = 'TERMINAL' AND last_error_code IN (
      'PROJECTION_CONFLICT', 'CIPHERTEXT_DIGEST_MISMATCH',
      'PARENT_MAPPING_NOT_FOUND', 'READ_BEFORE_MIRROR', 'IDENTITY_ASSERTION_INVALID'
    ))
  )
);

CREATE INDEX bridge_outbox_ready
  ON bridge_outbox (state, next_attempt_at, hub_node_id, recipient_agent_id, partition_seq);
CREATE INDEX bridge_outbox_dependency
  ON bridge_outbox (depends_on_event_id)
  WHERE depends_on_event_id IS NOT NULL;

-- +goose StatementBegin
CREATE TRIGGER bridge_outbox_identity_immutable
BEFORE UPDATE ON bridge_outbox
WHEN NEW.event_id IS NOT OLD.event_id
  OR NEW.logical_event_id IS NOT OLD.logical_event_id
  OR NEW.event_type IS NOT OLD.event_type
  OR NEW.gifford_message_id IS NOT OLD.gifford_message_id
  OR NEW.hub_node_id IS NOT OLD.hub_node_id
  OR NEW.hub_store IS NOT OLD.hub_store
  OR NEW.hub_message_id IS NOT OLD.hub_message_id
  OR NEW.recipient_agent_id IS NOT OLD.recipient_agent_id
  OR NEW.recipient_ordinal IS NOT OLD.recipient_ordinal
  OR NEW.recipient_generation IS NOT OLD.recipient_generation
  OR NEW.partition_seq IS NOT OLD.partition_seq
  OR NEW.recipient_projection_json IS NOT OLD.recipient_projection_json
  OR NEW.merkle_proof_json IS NOT OLD.merkle_proof_json
  OR NEW.canonical_payload_json IS NOT OLD.canonical_payload_json
  OR NEW.payload_digest IS NOT OLD.payload_digest
  OR NEW.projection_digest IS NOT OLD.projection_digest
  OR NEW.watermarks_after_json IS NOT OLD.watermarks_after_json
  OR NEW.depends_on_event_id IS NOT OLD.depends_on_event_id
  OR NEW.created_at IS NOT OLD.created_at
BEGIN
  SELECT RAISE(ABORT, 'bridge outbox event identity is immutable');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER bridge_outbox_state_transition
BEFORE UPDATE ON bridge_outbox
WHEN NEW.attempt_count < OLD.attempt_count
  OR NEW.updated_at < OLD.updated_at
  OR NOT (
    NEW.state IS OLD.state
    OR (OLD.state = 'READY' AND NEW.state IN ('RETRY_WAIT', 'BLOCKED', 'UNCERTAIN', 'ACKED'))
    OR (OLD.state = 'RETRY_WAIT' AND NEW.state IN ('READY', 'BLOCKED', 'UNCERTAIN', 'ACKED'))
    OR (OLD.state = 'UNCERTAIN' AND NEW.state = 'ACKED')
  )
BEGIN
  SELECT RAISE(ABORT, 'invalid bridge outbox transition');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER bridge_outbox_no_delete
BEFORE DELETE ON bridge_outbox
BEGIN
  SELECT RAISE(ABORT, 'bridge outbox events are durable');
END;
-- +goose StatementEnd

-- ACK and NACK attempts are append-only evidence. The current outbox state is
-- a projection; these receipts are the recovery source after a crash.
CREATE TABLE bridge_outbox_receipts (
  receipt_id TEXT NOT NULL PRIMARY KEY CHECK (
    length(receipt_id) = 36 AND substr(receipt_id, 1, 4) = 'rcp-'
  ),
  event_id TEXT NOT NULL REFERENCES bridge_outbox(event_id),
  receipt_type TEXT NOT NULL CHECK (
    receipt_type IN ('ACK', 'NACK_RETRYABLE', 'NACK_TERMINAL', 'NACK_UNCERTAIN')
  ),
  attempt_id TEXT CHECK (attempt_id IS NULL OR length(attempt_id) BETWEEN 1 AND 128),
  hub_apply_receipt_id TEXT CHECK (
    hub_apply_receipt_id IS NULL OR (
      length(hub_apply_receipt_id) = 36 AND substr(hub_apply_receipt_id, 1, 4) = 'rcp-'
    )
  ),
  hub_apply_receipt_json TEXT,
  hub_apply_receipt_digest TEXT CHECK (
    hub_apply_receipt_digest IS NULL OR length(hub_apply_receipt_digest) = 71
  ),
  assigned_hub_store TEXT CHECK (
    assigned_hub_store IS NULL OR assigned_hub_store IN ('messages', 'cv2_outbound')
  ),
  assigned_hub_message_id INTEGER CHECK (
    assigned_hub_message_id IS NULL OR assigned_hub_message_id > 0
  ),
  assigned_hub_context_id TEXT CHECK (
    assigned_hub_context_id IS NULL OR length(assigned_hub_context_id) BETWEEN 1 AND 512
  ),
  nack_code TEXT CHECK (
    nack_code IS NULL OR nack_code IN (
      'HUB_STORAGE_BUSY', 'NOT_FOUND', 'HUB_APPLY_OUTCOME_UNKNOWN',
      'PROJECTION_CONFLICT', 'CIPHERTEXT_DIGEST_MISMATCH',
      'PARENT_MAPPING_NOT_FOUND', 'READ_BEFORE_MIRROR', 'IDENTITY_ASSERTION_INVALID'
    )
  ),
  request_digest TEXT NOT NULL CHECK (length(request_digest) = 71),
  disposition TEXT NOT NULL CHECK (disposition IN ('CREATED', 'DUPLICATE')),
  previous_state TEXT NOT NULL CHECK (
    previous_state IN ('READY', 'RETRY_WAIT', 'BLOCKED', 'UNCERTAIN', 'ACKED')
  ),
  resulting_state TEXT NOT NULL CHECK (
    resulting_state IN ('READY', 'RETRY_WAIT', 'BLOCKED', 'UNCERTAIN', 'ACKED')
  ),
  acked_through INTEGER CHECK (acked_through IS NULL OR acked_through >= 1),
  watermarks_after_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  UNIQUE (event_id, request_digest),
  CHECK ((assigned_hub_store IS NULL) = (assigned_hub_message_id IS NULL)),
  CHECK (
    (receipt_type = 'ACK' AND attempt_id IS NULL
     AND hub_apply_receipt_id IS NOT NULL AND hub_apply_receipt_json IS NOT NULL
     AND hub_apply_receipt_digest IS NOT NULL AND resulting_state = 'ACKED'
     AND acked_through IS NOT NULL AND nack_code IS NULL)
    OR (receipt_type != 'ACK' AND attempt_id IS NOT NULL
        AND hub_apply_receipt_id IS NULL AND hub_apply_receipt_json IS NULL
        AND hub_apply_receipt_digest IS NULL AND acked_through IS NULL
        AND assigned_hub_store IS NULL AND assigned_hub_message_id IS NULL
        AND assigned_hub_context_id IS NULL
        AND nack_code IS NOT NULL)
  ),
  CHECK (
    receipt_type = 'ACK'
    OR (receipt_type = 'NACK_RETRYABLE'
        AND nack_code IN ('HUB_STORAGE_BUSY', 'NOT_FOUND'))
    OR (receipt_type = 'NACK_UNCERTAIN' AND nack_code = 'HUB_APPLY_OUTCOME_UNKNOWN')
    OR (receipt_type = 'NACK_TERMINAL' AND nack_code IN (
      'PROJECTION_CONFLICT', 'CIPHERTEXT_DIGEST_MISMATCH',
      'PARENT_MAPPING_NOT_FOUND', 'READ_BEFORE_MIRROR', 'IDENTITY_ASSERTION_INVALID'
    ))
  )
);

CREATE UNIQUE INDEX bridge_outbox_receipts_attempt
  ON bridge_outbox_receipts (event_id, attempt_id)
  WHERE attempt_id IS NOT NULL;

-- +goose StatementBegin
CREATE TRIGGER bridge_outbox_receipts_immutable
BEFORE UPDATE ON bridge_outbox_receipts
BEGIN
  SELECT RAISE(ABORT, 'bridge outbox receipt is immutable');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER bridge_outbox_receipts_no_delete
BEFORE DELETE ON bridge_outbox_receipts
BEGIN
  SELECT RAISE(ABORT, 'bridge outbox receipts are durable');
END;
-- +goose StatementEnd

-- +goose Down
DROP TRIGGER bridge_outbox_receipts_no_delete;
DROP TRIGGER bridge_outbox_receipts_immutable;
DROP INDEX bridge_outbox_receipts_attempt;
DROP TABLE bridge_outbox_receipts;
DROP TRIGGER bridge_outbox_no_delete;
DROP TRIGGER bridge_outbox_state_transition;
DROP TRIGGER bridge_outbox_identity_immutable;
DROP INDEX bridge_outbox_dependency;
DROP INDEX bridge_outbox_ready;
DROP TABLE bridge_outbox;
DROP TRIGGER bridge_outbox_partitions_no_delete;
DROP TRIGGER bridge_outbox_partitions_monotonic;
DROP TABLE bridge_outbox_partitions;
DROP TRIGGER bridge_outbox_message_bases_no_delete;
DROP TRIGGER bridge_outbox_message_bases_immutable;
DROP INDEX bridge_outbox_base_hub_id;
DROP TABLE bridge_outbox_message_bases;
