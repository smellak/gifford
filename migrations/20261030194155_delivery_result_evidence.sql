-- +goose Up
-- Orchestrator approvals are recipient-scoped. Other approval sources retain
-- their existing object/source deduplication, and COALESCE closes the NULL
-- uniqueness hole for malformed orchestrator rows.
DROP INDEX approval_requests_object;
CREATE UNIQUE INDEX approval_requests_object
  ON approval_requests (object_type, object_id, source)
  WHERE state = 'PENDING' AND source <> 'orchestrator';
CREATE UNIQUE INDEX approval_requests_orchestrator_recipient
  ON approval_requests (object_type, object_id, source, COALESCE(agent_id, ''))
  WHERE state = 'PENDING' AND source = 'orchestrator';

-- Delivery results can arrive after a synchronous call times out. Keep the
-- operational evidence without storing message data or the Proxy error text;
-- attempts remain immutable, especially once they are UNCERTAIN.
CREATE TABLE delivery_result_evidence (
  delivery_id TEXT NOT NULL REFERENCES delivery_attempts (delivery_id),
  agent_id TEXT NOT NULL,
  adapter_session_id TEXT NOT NULL,
  result TEXT NOT NULL CHECK (result IN (
    'PREPARED',
    'COMMITTED',
    'ABORTED_HUMAN_PRIORITY',
    'CANCELLED',
    'COMMIT_NOT_CONSUMED',
    'FAILED'
  )),
  frame_id INTEGER NOT NULL CHECK (frame_id >= 1),
  reply_to INTEGER NOT NULL CHECK (reply_to >= 1),
  proxy_observed_at TEXT NOT NULL,
  received_at TEXT NOT NULL,
  observed_draft_generation INTEGER NOT NULL CHECK (observed_draft_generation >= 0),
  observed_input_guard_version INTEGER CHECK (observed_input_guard_version >= 0),
  writer_lease_id TEXT,
  cancel_reason TEXT,
  control_versions_json TEXT NOT NULL,
  PRIMARY KEY (delivery_id, result)
);

CREATE INDEX delivery_result_evidence_committed
  ON delivery_result_evidence (delivery_id)
  WHERE result = 'COMMITTED';

CREATE INDEX delivery_attempts_delivery_identity
  ON delivery_attempts (message_id, agent_id, delivery_generation, delivery_id);

-- +goose Down
DROP INDEX delivery_attempts_delivery_identity;
DROP INDEX delivery_result_evidence_committed;
DROP TABLE delivery_result_evidence;
DROP INDEX approval_requests_orchestrator_recipient;
DROP INDEX approval_requests_object;
CREATE UNIQUE INDEX approval_requests_object
  ON approval_requests (object_type, object_id, source)
  WHERE state = 'PENDING';
