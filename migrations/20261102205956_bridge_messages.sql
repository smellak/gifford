-- +goose Up
-- Ordinary admissions keep their existing node-local idempotency boundary.
-- Bridge admissions use the opaque source node as a separate namespace while
-- retaining the canonical source decimal unchanged in client_msg_id.
ALTER TABLE messages ADD COLUMN client_msg_namespace TEXT;
DROP INDEX messages_client_msg;
CREATE UNIQUE INDEX messages_client_msg
  ON messages(origin_node, from_agent, client_msg_id)
  WHERE client_msg_namespace IS NULL;
CREATE UNIQUE INDEX messages_namespaced_client_msg
  ON messages(client_msg_namespace, from_agent, client_msg_id)
  WHERE client_msg_namespace IS NOT NULL;

-- +goose StatementBegin
CREATE TRIGGER messages_client_msg_namespace_immutable
BEFORE UPDATE OF client_msg_namespace ON messages
WHEN NEW.client_msg_namespace IS NOT OLD.client_msg_namespace
BEGIN
  SELECT RAISE(ABORT, 'messages.client_msg_namespace is immutable');
END;
-- +goose StatementEnd

CREATE TABLE bridge_context_mappings (
  source_node_id TEXT NOT NULL CHECK (length(source_node_id) BETWEEN 1 AND 255),
  source_context_id TEXT NOT NULL CHECK (length(source_context_id) BETWEEN 1 AND 512),
  gifford_thread_id TEXT NOT NULL UNIQUE,
  gifford_thread_ref TEXT NOT NULL UNIQUE,
  created_at TEXT NOT NULL,
  PRIMARY KEY (source_node_id, source_context_id)
);

-- +goose StatementBegin
CREATE TRIGGER bridge_context_mappings_immutable BEFORE UPDATE ON bridge_context_mappings
BEGIN
  SELECT RAISE(ABORT, 'bridge_context_mappings is immutable');
END;
-- +goose StatementEnd

CREATE TABLE bridge_message_mappings (
  source_node_id TEXT NOT NULL CHECK (length(source_node_id) BETWEEN 1 AND 255),
  source_message_id INTEGER NOT NULL CHECK (source_message_id > 0),
  gifford_message_id TEXT NOT NULL UNIQUE REFERENCES messages(message_id),
  from_agent TEXT NOT NULL,
  to_agent TEXT NOT NULL,
  source_created_at TEXT NOT NULL,
  source_context_id TEXT CHECK (
    source_context_id IS NULL OR length(source_context_id) BETWEEN 1 AND 512
  ),
  source_parent_message_id INTEGER CHECK (source_parent_message_id > 0),
  gifford_parent_message_id TEXT REFERENCES messages(message_id),
  source_read_at TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  PRIMARY KEY (source_node_id, source_message_id),
  FOREIGN KEY (source_node_id, source_context_id)
    REFERENCES bridge_context_mappings(source_node_id, source_context_id)
);

-- The six-field source projection and both durable mappings are immutable.
-- source_read_at is the sole source-owned field that may make one NULL-to-value
-- transition after the mirror transaction commits.
-- +goose StatementBegin
CREATE TRIGGER bridge_message_projection_immutable BEFORE UPDATE ON bridge_message_mappings
WHEN NEW.source_node_id IS NOT OLD.source_node_id
  OR NEW.source_message_id IS NOT OLD.source_message_id
  OR NEW.gifford_message_id IS NOT OLD.gifford_message_id
  OR NEW.from_agent IS NOT OLD.from_agent
  OR NEW.to_agent IS NOT OLD.to_agent
  OR NEW.source_created_at IS NOT OLD.source_created_at
  OR NEW.source_context_id IS NOT OLD.source_context_id
  OR NEW.source_parent_message_id IS NOT OLD.source_parent_message_id
  OR NEW.gifford_parent_message_id IS NOT OLD.gifford_parent_message_id
BEGIN
  SELECT RAISE(ABORT, 'bridge message projection is immutable');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER bridge_message_read_once BEFORE UPDATE OF source_read_at ON bridge_message_mappings
WHEN OLD.source_read_at IS NOT NULL AND NEW.source_read_at IS NOT OLD.source_read_at
BEGIN
  SELECT RAISE(ABORT, 'bridge source_read_at is written once');
END;
-- +goose StatementEnd

-- +goose Down
-- A bridge row has no content envelope and cannot safely become an ordinary
-- message after the namespace column disappears. Remove its derived local
-- delivery state before restoring the legacy uniqueness boundary.
DELETE FROM delivery_result_evidence
WHERE delivery_id IN (
  SELECT delivery_id FROM delivery_attempts
  WHERE message_id IN (
    SELECT message_id FROM messages WHERE client_msg_namespace IS NOT NULL
  )
);
DELETE FROM delivery_attempts
WHERE message_id IN (
  SELECT message_id FROM messages WHERE client_msg_namespace IS NOT NULL
);
DELETE FROM message_triage
WHERE message_id IN (
  SELECT message_id FROM messages WHERE client_msg_namespace IS NOT NULL
);
DELETE FROM approval_requests
WHERE object_type = 'message' AND object_id IN (
  SELECT message_id FROM messages WHERE client_msg_namespace IS NOT NULL
);
DELETE FROM acks
WHERE message_id IN (
  SELECT message_id FROM messages WHERE client_msg_namespace IS NOT NULL
);
DELETE FROM delivery_queue
WHERE message_id IN (
  SELECT message_id FROM messages WHERE client_msg_namespace IS NOT NULL
);
DELETE FROM message_envelopes
WHERE message_id IN (
  SELECT message_id FROM messages WHERE client_msg_namespace IS NOT NULL
);
DELETE FROM message_recipients
WHERE message_id IN (
  SELECT message_id FROM messages WHERE client_msg_namespace IS NOT NULL
);
DROP TRIGGER bridge_message_read_once;
DROP TRIGGER bridge_message_projection_immutable;
DROP TABLE bridge_message_mappings;
DROP TRIGGER bridge_context_mappings_immutable;
DROP TABLE bridge_context_mappings;
DELETE FROM messages WHERE client_msg_namespace IS NOT NULL;
DROP TRIGGER messages_client_msg_namespace_immutable;
DROP INDEX messages_namespaced_client_msg;
DROP INDEX messages_client_msg;
ALTER TABLE messages DROP COLUMN client_msg_namespace;
CREATE UNIQUE INDEX messages_client_msg ON messages(origin_node, from_agent, client_msg_id);
