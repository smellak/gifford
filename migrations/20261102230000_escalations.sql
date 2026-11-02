-- +goose Up
-- Escalation marks: one row per message escalated to the operator.
-- The fallback loop writes it in its own transaction and reads it back
-- to fill Facts.Escalated, so each message escalates once. The operator
-- notice itself travels the normal admission path, keyed by a
-- deterministic client_msg_id, never through this table.
CREATE TABLE escalations (
  message_id TEXT PRIMARY KEY,
  agent_id TEXT NOT NULL,
  operator_agent TEXT NOT NULL,
  created_at TEXT NOT NULL
);

-- +goose Down
DROP TABLE escalations;
