-- +goose Up
-- Durable node-local record of every PTY delivery attempt. The daemon commits
-- COMMIT_SENT before it emits the commit frame, so ambiguity survives process
-- and adapter-session loss instead of becoming an automatic duplicate.
CREATE TABLE delivery_attempts (
  delivery_id TEXT PRIMARY KEY,
  message_id TEXT NOT NULL,
  agent_id TEXT NOT NULL,
  delivery_generation INTEGER NOT NULL CHECK (delivery_generation >= 1),
  adapter_session_id TEXT NOT NULL,
  orchestrator_decision_id TEXT NOT NULL,
  channel_decision_id TEXT NOT NULL,
  draft_generation INTEGER NOT NULL CHECK (draft_generation >= 0),
  input_guard_version INTEGER NOT NULL CHECK (input_guard_version >= 0),
  control_versions_json TEXT NOT NULL,
  writer_lease_id TEXT,
  state TEXT NOT NULL CHECK (state IN (
    'PREPARING',
    'PREPARED',
    'COMMIT_SENT',
    'COMMITTED',
    'COMMIT_NOT_CONSUMED',
    'ABORTED_HUMAN_PRIORITY',
    'CANCELLED',
    'FAILED',
    'UNCERTAIN'
  )),
  commit_sent_at TEXT,
  terminal_reason TEXT,
  recovery_of_delivery_id TEXT,
  prepared_at TEXT,
  finished_at TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY (message_id, agent_id, delivery_generation)
    REFERENCES delivery_queue (message_id, agent_id, delivery_generation),
  FOREIGN KEY (recovery_of_delivery_id) REFERENCES delivery_attempts (delivery_id)
);

-- UNCERTAIN is deliberately terminal for this constraint: only an explicit
-- manual recovery may create a successor while preserving the uncertain row.
CREATE UNIQUE INDEX delivery_attempts_active_delivery
  ON delivery_attempts (message_id, agent_id, delivery_generation)
  WHERE state IN ('PREPARING', 'PREPARED', 'COMMIT_SENT');

-- The Proxy can have only one commit in flight for an agent. Keeping the
-- invariant in SQLite also covers concurrent daemon workers.
CREATE UNIQUE INDEX delivery_attempts_agent_commit
  ON delivery_attempts (agent_id)
  WHERE state = 'COMMIT_SENT';

CREATE INDEX delivery_attempts_agent_uncertain
  ON delivery_attempts (agent_id, state)
  WHERE state = 'UNCERTAIN';

-- +goose Down
DROP INDEX delivery_attempts_agent_uncertain;
DROP INDEX delivery_attempts_agent_commit;
DROP INDEX delivery_attempts_active_delivery;
DROP TABLE delivery_attempts;
