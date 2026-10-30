-- +goose Up
-- The executor binds one authenticated gifford run session to its direct
-- harness child. BLOCKED is terminal for the session: a restart or socket
-- reconnect must never learn a replacement process after identity uncertainty.
CREATE TABLE orchestrator_process_bindings (
  session_id TEXT PRIMARY KEY REFERENCES sessions (session_id) ON DELETE CASCADE,
  agent_id TEXT NOT NULL REFERENCES agents (agent_id),
  state TEXT NOT NULL CHECK (state IN ('BOUND', 'BLOCKED')),
  boot_id TEXT,
  parent_pid INTEGER,
  parent_start_time INTEGER,
  child_pid INTEGER,
  child_start_time INTEGER,
  block_reason TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  CHECK (
    (state = 'BOUND'
      AND length(boot_id) BETWEEN 1 AND 128
      AND parent_pid >= 1
      AND parent_start_time >= 0
      AND child_pid >= 1
      AND child_start_time >= 0
      AND block_reason IS NULL)
    OR
    (state = 'BLOCKED' AND length(block_reason) BETWEEN 1 AND 64)
  )
);

CREATE INDEX orchestrator_process_bindings_agent
  ON orchestrator_process_bindings (agent_id, state);

-- +goose Down
DROP INDEX orchestrator_process_bindings_agent;
DROP TABLE orchestrator_process_bindings;
