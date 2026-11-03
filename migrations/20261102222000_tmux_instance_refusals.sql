-- +goose Up
-- Why the administrator's statement of an agent did not register, as a code
-- that names no user, path or process: doctor shows it. One row per agent,
-- only while its current statement is refused; it goes away when the agent
-- registers or its statement is removed.
CREATE TABLE tmux_instance_refusals (
  agent_id TEXT PRIMARY KEY,               -- AgentID
  source TEXT NOT NULL,                    -- the refused statement
  code TEXT NOT NULL CHECK (code IN (
    'invalid', 'shell', 'excluded', 'unknown_user', 'root_user',
    'reserved_uid', 'socket_dir', 'tmux_unreachable', 'pane_missing',
    'pane_comm', 'harness_missing', 'agent_uid', 'already_active',
    'internal')),
  refused_at TEXT NOT NULL                 -- first refusal with this code
);

-- +goose Down
DROP TABLE tmux_instance_refusals;
