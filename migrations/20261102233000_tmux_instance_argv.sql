-- +goose Up
-- The CLI's program, as the only parts of its command line the registry
-- keeps: the basename of argv[0], and of argv[1] when argv[0] is an
-- interpreter. Rows from before have neither, so their next check
-- invalidates them.
ALTER TABLE tmux_instances ADD COLUMN harness_argv0 TEXT NOT NULL DEFAULT '';
ALTER TABLE tmux_instances ADD COLUMN harness_argv1 TEXT NOT NULL DEFAULT '';

-- A refusal may now say that the CLI is not the stated program.
CREATE TABLE tmux_instance_refusals_next (
  agent_id TEXT PRIMARY KEY,               -- AgentID
  source TEXT NOT NULL,                    -- the refused statement
  code TEXT NOT NULL CHECK (code IN (
    'invalid', 'shell', 'excluded', 'unknown_user', 'root_user',
    'reserved_uid', 'socket_dir', 'tmux_unreachable', 'pane_missing',
    'pane_comm', 'harness_missing', 'harness_program', 'agent_uid',
    'already_active', 'internal')),
  refused_at TEXT NOT NULL                 -- first refusal with this code
);
INSERT INTO tmux_instance_refusals_next (agent_id, source, code, refused_at)
  SELECT agent_id, source, code, refused_at FROM tmux_instance_refusals;
DROP TABLE tmux_instance_refusals;
ALTER TABLE tmux_instance_refusals_next RENAME TO tmux_instance_refusals;

-- +goose Down
CREATE TABLE tmux_instance_refusals_prev (
  agent_id TEXT PRIMARY KEY,
  source TEXT NOT NULL,
  code TEXT NOT NULL CHECK (code IN (
    'invalid', 'shell', 'excluded', 'unknown_user', 'root_user',
    'reserved_uid', 'socket_dir', 'tmux_unreachable', 'pane_missing',
    'pane_comm', 'harness_missing', 'agent_uid', 'already_active',
    'internal')),
  refused_at TEXT NOT NULL
);
INSERT INTO tmux_instance_refusals_prev (agent_id, source, code, refused_at)
  SELECT agent_id, source, code, refused_at FROM tmux_instance_refusals
  WHERE code != 'harness_program';
DROP TABLE tmux_instance_refusals;
ALTER TABLE tmux_instance_refusals_prev RENAME TO tmux_instance_refusals;
ALTER TABLE tmux_instances DROP COLUMN harness_argv1;
ALTER TABLE tmux_instances DROP COLUMN harness_argv0;
