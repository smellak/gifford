-- +goose Up
-- Why a sudo-launcher chain looks the way it does: the S08
-- sudo_monitor_shell_v1 profile persists the exact pane-to-CLI chain
-- (sudo, sudo, shell, CLI) so Verify and the pre-Enter foreground check
-- can revalidate every node and edge instead of only the endpoints.
CREATE TABLE tmux_monitor_chains (
  instance_id TEXT NOT NULL,                -- tmux_instances.instance_id
  position INTEGER NOT NULL,                 -- 0 is the pane, the harness is last
  pid INTEGER NOT NULL,                      -- node pid
  start INTEGER NOT NULL,                    -- node start time, clock ticks after boot
  comm TEXT NOT NULL,                        -- node comm
  uid INTEGER NOT NULL,                      -- node real uid
  gid INTEGER NOT NULL,                      -- node real gid
  euid INTEGER NOT NULL,                     -- node effective uid
  egid INTEGER NOT NULL,                     -- node effective gid
  ppid INTEGER NOT NULL,                     -- parent pid: the edge into this node
  PRIMARY KEY (instance_id, position),
  FOREIGN KEY (instance_id) REFERENCES tmux_instances (instance_id) ON DELETE CASCADE
);

ALTER TABLE tmux_instances ADD COLUMN monitor_profile TEXT NOT NULL DEFAULT '';

-- The chain_shape refusal joins the closed vocabulary: a sudo-monitor
-- statement whose live chain is not exactly sudo, sudo, shell and the
-- registered CLI, in that order.
CREATE TABLE tmux_instance_refusals_new (
  agent_id TEXT PRIMARY KEY,
  source TEXT NOT NULL,
  code TEXT NOT NULL CHECK (code IN (
    'invalid', 'shell', 'excluded', 'unknown_user', 'root_user',
    'reserved_uid', 'socket_dir', 'tmux_unreachable', 'pane_missing',
    'pane_comm', 'harness_missing', 'harness_program', 'agent_uid',
    'chain_shape', 'already_active', 'internal')),
  refused_at TEXT NOT NULL
);

INSERT INTO tmux_instance_refusals_new SELECT * FROM tmux_instance_refusals;
DROP TABLE tmux_instance_refusals;
ALTER TABLE tmux_instance_refusals_new RENAME TO tmux_instance_refusals;

-- +goose Down
ALTER TABLE tmux_instances DROP COLUMN monitor_profile;
DROP TABLE tmux_monitor_chains;
CREATE TABLE tmux_instance_refusals_old (
  agent_id TEXT PRIMARY KEY,
  source TEXT NOT NULL,
  code TEXT NOT NULL CHECK (code IN (
    'invalid', 'shell', 'excluded', 'unknown_user', 'root_user',
    'reserved_uid', 'socket_dir', 'tmux_unreachable', 'pane_missing',
    'pane_comm', 'harness_missing', 'harness_program', 'agent_uid',
    'already_active', 'internal')),
  refused_at TEXT NOT NULL
);
INSERT INTO tmux_instance_refusals_old
  SELECT agent_id, source,
    CASE WHEN code = 'chain_shape' THEN 'harness_missing' ELSE code END,
    refused_at
  FROM tmux_instance_refusals;
DROP TABLE tmux_instance_refusals;
ALTER TABLE tmux_instance_refusals_old RENAME TO tmux_instance_refusals;
