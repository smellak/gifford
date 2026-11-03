-- +goose Up
-- Agents that already run in a tmux pane on this machine (capture mode
-- tmux_attach). Only the node and the administrator write this table. A row
-- is never deleted: an instance that changes becomes INVALID, and
-- registering it again creates a new row.
CREATE TABLE tmux_instances (
  instance_id TEXT PRIMARY KEY,            -- ULID
  agent_id TEXT NOT NULL,                  -- AgentID
  machine_id TEXT NOT NULL,                -- /etc/machine-id at registration
  boot_id TEXT NOT NULL,                   -- kernel boot id at registration
  owner_user TEXT NOT NULL,                -- tmux server owner: only the sudo user and the socket
  owner_uid INTEGER NOT NULL CHECK (owner_uid >= 0),
  socket TEXT NOT NULL,                    -- absolute path of the tmux server socket
  pane_id TEXT NOT NULL,                   -- TmuxPaneID
  pane_pid INTEGER NOT NULL CHECK (pane_pid > 0),
  pane_start INTEGER NOT NULL CHECK (pane_start >= 0),
  pane_comm TEXT NOT NULL,
  harness TEXT NOT NULL,                   -- Harness
  harness_version TEXT NOT NULL,           -- stated at registration
  harness_pid INTEGER NOT NULL CHECK (harness_pid > 0),
  harness_start INTEGER NOT NULL CHECK (harness_start >= 0),
  harness_comm TEXT NOT NULL,
  agent_uid INTEGER NOT NULL CHECK (agent_uid >= 0),   -- the CLI's: the agent's identity
  agent_gid INTEGER NOT NULL CHECK (agent_gid >= 0),
  source TEXT NOT NULL,                    -- where the registration came from
  registered_by INTEGER NOT NULL CHECK (registered_by >= 0),
  session_id TEXT,                         -- the tmux_attach session bound to it
  state TEXT NOT NULL CHECK (state IN ('ACTIVE', 'INVALID')),
  invalid_reason TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  CHECK ((state = 'ACTIVE') = (invalid_reason IS NULL))
);
CREATE UNIQUE INDEX tmux_instances_active_agent
  ON tmux_instances(agent_id) WHERE state = 'ACTIVE';
CREATE UNIQUE INDEX tmux_instances_active_pane
  ON tmux_instances(socket, pane_id) WHERE state = 'ACTIVE';

-- +goose Down
DROP INDEX tmux_instances_active_pane;
DROP INDEX tmux_instances_active_agent;
DROP TABLE tmux_instances;
