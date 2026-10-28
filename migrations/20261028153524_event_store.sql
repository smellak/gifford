-- +goose Up
-- Minimal: one row per agent known to this node. Later subphases add
-- columns with new migrations.
CREATE TABLE agents (
  agent_id TEXT PRIMARY KEY,             -- AgentID
  node_id TEXT NOT NULL,                 -- NodeID of the node it runs on
  harness TEXT NOT NULL,                 -- Harness of its latest manifest
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

-- Minimal: one row per register_agent answered by a register_agent_result.
-- Later subphases add columns with new migrations.
CREATE TABLE sessions (
  session_id TEXT PRIMARY KEY,           -- OpaqueID of the register_agent_result
  agent_id TEXT NOT NULL REFERENCES agents(agent_id),
  state TEXT NOT NULL,                   -- STARTING | CONNECTED | OBSERVED | DETACHED | DISCONNECTED | TERMINATED
  capture_mode TEXT NOT NULL,            -- manifest.capture_mode: pty_proxy | bootstrap_pipe_pane
  deliverable INTEGER NOT NULL,          -- manifest.deliverable: 1 | 0
  manifest_json TEXT NOT NULL,           -- the CapabilityManifest of register_agent
  process_json TEXT,                     -- its ProcessIdentity; NULL when absent
  started_at TEXT NOT NULL,
  ended_at TEXT,
  updated_at TEXT NOT NULL
);

CREATE INDEX sessions_agent ON sessions(agent_id, started_at);

CREATE TABLE events (
  event_id TEXT PRIMARY KEY,
  schema_version INTEGER NOT NULL,
  node_id TEXT NOT NULL,
  session_id TEXT NOT NULL,
  agent_id TEXT NOT NULL,
  adapter_type TEXT NOT NULL,
  source_quality TEXT NOT NULL,
  event_type TEXT NOT NULL,
  direction TEXT,
  stream TEXT,
  origin TEXT,
  wall_time TEXT NOT NULL,
  monotonic_ns INTEGER NOT NULL,
  sequence INTEGER NOT NULL,
  turn_id TEXT,
  message_id TEXT,
  correlation_id TEXT,
  payload_json TEXT NOT NULL,
  sensitivity TEXT NOT NULL DEFAULT 'normal',
  redaction_applied INTEGER NOT NULL DEFAULT 0,
  -- Added to design §17.2: envelope fields that have no column there.
  harness TEXT,
  harness_version TEXT,
  tool_call_id TEXT,
  delivery_id TEXT,
  decision_id TEXT,
  parent_event_id TEXT
);

CREATE UNIQUE INDEX events_session_sequence ON events(session_id, sequence);
CREATE INDEX events_agent_time ON events(agent_id, wall_time);
CREATE INDEX events_message ON events(message_id) WHERE message_id IS NOT NULL;

-- Events are append-only (design §9.4).
-- +goose StatementBegin
CREATE TRIGGER events_no_update BEFORE UPDATE ON events
BEGIN
  SELECT RAISE(ABORT, 'events are append-only');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER events_no_delete BEFORE DELETE ON events
BEGIN
  SELECT RAISE(ABORT, 'events are append-only');
END;
-- +goose StatementEnd

CREATE TABLE input_guards (
  session_id TEXT PRIMARY KEY,
  agent_id TEXT NOT NULL,
  draft_generation INTEGER NOT NULL DEFAULT 0,
  draft_state TEXT NOT NULL,
  activity_state TEXT NOT NULL,
  draft_opened_at TEXT,
  last_human_key_at TEXT,
  last_submit_at TEXT,
  last_cancel_at TEXT,
  detection_source TEXT NOT NULL,        -- pty_proxy | native_adapter
  submit_detector_profile TEXT NOT NULL,
  confidence REAL NOT NULL,              -- 1.0 when detection_source = pty_proxy
  version INTEGER NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE capture_sessions (
  capture_session_id TEXT PRIMARY KEY,
  agent_id TEXT NOT NULL,
  harness TEXT NOT NULL,
  tmux_server TEXT NOT NULL,
  tmux_session TEXT NOT NULL,
  tmux_window TEXT NOT NULL,
  tmux_pane TEXT NOT NULL,               -- current %N; changes when the pane is recreated
  pane_cols INTEGER NOT NULL,
  pane_rows INTEGER NOT NULL,
  state TEXT NOT NULL,                   -- ATTACHED | REATTACHING | STOPPED | QUOTA | ERROR
  initial_screen_blob_ref TEXT,
  started_at TEXT NOT NULL,
  stopped_at TEXT,
  stop_reason TEXT,
  reattach_count INTEGER NOT NULL DEFAULT 0,
  bytes_captured INTEGER NOT NULL DEFAULT 0,
  source_quality TEXT NOT NULL           -- bootstrap_pipe_pane | bootstrap_import
);

CREATE TABLE capture_segments (
  segment_id TEXT PRIMARY KEY,
  capture_session_id TEXT NOT NULL,
  seq_from INTEGER NOT NULL,
  seq_to INTEGER NOT NULL,
  started_at TEXT NOT NULL,
  ended_at TEXT,
  storage_path TEXT NOT NULL,
  index_path TEXT NOT NULL,
  bytes_raw INTEGER NOT NULL,
  bytes_compressed INTEGER,
  compressed INTEGER NOT NULL DEFAULT 0,
  sha256 TEXT,
  redacted_export_sha256 TEXT
);

-- Added to design §17.8: captures of one agent, segments of one capture.
CREATE INDEX capture_sessions_agent ON capture_sessions(agent_id, started_at);
CREATE INDEX capture_segments_capture ON capture_segments(capture_session_id, seq_from);

-- +goose Down
DROP INDEX capture_segments_capture;
DROP INDEX capture_sessions_agent;
DROP TABLE capture_segments;
DROP TABLE capture_sessions;
DROP TABLE input_guards;
DROP TRIGGER events_no_delete;
DROP TRIGGER events_no_update;
DROP INDEX events_message;
DROP INDEX events_agent_time;
DROP INDEX events_session_sequence;
DROP TABLE events;
DROP INDEX sessions_agent;
DROP TABLE sessions;
DROP TABLE agents;
