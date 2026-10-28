-- +goose Up
CREATE TABLE channel_controls (
  control_id TEXT PRIMARY KEY,
  scope_type TEXT NOT NULL,              -- global | node | agent | link | thread | loop
  scope_key TEXT NOT NULL,               -- '*' | node_id | agent_id | 'from→to' | thread_id | loop_id
  plane TEXT NOT NULL DEFAULT 'all',     -- message | artifact | all
  mode TEXT,                             -- global, node: ACTIVE | PAUSED | DRAINING | OFF
                                         -- link, thread: ALLOW | HOLD | REQUIRE_APPROVAL | DENY
                                         -- loop: PAUSED | STOPPED (local override of the replica)
  tx_enabled INTEGER,                    -- agent only
  rx_enabled INTEGER,
  auto_delivery_enabled INTEGER,
  auto_continue_enabled INTEGER,
  from_agent TEXT,                       -- link only
  to_agent TEXT,
  reason TEXT,
  actor_type TEXT NOT NULL,              -- human | system | policy | remote_owner
  created_by TEXT NOT NULL,
  expires_at TEXT,
  restore_json TEXT,                     -- ControlSnapshot restored at expiry; NULL = remove the override
  then_mode TEXT,                        -- global/node in DRAINING only: 'OFF' when "--then off" was asked
  version INTEGER NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE UNIQUE INDEX channel_controls_scope ON channel_controls(scope_type, scope_key, plane);
CREATE INDEX channel_controls_link ON channel_controls(from_agent, to_agent, plane) WHERE scope_type = 'link';
CREATE INDEX channel_controls_expiry ON channel_controls(expires_at) WHERE expires_at IS NOT NULL;

CREATE TABLE communication_loops (
  loop_id TEXT PRIMARY KEY,
  thread_id TEXT NOT NULL,
  loop_generation INTEGER NOT NULL,
  owner_node TEXT NOT NULL,              -- the loop owner node
  origin TEXT NOT NULL,                  -- human | agent_request | workflow | implicit
  participants_json TEXT NOT NULL,
  mode TEXT NOT NULL,                    -- AUTO | STEP | PAUSED | DRAINING | STOPPED | COMPLETED
  state_reason TEXT,                     -- MANUAL | BUDGET_EXHAUSTED | LOOP_PATTERN_DETECTED | OWNER_STOP | ...
  budget_profile TEXT NOT NULL,          -- demo | default | strict | custom
  root_message_id TEXT,
  max_turns INTEGER NOT NULL,
  max_messages INTEGER NOT NULL,
  max_duration_ms INTEGER NOT NULL,
  max_messages_per_minute INTEGER NOT NULL,
  max_hops INTEGER NOT NULL,
  max_consecutive_same_pair INTEGER NOT NULL,
  max_retries INTEGER NOT NULL,
  max_artifact_count INTEGER NOT NULL,
  max_artifact_bytes INTEGER NOT NULL,
  turn_count INTEGER NOT NULL DEFAULT 0,          -- turns reserved
  message_count INTEGER NOT NULL DEFAULT 0,       -- messages admitted
  hop_count INTEGER NOT NULL DEFAULT 0,
  artifact_count INTEGER NOT NULL DEFAULT 0,
  artifact_bytes INTEGER NOT NULL DEFAULT 0,
  step_tokens INTEGER NOT NULL DEFAULT 0,
  last_sender_agent TEXT,                -- detects a change of voice
  pattern_state_json TEXT,               -- recent digests, alternation, rate
  control_version INTEGER NOT NULL,
  started_at TEXT NOT NULL,
  expires_at TEXT NOT NULL,
  last_progress_at TEXT,
  stopped_at TEXT,
  created_by TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(thread_id, loop_generation)
);

CREATE TABLE channel_control_decisions (
  decision_id TEXT PRIMARY KEY,
  object_type TEXT NOT NULL,             -- message | artifact | loop_action
  object_id TEXT NOT NULL,
  stage TEXT NOT NULL,                   -- admission | acceptance | delivery | continuation | revalidation
  plane TEXT NOT NULL,
  from_agent TEXT,
  to_agent TEXT,
  outcome TEXT NOT NULL,                 -- ALLOW | HOLD | APPROVAL | DENY
  reason TEXT NOT NULL,                  -- typed reason
  matched_controls_json TEXT NOT NULL,   -- JSON array of control ids
  effective_control_json TEXT NOT NULL,
  control_versions_json TEXT NOT NULL,   -- version vector of the decision
  explanation_json TEXT NOT NULL,        -- what the panel and the CLI show
  actor_id TEXT,
  created_at TEXT NOT NULL
);

CREATE INDEX channel_control_decisions_object ON channel_control_decisions(object_type, object_id, created_at);

CREATE TABLE approval_requests (
  approval_id TEXT PRIMARY KEY,
  source TEXT NOT NULL,                  -- link | loop_step | loop_budget | loop_pattern | orchestrator | triage | agent_request
  object_type TEXT NOT NULL,             -- message | artifact | loop | thread_link (pre-approval of a thread, already resolved)
  object_id TEXT NOT NULL,
  agent_id TEXT,                         -- affected receiver
  loop_id TEXT,
  options_json TEXT NOT NULL,            -- e.g. ["approve_once","approve_thread:1h","reject"] or ["extend:+5","set_mode:step","stop"]
  requested_by TEXT,                     -- agent_id when request_loop_extension opened or annotated it
  request_note TEXT,                     -- the agent's reason: untrusted text, redacted when shown
  state TEXT NOT NULL,                   -- PENDING | APPROVED | REJECTED | EXPIRED | SUPERSEDED
  resolution_json TEXT,
  resolved_by TEXT,
  expires_at TEXT,
  created_at TEXT NOT NULL,
  resolved_at TEXT
);

CREATE INDEX approval_requests_pending ON approval_requests(state, created_at) WHERE state = 'PENDING';
CREATE UNIQUE INDEX approval_requests_object ON approval_requests(object_type, object_id, source) WHERE state = 'PENDING';

-- +goose Down
DROP INDEX approval_requests_object;
DROP INDEX approval_requests_pending;
DROP TABLE approval_requests;
DROP INDEX channel_control_decisions_object;
DROP TABLE channel_control_decisions;
DROP TABLE communication_loops;
DROP INDEX channel_controls_expiry;
DROP INDEX channel_controls_link;
DROP INDEX channel_controls_scope;
DROP TABLE channel_controls;
