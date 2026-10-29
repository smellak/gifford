-- +goose Up
CREATE TABLE state_snapshots (
  state_id TEXT PRIMARY KEY,
  agent_id TEXT NOT NULL,
  observed_at TEXT NOT NULL,
  session_state TEXT NOT NULL,
  human_draft_state TEXT NOT NULL,
  human_activity_state TEXT NOT NULL,
  draft_generation INTEGER NOT NULL,
  input_guard_version INTEGER NOT NULL,
  agent_phase TEXT NOT NULL,
  attention_state TEXT NOT NULL,
  health_state TEXT NOT NULL,
  delivery_safety TEXT NOT NULL,
  confidence_json TEXT NOT NULL,
  evidence_json TEXT NOT NULL,
  analyst_version TEXT NOT NULL,
  stale INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE orchestrator_decisions (
  decision_id TEXT PRIMARY KEY,
  agent_id TEXT NOT NULL,
  message_id TEXT,
  artifact_id TEXT,
  state_id TEXT NOT NULL,
  allowed_actions_json TEXT NOT NULL,
  selected_action TEXT NOT NULL,
  probabilities_json TEXT,
  hard_guards_json TEXT NOT NULL,
  policy_version TEXT NOT NULL,
  decision_model TEXT,
  glimmer_call_id TEXT,
  channel_decision_id TEXT NOT NULL,     -- decision_id of the Channel Delivery Gate (design §9.18, §17.11)
  created_at TEXT NOT NULL,
  execution_status TEXT NOT NULL
);

CREATE TABLE glimmer_calls (
  call_id TEXT PRIMARY KEY,
  contract TEXT NOT NULL,                -- ANALYZE_AGENT_STATE | TRIAGE_MESSAGE | CHOOSE_ORCHESTRATION_ACTION
  agent_id TEXT,
  inference_profile TEXT NOT NULL,       -- base | analyst-lora
  base_model_repo TEXT NOT NULL,
  base_model_revision TEXT NOT NULL,
  adapter_name TEXT,                     -- NULL in the base profile
  adapter_revision TEXT,
  adapter_sha256 TEXT,
  dataset_version TEXT,                  -- the adapter's; NULL in the base profile
  dataset_sha256 TEXT,
  training_run_id TEXT,
  training_code_revision TEXT,
  quantization TEXT,
  runtime TEXT NOT NULL,
  runtime_version TEXT,
  gpu TEXT,
  request_id TEXT,
  latency_ms INTEGER NOT NULL,
  redaction_policy_version TEXT NOT NULL,
  redactions_count INTEGER NOT NULL DEFAULT 0,
  input_sha256 TEXT NOT NULL,            -- hash of the input after redaction
  output_sha256 TEXT NOT NULL,
  probabilities_json TEXT,
  status TEXT NOT NULL,                  -- ok | timeout | invalid | error | redaction_failed
  created_at TEXT NOT NULL
);

CREATE INDEX glimmer_calls_contract_profile ON glimmer_calls(contract, inference_profile, created_at);

-- Added: an agent's latest state; decisions of one agent and of one message.
CREATE INDEX state_snapshots_agent ON state_snapshots(agent_id, observed_at);
CREATE INDEX orchestrator_decisions_agent ON orchestrator_decisions(agent_id, created_at);
CREATE INDEX orchestrator_decisions_message ON orchestrator_decisions(message_id) WHERE message_id IS NOT NULL;

-- +goose Down
DROP INDEX orchestrator_decisions_message;
DROP INDEX orchestrator_decisions_agent;
DROP INDEX state_snapshots_agent;
DROP INDEX glimmer_calls_contract_profile;
DROP TABLE glimmer_calls;
DROP TABLE orchestrator_decisions;
DROP TABLE state_snapshots;
