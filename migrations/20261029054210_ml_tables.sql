-- +goose Up
CREATE TABLE dataset_versions (
  dataset_version TEXT PRIMARY KEY,      -- e.g. analyst-v1
  ml_contract_version TEXT NOT NULL,     -- e.g. ml-contract-v1
  manifest_path TEXT NOT NULL,
  dataset_sha256 TEXT NOT NULL,
  window_count INTEGER NOT NULL,
  counts_by_label_json TEXT NOT NULL,
  counts_by_harness_json TEXT NOT NULL,
  split_strategy TEXT NOT NULL,
  redaction_policy_version TEXT NOT NULL,
  readiness_status TEXT NOT NULL,        -- passed | failed
  readiness_json TEXT NOT NULL,          -- threshold → observed value → PASS | FAIL
  created_at TEXT NOT NULL
);

CREATE TABLE dataset_sources (
  dataset_version TEXT NOT NULL,
  source_type TEXT NOT NULL,             -- bootstrap_pipe_pane | bootstrap_import | pty_screen | native_hook | transcript | human_annotation | panel_correction | synthetic
  source_ref TEXT NOT NULL,              -- capture_session_id, session_id, hooks file, etc.
  harness TEXT,
  harness_version TEXT,
  window_count INTEGER NOT NULL,
  split TEXT NOT NULL,                   -- train | validation | test
  PRIMARY KEY (dataset_version, source_type, source_ref)
);

CREATE TABLE training_runs (
  training_run_id TEXT PRIMARY KEY,
  dataset_version TEXT NOT NULL,
  base_model_repo TEXT NOT NULL,
  base_model_revision TEXT NOT NULL,
  method TEXT NOT NULL,                  -- lora | qlora
  hyperparameters_json TEXT NOT NULL,
  seed INTEGER NOT NULL,
  training_code_revision TEXT NOT NULL,
  gpu TEXT,
  readiness_status TEXT NOT NULL,        -- passed | underpowered (trained with --force-train)
  started_at TEXT NOT NULL,
  ended_at TEXT,
  status TEXT NOT NULL,                  -- running | completed | failed | aborted
  log_path TEXT
);

CREATE TABLE model_adapters (
  adapter_name TEXT NOT NULL,
  adapter_revision TEXT NOT NULL,
  adapter_sha256 TEXT NOT NULL,
  training_run_id TEXT NOT NULL,
  contract TEXT NOT NULL,                -- ANALYZE_AGENT_STATE
  gate_status TEXT NOT NULL,             -- pending | passed | failed
  active INTEGER NOT NULL DEFAULT 0,
  activated_at TEXT,
  deactivated_at TEXT,
  PRIMARY KEY (adapter_name, adapter_revision)
);

CREATE TABLE evaluation_runs (
  evaluation_run_id TEXT PRIMARY KEY,
  dataset_version TEXT NOT NULL,
  inference_profile TEXT NOT NULL,       -- base | analyst-lora
  adapter_revision TEXT,
  split TEXT NOT NULL,
  metrics_json TEXT NOT NULL,            -- macro_f1, recall_by_label, false_idle, false_safe_pre, false_safe_post, ece, brier, p50, p95, unknown_coverage, by_harness, cross_harness, unseen_versions
  gate_verdict_json TEXT,                -- criterion → PASS | FAIL
  created_at TEXT NOT NULL
);

-- Added: runs of one dataset; evaluations of one dataset and profile.
CREATE INDEX training_runs_dataset ON training_runs(dataset_version, started_at);
CREATE INDEX evaluation_runs_dataset ON evaluation_runs(dataset_version, inference_profile, created_at);
-- Added: at most one active adapter per contract (design §17.9).
CREATE UNIQUE INDEX model_adapters_active ON model_adapters(contract) WHERE active = 1;

-- +goose Down
DROP INDEX model_adapters_active;
DROP INDEX evaluation_runs_dataset;
DROP INDEX training_runs_dataset;
DROP TABLE evaluation_runs;
DROP TABLE model_adapters;
DROP TABLE training_runs;
DROP TABLE dataset_sources;
DROP TABLE dataset_versions;
