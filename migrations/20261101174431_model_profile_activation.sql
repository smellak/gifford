-- +goose Up
-- Immutable, metadata-only activation registry. Adapter weights remain in the
-- serving system; this table seals the digests and policy tuple needed to
-- decide whether a named revision is eligible.
CREATE TABLE model_profile_registry (
  adapter_name TEXT NOT NULL,
  adapter_revision TEXT NOT NULL,
  adapter_sha256 TEXT NOT NULL,
  contract TEXT NOT NULL,
  base_model_repo TEXT NOT NULL,
  base_model_revision TEXT NOT NULL,
  runtime TEXT NOT NULL,
  quantization TEXT,
  ml_contract_version TEXT NOT NULL,
  dataset_version TEXT NOT NULL,
  dataset_sha256 TEXT NOT NULL,
  dataset_readiness_status TEXT NOT NULL,
  dataset_readiness_sha256 TEXT NOT NULL,
  training_run_id TEXT NOT NULL,
  training_code_revision TEXT NOT NULL,
  training_readiness_status TEXT NOT NULL,
  training_status TEXT NOT NULL,
  evaluation_run_id TEXT NOT NULL,
  evaluation_sha256 TEXT NOT NULL,
  evaluation_report_json TEXT NOT NULL,
  gate_verdict_json TEXT NOT NULL,
  prompt_version TEXT NOT NULL,
  projection_policy_version TEXT NOT NULL,
  projection_policy_sha256 TEXT NOT NULL,
  profile_revision TEXT NOT NULL,
  profile_sha256 TEXT NOT NULL,
  calibration_revision TEXT NOT NULL,
  calibration_sha256 TEXT NOT NULL,
  calibration_temperature REAL NOT NULL,
  idle_threshold REAL NOT NULL,
  analysis_policy_version TEXT NOT NULL,
  metadata_json TEXT NOT NULL,
  registered_at TEXT NOT NULL,
  PRIMARY KEY (adapter_name, adapter_revision)
);

CREATE TRIGGER model_profile_registry_no_update
BEFORE UPDATE ON model_profile_registry
BEGIN SELECT RAISE(ABORT, 'model_profile_registry is immutable'); END;

CREATE TRIGGER model_profile_registry_no_delete
BEFORE DELETE ON model_profile_registry
BEGIN SELECT RAISE(ABORT, 'model_profile_registry is immutable'); END;

-- One durable current selection per contract. selection_json is the complete
-- immutable tuple returned by status and restored on restart.
CREATE TABLE model_profile_selections (
  contract TEXT PRIMARY KEY,
  generation INTEGER NOT NULL,
  inference_profile TEXT NOT NULL,
  adapter_name TEXT,
  adapter_revision TEXT,
  reason TEXT NOT NULL,
  gate_status TEXT NOT NULL,
  operational_state TEXT NOT NULL,
  event_id TEXT NOT NULL UNIQUE,
  selected_at TEXT NOT NULL,
  selection_json TEXT NOT NULL
);

CREATE UNIQUE INDEX model_profile_selection_generation
  ON model_profile_selections(contract, generation);

ALTER TABLE glimmer_calls ADD COLUMN selection_generation INTEGER;
ALTER TABLE glimmer_calls ADD COLUMN evaluation_run_id TEXT;
ALTER TABLE glimmer_calls ADD COLUMN evaluation_sha256 TEXT;
ALTER TABLE glimmer_calls ADD COLUMN projection_policy_version TEXT;
ALTER TABLE glimmer_calls ADD COLUMN projection_policy_sha256 TEXT;
ALTER TABLE glimmer_calls ADD COLUMN profile_revision TEXT;
ALTER TABLE glimmer_calls ADD COLUMN profile_sha256 TEXT;
ALTER TABLE glimmer_calls ADD COLUMN calibration_revision TEXT;
ALTER TABLE glimmer_calls ADD COLUMN calibration_sha256 TEXT;
ALTER TABLE glimmer_calls ADD COLUMN calibration_temperature REAL;
ALTER TABLE glimmer_calls ADD COLUMN idle_threshold REAL;
ALTER TABLE glimmer_calls ADD COLUMN analysis_policy_version TEXT;

ALTER TABLE state_snapshots ADD COLUMN selection_generation INTEGER;

-- +goose Down
ALTER TABLE state_snapshots DROP COLUMN selection_generation;

ALTER TABLE glimmer_calls DROP COLUMN analysis_policy_version;
ALTER TABLE glimmer_calls DROP COLUMN idle_threshold;
ALTER TABLE glimmer_calls DROP COLUMN calibration_temperature;
ALTER TABLE glimmer_calls DROP COLUMN calibration_sha256;
ALTER TABLE glimmer_calls DROP COLUMN calibration_revision;
ALTER TABLE glimmer_calls DROP COLUMN profile_sha256;
ALTER TABLE glimmer_calls DROP COLUMN profile_revision;
ALTER TABLE glimmer_calls DROP COLUMN projection_policy_sha256;
ALTER TABLE glimmer_calls DROP COLUMN projection_policy_version;
ALTER TABLE glimmer_calls DROP COLUMN evaluation_sha256;
ALTER TABLE glimmer_calls DROP COLUMN evaluation_run_id;
ALTER TABLE glimmer_calls DROP COLUMN selection_generation;

DROP INDEX model_profile_selection_generation;
DROP TABLE model_profile_selections;
DROP TRIGGER model_profile_registry_no_delete;
DROP TRIGGER model_profile_registry_no_update;
DROP TABLE model_profile_registry;
