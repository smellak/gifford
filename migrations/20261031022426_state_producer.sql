-- +goose Up
-- Nullable additions keep every pre-5.4 row readable but ineligible. Only
-- StateProducer populates producer_version and the coherence evidence below.
ALTER TABLE state_snapshots ADD COLUMN state_version INTEGER;
ALTER TABLE state_snapshots ADD COLUMN last_human_key_at TEXT;
ALTER TABLE state_snapshots ADD COLUMN background_activity INTEGER;
ALTER TABLE state_snapshots ADD COLUMN channel_effective_json TEXT;
ALTER TABLE state_snapshots ADD COLUMN active_loops_json TEXT;
ALTER TABLE state_snapshots ADD COLUMN pending_approvals INTEGER;
ALTER TABLE state_snapshots ADD COLUMN session_id TEXT;
ALTER TABLE state_snapshots ADD COLUMN session_updated_at TEXT;
ALTER TABLE state_snapshots ADD COLUMN manifest_sha256 TEXT;
ALTER TABLE state_snapshots ADD COLUMN observation_id TEXT;
ALTER TABLE state_snapshots ADD COLUMN observation_generation INTEGER;
ALTER TABLE state_snapshots ADD COLUMN output_generation INTEGER;
ALTER TABLE state_snapshots ADD COLUMN event_generation INTEGER;
ALTER TABLE state_snapshots ADD COLUMN committed_at TEXT;
ALTER TABLE state_snapshots ADD COLUMN expires_at TEXT;
ALTER TABLE state_snapshots ADD COLUMN producer_version TEXT;
ALTER TABLE state_snapshots ADD COLUMN analysis_route TEXT;
ALTER TABLE state_snapshots ADD COLUMN glimmer_call_id TEXT REFERENCES glimmer_calls(call_id);
ALTER TABLE state_snapshots ADD COLUMN inference_profile TEXT;
ALTER TABLE state_snapshots ADD COLUMN base_model_revision TEXT;
ALTER TABLE state_snapshots ADD COLUMN adapter_revision TEXT;
ALTER TABLE state_snapshots ADD COLUMN adapter_sha256 TEXT;
ALTER TABLE state_snapshots ADD COLUMN profile_revision TEXT;
ALTER TABLE state_snapshots ADD COLUMN profile_sha256 TEXT;
ALTER TABLE state_snapshots ADD COLUMN calibration_revision TEXT;
ALTER TABLE state_snapshots ADD COLUMN calibration_sha256 TEXT;
ALTER TABLE state_snapshots ADD COLUMN calibration_temperature REAL;
ALTER TABLE state_snapshots ADD COLUMN idle_threshold REAL;
ALTER TABLE state_snapshots ADD COLUMN idle_probability REAL;
ALTER TABLE state_snapshots ADD COLUMN prompt_version TEXT;
ALTER TABLE state_snapshots ADD COLUMN analysis_policy_version TEXT;

CREATE UNIQUE INDEX state_snapshots_version
  ON state_snapshots(agent_id, session_id, state_version)
  WHERE producer_version IS NOT NULL;
CREATE UNIQUE INDEX state_snapshots_observation
  ON state_snapshots(observation_id)
  WHERE producer_version IS NOT NULL;
CREATE INDEX state_snapshots_latest
  ON state_snapshots(agent_id, session_id, observed_at DESC, state_version DESC)
  WHERE producer_version IS NOT NULL;

-- +goose Down
DROP INDEX state_snapshots_latest;
DROP INDEX state_snapshots_observation;
DROP INDEX state_snapshots_version;
ALTER TABLE state_snapshots DROP COLUMN analysis_policy_version;
ALTER TABLE state_snapshots DROP COLUMN prompt_version;
ALTER TABLE state_snapshots DROP COLUMN idle_probability;
ALTER TABLE state_snapshots DROP COLUMN idle_threshold;
ALTER TABLE state_snapshots DROP COLUMN calibration_temperature;
ALTER TABLE state_snapshots DROP COLUMN calibration_sha256;
ALTER TABLE state_snapshots DROP COLUMN calibration_revision;
ALTER TABLE state_snapshots DROP COLUMN profile_sha256;
ALTER TABLE state_snapshots DROP COLUMN profile_revision;
ALTER TABLE state_snapshots DROP COLUMN adapter_sha256;
ALTER TABLE state_snapshots DROP COLUMN adapter_revision;
ALTER TABLE state_snapshots DROP COLUMN base_model_revision;
ALTER TABLE state_snapshots DROP COLUMN inference_profile;
ALTER TABLE state_snapshots DROP COLUMN glimmer_call_id;
ALTER TABLE state_snapshots DROP COLUMN analysis_route;
ALTER TABLE state_snapshots DROP COLUMN producer_version;
ALTER TABLE state_snapshots DROP COLUMN expires_at;
ALTER TABLE state_snapshots DROP COLUMN committed_at;
ALTER TABLE state_snapshots DROP COLUMN event_generation;
ALTER TABLE state_snapshots DROP COLUMN output_generation;
ALTER TABLE state_snapshots DROP COLUMN observation_generation;
ALTER TABLE state_snapshots DROP COLUMN observation_id;
ALTER TABLE state_snapshots DROP COLUMN manifest_sha256;
ALTER TABLE state_snapshots DROP COLUMN session_updated_at;
ALTER TABLE state_snapshots DROP COLUMN session_id;
ALTER TABLE state_snapshots DROP COLUMN pending_approvals;
ALTER TABLE state_snapshots DROP COLUMN active_loops_json;
ALTER TABLE state_snapshots DROP COLUMN channel_effective_json;
ALTER TABLE state_snapshots DROP COLUMN background_activity;
ALTER TABLE state_snapshots DROP COLUMN last_human_key_at;
ALTER TABLE state_snapshots DROP COLUMN state_version;
