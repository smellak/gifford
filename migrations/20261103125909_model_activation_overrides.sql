-- +goose Up
-- Immutable evidence and actor binding for one explicit operator override.
-- The complete selection and the exact JSON evidence survive restart without
-- depending on the original local files.
CREATE TABLE model_activation_overrides (
  evaluation_run_id TEXT PRIMARY KEY,
  contract TEXT NOT NULL,
  generation INTEGER NOT NULL,
  adapter_name TEXT NOT NULL,
  adapter_revision TEXT NOT NULL,
  adapter_sha256 TEXT NOT NULL,
  training_run_id TEXT NOT NULL,
  adapter_metadata_sha256 TEXT NOT NULL,
  training_run_metadata_sha256 TEXT NOT NULL,
  evaluation_report_sha256 TEXT NOT NULL,
  reason TEXT NOT NULL,
  actor_type TEXT NOT NULL,
  actor_id TEXT NOT NULL,
  created_at TEXT NOT NULL,
  selection_json TEXT NOT NULL,
  adapter_metadata_json TEXT NOT NULL,
  training_run_metadata_json TEXT NOT NULL,
  evaluation_report_json TEXT NOT NULL,
  UNIQUE (contract, generation)
);

-- +goose StatementBegin
CREATE TRIGGER model_activation_overrides_no_update
BEFORE UPDATE ON model_activation_overrides
BEGIN SELECT RAISE(ABORT, 'model_activation_overrides is immutable'); END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER model_activation_overrides_no_delete
BEFORE DELETE ON model_activation_overrides
BEGIN SELECT RAISE(ABORT, 'model_activation_overrides is immutable'); END;
-- +goose StatementEnd

-- +goose Down
DROP TRIGGER model_activation_overrides_no_delete;
DROP TRIGGER model_activation_overrides_no_update;
DROP TABLE model_activation_overrides;
