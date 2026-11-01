-- +goose Up
-- Keep the verified local references and the report computed by eval.Evaluate.
-- These columns distinguish product-path imports from self-consistent
-- declarations inserted outside the importer.
ALTER TABLE dataset_versions ADD COLUMN manifest_sha256 TEXT;

ALTER TABLE training_runs ADD COLUMN metadata_path TEXT;
ALTER TABLE training_runs ADD COLUMN metadata_sha256 TEXT;

ALTER TABLE model_adapters ADD COLUMN metadata_path TEXT;
ALTER TABLE model_adapters ADD COLUMN metadata_sha256 TEXT;

ALTER TABLE evaluation_runs ADD COLUMN evaluation_sha256 TEXT;
ALTER TABLE evaluation_runs ADD COLUMN evaluation_report_json TEXT;
ALTER TABLE evaluation_runs ADD COLUMN evidence_json TEXT;

-- +goose Down
ALTER TABLE evaluation_runs DROP COLUMN evidence_json;
ALTER TABLE evaluation_runs DROP COLUMN evaluation_report_json;
ALTER TABLE evaluation_runs DROP COLUMN evaluation_sha256;

ALTER TABLE model_adapters DROP COLUMN metadata_sha256;
ALTER TABLE model_adapters DROP COLUMN metadata_path;

ALTER TABLE training_runs DROP COLUMN metadata_sha256;
ALTER TABLE training_runs DROP COLUMN metadata_path;

ALTER TABLE dataset_versions DROP COLUMN manifest_sha256;
