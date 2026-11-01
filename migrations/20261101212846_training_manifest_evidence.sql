-- +goose Up
-- Keep the exact dataset seal used by training separate from the expanded
-- final-evaluation manifest retained in dataset_versions. Nullable columns
-- allow deterministic backfill of historical rows without inventing evidence.
ALTER TABLE training_runs ADD COLUMN training_dataset_sha256 TEXT
    CHECK (training_dataset_sha256 IS NULL OR length(training_dataset_sha256) = 64);
ALTER TABLE training_runs ADD COLUMN training_manifest_path TEXT;
ALTER TABLE training_runs ADD COLUMN training_manifest_sha256 TEXT
    CHECK (training_manifest_sha256 IS NULL OR length(training_manifest_sha256) = 64);

UPDATE training_runs
SET training_dataset_sha256 = (
        SELECT CASE
            WHEN substr(dataset_versions.dataset_sha256, 1, 7) = 'sha256:'
            THEN substr(dataset_versions.dataset_sha256, 8)
            ELSE dataset_versions.dataset_sha256
        END
        FROM dataset_versions
        WHERE dataset_versions.dataset_version = training_runs.dataset_version
    ),
    training_manifest_path = (
        SELECT dataset_versions.manifest_path
        FROM dataset_versions
        WHERE dataset_versions.dataset_version = training_runs.dataset_version
    ),
    training_manifest_sha256 = (
        SELECT dataset_versions.manifest_sha256
        FROM dataset_versions
        WHERE dataset_versions.dataset_version = training_runs.dataset_version
    )
WHERE EXISTS (
    SELECT 1 FROM dataset_versions
    WHERE dataset_versions.dataset_version = training_runs.dataset_version
);

-- +goose Down
ALTER TABLE training_runs DROP COLUMN training_manifest_sha256;
ALTER TABLE training_runs DROP COLUMN training_manifest_path;
ALTER TABLE training_runs DROP COLUMN training_dataset_sha256;
