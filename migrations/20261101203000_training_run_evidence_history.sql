-- +goose Up
-- Preserve every verified training metadata document while training_runs keeps
-- the current lifecycle state. A run may advance once from running to a
-- terminal state without losing the retained running evidence.
CREATE TABLE training_run_evidence (
    training_run_id TEXT NOT NULL,
    metadata_sha256 TEXT NOT NULL CHECK (length(metadata_sha256) = 64),
    metadata_path TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('running', 'completed', 'failed', 'aborted')),
    ended_at TEXT,
    imported_at TEXT NOT NULL,
    PRIMARY KEY (training_run_id, metadata_sha256),
    FOREIGN KEY (training_run_id) REFERENCES training_runs(training_run_id) ON DELETE RESTRICT
);

CREATE INDEX training_run_evidence_status
    ON training_run_evidence(training_run_id, status, imported_at);

-- Seed retained provenance for runs imported before this table existed. The
-- original import instant was not stored, so use the lifecycle timestamp as a
-- deterministic lower bound rather than inventing a wall-clock value here.
INSERT INTO training_run_evidence (
    training_run_id, metadata_sha256, metadata_path, status, ended_at, imported_at
)
SELECT training_run_id, metadata_sha256, metadata_path, status, ended_at,
       COALESCE(ended_at, started_at)
FROM training_runs
WHERE metadata_sha256 IS NOT NULL AND metadata_path IS NOT NULL;

-- +goose Down
DROP INDEX training_run_evidence_status;
DROP TABLE training_run_evidence;
