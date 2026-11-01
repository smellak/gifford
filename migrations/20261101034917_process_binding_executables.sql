-- +goose Up
-- Bind the real harness executable and the optional measured auxiliary
-- process identity. The nullable harness path lets pre-existing bindings be
-- adopted only by the normal pre-PREPARE guard after this migration lands.
ALTER TABLE orchestrator_process_bindings ADD COLUMN layout_version INTEGER
  CHECK (layout_version IS NULL OR layout_version >= 1);
ALTER TABLE orchestrator_process_bindings ADD COLUMN child_executable TEXT;
ALTER TABLE orchestrator_process_bindings ADD COLUMN auxiliary_pid INTEGER;
ALTER TABLE orchestrator_process_bindings ADD COLUMN auxiliary_start_time INTEGER;
ALTER TABLE orchestrator_process_bindings ADD COLUMN auxiliary_executable TEXT;

-- +goose Down
ALTER TABLE orchestrator_process_bindings DROP COLUMN auxiliary_executable;
ALTER TABLE orchestrator_process_bindings DROP COLUMN auxiliary_start_time;
ALTER TABLE orchestrator_process_bindings DROP COLUMN auxiliary_pid;
ALTER TABLE orchestrator_process_bindings DROP COLUMN child_executable;
ALTER TABLE orchestrator_process_bindings DROP COLUMN layout_version;
