-- +goose Up
ALTER TABLE sessions ADD COLUMN boot_id TEXT;

-- +goose Down
ALTER TABLE sessions DROP COLUMN boot_id;
