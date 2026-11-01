-- +goose Up
-- The version of the prompt the gateway rendered for the call. Rows from
-- before it, and calls that failed before a prompt was rendered, are NULL.
ALTER TABLE glimmer_calls ADD COLUMN prompt_version TEXT;

-- +goose Down
ALTER TABLE glimmer_calls DROP COLUMN prompt_version;
