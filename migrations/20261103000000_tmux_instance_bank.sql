-- +goose Up
-- The helper registry bank authorizing the instance. Rows from before
-- have none, so the helper path refuses writes for them until their
-- statement carries an explicit bank.
ALTER TABLE tmux_instances ADD COLUMN bank TEXT NOT NULL DEFAULT '';

-- +goose Down
ALTER TABLE tmux_instances DROP COLUMN bank;
