-- +goose Up
ALTER TABLE nodes ADD COLUMN signing_public_key BLOB;

-- +goose Down
ALTER TABLE nodes DROP COLUMN signing_public_key;
