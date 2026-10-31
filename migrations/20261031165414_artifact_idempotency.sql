-- +goose Up
ALTER TABLE artifact_recipients
ADD COLUMN recipient_key_id TEXT NOT NULL DEFAULT '';

CREATE UNIQUE INDEX artifact_transfers_sender_client
ON artifact_transfers(sender_agent, client_msg_id);

-- +goose Down
DROP INDEX artifact_transfers_sender_client;
ALTER TABLE artifact_recipients DROP COLUMN recipient_key_id;
