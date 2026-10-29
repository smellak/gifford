-- +goose Up
ALTER TABLE messages ADD COLUMN admission_result_json TEXT;   -- the AdmissionResult this Hub answered; NULL if admitted elsewhere
-- +goose StatementBegin
CREATE TRIGGER messages_admission_result_once BEFORE UPDATE OF admission_result_json ON messages
WHEN OLD.admission_result_json IS NOT NULL
BEGIN SELECT RAISE(ABORT, 'messages.admission_result_json is written once'); END;
-- +goose StatementEnd
CREATE TABLE message_recipients (
  message_id TEXT NOT NULL REFERENCES messages(message_id),
  agent_id TEXT NOT NULL,
  ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
  delivery_mode TEXT NOT NULL CHECK (delivery_mode IN ('WHEN_SAFE', 'DISPLAY_ONLY')),
  requires_ack INTEGER NOT NULL CHECK (requires_ack IN (0, 1)),
  created_at TEXT NOT NULL,
  PRIMARY KEY (message_id, agent_id)
);
CREATE UNIQUE INDEX message_recipients_ordinal ON message_recipients(message_id, ordinal);
-- +goose StatementBegin
CREATE TRIGGER message_recipients_immutable BEFORE UPDATE ON message_recipients
BEGIN SELECT RAISE(ABORT, 'message_recipients is immutable'); END;
-- +goose StatementEnd
CREATE TABLE acks (
  message_id TEXT NOT NULL,
  agent_id TEXT NOT NULL,
  delivery_generation INTEGER NOT NULL CHECK (delivery_generation >= 1),
  status TEXT NOT NULL CHECK (status IN ('PROCESSING', 'ACKNOWLEDGED')),   -- AckStatus
  acked_at TEXT NOT NULL,
  PRIMARY KEY (message_id, agent_id, delivery_generation, status)
);
-- +goose Down
DROP TABLE acks;
DROP TRIGGER message_recipients_immutable;
DROP INDEX message_recipients_ordinal;
DROP TABLE message_recipients;
DROP TRIGGER messages_admission_result_once;
ALTER TABLE messages DROP COLUMN admission_result_json;
