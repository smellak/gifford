-- +goose Up
-- One triage verdict per message, written once by internal/triage: a label,
-- or none when the message could not be classified, and how it came about.
-- It holds no text. glimmer_call_id is the TRIAGE_MESSAGE call, when one was
-- made and recorded; encrypted, confidential and unreadable messages are
-- never sent, so they have none.
CREATE TABLE message_triage (
  message_id TEXT NOT NULL PRIMARY KEY REFERENCES messages(message_id),
  label TEXT CHECK (label IS NULL OR label IN (
    'NORMAL',
    'CONFLICT_OR_URGENT',
    'LOW_PRIORITY',
    'REQUIRES_HUMAN'
  )),
  outcome TEXT NOT NULL CHECK (outcome IN (
    'classified',
    'timeout',
    'invalid',
    'error',
    'circuit_open',
    'redaction_failed',
    'encrypted',
    'confidential',
    'unreadable'
  )),
  glimmer_call_id TEXT,
  created_at TEXT NOT NULL,
  CHECK ((outcome = 'classified') = (label IS NOT NULL)),
  CHECK (outcome <> 'classified' OR glimmer_call_id IS NOT NULL),
  CHECK (outcome NOT IN ('encrypted', 'confidential', 'unreadable') OR glimmer_call_id IS NULL)
);
-- +goose StatementBegin
CREATE TRIGGER message_triage_once BEFORE UPDATE ON message_triage
BEGIN SELECT RAISE(ABORT, 'message_triage is written once'); END;
-- +goose StatementEnd

-- +goose Down
DROP TRIGGER message_triage_once;
DROP TABLE message_triage;
