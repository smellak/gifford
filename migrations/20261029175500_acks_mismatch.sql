-- +goose Up
-- D5 acks a third status for metadata mismatch. SQLite cannot alter a
-- CHECK, so the table is rebuilt: same columns and key, wider status
-- vocabulary.
CREATE TABLE acks_new (
  message_id TEXT NOT NULL REFERENCES messages(message_id) ON DELETE CASCADE,
  agent_id TEXT NOT NULL,
  delivery_generation INTEGER NOT NULL CHECK(delivery_generation >= 1),
  status TEXT NOT NULL CHECK(status IN ('PROCESSING','ACKNOWLEDGED','MISMATCH')),   -- AckStatus
  acked_at TEXT NOT NULL,
  PRIMARY KEY (message_id, agent_id, delivery_generation)
);
INSERT INTO acks_new SELECT message_id, agent_id, delivery_generation, status, acked_at FROM acks;
DROP TABLE acks;
ALTER TABLE acks_new RENAME TO acks;

-- +goose Down
-- Restores the two-status check; downgrade only past clean states
-- because rows already marked MISMATCH would violate it.
CREATE TABLE acks_old (
  message_id TEXT NOT NULL REFERENCES messages(message_id) ON DELETE CASCADE,
  agent_id TEXT NOT NULL,
  delivery_generation INTEGER NOT NULL CHECK(delivery_generation >= 1),
  status TEXT NOT NULL CHECK(status IN ('PROCESSING','ACKNOWLEDGED')),
  acked_at TEXT NOT NULL,
  PRIMARY KEY (message_id, agent_id, delivery_generation)
);
INSERT INTO acks_old SELECT message_id, agent_id, delivery_generation, status, acked_at FROM acks;
DROP TABLE acks;
ALTER TABLE acks_old RENAME TO acks;
