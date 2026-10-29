-- +goose Up
-- MISMATCH belongs only to hub.sock's internal acknowledgement RPC. The
-- frozen protocol.AckStatus remains PROCESSING | ACKNOWLEDGED.
CREATE TABLE acks_with_mismatch (
  message_id TEXT NOT NULL,
  agent_id TEXT NOT NULL,
  delivery_generation INTEGER NOT NULL CHECK (delivery_generation >= 1),
  status TEXT NOT NULL CHECK (status IN ('PROCESSING', 'ACKNOWLEDGED', 'MISMATCH')),
  acked_at TEXT NOT NULL,
  PRIMARY KEY (message_id, agent_id, delivery_generation, status)
);
INSERT INTO acks_with_mismatch
  SELECT message_id, agent_id, delivery_generation, status, acked_at FROM acks;
DROP TABLE acks;
ALTER TABLE acks_with_mismatch RENAME TO acks;

-- +goose Down
-- A downgrade cannot represent mismatch rows, so it deliberately keeps only
-- the two frozen acknowledgement states.
CREATE TABLE acks_without_mismatch (
  message_id TEXT NOT NULL,
  agent_id TEXT NOT NULL,
  delivery_generation INTEGER NOT NULL CHECK (delivery_generation >= 1),
  status TEXT NOT NULL CHECK (status IN ('PROCESSING', 'ACKNOWLEDGED')),
  acked_at TEXT NOT NULL,
  PRIMARY KEY (message_id, agent_id, delivery_generation, status)
);
INSERT INTO acks_without_mismatch
  SELECT message_id, agent_id, delivery_generation, status, acked_at
  FROM acks WHERE status IN ('PROCESSING', 'ACKNOWLEDGED');
DROP TABLE acks;
ALTER TABLE acks_without_mismatch RENAME TO acks;
