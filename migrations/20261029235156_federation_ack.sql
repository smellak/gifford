-- +goose Up
CREATE TABLE federation_outbox_acks (
  outbox_id TEXT PRIMARY KEY REFERENCES federation_outbox(outbox_id),
  destination_node TEXT NOT NULL,
  request BLOB NOT NULL,
  request_sha256 TEXT NOT NULL CHECK (length(request_sha256) = 64),
  sequence INTEGER NOT NULL CHECK (sequence > 0),
  nonce BLOB NOT NULL CHECK (length(nonce) = 16)
);

CREATE UNIQUE INDEX federation_outbox_acks_sequence
  ON federation_outbox_acks(destination_node, sequence);
CREATE UNIQUE INDEX federation_outbox_acks_nonce
  ON federation_outbox_acks(destination_node, nonce);

CREATE TABLE federation_ack_inbox (
  origin_node TEXT NOT NULL,
  sequence INTEGER NOT NULL CHECK (sequence > 0),
  nonce BLOB NOT NULL CHECK (length(nonce) = 16),
  message_id TEXT NOT NULL,
  agent_id TEXT NOT NULL,
  delivery_generation INTEGER NOT NULL CHECK (delivery_generation > 0),
  status TEXT NOT NULL CHECK (status IN ('PROCESSING', 'ACKNOWLEDGED')),
  request_sha256 TEXT NOT NULL CHECK (length(request_sha256) = 64),
  receipt BLOB NOT NULL,
  received_at TEXT NOT NULL,
  PRIMARY KEY (origin_node, message_id, agent_id, delivery_generation, status)
);

CREATE UNIQUE INDEX federation_ack_inbox_sequence
  ON federation_ack_inbox(origin_node, sequence);
CREATE UNIQUE INDEX federation_ack_inbox_nonce
  ON federation_ack_inbox(origin_node, nonce);

CREATE TABLE federation_ack_receipts (
  outbox_id TEXT PRIMARY KEY REFERENCES federation_outbox(outbox_id),
  receipt BLOB NOT NULL,
  received_at TEXT NOT NULL
);

-- +goose Down
DROP TABLE federation_ack_receipts;
DROP INDEX federation_ack_inbox_nonce;
DROP INDEX federation_ack_inbox_sequence;
DROP TABLE federation_ack_inbox;
DROP INDEX federation_outbox_acks_nonce;
DROP INDEX federation_outbox_acks_sequence;
DROP TABLE federation_outbox_acks;
