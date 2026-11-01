-- +goose Up
CREATE TABLE federation_artifact_ack_outbox (
  outbox_id TEXT PRIMARY KEY REFERENCES federation_outbox(outbox_id),
  destination_node TEXT NOT NULL,
  request BLOB NOT NULL,
  request_sha256 TEXT NOT NULL CHECK (length(request_sha256) = 64),
  sequence INTEGER NOT NULL CHECK (sequence > 0),
  nonce BLOB NOT NULL CHECK (length(nonce) = 16)
);

CREATE UNIQUE INDEX federation_artifact_ack_outbox_sequence
  ON federation_artifact_ack_outbox(destination_node, sequence);
CREATE UNIQUE INDEX federation_artifact_ack_outbox_nonce
  ON federation_artifact_ack_outbox(destination_node, nonce);

CREATE TABLE federation_artifact_ack_inbox (
  destination_node TEXT NOT NULL,
  sequence INTEGER NOT NULL CHECK (sequence > 0),
  nonce BLOB NOT NULL CHECK (length(nonce) = 16),
  artifact_id TEXT NOT NULL,
  recipient_agent TEXT NOT NULL,
  request_sha256 TEXT NOT NULL CHECK (length(request_sha256) = 64),
  receipt BLOB NOT NULL,
  received_at TEXT NOT NULL,
  PRIMARY KEY (destination_node, artifact_id, recipient_agent)
);

CREATE UNIQUE INDEX federation_artifact_ack_inbox_sequence
  ON federation_artifact_ack_inbox(destination_node, sequence);
CREATE UNIQUE INDEX federation_artifact_ack_inbox_nonce
  ON federation_artifact_ack_inbox(destination_node, nonce);

CREATE TABLE federation_artifact_ack_receipts (
  outbox_id TEXT PRIMARY KEY REFERENCES federation_outbox(outbox_id),
  receipt BLOB NOT NULL,
  received_at TEXT NOT NULL
);

-- +goose Down
DROP TABLE federation_artifact_ack_receipts;
DROP INDEX federation_artifact_ack_inbox_nonce;
DROP INDEX federation_artifact_ack_inbox_sequence;
DROP TABLE federation_artifact_ack_inbox;
DROP INDEX federation_artifact_ack_outbox_nonce;
DROP INDEX federation_artifact_ack_outbox_sequence;
DROP TABLE federation_artifact_ack_outbox;
