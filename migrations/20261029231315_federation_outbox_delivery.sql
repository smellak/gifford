-- +goose Up
CREATE TABLE federation_outbox_requests (
  outbox_id TEXT PRIMARY KEY REFERENCES federation_outbox(outbox_id),
  destination_node TEXT NOT NULL,
  request BLOB NOT NULL,
  request_sha256 TEXT NOT NULL CHECK (length(request_sha256) = 64),
  sequence INTEGER NOT NULL CHECK (sequence > 0),
  nonce BLOB NOT NULL CHECK (length(nonce) = 16)
);

CREATE UNIQUE INDEX federation_outbox_requests_sequence
  ON federation_outbox_requests(destination_node, sequence);
CREATE UNIQUE INDEX federation_outbox_requests_nonce
  ON federation_outbox_requests(destination_node, nonce);

CREATE TABLE federation_receipts (
  outbox_id TEXT PRIMARY KEY REFERENCES federation_outbox(outbox_id),
  receipt BLOB NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('STORED_REMOTE', 'CONTROL_DENIED')),
  received_at TEXT NOT NULL
);

-- +goose Down
DROP TABLE federation_receipts;
DROP INDEX federation_outbox_requests_nonce;
DROP INDEX federation_outbox_requests_sequence;
DROP TABLE federation_outbox_requests;
