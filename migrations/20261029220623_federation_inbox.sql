-- +goose Up
CREATE TABLE federation_inbox (
  origin_node TEXT NOT NULL,
  sequence INTEGER NOT NULL CHECK (sequence > 0),
  nonce BLOB NOT NULL CHECK (length(nonce) = 16),
  message_id TEXT NOT NULL,
  delivery_generation INTEGER NOT NULL CHECK (delivery_generation >= 1),
  request_sha256 TEXT NOT NULL CHECK (length(request_sha256) = 64),
  envelope_sha256 TEXT NOT NULL CHECK (length(envelope_sha256) = 64),
  receipt BLOB NOT NULL,
  received_at TEXT NOT NULL,
  PRIMARY KEY (origin_node, message_id),
  UNIQUE (origin_node, sequence),
  UNIQUE (origin_node, nonce)
);

-- +goose Down
DROP TABLE federation_inbox;
