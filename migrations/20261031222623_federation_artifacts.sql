-- +goose Up
CREATE TABLE federation_artifact_outbox (
  outbox_id TEXT PRIMARY KEY REFERENCES federation_outbox(outbox_id),
  manifest BLOB NOT NULL,
  manifest_sha256 TEXT NOT NULL CHECK (length(manifest_sha256) = 64),
  manifest_accepted INTEGER NOT NULL DEFAULT 0 CHECK (manifest_accepted IN (0, 1)),
  next_chunk INTEGER NOT NULL DEFAULT 0 CHECK (next_chunk >= 0),
  last_receipt BLOB
);

CREATE TABLE federation_artifact_receipts (
  receipt_id INTEGER PRIMARY KEY AUTOINCREMENT,
  outbox_id TEXT NOT NULL REFERENCES federation_outbox(outbox_id),
  request_sha256 TEXT NOT NULL CHECK (length(request_sha256) = 64),
  receipt BLOB NOT NULL,
  next_chunk INTEGER NOT NULL CHECK (next_chunk >= 0),
  state TEXT NOT NULL CHECK (state IN ('REPLICATING', 'STORED_REMOTE')),
  received_at TEXT NOT NULL,
  UNIQUE (outbox_id, request_sha256, next_chunk, state)
);

CREATE TABLE federation_artifact_inbox (
  artifact_id TEXT PRIMARY KEY,
  origin_node TEXT NOT NULL,
  request_sha256 TEXT NOT NULL CHECK (length(request_sha256) = 64),
  received_at TEXT NOT NULL
);

CREATE INDEX federation_artifact_receipts_outbox
  ON federation_artifact_receipts(outbox_id, receipt_id);

-- +goose Down
DROP INDEX federation_artifact_receipts_outbox;
DROP TABLE federation_artifact_inbox;
DROP TABLE federation_artifact_receipts;
DROP TABLE federation_artifact_outbox;
