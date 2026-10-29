-- +goose Up
CREATE TABLE artifact_transfers (
  artifact_id TEXT PRIMARY KEY,
  client_msg_id TEXT NOT NULL,
  origin_node TEXT NOT NULL,
  sender_agent TEXT NOT NULL,
  destination_node TEXT NOT NULL,
  state TEXT NOT NULL,
  encryption_profile TEXT NOT NULL,
  ciphertext_size INTEGER,
  chunk_count INTEGER,
  ciphertext_sha256 TEXT,
  encrypted_manifest BLOB,
  sender_key_id TEXT NOT NULL,
  signature BLOB NOT NULL,
  expires_at TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  -- Added (design §17.11): loop and control columns.
  thread_ref TEXT,                       -- thr_<ULID>; set only when the transfer belongs to a loop (design §16.4)
  loop_id TEXT,
  loop_generation INTEGER,
  hop_count INTEGER NOT NULL DEFAULT 0,
  route_digest TEXT,                     -- sha256:<hex>
  control_state TEXT NOT NULL DEFAULT 'HELD',                -- ALLOWED | HELD | APPROVAL_REQUIRED | DENIED; written only by internal/channel
  control_reason TEXT NOT NULL DEFAULT 'CONTROL_UNAVAILABLE', -- a DecisionReason wire value
  control_version INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE artifact_recipients (
  artifact_id TEXT NOT NULL,
  recipient_agent TEXT NOT NULL,
  recipient_node TEXT NOT NULL,
  key_envelope BLOB NOT NULL,
  state TEXT NOT NULL,
  acknowledged_at TEXT,
  PRIMARY KEY (artifact_id, recipient_agent)
);

CREATE TABLE artifact_chunks (
  artifact_id TEXT NOT NULL,
  chunk_index INTEGER NOT NULL,
  ciphertext_size INTEGER NOT NULL,
  ciphertext_sha256 TEXT,
  storage_path TEXT NOT NULL,
  fsynced INTEGER NOT NULL DEFAULT 0,
  replicated INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  PRIMARY KEY (artifact_id, chunk_index)
);

-- Added: held artifacts without scanning the table; an agent's incoming artifacts.
CREATE INDEX artifact_transfers_control ON artifact_transfers(control_state, state);
CREATE INDEX artifact_recipients_agent ON artifact_recipients(recipient_agent, state);

-- +goose Down
DROP INDEX artifact_recipients_agent;
DROP INDEX artifact_transfers_control;
DROP TABLE artifact_chunks;
DROP TABLE artifact_recipients;
DROP TABLE artifact_transfers;
