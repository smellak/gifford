-- +goose Up
CREATE TABLE nodes (
  node_id TEXT PRIMARY KEY,
  organization_id TEXT NOT NULL,
  local INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL,
  federation_endpoint TEXT,
  certificate_fingerprint TEXT,
  policy_version TEXT NOT NULL,
  last_seen_at TEXT,
  created_at TEXT NOT NULL
);

CREATE TABLE agent_keys (
  key_id TEXT PRIMARY KEY,
  agent_id TEXT NOT NULL,
  node_id TEXT NOT NULL,
  purpose TEXT NOT NULL,                 -- signing | encryption
  algorithm TEXT NOT NULL,               -- ed25519 | x25519
  public_key BLOB NOT NULL,
  status TEXT NOT NULL,
  valid_from TEXT NOT NULL,
  valid_until TEXT,
  issuer TEXT NOT NULL,
  record_signature BLOB NOT NULL
);

CREATE TABLE federation_outbox (
  outbox_id TEXT PRIMARY KEY,
  object_type TEXT NOT NULL,             -- message | artifact_manifest | artifact_chunk | ack | revocation
                                         -- | control (the loop owner's control envelope, design §15.15)
  object_id TEXT NOT NULL,
  destination_node TEXT NOT NULL,
  state TEXT NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0,
  next_attempt_at TEXT,
  last_error TEXT,
  idempotency_key TEXT NOT NULL,
  lease_owner TEXT,
  lease_until TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE UNIQUE INDEX federation_outbox_idempotency ON federation_outbox(destination_node, idempotency_key);

-- Added: an agent's keys by purpose; what a replicator claims next (design §17.10 leases).
CREATE INDEX agent_keys_agent ON agent_keys(agent_id, purpose, valid_from);
CREATE INDEX federation_outbox_pending ON federation_outbox(state, next_attempt_at);

-- +goose Down
DROP INDEX federation_outbox_pending;
DROP INDEX agent_keys_agent;
DROP INDEX federation_outbox_idempotency;
DROP TABLE federation_outbox;
DROP TABLE agent_keys;
DROP TABLE nodes;
