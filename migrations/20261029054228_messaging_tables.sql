-- +goose Up
-- One row per logical message on this node's Hub: routing and state, no content.
CREATE TABLE messages (
  message_id TEXT PRIMARY KEY,           -- ULID
  client_msg_id TEXT NOT NULL,           -- idempotency key of send_message (design §14.1)
  origin_node TEXT NOT NULL,
  from_agent TEXT NOT NULL,
  destination_node TEXT NOT NULL,
  thread_ref TEXT NOT NULL,              -- thr_<ULID> of the signed header
  thread_id TEXT,                        -- the readable thread; NULL where only thread_ref is known
  priority TEXT NOT NULL,                -- LOW | NORMAL | HIGH | URGENT
  state TEXT NOT NULL,                   -- PENDING | CLAIMED | DELIVERY_QUEUED | DELIVERED_TO_HARNESS | FETCHED_BY_AGENT
                                         -- | DECRYPTED | PROCESSING | ACKNOWLEDGED | REPLIED | FAILED | EXPIRED | CANCELLED (design §14.3)
  human_notified_at TEXT,                -- HUMAN_NOTIFIED coexists with the other states (design §14.3)
  requested_autonomy TEXT,               -- AUTO | STEP, a root message's request for the loop a reply may open (design §14.7)
  requested_budget_json TEXT,            -- RequestedBudget, the same request
  created_at TEXT NOT NULL,
  expires_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  -- Loop and control columns (design §17.11).
  parent_message_id TEXT,
  thread_sequence INTEGER,
  loop_id TEXT,
  loop_generation INTEGER,
  turn_index INTEGER,
  hop_count INTEGER NOT NULL,
  route_digest TEXT NOT NULL,            -- sha256:<hex>
  control_state TEXT NOT NULL DEFAULT 'HELD',                -- ALLOWED | HELD | APPROVAL_REQUIRED | DENIED; written only by internal/channel
  control_reason TEXT NOT NULL DEFAULT 'CONTROL_UNAVAILABLE', -- a DecisionReason wire value
  control_version INTEGER NOT NULL DEFAULT 0                  -- the version of the local channel evaluation (admission or acceptance)
);

-- One row per signed wire envelope stored by this Hub, sent or received.
CREATE TABLE message_envelopes (
  message_id TEXT PRIMARY KEY,
  envelope BLOB NOT NULL,                -- the signed WireEnvelope, byte for byte as received or sent; never rewritten
  -- Projections of signed header fields, for queries; each equals its value inside envelope.
  protocol TEXT NOT NULL,                -- gifford-message
  version INTEGER NOT NULL,              -- 1
  client_msg_id TEXT NOT NULL,
  origin_node TEXT NOT NULL,
  origin_agent TEXT NOT NULL,
  destination_node TEXT NOT NULL,
  recipient_agents_json TEXT NOT NULL,   -- JSON array of AgentID
  created_at TEXT NOT NULL,
  expires_at TEXT NOT NULL,
  priority TEXT NOT NULL,
  thread_ref TEXT NOT NULL,
  encryption_profile TEXT NOT NULL,      -- none | gifford-e2e-v1
  sender_signing_key_id TEXT NOT NULL,   -- ed25519:<agent>:v<n>
  ciphertext_sha256 TEXT NOT NULL,
  stored_at TEXT NOT NULL,
  -- Loop and control columns (design §17.11).
  parent_message_id TEXT,
  thread_sequence INTEGER,               -- LOCAL PROJECTION, not in the signed envelope: the admitting Hub may fill it; NULL when received (§11)
  loop_id TEXT,
  loop_generation INTEGER,
  turn_index INTEGER,
  hop_count INTEGER NOT NULL,
  route_digest TEXT NOT NULL,
  wire_control_version INTEGER NOT NULL, -- the signed header's control version: an immutable projection, equal to the value inside envelope
  -- The local channel evaluation, written only by internal/channel; not a projection of the envelope.
  control_state TEXT NOT NULL DEFAULT 'HELD',
  control_reason TEXT NOT NULL DEFAULT 'CONTROL_UNAVAILABLE',
  control_version INTEGER NOT NULL DEFAULT 0
);

-- One row per delivery of a message to one recipient agent in one
-- delivery generation (design §23.6). Written by the daemon.
CREATE TABLE delivery_queue (
  message_id TEXT NOT NULL,
  agent_id TEXT NOT NULL,                -- the recipient
  delivery_generation INTEGER NOT NULL DEFAULT 1,
  from_agent TEXT NOT NULL,              -- the sender: link controls are per sender → recipient
  thread_ref TEXT NOT NULL,
  state TEXT NOT NULL,                   -- internal: QUEUED | CLAIMED | DELIVERING | RETRY_WAIT | DELIVERED | FAILED | EXPIRED | CANCELLED (§5.4.1)
  priority INTEGER NOT NULL,             -- internal rank, 0 URGENT, 1 HIGH, 2 NORMAL, 3 LOW: lower goes first (§6.1)
  attempts INTEGER NOT NULL DEFAULT 0,   -- delivery attempts started (design §14.3: at most 3 retries)
  next_attempt_at TEXT,                  -- RETRY_WAIT only
  claimed_by TEXT,                       -- CLAIMED and DELIVERING: the daemon worker holding the row
  claim_until TEXT,                      -- CLAIMED and DELIVERING: after this, recovery applies (§5.4.1)
  created_at TEXT NOT NULL,              -- enqueue time
  expires_at TEXT NOT NULL,              -- the message's
  updated_at TEXT NOT NULL,
  -- Loop and control columns (design §17.11).
  parent_message_id TEXT,
  thread_sequence INTEGER,
  loop_id TEXT,
  loop_generation INTEGER,
  turn_index INTEGER,
  hop_count INTEGER NOT NULL,
  route_digest TEXT NOT NULL,
  control_state TEXT NOT NULL DEFAULT 'HELD',
  control_reason TEXT NOT NULL DEFAULT 'CONTROL_UNAVAILABLE',
  control_version INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (message_id, agent_id, delivery_generation)
);

-- Added: one message per sender and client_msg_id (design §14.1); thread order; loop members.
CREATE UNIQUE INDEX messages_client_msg ON messages(origin_node, from_agent, client_msg_id);
CREATE INDEX messages_thread ON messages(thread_ref, thread_sequence);
CREATE INDEX messages_loop ON messages(loop_id, loop_generation) WHERE loop_id IS NOT NULL;
-- Design §17.10 and §17.11: the pending queue and the held objects of each agent.
CREATE INDEX delivery_queue_pending ON delivery_queue(agent_id, state, priority, created_at);
CREATE INDEX delivery_queue_control ON delivery_queue(agent_id, control_state, state);

-- +goose Down
DROP INDEX delivery_queue_control;
DROP INDEX delivery_queue_pending;
DROP INDEX messages_loop;
DROP INDEX messages_thread;
DROP INDEX messages_client_msg;
DROP TABLE delivery_queue;
DROP TABLE message_envelopes;
DROP TABLE messages;
