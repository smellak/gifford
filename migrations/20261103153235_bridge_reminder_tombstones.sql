-- +goose Up
-- A tombstone seals one reminder key before or after a PUT. It stores the
-- exact terminal receipt returned to retries; no message, queue row or delivery
-- attempt is created for a key that was absent when sealed.
CREATE TABLE bridge_reminder_tombstones (
  source_node_id TEXT NOT NULL,
  expectation_id TEXT NOT NULL,
  generation INTEGER NOT NULL CHECK (generation > 0),
  reason TEXT NOT NULL CHECK (reason IN (
    'EXPECTATION_RESOLVED', 'MODE_SWITCH', 'ROLLBACK'
  )),
  reminder_id TEXT NOT NULL UNIQUE,
  gifford_message_id TEXT NOT NULL UNIQUE,
  disposition TEXT NOT NULL CHECK (disposition IN ('CREATED', 'DUPLICATE')),
  state TEXT NOT NULL CHECK (state IN (
    'DELIVERED', 'EXPIRED', 'CANCELLED', 'UNCERTAIN'
  )),
  accepted_at TEXT NOT NULL,
  expires_at TEXT NOT NULL,
  delivery_attempt_id TEXT,
  created_at TEXT NOT NULL,
  PRIMARY KEY (source_node_id, expectation_id, generation)
);

-- +goose Down
DROP TABLE bridge_reminder_tombstones;
