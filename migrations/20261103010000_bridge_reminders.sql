-- +goose Up
-- Bridge response reminders: one durable reminder per expectation
-- generation. The reminder owns its lifecycle state machine; the materialized bridge message,
-- its queue row and its source mapping commit in the same transaction as
-- the reminder row. No body, no panel text and no caller identity ever
-- land here: only the stable key, the immutable tuple and the lifecycle.
CREATE TABLE bridge_reminders (
  source_node_id TEXT NOT NULL,
  expectation_id TEXT NOT NULL,
  generation INTEGER NOT NULL CHECK (generation > 0),
  source_message_id INTEGER NOT NULL CHECK (source_message_id > 0),
  from_agent TEXT NOT NULL,
  to_agent TEXT NOT NULL,
  source_read_at TEXT NOT NULL,
  eligible_at TEXT NOT NULL,
  accepted_at TEXT NOT NULL,
  expires_at TEXT NOT NULL,
  state TEXT NOT NULL CHECK (state IN (
    'PENDING', 'DELIVERING', 'DELIVERED', 'EXPIRED', 'CANCELLED', 'UNCERTAIN'
  )),
  reminder_id TEXT NOT NULL UNIQUE,
  gifford_message_id TEXT NOT NULL UNIQUE,
  delivery_attempt_id TEXT,
  delivered_at TEXT,
  terminal_at TEXT,
  last_reconciled_at TEXT,
  cancel_reason TEXT CHECK (
    cancel_reason IS NULL OR cancel_reason IN (
      'EXPECTATION_RESOLVED', 'MODE_SWITCH', 'ROLLBACK'
    )
  ),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  PRIMARY KEY (source_node_id, expectation_id, generation)
);

-- At most one DELIVERED reminder per expectation, enforced by the base.
CREATE UNIQUE INDEX bridge_reminders_single_delivered
  ON bridge_reminders (source_node_id, expectation_id)
  WHERE state = 'DELIVERED';

-- +goose Down
DROP TABLE bridge_reminders;
