-- +goose Up
-- Node-local projection of repeated terminal delivery failures. The incident
-- worker is the only writer; delivery_attempts remains its read-only source.
CREATE TABLE delivery_incidents (
  recipient_agent TEXT NOT NULL,
  normalized_reason TEXT NOT NULL,
  attempt_count INTEGER NOT NULL CHECK (attempt_count >= 1),
  first_seen TEXT NOT NULL,
  last_seen TEXT NOT NULL,
  alerted_at TEXT,
  last_alerted_at TEXT,
  recovered_at TEXT,
  recovery_notified_at TEXT,
  pending_kind TEXT CHECK (pending_kind IN ('ALERT', 'RECOVERY')),
  pending_at TEXT,
  pending_minutes INTEGER CHECK (pending_minutes IS NULL OR pending_minutes >= 0),
  pending_attempt_count INTEGER CHECK (pending_attempt_count IS NULL OR pending_attempt_count >= 1),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  PRIMARY KEY (recipient_agent, normalized_reason),
  CHECK ((pending_kind IS NULL AND pending_at IS NULL AND pending_minutes IS NULL AND pending_attempt_count IS NULL)
    OR (pending_kind IS NOT NULL AND pending_at IS NOT NULL AND pending_minutes IS NOT NULL AND pending_attempt_count IS NOT NULL))
);

-- The current episode retains its contributing attempts and relational links
-- to the original messages without copying message content into the register.
CREATE TABLE delivery_incident_messages (
  recipient_agent TEXT NOT NULL,
  normalized_reason TEXT NOT NULL,
  delivery_id TEXT NOT NULL,
  message_id TEXT NOT NULL,
  observed_at TEXT NOT NULL,
  PRIMARY KEY (recipient_agent, normalized_reason, delivery_id),
  FOREIGN KEY (recipient_agent, normalized_reason)
    REFERENCES delivery_incidents (recipient_agent, normalized_reason) ON DELETE CASCADE,
  FOREIGN KEY (delivery_id) REFERENCES delivery_attempts (delivery_id),
  FOREIGN KEY (message_id) REFERENCES messages (message_id)
);

CREATE INDEX delivery_incident_messages_message
  ON delivery_incident_messages (message_id);

-- Each original sender gets one durable, metadata-only observation per
-- incident filing. queued_at is also the pair-level cooldown reservation, so
-- another reason cannot enqueue a second alert while the first is pending.
CREATE TABLE delivery_incident_sender_notices (
  recipient_agent TEXT NOT NULL,
  normalized_reason TEXT NOT NULL,
  sender_agent TEXT NOT NULL,
  kind TEXT NOT NULL CHECK (kind IN ('ALERT', 'RECOVERY')),
  queued_at TEXT NOT NULL,
  minutes INTEGER NOT NULL CHECK (minutes >= 0),
  attempt_count INTEGER NOT NULL CHECK (attempt_count >= 1),
  filed_at TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  PRIMARY KEY (recipient_agent, normalized_reason, sender_agent, kind, queued_at),
  FOREIGN KEY (recipient_agent, normalized_reason)
    REFERENCES delivery_incidents (recipient_agent, normalized_reason) ON DELETE CASCADE
);

CREATE INDEX delivery_incident_sender_notices_pending
  ON delivery_incident_sender_notices (queued_at, recipient_agent, normalized_reason, sender_agent)
  WHERE filed_at IS NULL;

CREATE INDEX delivery_incident_sender_notices_cooldown
  ON delivery_incident_sender_notices (sender_agent, recipient_agent, kind, filed_at);

-- +goose Down
DROP INDEX delivery_incident_sender_notices_cooldown;
DROP INDEX delivery_incident_sender_notices_pending;
DROP TABLE delivery_incident_sender_notices;
DROP INDEX delivery_incident_messages_message;
DROP TABLE delivery_incident_messages;
DROP TABLE delivery_incidents;
