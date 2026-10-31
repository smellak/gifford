-- +goose Up
-- Durable metadata-only outbox for human notifications. internal/notifications
-- is its only writer. SERVICE/INITIAL is the hidden first-start marker; the
-- panel-facing rows are MESSAGE and APPROVAL.
CREATE TABLE human_notifications (
  notification_id TEXT PRIMARY KEY,
  class TEXT NOT NULL CHECK (class IN ('MESSAGE', 'APPROVAL', 'SERVICE')),
  object_id TEXT NOT NULL,
  sender_agent TEXT,
  recipient_agents_json TEXT NOT NULL,
  priority TEXT,
  reason TEXT NOT NULL CHECK (reason IN (
    'BASELINE',
    'MESSAGE_AVAILABLE',
    'DISPLAY_ONLY',
    'CHANNEL_HELD',
    'CHANNEL_APPROVAL_REQUIRED',
    'APPROVAL_PENDING'
  )),
  state TEXT NOT NULL CHECK (state IN ('BASELINED', 'PENDING', 'RENDERED')),
  adapter TEXT NOT NULL CHECK (adapter IN ('NONE', 'TMUX')),
  attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  last_error_reason TEXT CHECK (last_error_reason IS NULL OR last_error_reason IN (
    'ADAPTER_DISABLED',
    'ADAPTER_UNAVAILABLE',
    'UNSUPPORTED_FLAGS',
    'CLIENT_UNAVAILABLE',
    'ADAPTER_TIMEOUT',
    'INVALID_TEMPLATE',
    'ADAPTER_FAILED'
  )),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  last_attempt_at TEXT,
  next_attempt_at TEXT,
  rendered_at TEXT,
  UNIQUE (class, object_id)
);

CREATE INDEX human_notifications_pending
  ON human_notifications (state, next_attempt_at, created_at)
  WHERE state = 'PENDING';

-- +goose Down
DROP INDEX human_notifications_pending;
DROP TABLE human_notifications;
