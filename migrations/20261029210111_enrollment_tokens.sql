-- +goose Up
CREATE TABLE enrollment_tokens (
  hash BLOB PRIMARY KEY,
  org TEXT NOT NULL,
  created_at TEXT NOT NULL,
  expires_at TEXT NOT NULL,
  used_at TEXT,
  used_by TEXT
);

-- +goose Down
DROP TABLE enrollment_tokens;
