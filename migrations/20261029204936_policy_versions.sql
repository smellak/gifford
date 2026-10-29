-- +goose Up
CREATE TABLE policy_versions (
  version TEXT PRIMARY KEY,
  digest TEXT NOT NULL,
  compiled_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);

-- +goose Down
DROP TABLE policy_versions;
