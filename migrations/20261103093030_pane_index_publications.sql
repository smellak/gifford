-- +goose Up
-- Pane index publications: one row per Alta index publication the node
-- completes. The row is written by the caller in its own transaction after
-- provision returns, using the publication timestamp the provision captured
-- right after the U/index directory fsync. The panel key is the lock path
-- relative to the managed root: <owner>/<socket-sha>/<pane>.lock.
CREATE TABLE pane_index_publications (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  panel_key TEXT NOT NULL,
  published_at TEXT NOT NULL
);

-- +goose Down
DROP TABLE pane_index_publications;
