-- +goose Up
-- Captured when a generation becomes terminal. A side table preserves the
-- frozen communication_loops schema while distinguishing a genuinely new
-- root admitted after closure from a delayed reply to an older root.
CREATE TABLE communication_loop_closures (
  loop_id TEXT PRIMARY KEY NOT NULL REFERENCES communication_loops(loop_id) ON DELETE CASCADE,
  closed_thread_sequence INTEGER NOT NULL CHECK (closed_thread_sequence >= 0)
);

CREATE TRIGGER communication_loops_capture_closure
AFTER UPDATE OF mode ON communication_loops
WHEN NEW.mode IN ('STOPPED', 'COMPLETED')
 AND OLD.mode NOT IN ('STOPPED', 'COMPLETED')
BEGIN
  INSERT OR IGNORE INTO communication_loop_closures(loop_id, closed_thread_sequence)
  SELECT NEW.loop_id, COALESCE(MAX(thread_sequence), 0)
  FROM messages WHERE thread_id = NEW.thread_id;
END;

-- +goose Down
DROP TRIGGER communication_loops_capture_closure;
DROP TABLE communication_loop_closures;
