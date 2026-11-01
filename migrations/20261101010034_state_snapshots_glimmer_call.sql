-- +goose Up
-- The Model tab reads the state snapshot of the latest ANALYZE_AGENT_STATE
-- call. This index finds a call's snapshots, newest first, without scanning
-- state_snapshots.
CREATE INDEX state_snapshots_glimmer_call
  ON state_snapshots(glimmer_call_id, observed_at, state_id);

-- +goose Down
DROP INDEX state_snapshots_glimmer_call;
