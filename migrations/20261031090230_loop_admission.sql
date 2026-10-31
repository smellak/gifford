-- +goose Up
-- One live generation owns a thread. PAUSED and DRAINING remain nonterminal.
CREATE UNIQUE INDEX communication_loops_nonterminal_thread
ON communication_loops(thread_id)
WHERE mode NOT IN ('STOPPED', 'COMPLETED');

CREATE INDEX communication_loops_root_message
ON communication_loops(root_message_id)
WHERE root_message_id IS NOT NULL;

-- The admitting Hub owns thread_sequence. Legacy NULL projections stay NULL;
-- only newly admitted messages receive an ordinal.
CREATE UNIQUE INDEX messages_thread_sequence_unique
ON messages(thread_ref, thread_sequence)
WHERE thread_sequence IS NOT NULL;

-- +goose Down
DROP INDEX messages_thread_sequence_unique;
DROP INDEX communication_loops_root_message;
DROP INDEX communication_loops_nonterminal_thread;
