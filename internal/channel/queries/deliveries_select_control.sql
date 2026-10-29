-- Read one queued delivery's mutable local control triple and operational state.
SELECT control_state, control_reason, control_version, state FROM delivery_queue
WHERE message_id = ? AND agent_id = ? AND delivery_generation = ?;
