-- Initialize one queued delivery from its message's final triple.
UPDATE delivery_queue SET control_state = ?, control_reason = ?, control_version = ?
WHERE message_id = ? AND agent_id = ? AND delivery_generation = ?;
