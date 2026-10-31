SELECT thread_id, from_agent, parent_message_id, loop_id, loop_generation, turn_index,
       requested_autonomy, requested_budget_json, thread_sequence
FROM messages
WHERE message_id = ?
LIMIT 1
