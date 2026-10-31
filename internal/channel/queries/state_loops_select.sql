SELECT loop_id, participants_json, mode, turn_count, max_turns,
       message_count, max_messages, started_at, max_duration_ms
FROM communication_loops
WHERE mode NOT IN ('STOPPED', 'COMPLETED')
ORDER BY loop_id
