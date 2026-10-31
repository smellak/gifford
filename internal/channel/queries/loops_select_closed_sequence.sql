SELECT closed_thread_sequence
FROM communication_loop_closures
WHERE loop_id = ?
LIMIT 1
