SELECT thread_id, parent_message_id, loop_id, loop_generation, turn_index,
       control_state,
       EXISTS(
         SELECT 1 FROM approval_requests ar
         WHERE ar.object_type = 'message' AND ar.object_id = messages.message_id
           AND ar.state = 'PENDING'
       )
FROM messages
WHERE message_id = ?
LIMIT 1
