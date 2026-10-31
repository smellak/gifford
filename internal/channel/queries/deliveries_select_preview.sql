-- Read the immutable message metadata and mutable queue state needed by a
-- read-only delivery preview. Parameters: message_id, recipient, generation.
SELECT dq.control_state, dq.control_reason, dq.control_version, dq.state,
       dq.from_agent, m.origin_node, m.destination_node, m.thread_id,
       m.parent_message_id, m.hop_count,
       EXISTS(
         SELECT 1 FROM sessions AS s
         WHERE s.agent_id = dq.agent_id AND s.state = 'CONNECTED'
           AND s.deliverable = 1 AND s.ended_at IS NULL
       )
FROM delivery_queue AS dq
JOIN messages AS m ON m.message_id = dq.message_id
WHERE dq.message_id = ? AND dq.agent_id = ? AND dq.delivery_generation = ?;
