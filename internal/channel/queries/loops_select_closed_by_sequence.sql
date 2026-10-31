SELECT l.loop_id, l.thread_id, l.loop_generation, l.owner_node, l.origin,
       l.participants_json, l.mode, l.state_reason, l.budget_profile, l.root_message_id,
       l.max_turns, l.max_messages, l.max_duration_ms, l.max_messages_per_minute,
       l.max_hops, l.max_consecutive_same_pair, l.max_retries, l.max_artifact_count,
       l.max_artifact_bytes, l.turn_count, l.message_count, l.hop_count,
       l.artifact_count, l.artifact_bytes, l.step_tokens, l.last_sender_agent,
       l.pattern_state_json, l.control_version, l.started_at, l.expires_at,
       l.last_progress_at, l.stopped_at, l.created_by, l.updated_at
FROM communication_loops AS l
JOIN communication_loop_closures AS c ON c.loop_id = l.loop_id
WHERE l.thread_id = ? AND c.closed_thread_sequence >= ?
  AND l.mode IN ('STOPPED', 'COMPLETED')
ORDER BY l.loop_generation ASC
LIMIT 1
