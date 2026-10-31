SELECT loop_id, thread_id, loop_generation, owner_node, origin,
       participants_json, mode, state_reason, budget_profile, root_message_id,
       max_turns, max_messages, max_duration_ms, max_messages_per_minute,
       max_hops, max_consecutive_same_pair, max_retries, max_artifact_count,
       max_artifact_bytes, turn_count, message_count, hop_count,
       artifact_count, artifact_bytes, step_tokens, last_sender_agent,
       pattern_state_json, control_version, started_at, expires_at,
       last_progress_at, stopped_at, created_by, updated_at
FROM communication_loops
WHERE loop_id = ?
LIMIT 1
