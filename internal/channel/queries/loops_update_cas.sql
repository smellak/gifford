UPDATE communication_loops
SET mode = ?, state_reason = ?, budget_profile = ?,
    max_turns = ?, max_messages = ?, max_duration_ms = ?,
    max_messages_per_minute = ?, max_hops = ?,
    max_consecutive_same_pair = ?, max_retries = ?, max_artifact_count = ?,
    max_artifact_bytes = ?, turn_count = ?, message_count = ?, hop_count = ?,
    artifact_count = ?, artifact_bytes = ?, step_tokens = ?,
    last_sender_agent = ?, control_version = ?, expires_at = ?,
    last_progress_at = ?, stopped_at = ?, updated_at = ?
WHERE loop_id = ? AND control_version = ?
