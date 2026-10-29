SELECT message_id, priority, expires_at, state, control_reason, admission_result_json,
  created_at, thread_ref
FROM messages WHERE origin_node = ? AND from_agent = ? AND client_msg_id = ?;
