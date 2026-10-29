SELECT message_id, state, control_reason, client_msg_id, from_agent, expires_at
FROM messages WHERE message_id = ?;
