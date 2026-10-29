-- Read a message control triple. Parameters: message_id.
SELECT control_state, control_reason, control_version FROM messages WHERE message_id = ?;
