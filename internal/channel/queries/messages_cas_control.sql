-- Replace a message triple if unchanged. Parameters: new triple, message_id, old triple.
UPDATE messages SET control_state = ?, control_reason = ?, control_version = ?
WHERE message_id = ? AND control_state = ? AND control_reason = ? AND control_version = ?;
