-- Set an envelope's local triple. Parameters: control_state, control_reason, control_version, message_id.
UPDATE message_envelopes SET control_state = ?, control_reason = ?, control_version = ?
WHERE message_id = ?;
