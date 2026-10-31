-- Read an artifact control triple. Parameters: artifact_id.
SELECT control_state, control_reason, control_version
FROM artifact_transfers WHERE artifact_id = ?;
