-- Set an admitted artifact triple. Parameters: state, reason, version, artifact_id.
UPDATE artifact_transfers
SET control_state = ?, control_reason = ?, control_version = ?
WHERE artifact_id = ?;
