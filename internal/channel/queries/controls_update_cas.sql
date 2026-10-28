-- Compare-and-swap update of one control. Parameters: mode, tx_enabled,
-- rx_enabled, auto_delivery_enabled, auto_continue_enabled, reason,
-- actor_type, created_by, expires_at, restore_json, then_mode, new version,
-- updated_at, then scope_type, scope_key, plane and the expected version.
-- Zero rows affected means CONTROL_VERSION_CONFLICT.
UPDATE channel_controls
SET mode = ?, tx_enabled = ?, rx_enabled = ?, auto_delivery_enabled = ?,
    auto_continue_enabled = ?, reason = ?, actor_type = ?, created_by = ?,
    expires_at = ?, restore_json = ?, then_mode = ?, version = ?, updated_at = ?
WHERE scope_type = ? AND scope_key = ? AND plane = ? AND version = ?;
