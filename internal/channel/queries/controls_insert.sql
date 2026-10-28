-- Create a control row. Parameters: the 20 columns in the order listed.
INSERT INTO channel_controls (
  control_id, scope_type, scope_key, plane, mode,
  tx_enabled, rx_enabled, auto_delivery_enabled, auto_continue_enabled,
  from_agent, to_agent, reason, actor_type, created_by, expires_at,
  restore_json, then_mode, version, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
