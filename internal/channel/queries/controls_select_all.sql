-- Every control row, to materialize the effective-control cache (Reload).
SELECT control_id, scope_type, scope_key, plane, mode,
       tx_enabled, rx_enabled, auto_delivery_enabled, auto_continue_enabled,
       from_agent, to_agent, reason, actor_type, created_by, expires_at,
       restore_json, then_mode, version, created_at, updated_at
FROM channel_controls
ORDER BY scope_type, scope_key, plane;
