INSERT INTO channel_controls
  (control_id, scope_type, scope_key, plane, rx_enabled, actor_type,
   created_by, version, created_at, updated_at)
VALUES
  ('ctl_00000000000000000000000901', 'agent', 'agent-recipient', 'all', 0,
   'human', 'test-operator', 1, '2026-10-29T12:00:00.000000000Z',
   '2026-10-29T12:00:00.000000000Z');
UPDATE channel_epoch SET epoch = 1 WHERE id = 1;
