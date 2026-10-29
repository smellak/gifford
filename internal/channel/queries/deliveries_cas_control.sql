-- Replace one queued delivery's triple after a delivery-stage evaluation.
UPDATE delivery_queue SET control_state = ?, control_reason = ?, control_version = ?
WHERE message_id = ? AND agent_id = ? AND delivery_generation = ?
  AND control_state = ? AND control_reason = ? AND control_version = ?;
