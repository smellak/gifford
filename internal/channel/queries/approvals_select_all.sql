SELECT approval_id, source, object_type, object_id, agent_id, loop_id,
  options_json, requested_by, request_note, state, resolution_json,
  resolved_by, expires_at, created_at, resolved_at
FROM approval_requests
ORDER BY created_at, approval_id
