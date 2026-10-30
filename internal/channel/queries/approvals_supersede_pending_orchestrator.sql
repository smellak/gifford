UPDATE approval_requests
SET state = 'SUPERSEDED', resolution_json = NULL,
  resolved_by = 'orchestrator', resolved_at = ?
WHERE source = 'orchestrator' AND object_type = 'message'
  AND object_id = ? AND agent_id = ? AND state = 'PENDING'
