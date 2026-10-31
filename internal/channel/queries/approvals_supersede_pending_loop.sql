UPDATE approval_requests
SET state = 'SUPERSEDED', resolution_json = NULL,
  resolved_by = ?, resolved_at = ?
WHERE loop_id = ? AND state = 'PENDING'
