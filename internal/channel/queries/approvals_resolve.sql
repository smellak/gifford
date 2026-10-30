UPDATE approval_requests
SET state = ?, resolution_json = ?, resolved_by = ?, resolved_at = ?
WHERE approval_id = ? AND source = 'orchestrator' AND state = 'PENDING'
