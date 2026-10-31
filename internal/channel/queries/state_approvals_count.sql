SELECT count(*)
FROM approval_requests
WHERE agent_id = ? AND state = 'PENDING'
