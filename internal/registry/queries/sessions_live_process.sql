SELECT session_id, agent_id, process_json FROM sessions
WHERE process_json IS NOT NULL AND state = 'CONNECTED' AND ended_at IS NULL;
