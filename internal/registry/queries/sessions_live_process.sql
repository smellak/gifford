SELECT agent_id, process_json FROM sessions
WHERE process_json IS NOT NULL AND ended_at IS NULL;
