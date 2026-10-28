-- Decisions about one object, oldest first. Parameters: object_type, object_id.
SELECT decision_id, object_type, object_id, stage, plane, from_agent, to_agent,
       outcome, reason, matched_controls_json, effective_control_json,
       control_versions_json, explanation_json, actor_id, created_at
FROM channel_control_decisions
WHERE object_type = ? AND object_id = ?
ORDER BY created_at, decision_id;
