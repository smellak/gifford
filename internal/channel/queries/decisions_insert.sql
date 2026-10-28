-- Record one gate decision. Parameters: the 15 columns in the order listed.
INSERT INTO channel_control_decisions (
  decision_id, object_type, object_id, stage, plane, from_agent, to_agent,
  outcome, reason, matched_controls_json, effective_control_json,
  control_versions_json, explanation_json, actor_id, created_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
