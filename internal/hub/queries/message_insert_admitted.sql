INSERT INTO messages
  (message_id, client_msg_id, origin_node, from_agent, destination_node, thread_ref, thread_id,
   priority, state, requested_autonomy, requested_budget_json,
   created_at, expires_at, updated_at, hop_count, route_digest, admission_result_json)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'ADMITTED', ?, ?, ?, ?, ?, 0, ?, ?);
