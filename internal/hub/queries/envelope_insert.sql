INSERT INTO message_envelopes
  (message_id, envelope, protocol, version, client_msg_id, origin_node, origin_agent,
   destination_node, recipient_agents_json, created_at, expires_at, priority, thread_ref,
   encryption_profile, sender_signing_key_id, ciphertext_sha256, stored_at,
   hop_count, route_digest, wire_control_version)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
