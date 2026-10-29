INSERT INTO agent_keys
  (key_id, agent_id, node_id, purpose, algorithm, public_key, status,
   valid_from, valid_until, issuer, record_signature)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
