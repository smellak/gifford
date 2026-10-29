UPDATE agent_keys
SET agent_id = ?, node_id = ?, purpose = ?, algorithm = ?, public_key = ?,
  status = ?, valid_from = ?, valid_until = ?, issuer = ?, record_signature = ?
WHERE key_id = ?;
