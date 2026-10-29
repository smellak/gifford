UPDATE nodes
SET policy_version = ?
WHERE node_id = ? AND local = 1;
