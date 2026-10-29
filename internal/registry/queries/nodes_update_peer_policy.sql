UPDATE nodes
SET policy_version = ?
WHERE node_id = ? AND local = 0 AND policy_version = '';
