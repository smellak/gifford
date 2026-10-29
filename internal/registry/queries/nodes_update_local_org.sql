UPDATE nodes
SET organization_id = ?
WHERE node_id = ? AND local = 1 AND organization_id = '';
