UPDATE nodes
SET federation_endpoint = ?, certificate_fingerprint = ?
WHERE node_id = ? AND local = 0 AND federation_endpoint IS NULL
  AND certificate_fingerprint IS NULL;
