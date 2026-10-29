INSERT INTO nodes
  (node_id, organization_id, local, status, federation_endpoint,
   certificate_fingerprint, policy_version, last_seen_at, created_at,
   signing_public_key)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
