SELECT node_id, organization_id, status, federation_endpoint,
  certificate_fingerprint, policy_version
FROM nodes
WHERE local = 0
ORDER BY node_id;
