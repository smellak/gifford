UPDATE enrollment_tokens
SET used_at = ?, used_by = ?
WHERE hash = ? AND org = ? AND used_at IS NULL AND used_by IS NULL
  AND expires_at > ?;
