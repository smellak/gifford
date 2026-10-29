SELECT org, created_at, expires_at, used_at, used_by
FROM enrollment_tokens
WHERE hash = ?;
