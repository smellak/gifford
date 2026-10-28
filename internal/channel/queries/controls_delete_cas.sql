-- Compare-and-swap removal of an override (back to the default).
-- Parameters: scope_type, scope_key, plane, expected version.
DELETE FROM channel_controls
WHERE scope_type = ? AND scope_key = ? AND plane = ? AND version = ?;
