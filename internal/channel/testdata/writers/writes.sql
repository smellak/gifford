UPDATE messages SET control_state = ? WHERE message_id = ?; -- want:control-write
UPDATE messages SET "control_reason" = ? WHERE message_id = ?; -- want:control-write
UPDATE messages SET (status, [control_version]) = (?, ?) WHERE message_id = ?; -- want:control-write
INSERT INTO message_envelopes (message_id, control_state) VALUES (?, ?); -- want:control-write
REPLACE INTO delivery_queue (message_id, `control_reason`) SELECT ?, ?; -- want:control-write
(message_id, control_version) VALUES (?, ?); -- want:control-write
(message_id, control_state) SELECT ?, ?; -- want:control-write
INSERT INTO messages VALUES (?, ?); -- want:control-write
INSERT INTO message_envelopes DEFAULT VALUES; -- want:control-write
REPLACE INTO delivery_queue VALUES (?, ?); -- want:control-write
INSERT OR REPLACE INTO artifact_transfers SELECT * FROM source; -- want:control-write
INSERT INTO "main"."messages" VALUES (?); -- want:control-write
REPLACE INTO messages (message_id, status) VALUES (?, ?); -- want:control-write
INSERT OR REPLACE INTO message_envelopes (message_id, envelope) VALUES (?, ?); -- want:control-write
UPDATE messages SET thread_id = 'where', control_state = ? WHERE message_id = ?; -- want:control-write
UPDATE messages SET thread_id = 'note#tag', control_reason = ? WHERE message_id = ?; -- want:control-write
UPDATE messages SET thread_id = 'https://example.invalid', control_version = ? WHERE message_id = ?; -- want:control-write
UPDATE messages SET thread_id = 'two--dashes', control_state = ? WHERE message_id = ?; -- want:control-write

SELECT control_state, control_reason, control_version FROM messages;
UPDATE message_envelopes SET wire_control_version = ? WHERE message_id = ?;
UPDATE messages SET status = ? WHERE control_state = ?;
CREATE TABLE sample (control_state TEXT, control_reason TEXT, control_version INTEGER);
INSERT INTO messages (message_id, status) VALUES (?, ?);
INSERT INTO unrelated VALUES (?);
INSERT OR IGNORE INTO delivery_queue (message_id, status) VALUES (?, ?);
REPLACE INTO unrelated (message_id, status) VALUES (?, ?);
INSERT OR REPLACE INTO unrelated (message_id, status) VALUES (?, ?);
UPDATE messages SET thread_id = 'control_state = ?' WHERE message_id = ?;
-- UPDATE messages SET control_state = ?;
