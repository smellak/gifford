#!/bin/sh
update='UPDATE messages SET control_reason = ? WHERE message_id = ?' # want:control-write
insert='INSERT INTO delivery_queue VALUES (?, ?)' # want:control-write
columns='(message_id, control_state) VALUES (?, ?)' # want:control-write
replace_defaults='REPLACE INTO delivery_queue (message_id, status) VALUES (?, ?)' # want:control-write
insert_or_replace_defaults='INSERT OR REPLACE INTO artifact_transfers (artifact_id, status) VALUES (?, ?)' # want:control-write

select_only='SELECT control_state FROM messages'
wire_only='UPDATE message_envelopes SET wire_control_version = ?'
where_only='UPDATE messages SET status = ? WHERE control_version = ?'
ddl_only='CREATE TABLE sample (control_state TEXT)'
defaulted='INSERT INTO messages (message_id, status) VALUES (?, ?)'
ignored='INSERT OR IGNORE INTO messages (message_id, status) VALUES (?, ?)'
unrelated_replace='REPLACE INTO unrelated (message_id, status) VALUES (?, ?)'
