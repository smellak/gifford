package writerfixture

const scalarUpdate = "UPDATE messages SET control_state = ? WHERE message_id = ?" // want:control-write

const tupleUpdate = `UPDATE message_envelopes
SET ("control_reason", other) = (?, ?) -- want:control-write
WHERE message_id = ?`

const insertColumns = "INSERT INTO delivery_queue (message_id, control_version) VALUES (?, ?)"                  // want:control-write
const replaceColumns = "REPLACE INTO artifact_transfers (artifact_id, control_state) SELECT ?, ?"               // want:control-write
const replaceDefaults = "REPLACE INTO messages (message_id, status) VALUES (?, ?)"                              // want:control-write
const insertOrReplaceDefaults = "INSERT OR REPLACE INTO message_envelopes (message_id, envelope) VALUES (?, ?)" // want:control-write
const splitName = "UPDATE messages SET control_" + "version = ?"                                                // want:control-write
var parenthesized = ("UPDATE messages SET ") + ("control_state = ? WHERE message_id = ?")                       // want:control-write

func unknownTable(table string) string {
	return "INSERT INTO " + table + " (control_reason) VALUES (?)" // want:control-write
}

const selectOnly = "SELECT control_state, control_reason, control_version FROM messages"
const wireOnly = "UPDATE message_envelopes SET wire_control_version = ?"
const whereOnly = "UPDATE messages SET status = ? WHERE control_state = ?"
const ddlOnly = "CREATE TABLE sample (control_state TEXT, control_reason TEXT, control_version INTEGER)"
const defaultedInsert = "INSERT INTO messages (message_id, status) VALUES (?, ?)"
const ignoredInsert = "INSERT OR IGNORE INTO messages (message_id, status) VALUES (?, ?)"
const unrelatedReplace = "REPLACE INTO unrelated (message_id, status) VALUES (?, ?)"
