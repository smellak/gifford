-- Count stored envelopes. Parameters: message_id.
SELECT count(*) FROM message_envelopes WHERE message_id = ?;
