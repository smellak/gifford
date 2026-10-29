SELECT agent_id, delivery_mode, requires_ack FROM message_recipients
WHERE message_id = ? ORDER BY ordinal;
