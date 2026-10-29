INSERT INTO acks (message_id, agent_id, delivery_generation, status, acked_at)
VALUES (?, ?, ?, ?, ?) ON CONFLICT DO NOTHING;
