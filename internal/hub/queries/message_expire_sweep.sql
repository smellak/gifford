UPDATE messages SET state = 'EXPIRED', updated_at = ? WHERE state = 'ADMITTED' AND expires_at <= ?;
