-- +goose Up
-- An uncertain commit stays immutable. One fresh approval is linked to the
-- episode; resolving it records which audited request reserved the recovery
-- or cancellation that followed.
ALTER TABLE delivery_attempts
  ADD COLUMN uncertain_approval_id TEXT REFERENCES approval_requests (approval_id);
ALTER TABLE delivery_attempts
  ADD COLUMN resolution_approval_id TEXT REFERENCES approval_requests (approval_id);

CREATE UNIQUE INDEX delivery_attempts_uncertain_approval
  ON delivery_attempts (uncertain_approval_id)
  WHERE uncertain_approval_id IS NOT NULL;

DROP INDEX delivery_attempts_agent_uncertain;
CREATE INDEX delivery_attempts_agent_uncertain
  ON delivery_attempts (agent_id, state)
  WHERE state = 'UNCERTAIN' AND resolution_approval_id IS NULL;

-- +goose Down
DROP INDEX delivery_attempts_agent_uncertain;
CREATE INDEX delivery_attempts_agent_uncertain
  ON delivery_attempts (agent_id, state)
  WHERE state = 'UNCERTAIN';
DROP INDEX delivery_attempts_uncertain_approval;
ALTER TABLE delivery_attempts DROP COLUMN resolution_approval_id;
ALTER TABLE delivery_attempts DROP COLUMN uncertain_approval_id;
