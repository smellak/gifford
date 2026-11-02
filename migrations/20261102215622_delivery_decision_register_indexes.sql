-- +goose Up
CREATE INDEX delivery_attempts_decision
  ON delivery_attempts (orchestrator_decision_id, created_at DESC, delivery_id DESC);

CREATE INDEX orchestrator_decisions_register
  ON orchestrator_decisions (created_at DESC, decision_id DESC)
  WHERE message_id IS NOT NULL;

CREATE INDEX delivery_result_evidence_delivery
  ON delivery_result_evidence (delivery_id, received_at ASC, result ASC);

-- +goose Down
DROP INDEX delivery_result_evidence_delivery;
DROP INDEX orchestrator_decisions_register;
DROP INDEX delivery_attempts_decision;
