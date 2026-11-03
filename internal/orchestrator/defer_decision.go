package orchestrator

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"github.com/smellak/gifford/internal/protocol"
)

// unchangedDefer coalesces repeated deterministic observations, not delivery
// attempts or model calls. In particular FAILED/STALE rows are the worker's
// durable retry budget and must not disappear. created_at remains the first
// observation of the unchanged decision, not a fabricated new decision time.
func (e *Executor) unchangedDefer(
	ctx context.Context,
	tx *sql.Tx,
	row decisionRow,
	allowed string,
) (persistedEvaluation, bool, error) {
	if row.selected != protocol.OrchestratorActionDefer ||
		row.status != protocol.ExecutionStatusExecuted || row.callID != "" ||
		row.model != "" || len(row.probabilities) != 0 {
		return persistedEvaluation{}, false, nil
	}
	var previous persistedEvaluation
	var guardsJSON string
	var sameContext bool
	// Look at the latest row, not the latest matching row: A -> B -> A is
	// three transitions. Session, queue age and controls prevent reusing an
	// observation from an earlier session, queue generation or control state.
	err := tx.QueryRowContext(ctx, `SELECT od.decision_id, od.hard_guards_json,
		COALESCE(od.selected_action = 'DEFER' AND od.execution_status = 'EXECUTED'
			AND od.glimmer_call_id IS NULL AND od.decision_model IS NULL
			AND od.probabilities_json IS NULL AND od.allowed_actions_json = ?
			AND od.policy_version = ? AND ss.session_id = ?
			AND od.created_at >= (SELECT created_at FROM delivery_queue
				WHERE message_id = ? AND agent_id = ? AND delivery_generation = ?)
			AND old_control.outcome = current_control.outcome
			AND old_control.reason = current_control.reason
			AND old_control.control_versions_json = current_control.control_versions_json, 0)
		FROM orchestrator_decisions AS od
		LEFT JOIN state_snapshots AS ss ON ss.state_id = od.state_id
		LEFT JOIN channel_control_decisions AS old_control
			ON old_control.decision_id = od.channel_decision_id
		LEFT JOIN channel_control_decisions AS current_control
			ON current_control.decision_id = ?
		WHERE od.message_id = ? AND od.agent_id = ?
		ORDER BY od.created_at DESC, od.decision_id DESC LIMIT 1`,
		allowed, e.policy, row.record.sessionID,
		row.record.MessageID, row.record.AgentID, row.record.DeliveryGeneration,
		row.channel, row.record.MessageID, row.record.AgentID,
	).Scan(&previous.decisionID, &guardsJSON, &sameContext)
	if errors.Is(err, sql.ErrNoRows) {
		return persistedEvaluation{}, false, nil
	}
	if err != nil {
		return persistedEvaluation{}, false, fmt.Errorf("read previous deferred decision: %w", err)
	}
	if !sameContext {
		return persistedEvaluation{}, false, nil
	}
	var previousGuards []protocol.GuardResult
	if err := json.Unmarshal([]byte(guardsJSON), &previousGuards); err != nil {
		return persistedEvaluation{}, false, fmt.Errorf("decode previous deferred guards: %w", err)
	}
	return previous, sameDeferredGuards(previousGuards, row.guards), nil
}

// The guard set is unordered. Keep outcome and diagnostic changes visible as
// well as changes in the failed guard names; none contains screen content.
func sameDeferredGuards(a, b []protocol.GuardResult) bool {
	a, b = slices.Clone(a), slices.Clone(b)
	compare := func(a, b protocol.GuardResult) int {
		if order := cmp.Compare(a.Guard, b.Guard); order != 0 {
			return order
		}
		if order := cmp.Compare(a.Outcome, b.Outcome); order != 0 {
			return order
		}
		return cmp.Compare(a.Detail, b.Detail)
	}
	slices.SortFunc(a, compare)
	slices.SortFunc(b, compare)
	return slices.Equal(a, b)
}
