package controlapi

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

type stubPaneLockStatus struct {
	statuses map[protocol.AgentID]protocol.AgentPaneLockStatus
	err      error
}

func (s stubPaneLockStatus) Statuses(
	context.Context, []protocol.AgentID,
) (map[protocol.AgentID]protocol.AgentPaneLockStatus, error) {
	return s.statuses, s.err
}

type stubWaits map[protocol.AgentID]string

func (w stubWaits) DetectorWait(agent protocol.AgentID) (string, bool) {
	note, ok := w[agent]
	return note, ok
}

// agent.list carries an attached pane's P9 phase and the detector route's
// wait; a pane status that cannot be read leaves them out, never the list.
func TestAgentListCarriesPaneLockAndWait(t *testing.T) {
	t.Parallel()
	fixture := newReadFixture(t, true)
	seedReadRows(t, fixture)
	const wait = "p_idle_threshold: no orchestration model is available; " +
		"deterministic-pty-v1 is not eligible: harness is busy"
	fixture.server.paneStatus = stubPaneLockStatus{statuses: map[protocol.AgentID]protocol.AgentPaneLockStatus{
		readTestAgent: {Readable: true, Phase: protocol.PaneLockPhaseUncertain},
	}}
	fixture.server.waits = stubWaits{readTestAgent: wait}
	result := readResult[protocol.AgentListResult](t, fixture.server,
		protocol.ControlMethodAgentList, protocol.AgentListParams{})
	require.Len(t, result.Agents, 1)
	require.Equal(t, &protocol.AgentPaneLockStatus{Readable: true, Phase: protocol.PaneLockPhaseUncertain},
		result.Agents[0].PaneLock)
	require.Equal(t, wait, result.Agents[0].Waiting)

	fixture.server.paneStatus = stubPaneLockStatus{err: errors.New("synthetic registry failure")}
	result = readResult[protocol.AgentListResult](t, fixture.server,
		protocol.ControlMethodAgentList, protocol.AgentListParams{})
	require.Len(t, result.Agents, 1)
	require.Nil(t, result.Agents[0].PaneLock)
}

// A decision row names its policy and the guard that held it, with the
// guard's detail only when that is a fixed phrase.
func TestDeliveryDecisionCarriesPolicyAndPublicHold(t *testing.T) {
	t.Parallel()
	fixture := newReadFixture(t, true)
	seedReadRows(t, fixture)
	result := readResult[protocol.DeliveryDecisionListResult](t, fixture.server,
		protocol.ControlMethodDeliveryDecisionList, protocol.DeliveryDecisionListParams{})
	require.Len(t, result.Decisions, 1)
	require.Equal(t, protocol.ModelMetadataValue("policy-v1"), result.Decisions[0].PolicyVersion)
	require.Equal(t, "human_draft_closed", result.Decisions[0].Hold, "free text never leaves the row")

	held, err := decisionHold(protocol.ControlMethodDeliveryDecisionList,
		`[{"guard":"deliverable","outcome":"pass"},`+
			`{"guard":"p_idle_threshold","outcome":"fail","detail":"no orchestration model is available; deterministic-pty-v1 is not eligible: screen is not quiet yet"}]`)
	require.NoError(t, err)
	require.Equal(t, "p_idle_threshold: no orchestration model is available; "+
		"deterministic-pty-v1 is not eligible: screen is not quiet yet", held)
}

// Every decision row names its source in one word from what it recorded:
// the corroboration policy, a recorded Glimmer call with its model, or the
// detector alone; model and call leave the row as recorded.
func TestDeliveryDecisionNamesItsSource(t *testing.T) {
	t.Parallel()
	fixture := newReadFixture(t, true)
	seedReadRows(t, fixture)
	stamp := eventstore.FormatTime(readTestTime.Add(time.Minute))
	_, err := fixture.store.DB().ExecContext(t.Context(), `INSERT INTO orchestrator_decisions
		(decision_id, agent_id, message_id, state_id, allowed_actions_json,
		 selected_action, hard_guards_json, policy_version, decision_model,
		 glimmer_call_id, channel_decision_id, created_at, execution_status)
		VALUES ('01JB0000000000000000000101', ?, ?, '01JB0000000000000000000111',
		 '["INJECT_PTY_PROMPT","DEFER"]', 'INJECT_PTY_PROMPT', '[]',
		 'deterministic-pty-v1', NULL, NULL, ?, ?, 'EXECUTED'),
		       ('01JB0000000000000000000102', ?, ?, '01JB0000000000000000000112',
		 '["DEFER"]', 'DEFER', '[{"guard":"detector_fallback","outcome":"pass"}]',
		 'deterministic-pty-v1+analyst', NULL, NULL, ?, ?, 'EXECUTED'),
		       ('01JB0000000000000000000103', ?, ?, '01JB0000000000000000000113',
		 '["INJECT_PTY_PROMPT","DEFER"]', 'INJECT_PTY_PROMPT', '[]',
		 'automatic-5.8-detector-corroboration:748725a15c5f403247d1a817',
		 'analyst-lora', 'call-read', ?, ?, 'EXECUTED')`,
		readTestAgent, readTestMessage, fixture.channelDecision, stamp,
		readTestAgent, readTestMessage, fixture.channelDecision, stamp,
		readTestAgent, readTestMessage, fixture.channelDecision, stamp)
	require.NoError(t, err)

	result := readResult[protocol.DeliveryDecisionListResult](t, fixture.server,
		protocol.ControlMethodDeliveryDecisionList, protocol.DeliveryDecisionListParams{})
	byID := make(map[protocol.ULID]protocol.DeliveryDecisionListItem, len(result.Decisions))
	for _, item := range result.Decisions {
		require.NoError(t, item.Validate())
		byID[item.DecisionID] = item
	}
	detector := byID["01JB0000000000000000000101"]
	require.Equal(t, protocol.DeliveryDecisionSourceDetector, detector.Source)
	require.Equal(t, protocol.ModelMetadataValue("deterministic-pty-v1"), detector.PolicyVersion)
	require.Empty(t, detector.DecisionModel)
	require.Empty(t, detector.GlimmerCallID)
	require.Equal(t, protocol.DeliveryDecisionModeFallback, detector.Mode)

	corroborated := byID["01JB0000000000000000000102"]
	require.Equal(t, protocol.DeliveryDecisionSourceCorroboration, corroborated.Source)
	require.Equal(t, protocol.ModelMetadataValue("deterministic-pty-v1+analyst"), corroborated.PolicyVersion)
	require.Empty(t, corroborated.GlimmerCallID)

	analyst := byID["01JB0000000000000000000103"]
	require.Equal(t, protocol.DeliveryDecisionSourceAnalyst, analyst.Source)
	require.Equal(t, protocol.ModelMetadataValue("analyst-lora"), analyst.DecisionModel)
	require.Equal(t, protocol.OpaqueID("call-read"), analyst.GlimmerCallID)
	require.Equal(t, protocol.DeliveryDecisionModeGlimmer, analyst.Mode)

	// The seeded policy-v1 row without a model or call reads as the detector.
	require.Equal(t, protocol.DeliveryDecisionSourceDetector, byID[readTestOrchestrator].Source)
}
