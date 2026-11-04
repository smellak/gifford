package controlapi

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

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
