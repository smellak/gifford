package channel

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

type mutableControlClock struct{ now time.Time }

func (c *mutableControlClock) Now() time.Time { return c.now }

type recordingControlEvents struct{ events []ControlEvent }

func (s *recordingControlEvents) EmitControlEvents(_ context.Context, events []ControlEvent) error {
	s.events = append(s.events, events...)
	return nil
}

func testController(t *testing.T) (*Controller, *mutableControlClock, *recordingControlEvents) {
	t.Helper()
	store := testChannelStore(t)
	clk := &mutableControlClock{now: testEngineTime}
	engine, err := New(Options{Clock: clk})
	require.NoError(t, err)
	sink := &recordingControlEvents{}
	controller, err := NewController(ControllerOptions{DB: store.DB(), Engine: engine, Clock: clk, NodeID: testNodeA, Entropy: bytes.NewReader(make([]byte, 1024)), Events: sink})
	require.NoError(t, err)
	require.NoError(t, controller.Reload(t.Context()))
	return controller, clk, sink
}

func TestControllerCASConflictAndDrainIntent(t *testing.T) {
	t.Parallel()
	controller, _, _ := testController(t)
	actor := Actor{Type: protocol.ActorTypeHuman, ID: testOperator}

	state, err := controller.SetGlobal(t.Context(), protocol.SetGlobalParams{Mode: protocol.ChannelModeDraining, ExpectedVersion: 0}, actor)
	require.NoError(t, err)
	require.Equal(t, int64(1), state.Version)
	require.Empty(t, state.Control.ThenMode)

	_, err = controller.SetGlobal(t.Context(), protocol.SetGlobalParams{Mode: protocol.ChannelModeActive, ExpectedVersion: 0}, actor)
	conflict, ok := VersionConflict(err)
	require.True(t, ok)
	require.Equal(t, int64(1), conflict.CurrentVersion)

	state, err = controller.SetGlobal(t.Context(), protocol.SetGlobalParams{Mode: protocol.ChannelModeDraining, ThenMode: protocol.ChannelModeOff, ExpectedVersion: 1}, actor)
	require.NoError(t, err)
	require.Equal(t, protocol.ControlModeOff, state.Control.ThenMode)
}

func TestControllerTTLRestoresRecordedState(t *testing.T) {
	t.Parallel()
	controller, clk, sink := testController(t)
	actor := Actor{Type: protocol.ActorTypeHuman, ID: testOperator}
	first, err := controller.SetGlobal(t.Context(), protocol.SetGlobalParams{Mode: protocol.ChannelModePaused, Reason: "prior", ExpectedVersion: 0}, actor)
	require.NoError(t, err)
	ttl := int64(time.Minute / time.Millisecond)
	second, err := controller.SetGlobal(t.Context(), protocol.SetGlobalParams{Mode: protocol.ChannelModeOff, Reason: "temporary", TTLMS: &ttl, ExpectedVersion: first.Version}, actor)
	require.NoError(t, err)
	require.NotNil(t, second.Control.Restore)
	require.Equal(t, protocol.ControlModePaused, second.Control.Restore.Mode)

	clk.now = clk.now.Add(2 * time.Minute)
	count, err := controller.Expire(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, count)
	restored, err := controller.Global(t.Context())
	require.NoError(t, err)
	require.Equal(t, protocol.ChannelModePaused, restored.Mode)
	require.Equal(t, "prior", restored.Control.Reason)
	require.Equal(t, testOperator, restored.Control.CreatedBy)
	require.Equal(t, int64(3), restored.Version)

	last := sink.events[len(sink.events)-1]
	require.Equal(t, protocol.EventTypeControlTTLExpired, last.Type)
	require.NotNil(t, last.Payload.Restored)
	require.True(t, *last.Payload.Restored)
	require.Equal(t, "TTL_EXPIRED", last.Payload.Reason)
}

func TestControllerRejectsNestedTTLWithoutLosingRestore(t *testing.T) {
	t.Parallel()
	controller, clk, _ := testController(t)
	actor := Actor{Type: protocol.ActorTypeHuman, ID: testOperator}
	first, err := controller.SetGlobal(t.Context(), protocol.SetGlobalParams{Mode: protocol.ChannelModePaused, ExpectedVersion: 0}, actor)
	require.NoError(t, err)
	tenMinutes := int64((10 * time.Minute) / time.Millisecond)
	second, err := controller.SetGlobal(t.Context(), protocol.SetGlobalParams{Mode: protocol.ChannelModeOff, TTLMS: &tenMinutes, ExpectedVersion: first.Version}, actor)
	require.NoError(t, err)
	oneMinute := int64(time.Minute / time.Millisecond)
	_, err = controller.SetGlobal(t.Context(), protocol.SetGlobalParams{Mode: protocol.ChannelModeActive, TTLMS: &oneMinute, ExpectedVersion: second.Version}, actor)
	require.ErrorIs(t, err, ErrInvalidQuery)

	clk.now = clk.now.Add(11 * time.Minute)
	count, err := controller.Expire(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, count)
	restored, err := controller.Global(t.Context())
	require.NoError(t, err)
	require.Equal(t, protocol.ChannelModePaused, restored.Mode)
}

func TestAgentProjectsActualGlobalAndNodeModes(t *testing.T) {
	t.Parallel()
	controller, _, _ := testController(t)
	actor := Actor{Type: protocol.ActorTypeHuman, ID: testOperator}
	_, err := controller.SetGlobal(t.Context(), protocol.SetGlobalParams{Mode: protocol.ChannelModeOff, ExpectedVersion: 0}, actor)
	require.NoError(t, err)
	_, err = controller.SetNode(t.Context(), protocol.SetNodeParams{NodeID: testNodeA, Mode: protocol.ChannelModePaused, ExpectedVersion: 0}, actor)
	require.NoError(t, err)

	agent, err := controller.Agent(t.Context(), testAgent1)
	require.NoError(t, err)
	require.Equal(t, protocol.ChannelModeOff, agent.Effective.Global)
	require.Equal(t, protocol.ChannelModePaused, agent.Effective.Node)
}

func TestControllerExplainStoredObjectsUsesLatestDecision(t *testing.T) {
	t.Parallel()
	controller, _, _ := testController(t)
	insert := func(rec protocol.DecisionRecord) {
		decided, err := newDecision(rec, restrictionNone, "")
		require.NoError(t, err)
		params, err := decisionRecord(decided)
		require.NoError(t, err)
		_, err = controller.db.ExecContext(t.Context(), testQueryText(t, "decisions_insert.sql"), params...)
		require.NoError(t, err)
	}
	setObject := func(rec *protocol.DecisionRecord, objectType protocol.DecisionObjectType, objectID protocol.OpaqueID) {
		rec.ObjectType = objectType
		rec.ObjectID = objectID
		rec.Explanation.ObjectType = objectType
		rec.Explanation.ObjectID = objectID
		for i := range rec.Explanation.Lines {
			if rec.Explanation.Lines[i].Scope == protocol.ExplanationScopeObject {
				rec.Explanation.Lines[i].Key = string(objectID)
			}
		}
	}

	messageID := protocol.ULID("01ARZ3NDEKTSV4RRFFQ69G5FAV")
	first := testRecord()
	setObject(&first, protocol.DecisionObjectTypeMessage, protocol.OpaqueID(messageID))
	insert(first)
	latest := first
	latest.DecisionID = "chd_00000000000000000000000002"
	latest.ToAgent = "agent-3"
	latest.Explanation.ToAgent = latest.ToAgent
	latest.CreatedAt = latest.CreatedAt.Add(time.Second)
	insert(latest)
	explained, err := controller.Explain(t.Context(), protocol.ExplainParams{MessageID: messageID})
	require.NoError(t, err)
	require.Equal(t, protocol.AgentID("agent-3"), explained.ToAgent)
	require.Equal(t, protocol.OpaqueID(messageID), explained.ObjectID)

	artifactID := protocol.OpaqueID("artifact-1")
	artifact := testRecord()
	artifact.DecisionID = "chd_00000000000000000000000003"
	artifact.Plane = protocol.PlaneArtifact
	artifact.Explanation.Plane = protocol.PlaneArtifact
	setObject(&artifact, protocol.DecisionObjectTypeArtifact, artifactID)
	insert(artifact)
	explained, err = controller.Explain(t.Context(), protocol.ExplainParams{ArtifactID: artifactID})
	require.NoError(t, err)
	require.Equal(t, protocol.DecisionObjectTypeArtifact, explained.ObjectType)
	require.Equal(t, artifactID, explained.ObjectID)
}

func TestControllerExplainHeldLink(t *testing.T) {
	t.Parallel()
	controller, _, _ := testController(t)
	actor := Actor{Type: protocol.ActorTypeHuman, ID: testOperator}
	versions := protocol.ControlVersions{testLinkVersionKey: 0}
	_, err := controller.SetLink(t.Context(), protocol.SetLinkParams{FromAgent: "agent-1", ToAgent: "agent-2", Mode: protocol.LinkModeHold, Plane: protocol.PlaneMessage, ExpectedVersions: versions}, actor)
	require.NoError(t, err)
	explained, err := controller.ExplainPair(protocol.ExplainParams{FromAgent: "agent-1", ToAgent: "agent-2", Plane: protocol.PlaneMessage})
	require.NoError(t, err)
	require.Equal(t, protocol.DecisionOutcomeHold, explained.Outcome)
	require.Equal(t, protocol.ExplanationScopeLink, explained.Blocker)
}
