package hub

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

const (
	controlTestAgentOne protocol.AgentID = "agent-one"
	controlTestAgentTwo protocol.AgentID = "agent-two"
)

type recordingPreparedController struct {
	mu      sync.Mutex
	changed []protocol.AgentID
	paused  []protocol.AgentID
	block   bool
}

func (c *recordingPreparedController) ControlChanged(
	ctx context.Context,
	agent protocol.AgentID,
) error {
	if c.block {
		<-ctx.Done()
		return ctx.Err()
	}
	c.mu.Lock()
	c.changed = append(c.changed, agent)
	c.mu.Unlock()
	return nil
}

func (c *recordingPreparedController) PauseAutomation(
	_ context.Context,
	agent protocol.AgentID,
) error {
	c.mu.Lock()
	c.paused = append(c.paused, agent)
	c.mu.Unlock()
	return nil
}

func (c *recordingPreparedController) calls() ([]protocol.AgentID, []protocol.AgentID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]protocol.AgentID(nil), c.changed...), append([]protocol.AgentID(nil), c.paused...)
}

func (c *recordingPreparedController) reset() {
	c.mu.Lock()
	c.changed = nil
	c.paused = nil
	c.mu.Unlock()
}

func TestControlEventSinkTargetsPreparedAgentsAndPausesAutoDelivery(t *testing.T) {
	t.Parallel()

	store, err := eventstore.Open(t.Context(), filepath.Join(t.TempDir(), dbName))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	seedPreparedAttempt(t, store, 1, "agent-sender", controlTestAgentOne, "node-a", "node-b", "thread-one", "loop-one")
	seedPreparedAttempt(t, store, 2, "agent-sender", controlTestAgentTwo, "node-a", "node-c", "thread-two", "loop-two")

	worker := &recordingPreparedController{}
	sink := newControlEventSink(store.DB())
	sink.attach(worker)

	require.NoError(t, sink.EmitControlEvents(t.Context(), []channel.ControlEvent{{
		Payload: protocol.ChannelEventPayload{
			ScopeType: protocol.ScopeTypeGlobal, ScopeKey: "*", Plane: protocol.PlaneAll,
			NewValue: protocol.Value(`{"mode":"OFF"}`),
		},
	}}))
	changed, paused := worker.calls()
	require.Equal(t, []protocol.AgentID{controlTestAgentOne, controlTestAgentTwo}, changed)
	require.Empty(t, paused)

	worker.reset()
	require.NoError(t, sink.EmitControlEvents(t.Context(), []channel.ControlEvent{{
		Payload: protocol.ChannelEventPayload{
			ScopeType: protocol.ScopeTypeLink, ScopeKey: "agent-sender→agent-two",
			Plane: protocol.PlaneMessage, FromAgent: "agent-sender", ToAgent: controlTestAgentTwo,
			NewValue: protocol.Value(`{"mode":"HOLD"}`),
		},
	}}))
	changed, paused = worker.calls()
	require.Equal(t, []protocol.AgentID{controlTestAgentTwo}, changed)
	require.Empty(t, paused)

	worker.reset()
	require.NoError(t, sink.EmitControlEvents(t.Context(), []channel.ControlEvent{{
		Payload: protocol.ChannelEventPayload{
			ScopeType: protocol.ScopeTypeAgent, ScopeKey: string(controlTestAgentOne), Plane: protocol.PlaneAll,
			NewValue: protocol.Value(`{"tx":true,"rx":true,"auto_delivery":false,"auto_continue":true}`),
		},
	}}))
	changed, paused = worker.calls()
	require.Equal(t, []protocol.AgentID{controlTestAgentOne}, changed)
	require.Equal(t, []protocol.AgentID{controlTestAgentOne}, paused)

	worker.reset()
	require.NoError(t, sink.EmitControlEvents(t.Context(), []channel.ControlEvent{{
		Payload: protocol.ChannelEventPayload{
			ScopeType: protocol.ScopeTypeGlobal, ScopeKey: "*", Plane: protocol.PlaneAll,
			NewValue: protocol.Value(`{"mode":"ACTIVE"}`),
		},
	}}))
	changed, paused = worker.calls()
	require.Empty(t, changed)
	require.Empty(t, paused)
}

func TestControlEventSinkBoundsWorkerCalls(t *testing.T) {
	t.Parallel()

	store, err := eventstore.Open(t.Context(), filepath.Join(t.TempDir(), dbName))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	seedPreparedAttempt(t, store, 3, "agent-sender", "agent-three", "node-a", "node-b", "thread-three", "loop-three")
	worker := &recordingPreparedController{block: true}
	sink := newControlEventSink(store.DB())
	sink.timeout = 20 * time.Millisecond
	sink.attach(worker)
	err = sink.EmitControlEvents(t.Context(), []channel.ControlEvent{{
		Payload: protocol.ChannelEventPayload{
			ScopeType: protocol.ScopeTypeGlobal, ScopeKey: "*", Plane: protocol.PlaneAll,
			NewValue: protocol.Value(`{"mode":"OFF"}`),
		},
	}})
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func seedPreparedAttempt(
	t *testing.T,
	store *eventstore.Store,
	sequence int,
	from, to protocol.AgentID,
	origin, destination protocol.NodeID,
	thread protocol.ThreadID,
	loop protocol.LoopID,
) {
	t.Helper()
	messageID := protocol.ULID(fmt.Sprintf("01ARZ3NDEKTSV4RRFFQ69G%04d", sequence))
	deliveryID := fmt.Sprintf("delivery-%d", sequence)
	stamp := "2026-10-30T20:00:00.000000000Z"
	_, err := store.DB().ExecContext(t.Context(), `INSERT INTO messages
		(message_id, client_msg_id, origin_node, from_agent, destination_node,
		 thread_ref, thread_id, priority, state, created_at, expires_at, updated_at,
		 loop_id, hop_count, route_digest)
		VALUES (?, ?, ?, ?, ?, ?, ?, 'NORMAL', 'PENDING', ?, ?, ?, ?, 0, ?)`,
		messageID, fmt.Sprintf("client-%d", sequence), origin, from, destination,
		fmt.Sprintf("thr-%d", sequence), thread, stamp, stamp, stamp, loop,
		fmt.Sprintf("sha256:route-%d", sequence))
	require.NoError(t, err)
	_, err = store.DB().ExecContext(t.Context(), `INSERT INTO delivery_queue
		(message_id, agent_id, delivery_generation, from_agent, thread_ref, state,
		 priority, created_at, expires_at, updated_at, loop_id, hop_count, route_digest)
		VALUES (?, ?, 1, ?, ?, 'DELIVERING', 2, ?, ?, ?, ?, 0, ?)`,
		messageID, to, from, fmt.Sprintf("thr-%d", sequence), stamp, stamp, stamp, loop,
		fmt.Sprintf("sha256:route-%d", sequence))
	require.NoError(t, err)
	_, err = store.DB().ExecContext(t.Context(), `INSERT INTO delivery_attempts
		(delivery_id, message_id, agent_id, delivery_generation, adapter_session_id,
		 orchestrator_decision_id, channel_decision_id, draft_generation,
		 input_guard_version, control_versions_json, state, prepared_at, created_at, updated_at)
		VALUES (?, ?, ?, 1, ?, ?, ?, 1, 1, '{}', 'PREPARED', ?, ?, ?)`,
		deliveryID, messageID, to, fmt.Sprintf("session-%d", sequence),
		fmt.Sprintf("decision-%d", sequence), fmt.Sprintf("channel-%d", sequence),
		stamp, stamp, stamp)
	require.NoError(t, err)
}
