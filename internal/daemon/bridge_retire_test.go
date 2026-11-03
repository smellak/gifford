package daemon

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/daemon/agentlink"
	"github.com/smellak/gifford/internal/protocol"
)

// TestRetiredBridgeDeliveryNeverReturnsToQueued retires a QUEUED bridge
// delivery and runs the worker scan: the retired row stays CANCELLED and is
// never evaluated, while an untouched QUEUED row still is.
func TestRetiredBridgeDeliveryNeverReturnsToQueued(t *testing.T) {
	store := newWorkerTestStore(t)
	db := store.DB()
	agent := protocol.AgentID("retire-worker-agent")
	seedWorkerSession(t, store, agent, true)
	control := seedWorkerQueue(t, store, agent, 1, 2)

	retired := protocol.ULID("01JB0000000000000000000W1")
	_, err := db.ExecContext(t.Context(), `INSERT INTO messages
		(message_id, client_msg_id, origin_node, from_agent, destination_node,
		thread_ref, priority, state, created_at, expires_at, updated_at,
		hop_count, route_digest)
		VALUES (?, 'cm-retire', 'node-test-a', 'bridge-relay', 'local',
		'thr-retire', 'NORMAL', 'DELIVERY_QUEUED',
		'2026-11-03T00:00:00Z', '2026-11-04T00:00:00Z', '2026-11-03T00:00:00Z',
		0, 'sha256:abc')`, retired)
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), `INSERT INTO bridge_message_mappings
		(source_node_id, source_message_id, gifford_message_id, from_agent, to_agent,
		source_created_at, source_read_at, created_at, updated_at)
		VALUES ('node-test-a', 9, ?, 'bridge-relay', ?, '2026-11-03T00:00:00Z', NULL,
		'2026-11-03T00:00:00Z', '2026-11-03T00:00:00Z')`, retired, agent)
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), `INSERT INTO delivery_queue
		(message_id, agent_id, delivery_generation, from_agent, thread_ref, state,
		priority, created_at, expires_at, updated_at, hop_count, route_digest)
		VALUES (?, ?, 1, 'bridge-relay', 'thr-retire', 'QUEUED', 2,
		'2026-11-03T00:00:00Z', '2026-11-04T00:00:00Z', '2026-11-03T00:00:00Z',
		0, 'sha256:abc')`, retired, agent)
	require.NoError(t, err)

	engine, err := channel.New(channel.Options{Clock: clock.System{}})
	require.NoError(t, err)
	controller, err := channel.NewController(channel.ControllerOptions{DB: db, Engine: engine, NodeID: "test-node", Clock: clock.System{}, Entropy: bytes.NewReader(make([]byte, 1024))})
	require.NoError(t, err)
	res, err := controller.RetireBridgeRecipient(t.Context(),
		protocol.BridgeRetireRecipientParams{SourceNodeID: "node-test-a", AgentID: agent},
		channel.Actor{Type: protocol.ActorTypeHuman, ID: "uid:0:pid:1"})
	require.NoError(t, err)
	require.Equal(t, int64(1), res.Cancelled)

	backend := &workerBackend{}
	worker, err := NewWorker(WorkerOptions{
		DB: db, Processor: backend, Approvals: backend,
		Prepared: backend, Proxies: workerSnapshots{
			agent: {AgentID: agent, State: agentlink.StateUp},
		},
	})
	require.NoError(t, err)
	_, err = worker.RunOnce(t.Context())
	require.NoError(t, err)

	var state string
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT state FROM delivery_queue WHERE message_id = ?`, retired).Scan(&state))
	require.Equal(t, "CANCELLED", state)
	for _, got := range backend.candidates {
		require.NotEqual(t, retired, got.MessageID, "retired delivery evaluated")
	}
	found := false
	for _, got := range backend.candidates {
		if got.MessageID == control.MessageID {
			found = true
		}
	}
	require.True(t, found, "untouched QUEUED delivery not evaluated")
}
