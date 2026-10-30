package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/daemon/agentlink"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

type workerBackend struct {
	mu         sync.Mutex
	candidates []QueueDelivery
	started    chan struct{}
	block      chan struct{}
	resolved   protocol.ResolveApprovalParams
	changed    protocol.AgentID
	paused     protocol.AgentID
}

func (b *workerBackend) EvaluateAutomatic(_ context.Context, candidate QueueDelivery) error {
	b.mu.Lock()
	b.candidates = append(b.candidates, candidate)
	started, block := b.started, b.block
	b.mu.Unlock()
	if started != nil {
		select {
		case started <- struct{}{}:
		default:
		}
	}
	if block != nil {
		<-block
	}
	return nil
}

func (b *workerBackend) ResolveApproval(
	_ context.Context,
	params protocol.ResolveApprovalParams,
	_ channel.Actor,
) (protocol.ApprovalRequest, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.resolved = params
	return protocol.ApprovalRequest{ApprovalID: params.ApprovalID}, nil
}

func (b *workerBackend) ControlChanged(_ context.Context, agent protocol.AgentID) error {
	b.mu.Lock()
	b.changed = agent
	b.mu.Unlock()
	return nil
}

func (b *workerBackend) PauseAutomation(_ context.Context, agent protocol.AgentID) error {
	b.mu.Lock()
	b.paused = agent
	b.mu.Unlock()
	return nil
}

type workerSnapshots map[protocol.AgentID]agentlink.Snapshot

func (s workerSnapshots) Snapshot(agent protocol.AgentID) (agentlink.Snapshot, bool) {
	snapshot, ok := s[agent]
	return snapshot, ok
}

func seedWorkerSession(t *testing.T, store *eventstore.Store, agent protocol.AgentID, live bool) {
	t.Helper()
	stamp := eventstore.FormatTime(attemptTestTime)
	_, err := store.DB().ExecContext(t.Context(), `INSERT INTO agents
		(agent_id, node_id, harness, created_at, updated_at) VALUES (?, 'node-worker', 'codex', ?, ?)`,
		agent, stamp, stamp)
	require.NoError(t, err)
	state := protocol.SessionStateConnected
	if !live {
		state = protocol.SessionStateDisconnected
	}
	manifest, err := json.Marshal(protocol.CapabilityManifest{
		SchemaVersion: protocol.SchemaVersion, AgentID: agent,
		Harness: protocol.HarnessCodex, HarnessVersion: "1.0.0",
		AdapterType:        protocol.AdapterTypeGenericPTY,
		InputGuardProvider: protocol.InputGuardProviderPTYProxy,
		CaptureMode:        protocol.CaptureModePTYProxy, Deliverable: true,
		Capabilities: protocol.Capabilities{PTYCapture: true},
	})
	require.NoError(t, err)
	_, err = store.DB().ExecContext(t.Context(), `INSERT INTO sessions
		(session_id, agent_id, state, capture_mode, deliverable, manifest_json,
		 started_at, updated_at) VALUES (?, ?, ?, 'pty_proxy', 1, ?, ?, ?)`,
		"session-"+string(agent), agent, state, string(manifest), stamp, stamp)
	require.NoError(t, err)
}

func seedWorkerQueue(
	t *testing.T,
	store *eventstore.Store,
	agent protocol.AgentID,
	sequence, priority int,
) QueueDelivery {
	t.Helper()
	message := protocol.ULID(fmt.Sprintf("01ARZ3NDEKTSV4RRFFQ69G%04d", sequence))
	stamp := eventstore.FormatTime(attemptTestTime.Add(time.Duration(sequence) * time.Second))
	_, err := store.DB().ExecContext(t.Context(), `INSERT INTO delivery_queue
		(message_id, agent_id, delivery_generation, from_agent, thread_ref,
		 state, priority, created_at, expires_at, updated_at, hop_count, route_digest)
		VALUES (?, ?, 1, 'worker-sender', 'thr_01ARZ3NDEKTSV4RRFFQ69G5FAV',
		 'QUEUED', ?, ?, ?, ?, 0, 'sha256:worker-route')`,
		message, agent, priority, stamp, eventstore.FormatTime(attemptTestTime.Add(time.Hour)), stamp)
	require.NoError(t, err)
	return QueueDelivery{MessageID: message, AgentID: agent, DeliveryGeneration: 1}
}

func newWorkerTestStore(t *testing.T) *eventstore.Store {
	t.Helper()
	store, err := eventstore.Open(t.Context(), filepath.Join(t.TempDir(), "worker.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return store
}

func seedWorkerUncertain(t *testing.T, store *eventstore.Store, candidate QueueDelivery) {
	t.Helper()
	attempts, err := NewAttemptStore(&attemptClock{now: attemptTestTime})
	require.NoError(t, err)
	tx, err := store.DB().BeginTx(t.Context(), nil)
	require.NoError(t, err)
	deliveryID := protocol.OpaqueID("delivery-uncertain-" + string(candidate.AgentID))
	_, err = attempts.Create(t.Context(), tx, AttemptSpec{
		DeliveryID: deliveryID, MessageID: candidate.MessageID, AgentID: candidate.AgentID,
		DeliveryGeneration:     candidate.DeliveryGeneration,
		AdapterSessionID:       protocol.OpaqueID("session-" + string(candidate.AgentID)),
		OrchestratorDecisionID: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
		ChannelDecisionID:      "chd_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		DraftGeneration:        1, InputGuardVersion: 1,
		ControlVersions: protocol.ControlVersions{attemptTestControlKey: 0},
	})
	require.NoError(t, err)
	require.NoError(t, attempts.MarkPrepared(t.Context(), tx, deliveryID, "lease-worker"))
	require.NoError(t, attempts.MarkCommitSent(t.Context(), tx, deliveryID))
	require.NoError(t, attempts.MarkUncertain(t.Context(), tx, deliveryID, ReasonTimedOut))
	require.NoError(t, tx.Commit())
}

func TestWorkerScansOneQueuedDeliveryPerLiveAgent(t *testing.T) {
	t.Parallel()

	store := newWorkerTestStore(t)
	seedWorkerSession(t, store, "agent-worker-a", true)
	seedWorkerSession(t, store, "agent-worker-b", true)
	seedWorkerSession(t, store, "agent-worker-c", false)
	seedWorkerSession(t, store, "agent-worker-d", true)
	first := seedWorkerQueue(t, store, "agent-worker-a", 1, 0)
	second := seedWorkerQueue(t, store, "agent-worker-a", 2, 1)
	_ = seedWorkerQueue(t, store, "agent-worker-b", 3, 0)
	_ = seedWorkerQueue(t, store, "agent-worker-c", 4, 0)
	blocked := seedWorkerQueue(t, store, "agent-worker-d", 5, 0)
	seedWorkerUncertain(t, store, blocked)
	backend := &workerBackend{}
	worker, err := NewWorker(WorkerOptions{
		DB: store.DB(), Processor: backend, Approvals: backend, Prepared: backend,
		Proxies: workerSnapshots{
			"agent-worker-a": {AgentID: "agent-worker-a", State: agentlink.StateUp},
			"agent-worker-b": {AgentID: "agent-worker-b", State: agentlink.StateDown},
			"agent-worker-c": {AgentID: "agent-worker-c", State: agentlink.StateUp},
			"agent-worker-d": {AgentID: "agent-worker-d", State: agentlink.StateUp},
		},
	})
	require.NoError(t, err)
	processed, err := worker.RunOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, processed)
	require.Equal(t, []QueueDelivery{first}, backend.candidates)

	engine, err := channel.New(channel.Options{Clock: clock.System{}})
	require.NoError(t, err)
	controller, err := channel.NewController(channel.ControllerOptions{
		DB: store.DB(), Engine: engine, Clock: clock.System{}, NodeID: "node-worker",
		Entropy: bytes.NewReader(make([]byte, 64)),
	})
	require.NoError(t, err)
	tx, err := store.DB().BeginTx(t.Context(), nil)
	require.NoError(t, err)
	_, _, err = controller.EnsureOrchestratorApproval(
		t.Context(), tx, first.MessageID, first.AgentID, "already deferred",
	)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	processed, err = worker.RunOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, processed)
	require.Equal(t, []QueueDelivery{first, second}, backend.candidates)
}

func TestWorkerSerializesConcurrentScansPerAgentAndForwardsControls(t *testing.T) {
	store := newWorkerTestStore(t)
	seedWorkerSession(t, store, "agent-worker", true)
	_ = seedWorkerQueue(t, store, "agent-worker", 10, 0)
	backend := &workerBackend{started: make(chan struct{}, 1), block: make(chan struct{})}
	worker, err := NewWorker(WorkerOptions{
		DB: store.DB(), Processor: backend, Approvals: backend, Prepared: backend,
		Proxies: workerSnapshots{
			"agent-worker": {AgentID: "agent-worker", State: agentlink.StateUp},
		},
	})
	require.NoError(t, err)
	firstDone := make(chan struct {
		count int
		err   error
	}, 1)
	go func() {
		count, runErr := worker.RunOnce(t.Context())
		firstDone <- struct {
			count int
			err   error
		}{count: count, err: runErr}
	}()
	<-backend.started
	count, err := worker.RunOnce(t.Context())
	require.NoError(t, err)
	require.Zero(t, count)
	close(backend.block)
	result := <-firstDone
	require.NoError(t, result.err)
	require.Equal(t, 1, result.count)

	approvalID := protocol.ApprovalID("apr_01ARZ3NDEKTSV4RRFFQ69G5FAV")
	_, err = worker.ResolveApproval(t.Context(), protocol.ResolveApprovalParams{
		ApprovalID: approvalID, Option: "reject",
	}, channel.Actor{Type: protocol.ActorTypeHuman, ID: "operator"})
	require.NoError(t, err)
	require.NoError(t, worker.ControlChanged(t.Context(), "agent-worker"))
	require.NoError(t, worker.PauseAutomation(t.Context(), "agent-worker"))
	backend.mu.Lock()
	defer backend.mu.Unlock()
	require.Equal(t, approvalID, backend.resolved.ApprovalID)
	require.Equal(t, protocol.AgentID("agent-worker"), backend.changed)
	require.Equal(t, protocol.AgentID("agent-worker"), backend.paused)
}
