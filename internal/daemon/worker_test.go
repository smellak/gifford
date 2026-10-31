package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
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
	// evaluated, when set, runs at the start of each evaluation.
	evaluated func(QueueDelivery)
}

func (b *workerBackend) EvaluateAutomatic(_ context.Context, candidate QueueDelivery) error {
	b.mu.Lock()
	b.candidates = append(b.candidates, candidate)
	started, block, evaluated := b.started, b.block, b.evaluated
	b.mu.Unlock()
	if evaluated != nil {
		evaluated(candidate)
	}
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

// seedWorkerRow queues one delivery created at created.
func seedWorkerRow(
	t *testing.T,
	store *eventstore.Store,
	agent protocol.AgentID,
	sequence, priority int,
	created time.Time,
) QueueDelivery {
	t.Helper()
	message := protocol.ULID(fmt.Sprintf("01ARZ3NDEKTSV4RRFFQ69G%04d", sequence))
	stamp := eventstore.FormatTime(created)
	_, err := store.DB().ExecContext(t.Context(), `INSERT INTO delivery_queue
		(message_id, agent_id, delivery_generation, from_agent, thread_ref,
		 state, priority, created_at, expires_at, updated_at, hop_count, route_digest)
		VALUES (?, ?, 1, 'worker-sender', 'thr_01ARZ3NDEKTSV4RRFFQ69G5FAV',
		 'QUEUED', ?, ?, ?, ?, 0, 'sha256:worker-route')`,
		message, agent, priority, stamp, eventstore.FormatTime(created.Add(time.Hour)), stamp)
	require.NoError(t, err)
	return QueueDelivery{MessageID: message, AgentID: agent, DeliveryGeneration: 1}
}

// seedWorkerTriage stores the message's triage verdict, as internal/triage
// writes it.
func seedWorkerTriage(t *testing.T, store *eventstore.Store, message protocol.ULID, label protocol.TriageLabel) {
	t.Helper()
	stamp := eventstore.FormatTime(attemptTestTime)
	digest := "sha256:" + strings.Repeat("0", 64)
	_, err := store.DB().ExecContext(t.Context(), `INSERT INTO messages
		(message_id, client_msg_id, origin_node, from_agent, destination_node,
		 thread_ref, thread_id, priority, state, created_at, expires_at, updated_at,
		 hop_count, route_digest)
		VALUES (?, ?, 'node-worker', 'worker-sender', 'node-worker',
		 'thr_01ARZ3NDEKTSV4RRFFQ69G5FAV', 'thread-worker', 'NORMAL', 'PENDING', ?, ?, ?, 0, ?)`,
		message, "client-"+string(message), stamp, eventstore.FormatTime(attemptTestTime.Add(time.Hour)),
		stamp, digest)
	require.NoError(t, err)
	_, err = store.DB().ExecContext(t.Context(), `INSERT INTO message_triage
		(message_id, label, outcome, glimmer_call_id, created_at)
		VALUES (?, ?, 'classified', 'call-triage-worker', ?)`, message, label, stamp)
	require.NoError(t, err)
}

// Rows are taken by selection level, then age: a CONFLICT_OR_URGENT
// triage selects a row as HIGH, a row maxQueueAge old moves one level up
// and the triage never lowers a row. Only a row its sender sent as LOW,
// and the triage did not raise, waits lowBatchWindow.
func TestWorkerSelectsByLevelThenAge(t *testing.T) {
	t.Parallel()

	const agent = protocol.AgentID("agent-worker")
	const urgent, high, normal, low = 0, 1, 2, 3
	store := newWorkerTestStore(t)
	seedWorkerSession(t, store, agent, true)
	now := attemptTestTime.Add(time.Hour)
	ago := func(d time.Duration) time.Time { return now.Add(-d) }
	row := func(sequence, priority int, age time.Duration) QueueDelivery {
		return seedWorkerRow(t, store, agent, sequence, priority, ago(age))
	}
	triaged := func(candidate QueueDelivery, label protocol.TriageLabel) QueueDelivery {
		seedWorkerTriage(t, store, candidate.MessageID, label)
		return candidate
	}
	freshUrgent := row(1, urgent, 10*time.Second)
	agedHigh := row(2, high, 130*time.Second)
	normalAtMaxAge := row(3, normal, maxQueueAge)
	agedLow := row(4, low, 121*time.Second)
	raisedNormal := triaged(row(5, normal, 20*time.Second), protocol.TriageLabelConflictOrUrgent)
	quietNormal := triaged(row(6, normal, 100*time.Second), protocol.TriageLabelLowPriority)
	freshHigh := row(7, high, 5*time.Second)
	waitingLow := row(8, low, lowBatchWindow-time.Second)
	raisedLow := triaged(row(9, low, 15*time.Second), protocol.TriageLabelConflictOrUrgent)
	freshQuietNormal := triaged(row(10, normal, time.Second), protocol.TriageLabelLowPriority)
	quietUrgent := triaged(row(11, urgent, 2*time.Second), protocol.TriageLabelLowPriority)
	highBelowMaxAge := row(12, high, maxQueueAge-time.Second)

	clk := &attemptClock{now: now}
	backend := &workerBackend{}
	backend.evaluated = func(candidate QueueDelivery) {
		_, err := store.DB().ExecContext(t.Context(), `UPDATE delivery_queue SET state = 'DELIVERED'
			WHERE message_id = ? AND agent_id = ?`, candidate.MessageID, candidate.AgentID)
		if err != nil {
			t.Errorf("deliver %s: %v", candidate.MessageID, err)
		}
	}
	worker, err := NewWorker(WorkerOptions{
		DB: store.DB(), Processor: backend, Approvals: backend, Prepared: backend,
		Proxies: workerSnapshots{agent: {AgentID: agent, State: agentlink.StateUp}},
		Clock:   clk,
	})
	require.NoError(t, err)
	drain := func() {
		for {
			processed, err := worker.RunOnce(t.Context())
			require.NoError(t, err)
			if processed == 0 {
				return
			}
			require.Equal(t, 1, processed)
		}
	}
	drain()
	require.Equal(t, []QueueDelivery{
		// The top group: aged HIGH and NORMAL, then URGENT, by age.
		agedHigh, normalAtMaxAge, freshUrgent, quietUrgent,
		// HIGH, with what the triage raised.
		highBelowMaxAge, raisedNormal, raisedLow, freshHigh,
		// NORMAL, with aged LOW and what the triage found quiet.
		agedLow, quietNormal, freshQuietNormal,
	}, backend.candidates)

	clk.now = clk.now.Add(time.Second)
	drain()
	require.Equal(t, waitingLow, backend.candidates[len(backend.candidates)-1],
		"a LOW row is eligible once lowBatchWindow old")
	require.Len(t, backend.candidates, 12)
}

// countingBackend counts the evaluations running at once, holding each
// until release closes.
type countingBackend struct {
	mu      sync.Mutex
	running int
	peak    int
	started chan protocol.AgentID
	release chan struct{}
}

func (b *countingBackend) EvaluateAutomatic(_ context.Context, candidate QueueDelivery) error {
	b.mu.Lock()
	b.running++
	b.peak = max(b.peak, b.running)
	b.mu.Unlock()
	b.started <- candidate.AgentID
	<-b.release
	b.mu.Lock()
	b.running--
	b.mu.Unlock()
	return nil
}

// One agent's model call must not hold the others back, and no more than
// Concurrency agents are evaluated at once.
func TestWorkerBoundsTheAgentsEvaluatedAtOnce(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		concurrency, agents, peak int
	}{
		"one":     {concurrency: 1, agents: 2, peak: 1},
		"two":     {concurrency: 2, agents: 3, peak: 2},
		"default": {concurrency: 0, agents: defaultWorkerConcurrency + 1, peak: defaultWorkerConcurrency},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			store := newWorkerTestStore(t)
			snapshots := workerSnapshots{}
			for i := range tc.agents {
				agent := protocol.AgentID(fmt.Sprintf("agent-worker-%d", i))
				seedWorkerSession(t, store, agent, true)
				_ = seedWorkerQueue(t, store, agent, i+1, 2)
				snapshots[agent] = agentlink.Snapshot{AgentID: agent, State: agentlink.StateUp}
			}
			backend := &countingBackend{
				started: make(chan protocol.AgentID, tc.agents), release: make(chan struct{}),
			}
			worker, err := NewWorker(WorkerOptions{
				DB: store.DB(), Processor: backend, Approvals: &workerBackend{}, Prepared: &workerBackend{},
				Proxies: snapshots, Clock: &attemptClock{now: attemptTestTime}, Concurrency: tc.concurrency,
			})
			require.NoError(t, err)
			type result struct {
				processed int
				err       error
			}
			done := make(chan result, 1)
			go func() {
				processed, err := worker.RunOnce(t.Context())
				done <- result{processed: processed, err: err}
			}()
			for range tc.peak {
				select {
				case <-backend.started:
				case <-time.After(10 * time.Second):
					close(backend.release)
					t.Fatal("the evaluations did not start side by side")
				}
			}
			close(backend.release)
			got := <-done
			require.NoError(t, got.err)
			require.Equal(t, tc.agents, got.processed)
			backend.mu.Lock()
			defer backend.mu.Unlock()
			require.Equal(t, tc.peak, backend.peak)
		})
	}
}

// Approvals, control changes and pauses never take the in-flight mark:
// none of them waits for an evaluation that is still running.
func TestWorkerControlsNeverWaitForAnEvaluation(t *testing.T) {
	t.Parallel()

	const agent = protocol.AgentID("agent-worker")
	store := newWorkerTestStore(t)
	seedWorkerSession(t, store, agent, true)
	_ = seedWorkerQueue(t, store, agent, 20, 2)
	backend := &workerBackend{started: make(chan struct{}, 1), block: make(chan struct{})}
	worker, err := NewWorker(WorkerOptions{
		DB: store.DB(), Processor: backend, Approvals: backend, Prepared: backend,
		Proxies: workerSnapshots{agent: {AgentID: agent, State: agentlink.StateUp}},
		Clock:   &attemptClock{now: attemptTestTime},
	})
	require.NoError(t, err)
	var once sync.Once
	release := func() { once.Do(func() { close(backend.block) }) }
	done, finished := make(chan error, 1), make(chan struct{})
	go func() {
		defer close(finished)
		_, err := worker.RunOnce(t.Context())
		done <- err
	}()
	t.Cleanup(func() {
		release()
		<-finished
	})
	select {
	case <-backend.started:
	case err := <-done:
		t.Fatalf("the scan ended before its evaluation: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("the evaluation never started")
	}

	for name, call := range map[string]func(context.Context) error{
		"approval": func(ctx context.Context) error {
			_, err := worker.ResolveApproval(ctx, protocol.ResolveApprovalParams{
				ApprovalID: "apr_01ARZ3NDEKTSV4RRFFQ69G5FAV", Option: "reject",
			}, channel.Actor{Type: protocol.ActorTypeHuman, ID: "operator"})
			return err
		},
		"control change": func(ctx context.Context) error { return worker.ControlChanged(ctx, agent) },
		"pause":          func(ctx context.Context) error { return worker.PauseAutomation(ctx, agent) },
	} {
		result := make(chan error, 1)
		go func() { result <- call(t.Context()) }()
		select {
		case err := <-result:
			require.NoError(t, err, name)
		case <-time.After(10 * time.Second):
			t.Fatalf("%s waited for the evaluation", name)
		}
	}
	select {
	case err := <-done:
		t.Fatalf("the evaluation ended while held: %v", err)
	default:
	}
	release()
	require.NoError(t, <-done)
}
