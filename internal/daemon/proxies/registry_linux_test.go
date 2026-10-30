//go:build linux

package proxies

import (
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/agentsock"
	"github.com/smellak/gifford/internal/daemon/agentlink"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
)

var registryNow = time.Date(2026, 10, 30, 18, 30, 0, 0, time.UTC)

type registryClock struct {
	mu     sync.Mutex
	timers map[time.Duration][]chan time.Time
}

func newRegistryClock() *registryClock {
	return &registryClock{timers: make(map[time.Duration][]chan time.Time)}
}

func (*registryClock) Now() time.Time { return registryNow }

func (c *registryClock) After(duration time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	timer := make(chan time.Time, 1)
	c.timers[duration] = append(c.timers[duration], timer)
	return timer
}

func (c *registryClock) fire(t *testing.T, duration time.Duration) {
	t.Helper()
	var timers []chan time.Time
	require.Eventually(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		if len(c.timers[duration]) == 0 {
			return false
		}
		timers = append(timers, c.timers[duration]...)
		delete(c.timers, duration)
		return true
	}, 5*time.Second, time.Millisecond)
	for _, timer := range timers {
		timer <- registryNow
	}
}

type registryGuard struct {
	report  protocol.InputGuardReport
	changed chan struct{}
}

func (g *registryGuard) InputGuard() protocol.InputGuardReport { return g.report }
func (g *registryGuard) GuardChanged() <-chan struct{}         { return g.changed }

type noTimerClock struct{}

func (noTimerClock) Now() time.Time                       { return registryNow }
func (noTimerClock) After(time.Duration) <-chan time.Time { return nil }

type deliveryRegistryClock struct{}

func (deliveryRegistryClock) Now() time.Time { return registryNow }

func (deliveryRegistryClock) After(duration time.Duration) <-chan time.Time {
	if duration != agentsock.ConsumeWindow {
		return nil
	}
	timer := make(chan time.Time, 1)
	timer <- registryNow
	return timer
}

type registryDelivery struct{}

func (registryDelivery) CheckLine(string) error { return nil }
func (registryDelivery) Prepare(_ protocol.OpaqueID, generation uint64, version int64) (agentsock.DeliveryReport, error) {
	return agentsock.DeliveryReport{
		Result: protocol.DeliveryResultKindPrepared, Generation: generation, Version: version,
	}, nil
}

func (registryDelivery) Commit(protocol.OpaqueID, string, protocol.InjectMode) (agentsock.DeliveryReport, error) {
	return agentsock.DeliveryReport{
		Result: protocol.DeliveryResultKindCommitted, Generation: 1, Version: 1,
	}, nil
}

func (registryDelivery) Cancel(protocol.OpaqueID, protocol.CancelReason) (agentsock.DeliveryReport, error) {
	return agentsock.DeliveryReport{Result: protocol.DeliveryResultKindCancelled}, nil
}
func (registryDelivery) DefaultMode() protocol.InjectMode { return protocol.InjectModeTyped }
func (registryDelivery) InputHolds(string) bool           { return false }

func registryDB(t *testing.T) *sql.DB {
	t.Helper()
	store, err := eventstore.Open(t.Context(), filepath.Join(t.TempDir(), "gifford.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return store.DB()
}

func registryRuntime(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "gp") //nolint:usetesting // Unix socket paths must stay short.
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(dir)) })
	return dir
}

func registryRegistration(t *testing.T) protocol.RegisterAgent {
	t.Helper()
	process, err := registry.CurrentProcess()
	require.NoError(t, err)
	return protocol.RegisterAgent{
		Manifest: protocol.CapabilityManifest{
			SchemaVersion: protocol.SchemaVersion, AgentID: "agent-1",
			Harness: protocol.HarnessCodex, HarnessVersion: "1.2.3",
			AdapterType:        protocol.AdapterTypeGenericPTY,
			InputGuardProvider: protocol.InputGuardProviderPTYProxy,
			CaptureMode:        protocol.CaptureModePTYProxy, Deliverable: true,
			Capabilities: protocol.Capabilities{MCP: true, PTYCapture: true},
		},
		Process: &process,
	}
}

func insertRegistrySession(
	t *testing.T,
	db *sql.DB,
	registration protocol.RegisterAgent,
) {
	t.Helper()
	manifest, err := json.Marshal(registration.Manifest)
	require.NoError(t, err)
	process, err := json.Marshal(registration.Process)
	require.NoError(t, err)
	stamp := eventstore.FormatTime(registryNow)
	_, err = db.ExecContext(t.Context(), `INSERT INTO agents
		(agent_id, node_id, harness, created_at, updated_at) VALUES (?, 'node-test', ?, ?, ?)`,
		registration.Manifest.AgentID, registration.Manifest.Harness, stamp, stamp)
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), `INSERT INTO sessions
		(session_id, agent_id, state, capture_mode, deliverable, manifest_json,
		 process_json, started_at, ended_at, updated_at)
		VALUES (?, ?, 'CONNECTED', 'pty_proxy', 1, ?, ?, ?, NULL, ?)`,
		"session-1", registration.Manifest.AgentID, string(manifest), string(process), stamp, stamp)
	require.NoError(t, err)
}

func listenRegistryProxy(
	t *testing.T,
	runtimeDir string,
	registration protocol.RegisterAgent,
) {
	t.Helper()
	server, err := agentsock.Listen(t.Context(), agentsock.Options{
		RuntimeDir: runtimeDir, Register: registration,
		Guard: &registryGuard{
			report: protocol.InputGuardReport{
				DraftState: protocol.DraftStateClosed, ActivityState: protocol.ActivityStateIdle,
				DraftGeneration: 1, InputGuardVersion: 1,
			},
			changed: make(chan struct{}, 1),
		},
		Clock: noTimerClock{},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, server.Close()) })
}

func listenRegistryDeliveryProxy(
	t *testing.T,
	runtimeDir string,
	registration protocol.RegisterAgent,
) {
	t.Helper()
	server, err := agentsock.Listen(t.Context(), agentsock.Options{
		RuntimeDir: runtimeDir, Register: registration,
		Guard: &registryGuard{
			report: protocol.InputGuardReport{
				DraftState: protocol.DraftStateClosed, ActivityState: protocol.ActivityStateIdle,
				DraftGeneration: 1, InputGuardVersion: 1,
			},
			changed: make(chan struct{}, 1),
		},
		Delivery: registryDelivery{}, Clock: deliveryRegistryClock{},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, server.Close()) })
}

func TestRegistryReconcilesConnectedAndDisconnectedSession(t *testing.T) {
	t.Parallel()
	db := registryDB(t)
	runtimeDir := registryRuntime(t)
	clock := newRegistryClock()
	store, err := agentlink.NewInputGuardStore(db, clock)
	require.NoError(t, err)
	managed, err := Start(t.Context(), Options{
		DB: db, RuntimeDir: runtimeDir, Writer: store, Clock: clock,
		ReconcileEvery: time.Hour,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, managed.Close()) })
	_, found := managed.Snapshot("agent-1")
	require.False(t, found)

	registration := registryRegistration(t)
	insertRegistrySession(t, db, registration)
	listenRegistryProxy(t, runtimeDir, registration)
	require.NoError(t, managed.Reconcile(t.Context()))
	var snapshot agentlink.Snapshot
	require.Eventually(t, func() bool {
		var ok bool
		snapshot, ok = managed.Snapshot("agent-1")
		return ok && snapshot.State == agentlink.StateUp && snapshot.HasReport
	}, 5*time.Second, time.Millisecond)
	client, found := managed.Client("agent-1")
	require.True(t, found)

	stamp := eventstore.FormatTime(registryNow.Add(time.Second))
	_, err = db.ExecContext(t.Context(), `UPDATE sessions SET state = 'DISCONNECTED',
		ended_at = ?, updated_at = ? WHERE session_id = ?`, stamp, stamp, "session-1")
	require.NoError(t, err)
	require.NoError(t, managed.Reconcile(t.Context()))
	_, found = managed.Snapshot("agent-1")
	require.False(t, found)
	select {
	case <-client.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("removed client did not stop")
	}
}

func TestRegistryPeriodicReconcileStopsSessionsAfterHubRestart(t *testing.T) {
	t.Parallel()
	db := registryDB(t)
	runtimeDir := registryRuntime(t)
	registration := registryRegistration(t)
	insertRegistrySession(t, db, registration)
	listenRegistryProxy(t, runtimeDir, registration)
	clock := newRegistryClock()
	const interval = 25 * time.Millisecond
	managed, err := Start(t.Context(), Options{
		DB: db, RuntimeDir: runtimeDir, Clock: clock,
		ReconcileEvery: interval,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, managed.Close()) })
	require.Eventually(t, func() bool {
		snapshot, ok := managed.Snapshot("agent-1")
		return ok && snapshot.State == agentlink.StateUp
	}, 5*time.Second, time.Millisecond)
	client, found := managed.Client("agent-1")
	require.True(t, found)

	stamp := eventstore.FormatTime(registryNow.Add(time.Second))
	_, err = db.ExecContext(t.Context(), `UPDATE sessions SET state = 'DISCONNECTED',
		ended_at = ?, updated_at = ? WHERE capture_mode = 'pty_proxy' AND ended_at IS NULL`, stamp, stamp)
	require.NoError(t, err)
	clock.fire(t, interval)
	require.Eventually(t, func() bool {
		_, ok := managed.Snapshot("agent-1")
		return !ok
	}, 5*time.Second, time.Millisecond)
	select {
	case <-client.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("restart did not stop client")
	}
}

func TestRegistryLifecycleConsumerPersistsDeliveryResults(t *testing.T) {
	t.Parallel()
	db := registryDB(t)
	runtimeDir := registryRuntime(t)
	registration := registryRegistration(t)
	insertRegistrySession(t, db, registration)
	listenRegistryDeliveryProxy(t, runtimeDir, registration)
	stamp := eventstore.FormatTime(registryNow)
	const messageID = protocol.ULID("01ARZ3NDEKTSV4RRFFQ69G5FAV")
	_, err := db.ExecContext(t.Context(), `INSERT INTO delivery_queue
		(message_id, agent_id, delivery_generation, from_agent, thread_ref,
		 state, priority, created_at, expires_at, updated_at, hop_count, route_digest)
		VALUES (?, 'agent-1', 1, 'agent-2', 'thread-1', 'CLAIMED', 2, ?, ?, ?, 0, 'sha256:route')`,
		messageID, stamp, stamp, stamp)
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), `INSERT INTO delivery_attempts
		(delivery_id, message_id, agent_id, delivery_generation, adapter_session_id,
		 orchestrator_decision_id, channel_decision_id, draft_generation,
		 input_guard_version, control_versions_json, state, created_at, updated_at)
		VALUES ('delivery-1', ?, 'agent-1', 1, 'session-1', ?, ?, 1, 1,
		 '{"global":1}', 'PREPARING', ?, ?)`, messageID, messageID,
		protocol.ChannelDecisionID("chd_"+messageID), stamp, stamp)
	require.NoError(t, err)

	clock := newRegistryClock()
	managed, err := Start(t.Context(), Options{
		DB: db, RuntimeDir: runtimeDir, Clock: clock, ReconcileEvery: time.Hour,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, managed.Close()) })
	require.Eventually(t, func() bool {
		snapshot, ok := managed.Snapshot("agent-1")
		return ok && snapshot.State == agentlink.StateUp && snapshot.HasReport
	}, 5*time.Second, time.Millisecond)
	client, ok := managed.Client("agent-1")
	require.True(t, ok)
	prepare := protocol.PrepareDelivery{
		DeliveryID: "delivery-1", AgentID: "agent-1",
		MessageRef: protocol.MessageRef{
			MessageID: messageID, FromAgent: "agent-2",
			Priority: protocol.PriorityNormal, ThreadID: "thread-1",
		},
		NoticeLine: "[GIFFORD] pending", OrchestratorDecisionID: messageID,
		ExpectedStateVersion: 1, ExpectedInputGuardVersion: 1,
		ExpectedDraftGeneration: 1, ControlVersions: protocol.ControlVersions{"global": 1},
	}
	_, err = client.Call(t.Context(), protocol.AgentOpPrepareDelivery, prepare)
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), `UPDATE delivery_attempts SET state = 'COMMIT_SENT',
		writer_lease_id = 'lease-1', commit_sent_at = ?, updated_at = ?
		WHERE delivery_id = 'delivery-1'`, stamp, stamp)
	require.NoError(t, err)
	_, err = client.Call(t.Context(), protocol.AgentOpCommitDelivery,
		protocol.CommitDelivery{DeliveryID: "delivery-1", WriterLeaseID: "lease-1"})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		var count int
		queryErr := db.QueryRowContext(t.Context(), `SELECT count(*)
			FROM delivery_result_evidence WHERE delivery_id = 'delivery-1'
			AND result = 'COMMITTED'`).Scan(&count)
		return queryErr == nil && count == 1
	}, 5*time.Second, time.Millisecond)
}

func TestRegistryRejectsDuplicateLiveSessionsForAgent(t *testing.T) {
	t.Parallel()
	db := registryDB(t)
	registration := registryRegistration(t)
	insertRegistrySession(t, db, registration)
	manifest, err := json.Marshal(registration.Manifest)
	require.NoError(t, err)
	process, err := json.Marshal(registration.Process)
	require.NoError(t, err)
	stamp := eventstore.FormatTime(registryNow)
	_, err = db.ExecContext(t.Context(), `INSERT INTO sessions
		(session_id, agent_id, state, capture_mode, deliverable, manifest_json,
		 process_json, started_at, ended_at, updated_at)
		VALUES ('session-2', 'agent-1', 'CONNECTED', 'pty_proxy', 1, ?, ?, ?, NULL, ?)`,
		string(manifest), string(process), stamp, stamp)
	require.NoError(t, err)
	store, err := agentlink.NewInputGuardStore(db, newRegistryClock())
	require.NoError(t, err)
	_, err = Start(t.Context(), Options{
		DB: db, RuntimeDir: registryRuntime(t), Writer: store, Clock: newRegistryClock(),
	})
	require.ErrorIs(t, err, ErrDuplicateAgent)
}
