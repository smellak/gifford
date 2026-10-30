//go:build linux

package proxies

import (
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/agentsock"
	daemoncore "github.com/smellak/gifford/internal/daemon"
	"github.com/smellak/gifford/internal/daemon/agentlink"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
)

var registryNow = time.Date(2026, 10, 30, 18, 30, 0, 0, time.UTC)

const (
	registryAgentID   protocol.AgentID  = "agent-1"
	registrySessionID protocol.OpaqueID = "session-1"
)

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
	mu      sync.RWMutex
	report  protocol.InputGuardReport
	changed chan struct{}
}

func (g *registryGuard) InputGuard() protocol.InputGuardReport {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.report
}

func (g *registryGuard) GuardChanged() <-chan struct{} { return g.changed }

func (g *registryGuard) setVersion(generation, version int64) {
	g.mu.Lock()
	g.report.DraftGeneration = generation
	g.report.InputGuardVersion = version
	g.mu.Unlock()
}

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

type registrySessionLiveness struct {
	mu      sync.Mutex
	process protocol.ProcessIdentity
	bootID  string
	live    bool
}

func (l *registrySessionLiveness) alive(process protocol.ProcessIdentity, bootID string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.live && process == l.process && bootID == l.bootID {
		return nil
	}
	return errors.New("session process is not live")
}

func (l *registrySessionLiveness) setLive(live bool) {
	l.mu.Lock()
	l.live = live
	l.mu.Unlock()
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
			SchemaVersion: protocol.SchemaVersion, AgentID: registryAgentID,
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
		registrySessionID, registration.Manifest.AgentID, string(manifest), string(process), stamp, stamp)
	require.NoError(t, err)
}

func listenRegistryProxy(
	t *testing.T,
	runtimeDir string,
	registration protocol.RegisterAgent,
) *registryGuard {
	t.Helper()
	guard := &registryGuard{
		report: protocol.InputGuardReport{
			DraftState: protocol.DraftStateClosed, ActivityState: protocol.ActivityStateIdle,
			DraftGeneration: 1, InputGuardVersion: 1,
		},
		changed: make(chan struct{}, 1),
	}
	server, err := agentsock.Listen(t.Context(), agentsock.Options{
		RuntimeDir: runtimeDir, Register: registration,
		Guard: guard,
		Clock: noTimerClock{},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, server.Close()) })
	return guard
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
	_, found := managed.Snapshot(registryAgentID)
	require.False(t, found)

	registration := registryRegistration(t)
	insertRegistrySession(t, db, registration)
	listenRegistryProxy(t, runtimeDir, registration)
	require.NoError(t, managed.Reconcile(t.Context()))
	var snapshot agentlink.Snapshot
	require.Eventually(t, func() bool {
		var ok bool
		snapshot, ok = managed.Snapshot(registryAgentID)
		return ok && snapshot.State == agentlink.StateUp && snapshot.HasReport
	}, 5*time.Second, time.Millisecond)
	client, found := managed.Client(registryAgentID)
	require.True(t, found)

	stamp := eventstore.FormatTime(registryNow.Add(time.Second))
	_, err = db.ExecContext(t.Context(), `UPDATE sessions SET state = 'DISCONNECTED',
		ended_at = ?, updated_at = ? WHERE session_id = ?`, stamp, stamp, registrySessionID)
	require.NoError(t, err)
	require.NoError(t, managed.Reconcile(t.Context()))
	_, found = managed.Snapshot(registryAgentID)
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
		snapshot, ok := managed.Snapshot(registryAgentID)
		return ok && snapshot.State == agentlink.StateUp
	}, 5*time.Second, time.Millisecond)
	client, found := managed.Client(registryAgentID)
	require.True(t, found)

	stamp := eventstore.FormatTime(registryNow.Add(time.Second))
	_, err = db.ExecContext(t.Context(), `UPDATE sessions SET state = 'DISCONNECTED',
		ended_at = ?, updated_at = ? WHERE capture_mode = 'pty_proxy' AND ended_at IS NULL`, stamp, stamp)
	require.NoError(t, err)
	clock.fire(t, interval)
	require.Eventually(t, func() bool {
		_, ok := managed.Snapshot(registryAgentID)
		return !ok
	}, 5*time.Second, time.Millisecond)
	select {
	case <-client.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("restart did not stop client")
	}
}

func TestRegistryPeriodicReconcileDisconnectsDeadDetachedSession(t *testing.T) {
	t.Parallel()
	db := registryDB(t)
	registration := registryRegistration(t)
	insertRegistrySession(t, db, registration)
	const bootID = "boot-detached-registry"
	_, err := db.ExecContext(t.Context(), `UPDATE sessions SET state = ?, boot_id = ?
		WHERE session_id = ?`, protocol.SessionStateDetached, bootID, registrySessionID)
	require.NoError(t, err)
	liveness := &registrySessionLiveness{
		process: *registration.Process, bootID: bootID, live: true,
	}
	clock := newRegistryClock()
	const interval = 25 * time.Millisecond
	managed, err := Start(t.Context(), Options{
		DB: db, RuntimeDir: registryRuntime(t), Clock: clock,
		ReconcileEvery: interval, sessionLiveness: liveness,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, managed.Close()) })
	_, linked := managed.Client(registryAgentID)
	require.False(t, linked, "DETACHED must never create a proxy link")
	var state protocol.SessionState
	var endedAt sql.NullString
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT state, ended_at
		FROM sessions WHERE session_id = ?`, registrySessionID).Scan(&state, &endedAt))
	require.Equal(t, protocol.SessionStateDetached, state)
	require.False(t, endedAt.Valid)

	liveness.setLive(false)
	clock.fire(t, interval)
	require.Eventually(t, func() bool {
		queryErr := db.QueryRowContext(t.Context(), `SELECT state, ended_at
			FROM sessions WHERE session_id = ?`, registrySessionID).Scan(&state, &endedAt)
		return queryErr == nil && state == protocol.SessionStateDisconnected && endedAt.Valid
	}, 5*time.Second, time.Millisecond)
	_, linked = managed.Client(registryAgentID)
	require.False(t, linked)
}

func TestRegistryRecreatesLinkAfterDetachedSessionResumes(t *testing.T) {
	t.Parallel()
	db := registryDB(t)
	runtimeDir := registryRuntime(t)
	registration := registryRegistration(t)
	insertRegistrySession(t, db, registration)
	const bootID = "boot-resumed-registry"
	_, err := db.ExecContext(t.Context(), `UPDATE sessions SET boot_id = ?
		WHERE session_id = ?`, bootID, registrySessionID)
	require.NoError(t, err)
	guard := listenRegistryProxy(t, runtimeDir, registration)
	liveness := &registrySessionLiveness{
		process: *registration.Process, bootID: bootID, live: true,
	}
	managed, err := Start(t.Context(), Options{
		DB: db, RuntimeDir: runtimeDir, Clock: agentlink.SystemClock{},
		ReconcileEvery: time.Hour, sessionLiveness: liveness,
		ReconnectMin: 10 * time.Millisecond, ReconnectMax: 20 * time.Millisecond,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, managed.Close()) })
	require.Eventually(t, func() bool {
		snapshot, ok := managed.Snapshot(registryAgentID)
		return ok && snapshot.State == agentlink.StateUp && snapshot.HasReport
	}, 5*time.Second, time.Millisecond)
	first, found := managed.Client(registryAgentID)
	require.True(t, found)

	_, err = db.ExecContext(t.Context(), `UPDATE sessions SET state = ?
		WHERE session_id = ?`, protocol.SessionStateDetached, registrySessionID)
	require.NoError(t, err)
	require.NoError(t, managed.Reconcile(t.Context()))
	_, found = managed.Client(registryAgentID)
	require.False(t, found)
	select {
	case <-first.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("DETACHED did not stop the prior proxy link")
	}
	var state protocol.SessionState
	requireSessionState := func(want protocol.SessionState) {
		t.Helper()
		require.NoError(t, db.QueryRowContext(t.Context(),
			"SELECT state FROM sessions WHERE session_id = ?", registrySessionID).Scan(&state))
		require.Equal(t, want, state)
	}
	requireSessionState(protocol.SessionStateDetached)
	guard.setVersion(7, 11)

	_, err = db.ExecContext(t.Context(), `UPDATE sessions SET state = ?
		WHERE session_id = ?`, protocol.SessionStateConnected, registrySessionID)
	require.NoError(t, err)
	require.NoError(t, managed.Reconcile(t.Context()))
	var second *agentlink.Client
	require.Eventually(t, func() bool {
		var ok bool
		second, ok = managed.Client(registryAgentID)
		if !ok || second == first {
			return false
		}
		snapshot := second.Snapshot()
		return snapshot.State == agentlink.StateUp && snapshot.HasReport &&
			snapshot.LastReport.DraftGeneration == 7 && snapshot.LastReport.InputGuardVersion == 11
	}, 5*time.Second, time.Millisecond)
	require.NotSame(t, first, second, "resume must use a fresh client and handshake state")
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
		snapshot, ok := managed.Snapshot(registryAgentID)
		return ok && snapshot.State == agentlink.StateUp && snapshot.HasReport
	}, 5*time.Second, time.Millisecond)
	client, ok := managed.Client(registryAgentID)
	require.True(t, ok)
	prepare := protocol.PrepareDelivery{
		DeliveryID: "delivery-1", AgentID: registryAgentID,
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

func TestRegistryResultPersistenceFailureDurablyDisconnectsSession(t *testing.T) {
	t.Parallel()
	db := registryDB(t)
	runtimeDir := registryRuntime(t)
	registration := registryRegistration(t)
	insertRegistrySession(t, db, registration)
	listenRegistryDeliveryProxy(t, runtimeDir, registration)
	clock := newRegistryClock()
	managed, err := Start(t.Context(), Options{
		DB: db, RuntimeDir: runtimeDir, Clock: clock, ReconcileEvery: time.Hour,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, managed.Close()) })
	require.Eventually(t, func() bool {
		snapshot, ok := managed.Snapshot(registryAgentID)
		return ok && snapshot.State == agentlink.StateUp && snapshot.HasReport
	}, 5*time.Second, time.Millisecond)
	client, ok := managed.Client(registryAgentID)
	require.True(t, ok)
	const messageID = protocol.ULID("01ARZ3NDEKTSV4RRFFQ69G5FAV")
	_, err = client.Send(t.Context(), protocol.AgentOpPrepareDelivery, protocol.PrepareDelivery{
		DeliveryID: "missing-delivery", AgentID: registryAgentID,
		MessageRef: protocol.MessageRef{
			MessageID: messageID, FromAgent: "agent-2",
			Priority: protocol.PriorityNormal, ThreadID: "thread-1",
		},
		NoticeLine: "[GIFFORD] pending", OrchestratorDecisionID: messageID,
		ExpectedStateVersion: 1, ExpectedInputGuardVersion: 1,
		ExpectedDraftGeneration: 1, ControlVersions: protocol.ControlVersions{"global": 1},
	})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		var state protocol.SessionState
		var endedAt sql.NullString
		queryErr := db.QueryRowContext(t.Context(), `SELECT state, ended_at
			FROM sessions WHERE session_id = 'session-1'`).Scan(&state, &endedAt)
		_, linked := managed.Client(registryAgentID)
		return queryErr == nil && state == protocol.SessionStateDisconnected && endedAt.Valid && !linked
	}, 5*time.Second, time.Millisecond)
	require.ErrorIs(t, managed.LastError(), daemoncore.ErrAttemptNotFound)

	require.NoError(t, managed.Reconcile(t.Context()))
	_, recreated := managed.Client(registryAgentID)
	require.False(t, recreated, "reconcile must not recreate the evidence-losing session")
	require.ErrorIs(t, managed.LastError(), daemoncore.ErrAttemptNotFound)
	require.NoError(t, managed.Close())

	restarted, err := Start(t.Context(), Options{
		DB: db, RuntimeDir: runtimeDir, Clock: newRegistryClock(), ReconcileEvery: time.Hour,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, restarted.Close()) })
	_, recreated = restarted.Client(registryAgentID)
	require.False(t, recreated, "the durable block must survive registry restart")
	require.NoError(t, restarted.LastError(),
		"the restarted registry must rely on durable session state, not inherited memory")
}

func TestRegistryDoesNotPublishSessionBlockedDuringReconcile(t *testing.T) {
	t.Parallel()
	db := registryDB(t)
	runtimeDir := registryRuntime(t)
	clock := newRegistryClock()
	beforePublish := make(chan desiredLink)
	releasePublish := make(chan struct{})
	managed, err := Start(t.Context(), Options{
		DB: db, RuntimeDir: runtimeDir, Clock: clock, ReconcileEvery: time.Hour,
		beforeLinkPublish: func(want desiredLink) {
			beforePublish <- want
			<-releasePublish
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, managed.Close()) })
	registration := registryRegistration(t)
	insertRegistrySession(t, db, registration)

	reconciled := make(chan error, 1)
	go func() { reconciled <- managed.Reconcile(t.Context()) }()
	want := <-beforePublish
	require.Equal(t, registryAgentID, want.agent)
	require.Equal(t, registrySessionID, want.session)
	blockedErr := errors.New("delivery result persistence failed")
	managed.mu.Lock()
	managed.resultBlocks[registryAgentID] = registrySessionID
	managed.lastErr = blockedErr
	managed.mu.Unlock()
	_, exposed := managed.Client(registryAgentID)
	require.False(t, exposed, "the provisional client must not be published before the final check")
	close(releasePublish)
	require.NoError(t, <-reconciled)
	_, exposed = managed.Client(registryAgentID)
	require.False(t, exposed, "a stale desired snapshot must not publish the blocked session")
	require.ErrorIs(t, managed.LastError(), blockedErr)

	var state protocol.SessionState
	var endedAt sql.NullString
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT state, ended_at
		FROM sessions WHERE session_id = ?`, registrySessionID).Scan(&state, &endedAt))
	require.Equal(t, protocol.SessionStateConnected, state,
		"this interleaving models a durable disconnect that has not succeeded")
	require.False(t, endedAt.Valid)
	require.NoError(t, managed.Reconcile(t.Context()))
	_, exposed = managed.Client(registryAgentID)
	require.False(t, exposed, "the same Registry must remain closed after persistence failure")
	require.ErrorIs(t, managed.LastError(), blockedErr)
}

func TestRegistryDoesNotPublishLinkDetachedDuringReconcile(t *testing.T) {
	t.Parallel()
	db := registryDB(t)
	runtimeDir := registryRuntime(t)
	clock := newRegistryClock()
	beforePublish := make(chan desiredLink)
	releasePublish := make(chan struct{})
	registration := registryRegistration(t)
	const bootID = "boot-detach-before-publish"
	liveness := &registrySessionLiveness{
		process: *registration.Process, bootID: bootID, live: true,
	}
	managed, err := Start(t.Context(), Options{
		DB: db, RuntimeDir: runtimeDir, Clock: clock, ReconcileEvery: time.Hour,
		sessionLiveness: liveness,
		beforeLinkPublish: func(want desiredLink) {
			beforePublish <- want
			<-releasePublish
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, managed.Close()) })
	insertRegistrySession(t, db, registration)
	_, err = db.ExecContext(t.Context(), `UPDATE sessions SET boot_id = ?
		WHERE session_id = ?`, bootID, registrySessionID)
	require.NoError(t, err)
	listenRegistryProxy(t, runtimeDir, registration)

	reconciled := make(chan error, 1)
	go func() { reconciled <- managed.Reconcile(t.Context()) }()
	want := <-beforePublish
	require.Equal(t, desiredLink{session: registrySessionID, agent: registryAgentID}, want)
	_, err = db.ExecContext(t.Context(), `UPDATE sessions SET state = ?
		WHERE session_id = ?`, protocol.SessionStateDetached, registrySessionID)
	require.NoError(t, err)
	close(releasePublish)
	require.NoError(t, <-reconciled)
	_, exposed := managed.Client(registryAgentID)
	require.False(t, exposed, "a stale CONNECTED snapshot must not publish after DETACHED")
	var state protocol.SessionState
	var endedAt sql.NullString
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT state, ended_at
		FROM sessions WHERE session_id = ?`, registrySessionID).Scan(&state, &endedAt))
	require.Equal(t, protocol.SessionStateDetached, state)
	require.False(t, endedAt.Valid)
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
