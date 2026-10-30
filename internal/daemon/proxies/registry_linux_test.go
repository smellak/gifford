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

func registryRegistration(t *testing.T, agent protocol.AgentID) protocol.RegisterAgent {
	t.Helper()
	process, err := registry.CurrentProcess()
	require.NoError(t, err)
	return protocol.RegisterAgent{
		Manifest: protocol.CapabilityManifest{
			SchemaVersion: protocol.SchemaVersion, AgentID: agent,
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
	session protocol.OpaqueID,
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
		session, registration.Manifest.AgentID, string(manifest), string(process), stamp, stamp)
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

	registration := registryRegistration(t, "agent-1")
	insertRegistrySession(t, db, "session-1", registration)
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
	registration := registryRegistration(t, "agent-1")
	insertRegistrySession(t, db, "session-1", registration)
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

func TestRegistryRejectsDuplicateLiveSessionsForAgent(t *testing.T) {
	t.Parallel()
	db := registryDB(t)
	registration := registryRegistration(t, "agent-1")
	insertRegistrySession(t, db, "session-1", registration)
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
