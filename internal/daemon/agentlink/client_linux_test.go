//go:build linux

package agentlink

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/agentsock"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
)

type manualClock struct {
	mu     sync.Mutex
	now    time.Time
	timers map[time.Duration][]chan time.Time
}

func newManualClock() *manualClock {
	return &manualClock{now: testNow, timers: make(map[time.Duration][]chan time.Time)}
}

func (c *manualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *manualClock) After(duration time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	timer := make(chan time.Time, 1)
	c.timers[duration] = append(c.timers[duration], timer)
	return timer
}

func (c *manualClock) fire(t *testing.T, duration time.Duration) {
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
	}, 5*time.Second, time.Millisecond, "no %s timer was armed", duration)
	for _, timer := range timers {
		timer <- c.Now()
	}
}

type socketGuard struct {
	mu      sync.Mutex
	report  protocol.InputGuardReport
	changed chan struct{}
}

func newSocketGuard() *socketGuard {
	return &socketGuard{
		report: protocol.InputGuardReport{
			DraftState: protocol.DraftStateClosed, ActivityState: protocol.ActivityStateIdle,
			DraftGeneration: 1, InputGuardVersion: 1,
		},
		changed: make(chan struct{}, 1),
	}
}

func (g *socketGuard) InputGuard() protocol.InputGuardReport {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.report
}

func (g *socketGuard) GuardChanged() <-chan struct{} { return g.changed }

type recordingWriter struct {
	mu      sync.Mutex
	reports []protocol.InputGuardReport
	err     error
}

func (w *recordingWriter) WriteInputGuard(
	_ context.Context,
	report protocol.InputGuardReport,
) (bool, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return false, w.err
	}
	w.reports = append(w.reports, report)
	return true, nil
}

func (w *recordingWriter) count() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.reports)
}

type socketDelivery struct {
	commitStarted chan struct{}
	once          sync.Once
}

func newSocketDelivery() *socketDelivery {
	return &socketDelivery{commitStarted: make(chan struct{})}
}

func (*socketDelivery) CheckLine(string) error { return nil }

func (*socketDelivery) Prepare(
	_ protocol.OpaqueID,
	generation uint64,
	version int64,
) (agentsock.DeliveryReport, error) {
	return agentsock.DeliveryReport{
		Result: protocol.DeliveryResultKindPrepared, Generation: generation, Version: version,
	}, nil
}

func (d *socketDelivery) Commit(
	_ protocol.OpaqueID,
	_ string,
	_ protocol.InjectMode,
) (agentsock.DeliveryReport, error) {
	d.once.Do(func() { close(d.commitStarted) })
	return agentsock.DeliveryReport{
		Result: protocol.DeliveryResultKindCommitted, Generation: 1, Version: 1,
	}, nil
}

func (*socketDelivery) Cancel(
	_ protocol.OpaqueID,
	_ protocol.CancelReason,
) (agentsock.DeliveryReport, error) {
	return agentsock.DeliveryReport{Result: protocol.DeliveryResultKindCancelled}, nil
}

func (*socketDelivery) DefaultMode() protocol.InjectMode { return protocol.InjectModeTyped }
func (*socketDelivery) InputHolds(string) bool           { return false }

func shortRuntime(t *testing.T) string {
	t.Helper()
	runtimeDir, err := os.MkdirTemp("", "gl") //nolint:usetesting // Unix socket paths must stay short.
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(runtimeDir)) })
	return runtimeDir
}

func listenProxy(
	t *testing.T,
	runtimeDir string,
	registration protocol.RegisterAgent,
	guard *socketGuard,
	delivery agentsock.Delivery,
	clk agentsock.Clock,
) *agentsock.Server {
	t.Helper()
	server, err := agentsock.Listen(t.Context(), agentsock.Options{
		RuntimeDir: runtimeDir, Register: registration, Guard: guard,
		Delivery: delivery, Clock: clk,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, server.Close()) })
	return server
}

func currentRegistration(t *testing.T, agent protocol.AgentID) protocol.RegisterAgent {
	t.Helper()
	process, err := registry.CurrentProcess()
	require.NoError(t, err)
	return testRegistration(agent, process)
}

func startTestClient(
	t *testing.T,
	db *sql.DB,
	runtimeDir string,
	session protocol.OpaqueID,
	writer GuardWriter,
	clk Clock,
	patch func(*Options),
) *Client {
	t.Helper()
	opts := Options{
		RuntimeDir: runtimeDir, DB: db, AgentID: testAgentID, SessionID: session,
		Writer: writer, Clock: clk,
	}
	if patch != nil {
		patch(&opts)
	}
	client, err := Start(t.Context(), opts)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	return client
}

func waitForState(t *testing.T, client *Client, state State) Snapshot {
	t.Helper()
	var snapshot Snapshot
	require.Eventually(t, func() bool {
		snapshot = client.Snapshot()
		return snapshot.State == state
	}, 5*time.Second, time.Millisecond)
	return snapshot
}

func waitForReason(t *testing.T, client *Client, reason Reason) {
	t.Helper()
	require.Eventually(t, func() bool {
		snapshot := client.Snapshot()
		return snapshot.State == StateDown && snapshot.Reason == reason
	}, 5*time.Second, time.Millisecond)
}

func TestClientGreetsRealProxyAndPersistsReports(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	registration := currentRegistration(t, testAgentID)
	insertTestSession(t, db, testSessionID, registration)
	runtimeDir := shortRuntime(t)
	listenProxy(t, runtimeDir, registration, newSocketGuard(), nil, fixedClock{now: testNow})
	store, err := NewInputGuardStore(db, fixedClock{now: testNow})
	require.NoError(t, err)
	client := startTestClient(t, db, runtimeDir, testSessionID, store,
		fixedClock{now: testNow}, nil)
	snapshot := waitForState(t, client, StateUp)
	require.Equal(t, testSessionID, snapshot.SessionID)
	require.Eventually(t, func() bool {
		var version int64
		err := db.QueryRowContext(t.Context(), `SELECT version FROM input_guards WHERE session_id = ?`,
			testSessionID).Scan(&version)
		return err == nil && version == 1
	}, 5*time.Second, time.Millisecond)
	snapshot = client.Snapshot()
	require.True(t, snapshot.HasReport)
	require.Equal(t, testSessionID, snapshot.LastReport.SessionID)
}

func TestClientRejectsCrossedGreetingWithoutAnswer(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	expected := currentRegistration(t, testAgentID)
	insertTestSession(t, db, testSessionID, expected)
	runtimeDir := shortRuntime(t)
	crossed := currentRegistration(t, "agent-2")
	server := listenProxy(t, runtimeDir, crossed, newSocketGuard(), nil, fixedClock{now: testNow})
	target := filepath.Join(runtimeDir, agentsDirectory, "agent-1.sock")
	require.NoError(t, os.Rename(server.Path(), target))
	client := startTestClient(t, db, runtimeDir, testSessionID, &recordingWriter{},
		fixedClock{now: testNow}, nil)
	waitForReason(t, client, ReasonInvalidGreeting)
}

func TestClientRejectsMissingHubSessionWithoutAnswer(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	registration := currentRegistration(t, testAgentID)
	insertTestSession(t, db, "session-other", registration)
	runtimeDir := shortRuntime(t)
	listenProxy(t, runtimeDir, registration, newSocketGuard(), nil, fixedClock{now: testNow})
	client := startTestClient(t, db, runtimeDir, "session-missing", &recordingWriter{},
		fixedClock{now: testNow}, nil)
	waitForReason(t, client, ReasonSessionUnavailable)
}

func TestClientRejectsPeerOutsideSessionProcess(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	registration := currentRegistration(t, testAgentID)
	claimed := registration
	claimed.Process = &protocol.ProcessIdentity{
		PID: 999999, ProcessStartTime: 1, UID: registration.Process.UID, GID: registration.Process.GID,
	}
	insertTestSession(t, db, testSessionID, claimed)
	runtimeDir := shortRuntime(t)
	listenProxy(t, runtimeDir, registration, newSocketGuard(), nil, fixedClock{now: testNow})
	client := startTestClient(t, db, runtimeDir, testSessionID, &recordingWriter{},
		fixedClock{now: testNow}, nil)
	waitForReason(t, client, ReasonPeerMismatch)
}

func TestClientRejectsInsecureSocketAndDirectory(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		patch func(*testing.T, string)
	}{
		{name: "socket mode", patch: func(t *testing.T, path string) { require.NoError(t, os.Chmod(path, 0o660)) }},
		{name: "directory mode", patch: func(t *testing.T, path string) {
			require.NoError(t, os.Chmod(filepath.Dir(path), 0o755))
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			db := openTestDB(t)
			registration := currentRegistration(t, testAgentID)
			insertTestSession(t, db, testSessionID, registration)
			runtimeDir := shortRuntime(t)
			server := listenProxy(t, runtimeDir, registration, newSocketGuard(), nil, fixedClock{now: testNow})
			test.patch(t, server.Path())
			client := startTestClient(t, db, runtimeDir, testSessionID, &recordingWriter{},
				fixedClock{now: testNow}, nil)
			waitForReason(t, client, ReasonInsecureSocket)
		})
	}
}

func TestHeartbeatLossMarksLinkDown(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	registration := currentRegistration(t, testAgentID)
	insertTestSession(t, db, testSessionID, registration)
	runtimeDir := shortRuntime(t)
	listenProxy(t, runtimeDir, registration, newSocketGuard(), nil, fixedClock{now: testNow})
	linkClock := newManualClock()
	const heartbeatTimeout = 40 * time.Millisecond
	client := startTestClient(t, db, runtimeDir, testSessionID, &recordingWriter{}, linkClock,
		func(opts *Options) {
			opts.HeartbeatTimeout = heartbeatTimeout
			opts.ReconnectMin = 60 * time.Millisecond
		})
	waitForState(t, client, StateUp)
	linkClock.fire(t, heartbeatTimeout)
	waitForReason(t, client, ReasonHeartbeatTimeout)
}

func TestResyncWaitsForItsReportWithoutBlockingTheWriter(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	registration := currentRegistration(t, testAgentID)
	insertTestSession(t, db, testSessionID, registration)
	runtimeDir := shortRuntime(t)
	listenProxy(t, runtimeDir, registration, newSocketGuard(), nil, fixedClock{now: testNow})
	writer := &recordingWriter{}
	client := startTestClient(t, db, runtimeDir, testSessionID, writer,
		fixedClock{now: testNow}, nil)
	waitForState(t, client, StateUp)
	require.Eventually(t, func() bool { return writer.count() == 1 }, 5*time.Second, time.Millisecond)
	report, err := client.Resync(t.Context())
	require.NoError(t, err)
	require.Equal(t, testSessionID, report.SessionID)
	require.Eventually(t, func() bool { return writer.count() == 2 }, 5*time.Second, time.Millisecond)
}

func TestLateDeliveryResultRemainsOnResultChannel(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	registration := currentRegistration(t, testAgentID)
	insertTestSession(t, db, testSessionID, registration)
	runtimeDir := shortRuntime(t)
	proxyClock := newManualClock()
	delivery := newSocketDelivery()
	listenProxy(t, runtimeDir, registration, newSocketGuard(), delivery, proxyClock)
	client := startTestClient(t, db, runtimeDir, testSessionID, &recordingWriter{},
		fixedClock{now: testNow}, nil)
	waitForState(t, client, StateUp)
	prepare := testPrepare("delivery-1")
	frame, err := client.Call(t.Context(), protocol.AgentOpPrepareDelivery, prepare)
	require.NoError(t, err)
	require.Equal(t, protocol.AgentOpReportDeliveryResult, frame.Op)
	prepared := receiveResult(t, client)
	require.Equal(t, protocol.DeliveryResultKindPrepared, prepared.Result.Result)

	callCtx, cancel := context.WithCancel(t.Context())
	callDone := make(chan error, 1)
	go func() {
		_, callErr := client.Call(callCtx, protocol.AgentOpCommitDelivery, protocol.CommitDelivery{
			DeliveryID: "delivery-1", WriterLeaseID: "lease-1",
		})
		callDone <- callErr
	}()
	select {
	case <-delivery.commitStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("commit did not start")
	}
	cancel()
	require.ErrorIs(t, <-callDone, context.Canceled)
	proxyClock.fire(t, agentsock.ConsumeWindow)
	late := receiveResult(t, client)
	require.Equal(t, protocol.DeliveryResultKindCommitted, late.Result.Result)
	require.Equal(t, protocol.OpaqueID("lease-1"), late.Result.WriterLeaseID)
}

func TestReconnectReappliesPauseToProxy(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	registration := currentRegistration(t, testAgentID)
	insertTestSession(t, db, testSessionID, registration)
	runtimeDir := shortRuntime(t)
	first := listenProxy(t, runtimeDir, registration, newSocketGuard(), newSocketDelivery(),
		fixedClock{now: testNow})
	linkClock := newManualClock()
	const reconnectMin = 30 * time.Millisecond
	client := startTestClient(t, db, runtimeDir, testSessionID, &recordingWriter{}, linkClock,
		func(opts *Options) { opts.ReconnectMin = reconnectMin })
	waitForState(t, client, StateUp)
	require.NoError(t, client.Pause(t.Context()))
	frame, err := client.Call(t.Context(), protocol.AgentOpPrepareDelivery, testPrepare("delivery-1"))
	require.NoError(t, err)
	require.Equal(t, protocol.DeliveryResultKindCancelled, deliveryResultOf(t, frame).Result)
	_ = receiveResult(t, client)
	require.NoError(t, first.Close())
	waitForState(t, client, StateDown)

	listenProxy(t, runtimeDir, registration, newSocketGuard(), newSocketDelivery(),
		fixedClock{now: testNow})
	linkClock.fire(t, reconnectMin)
	waitForState(t, client, StateUp)
	frame, err = client.Call(t.Context(), protocol.AgentOpPrepareDelivery, testPrepare("delivery-2"))
	require.NoError(t, err)
	require.Equal(t, protocol.DeliveryResultKindCancelled, deliveryResultOf(t, frame).Result)
	require.NoError(t, client.Resume(t.Context()))
	frame, err = client.Call(t.Context(), protocol.AgentOpPrepareDelivery, testPrepare("delivery-3"))
	require.NoError(t, err)
	require.Equal(t, protocol.DeliveryResultKindPrepared, deliveryResultOf(t, frame).Result)
}

func testPrepare(id protocol.OpaqueID) protocol.PrepareDelivery {
	return protocol.PrepareDelivery{
		DeliveryID: id, AgentID: testAgentID,
		MessageRef: protocol.MessageRef{
			MessageID: "01JB0000000000000000000001", FromAgent: "agent-2",
			Priority: protocol.PriorityNormal, ThreadID: "thread-1",
		},
		NoticeLine:                "[GIFFORD] pending message",
		OrchestratorDecisionID:    "01JB0000000000000000000002",
		ExpectedStateVersion:      1,
		ExpectedInputGuardVersion: 1,
		ExpectedDraftGeneration:   1,
		ControlVersions:           protocol.ControlVersions{"global": 1},
	}
}

func deliveryResultOf(t *testing.T, frame protocol.AgentFrame) protocol.DeliveryResult {
	t.Helper()
	var result protocol.DeliveryResult
	require.NoError(t, json.Unmarshal(frame.Body, &result))
	return result
}

func receiveResult(t *testing.T, client *Client) DeliveryResult {
	t.Helper()
	select {
	case result := <-client.DeliveryResults():
		return result
	case <-time.After(5 * time.Second):
		t.Fatal("delivery result did not arrive")
		return DeliveryResult{}
	}
}
