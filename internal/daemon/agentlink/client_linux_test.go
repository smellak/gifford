//go:build linux

package agentlink

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
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

func (g *socketGuard) set(state protocol.DraftState, generation, version int64) {
	g.mu.Lock()
	g.report.DraftState = state
	g.report.DraftGeneration = generation
	g.report.InputGuardVersion = version
	g.mu.Unlock()
	select {
	case g.changed <- struct{}{}:
	default:
	}
}

type recordingWriter struct {
	mu      sync.Mutex
	reports []protocol.InputGuardReport
	err     error
}

type retryCapabilityWriter struct {
	mu    sync.Mutex
	calls int
	event protocol.EventEnvelope
	err   error
}

func (w *retryCapabilityWriter) ApplyCapabilitiesChanged(
	_ context.Context,
	event protocol.EventEnvelope,
) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.calls++
	if w.calls == 1 {
		return w.err
	}
	w.event = event
	return nil
}

func (w *retryCapabilityWriter) count() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.calls
}

type unusedCapabilityLineage struct{}

func (unusedCapabilityLineage) MatchesCapabilitiesChanged(
	context.Context,
	registry.Querier,
	protocol.OpaqueID,
	protocol.CapabilityManifest,
	protocol.CapabilityManifest,
) (bool, error) {
	return false, nil
}

type retainedSocketEvent struct {
	event   protocol.EventEnvelope
	changed chan struct{}
}

func (e *retainedSocketEvent) CurrentEvent() (protocol.EventEnvelope, bool) {
	event := e.event
	event.Payload = append(protocol.Payload(nil), event.Payload...)
	return event, true
}

func (e *retainedSocketEvent) EventChanged() <-chan struct{} { return e.changed }

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

type stalledProxy struct {
	listener *net.UnixListener
	mu       sync.Mutex
	conn     *net.UnixConn
	ready    chan struct{}
	err      chan error
	stop     chan struct{}
	done     chan struct{}
	once     sync.Once
}

func listenStalledProxy(
	t *testing.T,
	runtimeDir string,
	registration protocol.RegisterAgent,
) *stalledProxy {
	t.Helper()
	directory := filepath.Join(runtimeDir, agentsDirectory)
	require.NoError(t, os.MkdirAll(directory, 0o700))
	require.NoError(t, os.Chmod(directory, 0o700))
	path := agentsock.SocketPath(runtimeDir, registration.Manifest.AgentID)
	address, err := net.ResolveUnixAddr("unix", path)
	require.NoError(t, err)
	listener, err := net.ListenUnix("unix", address)
	require.NoError(t, err)
	require.NoError(t, os.Chmod(path, 0o600))
	proxy := &stalledProxy{
		listener: listener, ready: make(chan struct{}), err: make(chan error, 1),
		stop: make(chan struct{}), done: make(chan struct{}),
	}
	go proxy.run(registration)
	t.Cleanup(proxy.close)
	return proxy
}

func (p *stalledProxy) run(registration protocol.RegisterAgent) {
	defer close(p.done)
	conn, err := p.listener.AcceptUnix()
	if err != nil {
		p.fail(fmt.Errorf("accept stalled proxy: %w", err))
		return
	}
	p.mu.Lock()
	p.conn = conn
	p.mu.Unlock()
	raw, err := json.Marshal(registration)
	if err != nil {
		p.fail(fmt.Errorf("encode stalled greeting: %w", err))
		return
	}
	if err := agentsock.WriteFrame(conn, protocol.AgentFrame{
		Op: protocol.AgentOpRegisterAgent, FrameID: 1, SentAt: testNow, Body: raw,
	}); err != nil {
		p.fail(fmt.Errorf("write stalled greeting: %w", err))
		return
	}
	response, err := agentsock.ReadFrame(conn)
	if err != nil {
		p.fail(fmt.Errorf("read stalled greeting response: %w", err))
		return
	}
	if response.Op != protocol.AgentOpRegisterAgentResult || response.ReplyTo == nil ||
		*response.ReplyTo != 1 {
		p.fail(fmt.Errorf("unexpected stalled greeting response: %s", response.Op))
		return
	}
	if err := conn.SetReadBuffer(1 << 10); err != nil {
		p.fail(fmt.Errorf("shrink stalled receive buffer: %w", err))
		return
	}
	close(p.ready)
	<-p.stop
}

func (p *stalledProxy) fail(err error) {
	select {
	case p.err <- err:
	default:
	}
}

func (p *stalledProxy) wait(t *testing.T) {
	t.Helper()
	select {
	case <-p.ready:
	case err := <-p.err:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("stalled proxy did not complete its greeting")
	}
}

func (p *stalledProxy) close() {
	p.once.Do(func() {
		close(p.stop)
		_ = p.listener.Close()
		p.mu.Lock()
		conn := p.conn
		p.mu.Unlock()
		if conn != nil {
			_ = conn.Close()
		}
		<-p.done
	})
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

func TestCallStopsWaitingForTheWriterWhenContextEnds(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	registration := currentRegistration(t, testAgentID)
	insertTestSession(t, db, testSessionID, registration)
	runtimeDir := shortRuntime(t)
	listenProxy(t, runtimeDir, registration, newSocketGuard(), nil, fixedClock{now: testNow})
	client := startTestClient(t, db, runtimeDir, testSessionID, &recordingWriter{},
		fixedClock{now: testNow}, nil)
	waitForState(t, client, StateUp)

	client.sendGate <- struct{}{}
	defer client.releaseSend()
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err := client.Call(ctx, protocol.AgentOpResync, protocol.AgentTarget{AgentID: testAgentID})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, time.Since(started), 250*time.Millisecond)
}

func TestSendCancellationClosesAStalledConnection(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	registration := currentRegistration(t, testAgentID)
	insertTestSession(t, db, testSessionID, registration)
	runtimeDir := shortRuntime(t)
	proxy := listenStalledProxy(t, runtimeDir, registration)
	client := startTestClient(t, db, runtimeDir, testSessionID, &recordingWriter{},
		fixedClock{now: testNow}, nil)
	waitForState(t, client, StateUp)
	proxy.wait(t)
	client.mu.RLock()
	conn := client.conn
	client.mu.RUnlock()
	require.NotNil(t, conn)
	require.NoError(t, conn.SetWriteBuffer(1<<10))

	prepare := testPrepare("delivery-stalled")
	prepare.ControlVersions = make(protocol.ControlVersions, 64)
	for index := range 64 {
		prefix := fmt.Sprintf("scope-%02d-", index)
		prepare.ControlVersions[prefix+strings.Repeat("x", 200-len(prefix))] = int64(index + 1)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err := client.Send(ctx, protocol.AgentOpPrepareDelivery, prepare)
	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, time.Since(started), 250*time.Millisecond)
	waitForReason(t, client, ReasonConnectionLost)
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

func TestSnapshotIgnoresOlderReportsWithoutLosingResyncCorrelation(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	registration := currentRegistration(t, testAgentID)
	insertTestSession(t, db, testSessionID, registration)
	runtimeDir := shortRuntime(t)
	guard := newSocketGuard()
	listenProxy(t, runtimeDir, registration, guard, nil, fixedClock{now: testNow})
	writer := &recordingWriter{}
	client := startTestClient(t, db, runtimeDir, testSessionID, writer,
		fixedClock{now: testNow}, nil)
	waitForState(t, client, StateUp)
	require.Eventually(t, func() bool { return writer.count() == 1 }, 5*time.Second, time.Millisecond)

	guard.set(protocol.DraftStateOpen, 2, 2)
	require.Eventually(t, func() bool {
		snapshot := client.Snapshot()
		return writer.count() == 2 && snapshot.HasReport &&
			snapshot.LastReport.InputGuardVersion == 2
	}, 5*time.Second, time.Millisecond)
	guard.set(protocol.DraftStateClosed, 1, 1)
	require.Eventually(t, func() bool { return writer.count() == 3 }, 5*time.Second, time.Millisecond)
	snapshot := client.Snapshot()
	require.True(t, snapshot.HasReport)
	require.Equal(t, int64(2), snapshot.LastReport.InputGuardVersion)
	require.Equal(t, protocol.DraftStateOpen, snapshot.LastReport.DraftState)

	report, err := client.Resync(t.Context())
	require.NoError(t, err)
	require.Equal(t, int64(1), report.InputGuardVersion)
	require.Equal(t, protocol.DraftStateClosed, report.DraftState)
	require.Eventually(t, func() bool { return writer.count() == 4 }, 5*time.Second, time.Millisecond)
	snapshot = client.Snapshot()
	require.Equal(t, int64(2), snapshot.LastReport.InputGuardVersion)
	require.Equal(t, protocol.DraftStateOpen, snapshot.LastReport.DraftState)
}

func TestReconnectKeepsReportHighWaterStaleUntilConfirmed(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	registration := currentRegistration(t, testAgentID)
	insertTestSession(t, db, testSessionID, registration)
	runtimeDir := shortRuntime(t)
	firstGuard := newSocketGuard()
	firstGuard.set(protocol.DraftStateOpen, 2, 2)
	first := listenProxy(t, runtimeDir, registration, firstGuard, nil, fixedClock{now: testNow})
	linkClock := newManualClock()
	const minReconnectDelay = 35 * time.Millisecond
	writer := &recordingWriter{}
	client := startTestClient(t, db, runtimeDir, testSessionID, writer, linkClock,
		func(opts *Options) { opts.ReconnectMin = minReconnectDelay })
	require.Eventually(t, func() bool {
		snapshot := client.Snapshot()
		return snapshot.State == StateUp && snapshot.HasReport &&
			snapshot.LastReport.InputGuardVersion == 2
	}, 5*time.Second, time.Millisecond)
	require.NoError(t, first.Close())
	snapshot := waitForState(t, client, StateDown)
	require.False(t, snapshot.HasReport)
	require.Equal(t, int64(2), snapshot.LastReport.InputGuardVersion)

	secondGuard := newSocketGuard()
	listenProxy(t, runtimeDir, registration, secondGuard, nil, fixedClock{now: testNow})
	linkClock.fire(t, minReconnectDelay)
	require.Eventually(t, func() bool {
		snapshot = client.Snapshot()
		return snapshot.State == StateUp && writer.count() >= 2
	}, 5*time.Second, time.Millisecond)
	require.False(t, snapshot.HasReport)
	require.Equal(t, int64(2), snapshot.LastReport.InputGuardVersion)
	require.Equal(t, protocol.DraftStateOpen, snapshot.LastReport.DraftState)

	secondGuard.set(protocol.DraftStateClosed, 3, 3)
	require.Eventually(t, func() bool {
		snapshot = client.Snapshot()
		return snapshot.HasReport && snapshot.LastReport.InputGuardVersion == 3
	}, 5*time.Second, time.Millisecond)
	require.Equal(t, protocol.DraftStateClosed, snapshot.LastReport.DraftState)
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
	// The Proxy can start the commit while the Call is still writing it, and a
	// cancel then closes the connection under the write. The Call holds the
	// writer until the write returns: once the test takes it, only the wait
	// for the answer is left to cancel.
	require.NoError(t, client.acquireSend(t.Context()))
	client.releaseSend()
	cancel()
	require.ErrorIs(t, <-callDone, context.Canceled)
	proxyClock.fire(t, agentsock.ConsumeWindow)
	late := receiveResult(t, client)
	require.Equal(t, protocol.DeliveryResultKindCommitted, late.Result.Result)
	require.Equal(t, protocol.OpaqueID("lease-1"), late.Result.WriterLeaseID)
}

func TestTransientCapabilityWriteReconnectsAndReplays(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	registration := currentRegistration(t, testAgentID)
	registration.Manifest = testNativeCapabilityManifest()
	insertTestSession(t, db, testSessionID, registration)
	runtimeDir := shortRuntime(t)
	event := testCapabilityEnvelope(t, registration.Manifest)
	server, err := agentsock.Listen(t.Context(), agentsock.Options{
		RuntimeDir: runtimeDir, SessionID: testSessionID, Register: registration,
		Guard: newSocketGuard(), Events: &retainedSocketEvent{
			event: event, changed: make(chan struct{}, 1),
		},
		Clock: agentsock.SystemClock{},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, server.Close()) })
	capabilityWriter := &retryCapabilityWriter{err: errors.New("temporary database failure")}
	client := startTestClient(t, db, runtimeDir, testSessionID, &recordingWriter{},
		SystemClock{}, func(opts *Options) {
			opts.NodeID = testNodeID
			opts.CapabilityWriter = capabilityWriter
			opts.CapabilityLineage = unusedCapabilityLineage{}
			opts.ReconnectMin = 10 * time.Millisecond
			opts.ReconnectMax = 10 * time.Millisecond
		})
	require.Eventually(t, func() bool {
		snapshot := client.Snapshot()
		return capabilityWriter.count() == 2 && snapshot.State == StateUp && snapshot.HasReport
	}, 5*time.Second, time.Millisecond)
	require.Equal(t, event, capabilityWriter.event)
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
	const minReconnectDelay = 30 * time.Millisecond
	client := startTestClient(t, db, runtimeDir, testSessionID, &recordingWriter{}, linkClock,
		func(opts *Options) { opts.ReconnectMin = minReconnectDelay })
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
	linkClock.fire(t, minReconnectDelay)
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
