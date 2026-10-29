//go:build unix

package agentsock

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

// fakeClock is a clock the test fires by hand.
type fakeClock struct {
	mu     sync.Mutex
	timers []fakeTimer
}

type fakeTimer struct {
	d  time.Duration
	ch chan time.Time
}

func (*fakeClock) Now() time.Time { return sentAt }

func (c *fakeClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan time.Time, 1)
	c.timers = append(c.timers, fakeTimer{d: d, ch: ch})
	return ch
}

// fire fires every pending timer of d, once a first one is armed.
func (c *fakeClock) fire(t *testing.T, d time.Duration) {
	t.Helper()
	require.Eventually(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		fired := false
		kept := c.timers[:0]
		for _, tm := range c.timers {
			if tm.d == d {
				tm.ch <- sentAt
				fired = true
				continue
			}
			kept = append(kept, tm)
		}
		c.timers = kept
		return fired
	}, 10*time.Second, time.Millisecond, "no %s timer armed", d)
}

// fakeGuard is a guard reading the test sets.
type fakeGuard struct {
	mu      sync.Mutex
	r       protocol.InputGuardReport
	changed chan struct{}
}

func newFakeGuard() *fakeGuard {
	return &fakeGuard{
		r: protocol.InputGuardReport{
			DraftState: protocol.DraftStateClosed, ActivityState: protocol.ActivityStateIdle,
		},
		changed: make(chan struct{}, 1),
	}
}

func (g *fakeGuard) InputGuard() protocol.InputGuardReport {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.r
}

func (g *fakeGuard) GuardChanged() <-chan struct{} { return g.changed }

// set moves the guard and signals it, never waiting, as the session does.
func (g *fakeGuard) set(state protocol.DraftState, generation, version int64) {
	g.mu.Lock()
	g.r.DraftState, g.r.DraftGeneration, g.r.InputGuardVersion = state, generation, version
	g.mu.Unlock()
	select {
	case g.changed <- struct{}{}:
	default:
	}
}

// testRegister is the greeting of the test socket.
func testRegister() protocol.RegisterAgent {
	return protocol.RegisterAgent{
		Manifest: protocol.CapabilityManifest{
			SchemaVersion: protocol.SchemaVersion, AgentID: testAgent,
			Harness: protocol.HarnessCodex, HarnessVersion: "1.2.3",
			AdapterType:        protocol.AdapterTypeGenericPTY,
			InputGuardProvider: protocol.InputGuardProviderPTYProxy,
			CaptureMode:        protocol.CaptureModePTYProxy, Deliverable: true,
			Capabilities: protocol.Capabilities{MCP: true, PTYCapture: true},
		},
		Process: &protocol.ProcessIdentity{PID: 4242, UID: 1000, GID: 1000},
	}
}

// testSocket is one listening socket with its fakes.
type testSocket struct {
	srv     *Server
	runtime string
	clock   *fakeClock
	guard   *fakeGuard
}

// shortDir is a temporary directory with a path short enough for a
// socket: t.TempDir embeds the test name.
func shortDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "as") //nolint:usetesting // a socket path must stay short, and t.TempDir embeds the test name
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

func listenTest(t *testing.T, edit func(*Options)) *testSocket {
	t.Helper()
	ts := &testSocket{runtime: shortDir(t), clock: &fakeClock{}, guard: newFakeGuard()}
	opts := Options{RuntimeDir: ts.runtime, Register: testRegister(), Guard: ts.guard, Clock: ts.clock}
	if edit != nil {
		edit(&opts)
	}
	srv, err := Listen(context.Background(), opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = srv.Close() })
	ts.srv = srv
	return ts
}

// dialGreeted dials until the socket greets, as a Core retries while
// another connection, such as a probe, holds the socket, and completes
// the greeting.
func dialGreeted(t *testing.T, path string) {
	t.Helper()
	var k *core
	require.Eventually(t, func() bool {
		c, err := (&net.Dialer{}).DialContext(context.Background(), "unix", path)
		if err != nil {
			return false
		}
		if c.SetReadDeadline(time.Now().Add(time.Second)) != nil {
			_ = c.Close()
			return false
		}
		f, err := ReadFrame(c)
		if err != nil || f.Op != protocol.AgentOpRegisterAgent {
			_ = c.Close()
			return false
		}
		t.Cleanup(func() { _ = c.Close() })
		k = &core{t: t, c: c, next: 1}
		return true
	}, 10*time.Second, 10*time.Millisecond, "the socket never greeted")
	k.send(protocol.AgentOpRegisterAgentResult, greetingFrameID, protocol.RegisterAgentResult{AgentID: testAgent, SessionID: testSession})
	reportOf(t, k.read(), 0)
}

// core is a fake Core on one connection.
type core struct {
	t *testing.T
	c net.Conn
	// next is the next frame id the Core sends.
	next int64
}

func dialCore(t *testing.T, path string) *core {
	t.Helper()
	c, err := (&net.Dialer{}).DialContext(context.Background(), "unix", path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	return &core{t: t, c: c, next: 1}
}

// read reads one frame from the session.
func (k *core) read() protocol.AgentFrame {
	k.t.Helper()
	require.NoError(k.t, k.c.SetReadDeadline(time.Now().Add(10*time.Second)))
	f, err := ReadFrame(k.c)
	require.NoError(k.t, err)
	return f
}

// closed waits until the session closes the connection.
func (k *core) closed() {
	k.t.Helper()
	require.NoError(k.t, k.c.SetReadDeadline(time.Now().Add(10*time.Second)))
	for {
		_, err := ReadFrame(k.c)
		if err == nil {
			continue
		}
		var ne net.Error
		require.False(k.t, errors.As(err, &ne) && ne.Timeout(), "the connection never closed")
		return
	}
}

// send sends one frame of op with body and returns its frame id.
func (k *core) send(op protocol.AgentOp, replyTo int64, body any) int64 {
	k.t.Helper()
	id := k.next
	k.next++
	require.NoError(k.t, WriteFrame(k.c, testFrame(k.t, op, id, replyTo, body)))
	return id
}

// sendRaw sends a length prefix and json as they are.
func (k *core) sendRaw(json string) {
	k.t.Helper()
	_, err := k.c.Write(rawFrame(json))
	require.NoError(k.t, err)
}

// greet reads the greeting and answers it, then reads the first report.
func (k *core) greet() protocol.InputGuardReport {
	k.t.Helper()
	hello := k.read()
	require.Equal(k.t, protocol.AgentOpRegisterAgent, hello.Op)
	require.Equal(k.t, int64(greetingFrameID), hello.FrameID)
	k.send(protocol.AgentOpRegisterAgentResult, hello.FrameID, protocol.RegisterAgentResult{AgentID: testAgent, SessionID: testSession})
	return reportOf(k.t, k.read(), 0)
}

// reportOf decodes a report frame answering replyTo, or none.
func reportOf(t *testing.T, f protocol.AgentFrame, replyTo int64) protocol.InputGuardReport {
	t.Helper()
	require.Equal(t, protocol.AgentOpReportInputGuard, f.Op)
	requireReply(t, f, replyTo)
	var r protocol.InputGuardReport
	require.NoError(t, json.Unmarshal(f.Body, &r))
	require.Equal(t, testAgent, r.AgentID)
	require.Equal(t, testSession, r.SessionID)
	require.Equal(t, protocol.InputGuardProviderPTYProxy, r.DetectionSource)
	require.InDelta(t, 1.0, r.Confidence, 0)
	return r
}

// errorOf decodes a frame error answering replyTo, or none.
func errorOf(t *testing.T, f protocol.AgentFrame, replyTo int64) protocol.FrameErrorCode {
	t.Helper()
	require.Equal(t, protocol.AgentOpError, f.Op)
	requireReply(t, f, replyTo)
	var e protocol.FrameError
	require.NoError(t, json.Unmarshal(f.Body, &e))
	return e.Code
}

func requireReply(t *testing.T, f protocol.AgentFrame, replyTo int64) {
	t.Helper()
	if replyTo == 0 {
		require.Nil(t, f.ReplyTo, "%s answers nothing", f.Op)
		return
	}
	require.NotNil(t, f.ReplyTo, "%s answers %d", f.Op, replyTo)
	require.Equal(t, replyTo, *f.ReplyTo)
}

func TestGreetingGoesFirst(t *testing.T) {
	t.Parallel()
	ts := listenTest(t, nil)
	k := dialCore(t, ts.srv.Path())

	hello := k.read()
	require.Equal(t, protocol.AgentOpRegisterAgent, hello.Op)
	require.Equal(t, int64(1), hello.FrameID)
	require.Nil(t, hello.ReplyTo)
	var reg protocol.RegisterAgent
	require.NoError(t, json.Unmarshal(hello.Body, &reg))
	require.Equal(t, testRegister(), reg, "the manifest and process of the hub registration")

	k.send(protocol.AgentOpRegisterAgentResult, 1, protocol.RegisterAgentResult{AgentID: testAgent, SessionID: testSession})
	r := reportOf(t, k.read(), 0)
	require.Equal(t, protocol.DraftStateClosed, r.DraftState)
}

func TestGreetingFailsClosed(t *testing.T) {
	t.Parallel()
	t.Run("another agent", func(t *testing.T) {
		t.Parallel()
		ts := listenTest(t, nil)
		k := dialCore(t, ts.srv.Path())
		k.read()
		k.send(protocol.AgentOpRegisterAgentResult, 1, protocol.RegisterAgentResult{AgentID: otherAgent, SessionID: testSession})
		k.closed()
	})
	t.Run("no result in 5 s", func(t *testing.T) {
		t.Parallel()
		ts := listenTest(t, nil)
		k := dialCore(t, ts.srv.Path())
		k.read()
		ts.clock.fire(t, GreetingTimeout)
		k.closed()
	})
	t.Run("a result to another frame", func(t *testing.T) {
		t.Parallel()
		ts := listenTest(t, nil)
		k := dialCore(t, ts.srv.Path())
		k.read()
		id := k.send(protocol.AgentOpRegisterAgentResult, 7, protocol.RegisterAgentResult{AgentID: testAgent, SessionID: testSession})
		require.Equal(t, protocol.FrameErrorCodeInvalidFrame, errorOf(t, k.read(), id))
		ts.clock.fire(t, GreetingTimeout)
		k.closed()
	})
}

func TestOrdersBeforeTheGreeting(t *testing.T) {
	t.Parallel()
	ts := listenTest(t, nil)
	k := dialCore(t, ts.srv.Path())
	k.read()

	id := k.send(protocol.AgentOpResync, 0, protocol.AgentTarget{AgentID: testAgent})
	require.Equal(t, protocol.FrameErrorCodeNotRegistered, errorOf(t, k.read(), id))
	id = k.send(protocol.AgentOpPauseAutomation, 0, protocol.AgentTarget{AgentID: testAgent})
	require.Equal(t, protocol.FrameErrorCodeNotRegistered, errorOf(t, k.read(), id))

	// Nothing ran: the greeting still completes, with the state as it was.
	k.send(protocol.AgentOpRegisterAgentResult, 1, protocol.RegisterAgentResult{AgentID: testAgent, SessionID: testSession})
	require.Equal(t, protocol.DraftStateClosed, reportOf(t, k.read(), 0).DraftState)
}

func TestFrameErrors(t *testing.T) {
	t.Parallel()
	ts := listenTest(t, nil)
	k := dialCore(t, ts.srv.Path())
	k.read()

	k.sendRaw(`{"op":`)
	require.Equal(t, protocol.FrameErrorCodeInvalidFrame, errorOf(t, k.read(), 0), "no id to answer")
	k.sendRaw(`{"op":"resync","frame_id":40,"sent_at":"2026-10-29T12:00:00Z","body":{"agent_id":"agent-1","x":1}}`)
	require.Equal(t, protocol.FrameErrorCodeInvalidFrame, errorOf(t, k.read(), 40))

	// A heartbeat is the Proxy's, never the Core's: before and after
	// the greeting.
	id := k.send(protocol.AgentOpHeartbeat, 0, protocol.AgentTarget{AgentID: testAgent})
	require.Equal(t, protocol.FrameErrorCodeUnsupportedOp, errorOf(t, k.read(), id))
	k.send(protocol.AgentOpRegisterAgentResult, 1, protocol.RegisterAgentResult{AgentID: testAgent, SessionID: testSession})
	reportOf(t, k.read(), 0)
	id = k.send(protocol.AgentOpHeartbeat, 0, protocol.AgentTarget{AgentID: testAgent})
	require.Equal(t, protocol.FrameErrorCodeUnsupportedOp, errorOf(t, k.read(), id))

	// An error from the Core is dropped, never answered.
	k.send(protocol.AgentOpError, 0, protocol.FrameError{Code: protocol.FrameErrorCodeInternal, Message: "core side"})
	id = k.send(protocol.AgentOpResync, 0, protocol.AgentTarget{AgentID: testAgent})
	reportOf(t, k.read(), id)
}

// TestOrdersThisProxyDoesNotRun refuses every Core order but resync on
// a socket without a delivery, as one that only reports.
func TestOrdersThisProxyDoesNotRun(t *testing.T) {
	t.Parallel()
	ts := listenTest(t, nil)
	k := dialCore(t, ts.srv.Path())
	k.greet()

	ref := json.RawMessage(`{"message_id":"01JB0000000000000000000011","from_agent":"agent-1","priority":"NORMAL","thread_id":"thread-1"}`)
	for op, body := range map[protocol.AgentOp]string{
		protocol.AgentOpNativeDelivery:          `{"delivery_id":"dlv-1","agent_id":"agent-1","message_ref":` + string(ref) + `,"orchestrator_decision_id":"01JB0000000000000000000013","control_versions":{"global":18}}`,
		protocol.AgentOpNotifyMessageAvailable:  `{"message_ref":` + string(ref) + `}`,
		protocol.AgentOpNotifyArtifactAvailable: `{"artifact_ref":{"artifact_id":"art-1","from_agent":"agent-1","purpose":"review","state":"AVAILABLE"}}`,
		protocol.AgentOpPrepareDelivery:         `{"delivery_id":"dlv-1","agent_id":"agent-1","message_ref":` + string(ref) + `,"notice_line":"[GIFFORD] 1 pending message","orchestrator_decision_id":"01JB0000000000000000000002","expected_state_version":1,"expected_input_guard_version":0,"expected_draft_generation":0,"control_versions":{"global":18}}`,
		protocol.AgentOpCommitDelivery:          `{"delivery_id":"dlv-1","writer_lease_id":"lease-1"}`,
		protocol.AgentOpCancelDelivery:          `{"delivery_id":"dlv-1","reason":"HUMAN_PRIORITY"}`,
		protocol.AgentOpPauseAutomation:         `{"agent_id":"agent-1"}`,
		protocol.AgentOpResumeAutomation:        `{"agent_id":"agent-1"}`,
	} {
		id := k.send(op, 0, json.RawMessage(body))
		require.Equal(t, protocol.FrameErrorCodeUnsupportedOp, errorOf(t, k.read(), id), op)
	}
	// None of them moved anything.
	id := k.send(protocol.AgentOpResync, 0, protocol.AgentTarget{AgentID: testAgent})
	require.Equal(t, int64(0), reportOf(t, k.read(), id).InputGuardVersion)
}

func TestOversizedFrameClosesTheConnection(t *testing.T) {
	t.Parallel()
	ts := listenTest(t, nil)
	k := dialCore(t, ts.srv.Path())
	k.greet()
	_, err := k.c.Write(binary.BigEndian.AppendUint32(nil, MaxFrameBytes+1))
	require.NoError(t, err)
	k.closed()
}

func TestOneConnectionAtATime(t *testing.T) {
	t.Parallel()
	ts := listenTest(t, nil)
	first := dialCore(t, ts.srv.Path())
	first.greet()

	second := dialCore(t, ts.srv.Path())
	second.closed()

	require.NoError(t, first.c.Close())
	dialGreeted(t, ts.srv.Path())
}

func TestForeignPeerRefused(t *testing.T) {
	t.Parallel()
	ts := listenTest(t, func(o *Options) {
		o.Peer = func(net.Conn) error { return errors.New("another uid") }
	})
	k := dialCore(t, ts.srv.Path())
	require.NoError(t, k.c.SetReadDeadline(time.Now().Add(10*time.Second)))
	_, err := ReadFrame(k.c)
	require.ErrorIs(t, err, io.EOF, "refused before any frame")
}

func TestSocketModes(t *testing.T) {
	t.Parallel()
	ts := listenTest(t, nil)
	require.Equal(t, filepath.Join(ts.runtime, "agents", "agent-1.sock"), ts.srv.Path())
	dir, err := os.Lstat(filepath.Dir(ts.srv.Path()))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o700), dir.Mode().Perm())
	sock, err := os.Lstat(ts.srv.Path())
	require.NoError(t, err)
	require.Equal(t, os.ModeSocket, sock.Mode().Type())
	require.Equal(t, os.FileMode(0o600), sock.Mode().Perm())
}

func TestListenRefusesWhatIsNotItsOwn(t *testing.T) {
	t.Parallel()
	listen := func(runtime string) error {
		srv, err := Listen(context.Background(), Options{RuntimeDir: runtime, Register: testRegister(), Guard: newFakeGuard(), Clock: &fakeClock{}})
		if err == nil {
			_ = srv.Close()
		}
		return err
	}

	t.Run("a live socket of the same agent", func(t *testing.T) {
		t.Parallel()
		ts := listenTest(t, nil)
		require.ErrorIs(t, listen(ts.runtime), ErrSocketInUse)
		// The live one is untouched and still answers, once the
		// probe's own connection ended.
		dialGreeted(t, ts.srv.Path())
	})
	t.Run("a dead socket", func(t *testing.T) {
		t.Parallel()
		runtime := shortDir(t)
		path := SocketPath(runtime, testAgent)
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o700))
		l, err := (&net.ListenConfig{}).Listen(context.Background(), "unix", path)
		require.NoError(t, err)
		l.(*net.UnixListener).SetUnlinkOnClose(false)
		require.NoError(t, l.Close())

		ts := listenTest(t, func(o *Options) { o.RuntimeDir = runtime })
		dialCore(t, ts.srv.Path()).greet()
	})
	t.Run("a file that is not a socket", func(t *testing.T) {
		t.Parallel()
		runtime := shortDir(t)
		path := SocketPath(runtime, testAgent)
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o700))
		require.NoError(t, os.WriteFile(path, []byte("keep"), 0o600))
		require.ErrorIs(t, listen(runtime), ErrNotOurSocket)
		raw, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, "keep", string(raw))
	})
	t.Run("a directory open to others", func(t *testing.T) {
		t.Parallel()
		runtime := shortDir(t)
		dir := filepath.Join(runtime, "agents")
		require.NoError(t, os.Mkdir(dir, 0o700))
		require.NoError(t, os.Chmod(dir, 0o755))
		require.ErrorIs(t, listen(runtime), ErrPrivateDir)
		entries, err := os.ReadDir(dir)
		require.NoError(t, err)
		require.Empty(t, entries, "no socket opened")
	})
	t.Run("a link for the directory", func(t *testing.T) {
		t.Parallel()
		runtime := shortDir(t)
		target := shortDir(t)
		require.NoError(t, os.Chmod(target, 0o700))
		require.NoError(t, os.Symlink(target, filepath.Join(runtime, "agents")))
		require.ErrorIs(t, listen(runtime), ErrPrivateDir)
	})
}

func TestCloseRemovesOnlyItsOwnSocket(t *testing.T) {
	t.Parallel()
	ts := listenTest(t, nil)
	k := dialCore(t, ts.srv.Path())
	k.greet()
	require.NoError(t, ts.srv.Close())
	k.closed()
	_, err := os.Lstat(ts.srv.Path())
	require.ErrorIs(t, err, os.ErrNotExist, "its own socket is gone")

	// A file that took the path since is another session's: it stays.
	other := listenTest(t, nil)
	require.NoError(t, os.Remove(other.srv.Path()))
	require.NoError(t, os.WriteFile(other.srv.Path(), []byte("another session"), 0o600))
	require.NoError(t, other.srv.Close())
	raw, err := os.ReadFile(other.srv.Path())
	require.NoError(t, err)
	require.Equal(t, "another session", string(raw))
}

func TestReportsEveryGuardVersion(t *testing.T) {
	t.Parallel()
	ts := listenTest(t, nil)
	k := dialCore(t, ts.srv.Path())
	first := k.greet()
	require.Equal(t, int64(0), first.InputGuardVersion)

	ts.guard.set(protocol.DraftStateOpen, 1, 1)
	f := k.read()
	r := reportOf(t, f, 0)
	require.Equal(t, protocol.DraftStateOpen, r.DraftState)
	require.Equal(t, int64(1), r.DraftGeneration)
	require.Equal(t, int64(1), r.InputGuardVersion)
	require.Equal(t, int64(3), f.FrameID, "greeting 1, first report 2, this one 3")

	ts.guard.set(protocol.DraftStateClosed, 1, 2)
	f = k.read()
	require.Equal(t, int64(2), reportOf(t, f, 0).InputGuardVersion)
	require.Equal(t, int64(4), f.FrameID)

	// The report holds only the wire fields: no field for text.
	var fields map[string]any
	require.NoError(t, json.Unmarshal(f.Body, &fields))
	for name := range fields {
		require.Contains(t, []string{
			"agent_id", "session_id", "draft_state", "activity_state", "draft_generation",
			"input_guard_version", "last_human_key_at", "detection_source", "confidence",
		}, name)
	}
}

func TestResyncReemitsTheState(t *testing.T) {
	t.Parallel()
	ts := listenTest(t, nil)
	k := dialCore(t, ts.srv.Path())
	k.greet()

	ts.guard.set(protocol.DraftStateUnknown, 3, 9)
	reportOf(t, k.read(), 0)
	id := k.send(protocol.AgentOpResync, 0, protocol.AgentTarget{AgentID: testAgent})
	r := reportOf(t, k.read(), id)
	require.Equal(t, protocol.DraftStateUnknown, r.DraftState, "UNKNOWN stays UNKNOWN")
	require.Equal(t, int64(9), r.InputGuardVersion)

	id = k.send(protocol.AgentOpResync, 0, protocol.AgentTarget{AgentID: otherAgent})
	require.Equal(t, protocol.FrameErrorCodeInvalidFrame, errorOf(t, k.read(), id), "a resync for another agent")
}

func TestHeartbeatEveryThreeSeconds(t *testing.T) {
	t.Parallel()
	ts := listenTest(t, nil)
	k := dialCore(t, ts.srv.Path())
	k.greet()
	for range 2 {
		ts.clock.fire(t, HeartbeatEvery)
		f := k.read()
		require.Equal(t, protocol.AgentOpHeartbeat, f.Op)
		require.Nil(t, f.ReplyTo)
		var target protocol.AgentTarget
		require.NoError(t, json.Unmarshal(f.Body, &target))
		require.Equal(t, testAgent, target.AgentID)
	}
}

// A Core that stops reading never blocks the guard: every change
// returns at once, and when the Core reads again the reports it gets
// rise to the latest state, which no report invents.
func TestJammedCoreCoalescesReports(t *testing.T) {
	t.Parallel()
	ts := listenTest(t, nil)
	k := dialCore(t, ts.srv.Path())
	k.greet()

	const changes = 20000
	done := make(chan struct{})
	go func() {
		defer close(done)
		for v := int64(1); v <= changes; v++ {
			ts.guard.set(protocol.DraftStateOpen, 1, v)
		}
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the guard waited on the socket")
	}

	var last int64
	reports := 0
	for last < changes {
		r := reportOf(t, k.read(), 0)
		require.Greater(t, r.InputGuardVersion, last, "versions only rise")
		require.LessOrEqual(t, r.InputGuardVersion, int64(changes), "never a state the guard did not have")
		last = r.InputGuardVersion
		reports++
	}
	require.Less(t, reports, changes, "reports coalesced while the Core did not read")
}

// A Core that sends orders without reading their replies fills the
// bounded queue, and the connection closes.
func TestRepliesQueueIsBounded(t *testing.T) {
	t.Parallel()
	ts := listenTest(t, nil)
	k := dialCore(t, ts.srv.Path())
	k.greet()
	require.NoError(t, k.c.(*net.UnixConn).SetReadBuffer(4096))

	closed := false
	for range 200000 {
		if err := WriteFrame(k.c, testFrame(t, protocol.AgentOpResync, k.next, 0, protocol.AgentTarget{AgentID: testAgent})); err != nil {
			closed = true
			break
		}
		k.next++
	}
	require.True(t, closed, "the session kept queueing replies")
}
