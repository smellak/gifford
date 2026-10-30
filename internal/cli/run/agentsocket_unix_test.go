//go:build unix

package run

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/creack/pty"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/smellak/gifford/internal/agentsock"
	"github.com/smellak/gifford/internal/profiles"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy"
	"github.com/smellak/gifford/internal/proxy/session"
)

// socketAgent is the agent of the socket test runs, and socketSession
// the session the fake Core gives it.
const (
	socketAgent   = "agent-1"
	socketSession = "sess-1"
)

// TestRunAgentListensOnTheAgentSocket runs --agent: while the child
// lives, <runtime>/agents/<agent>.sock listens with mode 0600 in a 0700
// directory, greets with the register_agent the hub got and reports the
// guard. Once the run ends the socket is gone, and no notice was
// printed.
func TestRunAgentListensOnTheAgentSocket(t *testing.T) {
	runtimeHome := t.TempDir()
	t.Setenv("XDG_RUNTIME_DIR", runtimeHome)
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	t.Setenv("GORACE", "atexit_sleep_ms=0")
	fake := startFakeHub(t, filepath.Join(runtimeHome, "gifford", "hub.sock"))
	script, gate := writeGateScript(t, "fakeharness 1.2.3")

	outer, slave, err := pty.Open()
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = outer.Close()
		_ = slave.Close()
	})
	require.NoError(t, pty.Setsize(outer, &pty.Winsize{Rows: 24, Cols: 80}))
	cmd := NewCmd(nil)
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	cmd.SetIn(slave)
	cmd.SetOut(slave)
	var stderr syncBuffer
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{agentFlag, socketAgent, harnessFlag, codexHarness, "--", script, gate})
	done := make(chan error, 1)
	go func() { done <- cmd.Execute() }()

	readUntil(t, outer, "ready")
	require.Eventually(t, func() bool {
		return len(fake.registrations()) == 1
	}, 10*time.Second, 10*time.Millisecond, "hub never saw register_agent")

	path := filepath.Join(runtimeHome, "gifford", "agents", socketAgent+".sock")
	sock, err := os.Lstat(path)
	require.NoError(t, err)
	require.Equal(t, os.ModeSocket, sock.Mode().Type())
	require.Equal(t, os.FileMode(0o600), sock.Mode().Perm())
	dir, err := os.Lstat(filepath.Dir(path))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o700), dir.Mode().Perm())

	c, err := (&net.Dialer{}).DialContext(context.Background(), "unix", path)
	require.NoError(t, err)
	require.NoError(t, c.SetReadDeadline(time.Now().Add(10*time.Second)))
	hello, err := agentsock.ReadFrame(c)
	require.NoError(t, err)
	require.Equal(t, protocol.AgentOpRegisterAgent, hello.Op)
	var reg protocol.RegisterAgent
	require.NoError(t, json.Unmarshal(hello.Body, &reg))
	require.Equal(t, fake.registrations()[0], reg, "the manifest and process of the hub registration")

	replyTo := hello.FrameID
	body, err := json.Marshal(protocol.RegisterAgentResult{AgentID: socketAgent, SessionID: socketSession})
	require.NoError(t, err)
	require.NoError(t, agentsock.WriteFrame(c, protocol.AgentFrame{
		Op: protocol.AgentOpRegisterAgentResult, FrameID: 1, ReplyTo: &replyTo,
		SentAt: time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC), Body: body,
	}))
	f, err := agentsock.ReadFrame(c)
	require.NoError(t, err)
	require.Equal(t, protocol.AgentOpReportInputGuard, f.Op)
	var report protocol.InputGuardReport
	require.NoError(t, json.Unmarshal(f.Body, &report))
	require.Equal(t, protocol.DraftStateClosed, report.DraftState)
	require.Equal(t, protocol.AgentID(socketAgent), report.AgentID)
	require.NoError(t, c.Close())

	_, err = outer.Write([]byte("\n"))
	require.NoError(t, err)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(15 * time.Second):
		t.Fatal("run never ended after releasing the child")
	}
	_, err = os.Lstat(path)
	require.ErrorIs(t, err, os.ErrNotExist, "the run removes its own socket")
	require.Equal(t, proxy.DaemonNotice+"\n", stderr.String())
}

// dialRunAgentSocket waits past an old connection's cleanup, reads the new
// greeting and returns the still-open Core side.
func dialRunAgentSocket(t *testing.T, path string) (net.Conn, protocol.AgentFrame) {
	t.Helper()
	var conn net.Conn
	var hello protocol.AgentFrame
	require.Eventually(t, func() bool {
		candidate, err := (&net.Dialer{}).DialContext(context.Background(), "unix", path)
		if err != nil {
			return false
		}
		if err := candidate.SetReadDeadline(time.Now().Add(250 * time.Millisecond)); err != nil {
			_ = candidate.Close()
			return false
		}
		greeting, err := agentsock.ReadFrame(candidate)
		if err != nil {
			_ = candidate.Close()
			return false
		}
		conn, hello = candidate, greeting
		return true
	}, 10*time.Second, 10*time.Millisecond, "agent socket never greeted")
	require.Equal(t, protocol.AgentOpRegisterAgent, hello.Op)
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(10*time.Second)))
	return conn, hello
}

func answerRunAgentGreeting(
	t *testing.T,
	conn net.Conn,
	hello protocol.AgentFrame,
	sessionID protocol.OpaqueID,
) {
	t.Helper()
	body, err := json.Marshal(protocol.RegisterAgentResult{
		AgentID: socketAgent, SessionID: sessionID,
	})
	require.NoError(t, err)
	replyTo := hello.FrameID
	require.NoError(t, agentsock.WriteFrame(conn, protocol.AgentFrame{
		Op: protocol.AgentOpRegisterAgentResult, FrameID: 1, ReplyTo: &replyTo,
		SentAt: time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC), Body: body,
	}))
}

// TestRunAgentRecoversTheSameLiveHarness drops the node-side socket link while
// a child is waiting. The run repeats the exact Hub registration, rejects a
// different session on the fresh socket, then accepts the original session.
// No terminal byte appears during the outage and the child pid is unchanged.
func TestRunAgentRecoversTheSameLiveHarness(t *testing.T) {
	runtimeHome := t.TempDir()
	t.Setenv("XDG_RUNTIME_DIR", runtimeHome)
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	t.Setenv("GORACE", "atexit_sleep_ms=0")
	fake := startFakeHub(t, filepath.Join(runtimeHome, "gifford", "hub.sock"))
	counter := filepath.Join(t.TempDir(), "count")
	t.Setenv("COUNT_FILE", counter)
	script := filepath.Join(t.TempDir(), "reconnect-harness")
	writeTestExecutable(t, script, []byte("#!/bin/sh\n"+
		"echo invoked >> \"$COUNT_FILE\"\n"+
		"if [ \"$1\" = \"--version\" ]; then echo \"fakeharness 1.2.3\"; exit 0; fi\n"+
		"stty -echo\n"+
		"echo \"ready-pid=$$-end\"\n"+
		"IFS= read -r line\n"+
		"echo \"done-pid=$$-line=$line-end\"\n"))

	outer, slave, err := pty.Open()
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = outer.Close()
		_ = slave.Close()
	})
	require.NoError(t, pty.Setsize(outer, &pty.Winsize{Rows: 24, Cols: 80}))
	cmd := NewCmd(nil)
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	cmd.SetIn(slave)
	cmd.SetOut(slave)
	var stderr syncBuffer
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{agentFlag, socketAgent, harnessFlag, codexHarness, "--", script})
	done := make(chan error, 1)
	go func() { done <- cmd.Execute() }()

	ready := string(readUntil(t, outer, "-end\r\n"))
	start := strings.Index(ready, "ready-pid=")
	require.GreaterOrEqual(t, start, 0)
	end := strings.Index(ready[start:], "-end")
	require.Positive(t, end)
	pid := ready[start+len("ready-pid=") : start+end]
	require.NotEmpty(t, pid)
	require.Eventually(t, func() bool { return len(fake.registrations()) == 1 },
		10*time.Second, 10*time.Millisecond)

	path := filepath.Join(runtimeHome, "gifford", "agents", socketAgent+".sock")
	first, hello := dialRunAgentSocket(t, path)
	answerRunAgentGreeting(t, first, hello, socketSession)
	var firstReport protocol.InputGuardReport
	frame, err := agentsock.ReadFrame(first)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(frame.Body, &firstReport))
	require.Equal(t, protocol.OpaqueID(socketSession), firstReport.SessionID)
	require.NoError(t, first.Close(), "the node link drops")

	require.Eventually(t, func() bool { return len(fake.registrations()) == 2 },
		10*time.Second, 10*time.Millisecond, "run did not repeat register_agent")
	regs := fake.registrations()
	require.Equal(t, regs[0], regs[1], "recovery must preserve Manifest and Process")
	readyFDs, err := unix.Poll([]unix.PollFd{{
		Fd: int32(outer.Fd()), Events: unix.POLLIN,
	}}, int((3*registrationRetryInitial)/time.Millisecond))
	require.NoError(t, err)
	require.Zero(t, readyFDs, "Gifford wrote terminal bytes during the outage")

	wrong, wrongHello := dialRunAgentSocket(t, path)
	answerRunAgentGreeting(t, wrong, wrongHello, "sess-other")
	_, err = agentsock.ReadFrame(wrong)
	require.Error(t, err, "another session must be closed before its first report")
	require.NoError(t, wrong.Close())
	time.Sleep(2 * registrationRetryInitial)
	require.Len(t, fake.registrations(), 2, "a refused greeting is not a registered disconnect")

	second, secondHello := dialRunAgentSocket(t, path)
	t.Cleanup(func() { _ = second.Close() })
	answerRunAgentGreeting(t, second, secondHello, socketSession)
	frame, err = agentsock.ReadFrame(second)
	require.NoError(t, err)
	var resumed protocol.InputGuardReport
	require.NoError(t, json.Unmarshal(frame.Body, &resumed))
	require.Equal(t, protocol.OpaqueID(socketSession), resumed.SessionID)

	_, err = outer.Write([]byte("hello\n"))
	require.NoError(t, err)
	finished := string(readUntil(t, outer, "-line=hello-end"))
	require.Contains(t, finished, "done-pid="+pid+"-line=hello-end")
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(termTimeout):
		t.Fatal("run never ended after the child read the human line")
	}
	require.Equal(t, 2, countInvocations(t, counter), "one version probe and one unchanged harness")
	require.Equal(t, proxy.DaemonNotice+"\n", stderr.String())
}

// TestRunAgentSocketInUseRunsTransparent runs --agent while another
// session's socket for the same agent listens: that socket is left
// alone, and the run prints one notice line and runs the harness
// without a socket.
func TestRunAgentSocketInUseRunsTransparent(t *testing.T) {
	runtimeHome := t.TempDir()
	t.Setenv("XDG_RUNTIME_DIR", runtimeHome)
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	t.Setenv("GORACE", "atexit_sleep_ms=0")
	startFakeHub(t, filepath.Join(runtimeHome, "gifford", "hub.sock"))

	path := filepath.Join(runtimeHome, "gifford", "agents", "agent-9.sock")
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o700))
	live, err := (&net.ListenConfig{}).Listen(context.Background(), "unix", path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = live.Close() })
	go func() {
		for {
			c, err := live.Accept()
			if err != nil {
				return
			}
			_ = c.Close()
		}
	}()

	outer, slave, err := pty.Open()
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = outer.Close()
		_ = slave.Close()
	})
	require.NoError(t, pty.Setsize(outer, &pty.Winsize{Rows: 24, Cols: 80}))
	cmd := NewCmd(nil)
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	cmd.SetIn(slave)
	cmd.SetOut(slave)
	var stderr strings.Builder
	cmd.SetErr(&stderr)
	cmd.SetArgs(append([]string{agentFlag, "agent-9", "--"}, cliHelperArgv("say")...))
	require.NoError(t, cmd.Execute())
	readUntil(t, outer, "hello-agent-say")
	require.Equal(t, "gifford: no agent socket for this run: "+agentsock.ErrSocketInUse.Error()+"\n"+
		proxy.DaemonNotice+"\n", stderr.String())

	// The other session's socket is still there and still answers.
	c, err := (&net.Dialer{}).DialContext(context.Background(), "unix", path)
	require.NoError(t, err)
	require.NoError(t, c.Close())
}

// readAnswer reads the frame answering id, past the reports and
// heartbeats the run sends on its own.
func readAnswer(t *testing.T, c net.Conn, id int64) protocol.AgentFrame {
	t.Helper()
	for {
		f, err := agentsock.ReadFrame(c)
		require.NoError(t, err)
		if f.ReplyTo == nil && (f.Op == protocol.AgentOpReportInputGuard || f.Op == protocol.AgentOpHeartbeat) {
			continue
		}
		require.NotNil(t, f.ReplyTo, "%s answers nothing", f.Op)
		require.Equal(t, id, *f.ReplyTo)
		return f
	}
}

// outputQuiet is how long the session waits after the harness's last
// output before it takes a delivery.
const outputQuiet = 300 * time.Millisecond

// reasonUnmeasured is why the session refuses every delivery on a
// profile not yet measured.
const reasonUnmeasured = "profile not measured"

// deliveredNotice is the line the socket runs deliver.
const deliveredNotice = "[GIFFORD] 1 pending message"

// measuredCodex looks up, for one command, a measured fixture of the
// codex profile: the real profile's keys and input line, measured.
// Every real profile is still unmeasured, and a profile not yet
// measured never delivers.
func measuredCodex(t *testing.T) runProfileLookup {
	t.Helper()
	fixture, err := profiles.ParseFile(codexHarness, []byte(`harness: codex
measured: true
version_range: "*"
submit: "\r"
insert_newline: "\n"
clear_line: "\u0015"
cancel: "\u0003"
bracketed_paste: false
extended_keyboard: none
input_line: "^(.*)$"
`))
	require.NoError(t, err)
	return func(name, _ string) (profiles.Profile, error) {
		if name != codexHarness {
			return profiles.Profile{}, fmt.Errorf("no measured fixture for %s", name)
		}
		return fixture, nil
	}
}

// socketRun is one run --agent with a fake Core on its agent socket,
// registered, holding the guard's first report.
type socketRun struct {
	t        *testing.T
	dataHome string
	outer    *os.File
	conn     net.Conn
	guard    protocol.InputGuardReport
	next     int64
	done     chan error
}

// startSocketRun runs --agent over the fake harness at script, with
// the profiles lookup gives, or the embedded ones when lookup is nil,
// and takes its agent socket as the Core does.
func startSocketRun(t *testing.T, lookup runProfileLookup, script string) *socketRun {
	t.Helper()
	runtimeHome := t.TempDir()
	dataHome := t.TempDir()
	t.Setenv("XDG_RUNTIME_DIR", runtimeHome)
	t.Setenv("XDG_DATA_HOME", dataHome)
	t.Setenv("GORACE", "atexit_sleep_ms=0")
	startFakeHub(t, filepath.Join(runtimeHome, "gifford", "hub.sock"))
	outer, slave, err := pty.Open()
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = outer.Close()
		_ = slave.Close()
	})
	require.NoError(t, pty.Setsize(outer, &pty.Winsize{Rows: 24, Cols: 80}))
	cmd := NewCmd(nil)
	if lookup != nil {
		cmd = newCmdWithProfileLookup(nil, lookup)
	}
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	cmd.SetIn(slave)
	cmd.SetOut(slave)
	cmd.SetErr(&syncBuffer{})
	cmd.SetArgs([]string{agentFlag, socketAgent, harnessFlag, codexHarness, "--", script})
	r := &socketRun{t: t, dataHome: dataHome, outer: outer, next: 1, done: make(chan error, 1)}
	go func() { r.done <- cmd.Execute() }()
	readUntil(t, outer, "ready")

	path := filepath.Join(runtimeHome, "gifford", "agents", socketAgent+".sock")
	r.conn, err = (&net.Dialer{}).DialContext(context.Background(), "unix", path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.conn.Close() })
	require.NoError(t, r.conn.SetReadDeadline(time.Now().Add(20*time.Second)))
	hello, err := agentsock.ReadFrame(r.conn)
	require.NoError(t, err)
	require.Equal(t, protocol.AgentOpRegisterAgent, hello.Op)
	r.send(protocol.AgentOpRegisterAgentResult, hello.FrameID, protocol.RegisterAgentResult{AgentID: socketAgent, SessionID: socketSession})
	f, err := agentsock.ReadFrame(r.conn)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(f.Body, &r.guard))
	require.Equal(t, protocol.DraftStateClosed, r.guard.DraftState)
	return r
}

// send writes one frame from the Core and returns its id.
func (r *socketRun) send(op protocol.AgentOp, replyTo int64, body any) int64 {
	r.t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(r.t, err)
	f := protocol.AgentFrame{Op: op, FrameID: r.next, SentAt: time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC), Body: raw}
	if replyTo > 0 {
		f.ReplyTo = &replyTo
	}
	require.NoError(r.t, agentsock.WriteFrame(r.conn, f))
	r.next++
	return f.FrameID
}

// result reads the delivery result answering frame id.
func (r *socketRun) result(id int64) protocol.DeliveryResult {
	r.t.Helper()
	f := readAnswer(r.t, r.conn, id)
	require.Equal(r.t, protocol.AgentOpReportDeliveryResult, f.Op)
	var res protocol.DeliveryResult
	require.NoError(r.t, json.Unmarshal(f.Body, &res))
	return res
}

// prepare sends the prepare of delivery id, against the guard's first
// report.
func (r *socketRun) prepare(id protocol.OpaqueID) protocol.DeliveryResult {
	r.t.Helper()
	return r.result(r.send(protocol.AgentOpPrepareDelivery, 0, protocol.PrepareDelivery{
		DeliveryID: id, AgentID: socketAgent,
		MessageRef: protocol.MessageRef{
			MessageID: "01JB0000000000000000000011", FromAgent: "agent-2",
			Priority: protocol.PriorityNormal, ThreadID: "thread-1",
		},
		NoticeLine:             deliveredNotice,
		OrchestratorDecisionID: "01JB0000000000000000000002",
		ExpectedStateVersion:   1, ExpectedInputGuardVersion: r.guard.InputGuardVersion,
		ExpectedDraftGeneration: r.guard.DraftGeneration,
		ControlVersions:         protocol.ControlVersions{"global": 18},
	}))
}

// commit sends the commit of delivery id under the Core's lease.
func (r *socketRun) commit(id protocol.OpaqueID) protocol.DeliveryResult {
	r.t.Helper()
	return r.result(r.send(protocol.AgentOpCommitDelivery, 0, protocol.CommitDelivery{DeliveryID: id, WriterLeaseID: "lease-1"}))
}

// release types the human's empty line, the last the child reads
// before it exits, and waits for the run to end.
func (r *socketRun) release() {
	r.t.Helper()
	_, err := r.outer.Write([]byte("\n"))
	require.NoError(r.t, err)
	select {
	case err := <-r.done:
		require.NoError(r.t, err)
	case <-time.After(15 * time.Second):
		r.t.Fatal("run never ended after the child read its line")
	}
}

// events reads the events the run recorded, once it has ended.
func (r *socketRun) events() []session.LoggedEvent {
	r.t.Helper()
	dir := filepath.Join(r.dataHome, "gifford")
	ids, err := session.ListSessions(dir)
	require.NoError(r.t, err)
	require.Len(r.t, ids, 1)
	f, err := os.Open(filepath.Join(dir, "recordings", ids[0]+session.EventsSuffix))
	require.NoError(r.t, err)
	evs, err := session.ReadEventLog(f, ids[0])
	require.NoError(r.t, errors.Join(err, f.Close()))
	return evs
}

// TestRunAgentDeliversThroughTheSocket runs --agent on a measured
// fixture of the codex profile and delivers one notice through its
// agent socket: the Core prepares and commits, and the child reads the
// line in the profile's mode. The child lives on, as a harness does,
// until the test releases it.
func TestRunAgentDeliversThroughTheSocket(t *testing.T) {
	script := filepath.Join(t.TempDir(), "fakeharness")
	writeTestExecutable(t, script, []byte("#!/bin/sh\n"+
		"if [ \"$1\" = \"--version\" ]; then echo \"fakeharness 1.2.3\"; exit 0; fi\n"+
		"stty -echo\necho ready\nIFS= read -r line\necho \"got:$line\"\nread -r gate\n"))
	r := startSocketRun(t, measuredCodex(t), script)

	// The ready line is the harness's last output: the session takes
	// no delivery before it is outputQuiet old on the session's clock,
	// which counts from when the session read it, a moment after the
	// test did. Wait past that, then prepare as the Core does: an
	// ABORTED_HUMAN_PRIORITY defers, and a new delivery tries again.
	time.Sleep(outputQuiet + outputQuiet/2)
	var id protocol.OpaqueID
	var res protocol.DeliveryResult
	for attempt, deadline := 1, time.Now().Add(10*time.Second); ; attempt++ {
		id = protocol.OpaqueID("dlv-" + strconv.Itoa(attempt))
		res = r.prepare(id)
		if res.Result != protocol.DeliveryResultKindAbortedHumanPriority || time.Now().After(deadline) {
			break
		}
		time.Sleep(outputQuiet / 3)
	}
	require.Equal(t, protocol.DeliveryResultKindPrepared, res.Result)
	res = r.commit(id)
	require.Equal(t, protocol.DeliveryResultKindCommitted, res.Result)
	require.Equal(t, protocol.OpaqueID("lease-1"), res.WriterLeaseID)
	require.Equal(t, protocol.InjectModeTyped, res.InjectMode, "the codex profile types")
	readUntil(t, r.outer, "got:"+deliveredNotice)
	r.release()
}

// TestRunAgentUnmeasuredProfileWritesNothing runs --agent on the
// embedded codex profile, not yet measured, as no real profile is: the
// Core's prepares are all refused, from well past the output quiet
// period on, the commit of one has nothing to run, and the recording
// gives the profile as the reason every time. The child reads only the
// human's empty line: no byte of the notice reached it.
func TestRunAgentUnmeasuredProfileWritesNothing(t *testing.T) {
	ps, err := profiles.Load()
	require.NoError(t, err)
	embedded, err := ps.Profile(codexHarness, "1.2.3")
	require.NoError(t, err)
	require.False(t, embedded.Measured, "the real codex profile is not measured yet")
	script := filepath.Join(t.TempDir(), "fakeharness")
	writeTestExecutable(t, script, []byte("#!/bin/sh\n"+
		"if [ \"$1\" = \"--version\" ]; then echo \"fakeharness 1.2.3\"; exit 0; fi\n"+
		"stty -echo\necho ready\nIFS= read -r line\necho \"got:<$line>\"\n"))
	r := startSocketRun(t, nil, script)

	time.Sleep(outputQuiet + outputQuiet/2)
	var ids []protocol.OpaqueID
	for attempt, deadline := 1, time.Now().Add(4*outputQuiet); time.Now().Before(deadline); attempt++ {
		id := protocol.OpaqueID("dlv-" + strconv.Itoa(attempt))
		require.Equal(t, protocol.DeliveryResultKindAbortedHumanPriority, r.prepare(id).Result)
		ids = append(ids, id)
		time.Sleep(outputQuiet / 3)
	}
	require.Equal(t, protocol.DeliveryResultKindFailed, r.commit(ids[len(ids)-1]).Result)
	r.release()
	readUntil(t, r.outer, "got:<>")

	var reasons []string
	for _, e := range r.events() {
		switch e.Type {
		case protocol.EventTypeInputDeliveryAbortedHumanPriority:
			var aborted session.DeliveryAborted
			require.NoError(t, json.Unmarshal(e.Payload, &aborted))
			reasons = append(reasons, aborted.Reason)
		case protocol.EventTypeInputDeliveryPrepared, protocol.EventTypeInputDeliveryRevalidated,
			protocol.EventTypeInputDeliveryStarted, protocol.EventTypeInputDeliveryChunk,
			protocol.EventTypeInputDeliveryCommitted:
			t.Fatalf("a delivery ran: %s", e.Type)
		default:
		}
	}
	require.Equal(t, slices.Repeat([]string{reasonUnmeasured}, len(ids)), reasons)
}
