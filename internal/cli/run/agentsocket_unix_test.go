//go:build unix

package run

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/creack/pty"
	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/agentsock"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy"
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

// TestRunAgentSocketInUseRunsTransparent runs --agent while another
// session's socket for the same agent listens: that socket is left
// alone, and the run prints one notice line and runs the harness
// without a socket.
func TestRunAgentSocketInUseRunsTransparent(t *testing.T) {
	runtimeHome := t.TempDir()
	t.Setenv("XDG_RUNTIME_DIR", runtimeHome)
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	t.Setenv("GORACE", "atexit_sleep_ms=0")

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
	require.Equal(t, "gifford: no hub session for this run: dial hub: hub unavailable\n"+
		"gifford: no agent socket for this run: "+agentsock.ErrSocketInUse.Error()+"\n"+
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

// TestRunAgentDeliversThroughTheSocket runs --agent and delivers one
// notice through its agent socket: the Core prepares and commits, and
// the child reads the line in the profile's mode. The child lives on,
// as a harness does, until the test releases it.
func TestRunAgentDeliversThroughTheSocket(t *testing.T) {
	runtimeHome := t.TempDir()
	t.Setenv("XDG_RUNTIME_DIR", runtimeHome)
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	t.Setenv("GORACE", "atexit_sleep_ms=0")
	script := filepath.Join(t.TempDir(), "fakeharness")
	writeTestExecutable(t, script, []byte("#!/bin/sh\n"+
		"if [ \"$1\" = \"--version\" ]; then echo \"fakeharness 1.2.3\"; exit 0; fi\n"+
		"stty -echo\necho ready\nIFS= read -r line\necho \"got:$line\"\nread -r gate\n"))

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
	cmd.SetErr(&syncBuffer{})
	cmd.SetArgs([]string{agentFlag, socketAgent, harnessFlag, codexHarness, "--", script})
	done := make(chan error, 1)
	go func() { done <- cmd.Execute() }()
	readUntil(t, outer, "ready")

	path := filepath.Join(runtimeHome, "gifford", "agents", socketAgent+".sock")
	c, err := (&net.Dialer{}).DialContext(context.Background(), "unix", path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	require.NoError(t, c.SetReadDeadline(time.Now().Add(20*time.Second)))
	hello, err := agentsock.ReadFrame(c)
	require.NoError(t, err)
	require.Equal(t, protocol.AgentOpRegisterAgent, hello.Op)
	sentAt := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	next := int64(1)
	send := func(op protocol.AgentOp, replyTo int64, body any) int64 {
		t.Helper()
		raw, err := json.Marshal(body)
		require.NoError(t, err)
		f := protocol.AgentFrame{Op: op, FrameID: next, SentAt: sentAt, Body: raw}
		if replyTo > 0 {
			f.ReplyTo = &replyTo
		}
		require.NoError(t, agentsock.WriteFrame(c, f))
		next++
		return f.FrameID
	}
	send(protocol.AgentOpRegisterAgentResult, hello.FrameID, protocol.RegisterAgentResult{AgentID: socketAgent, SessionID: socketSession})
	f, err := agentsock.ReadFrame(c)
	require.NoError(t, err)
	var guard protocol.InputGuardReport
	require.NoError(t, json.Unmarshal(f.Body, &guard))
	require.Equal(t, protocol.DraftStateClosed, guard.DraftState)

	const notice = "[GIFFORD] 1 pending message"
	result := func(id int64) protocol.DeliveryResult {
		t.Helper()
		f := readAnswer(t, c, id)
		require.Equal(t, protocol.AgentOpReportDeliveryResult, f.Op)
		var r protocol.DeliveryResult
		require.NoError(t, json.Unmarshal(f.Body, &r))
		return r
	}
	r := result(send(protocol.AgentOpPrepareDelivery, 0, protocol.PrepareDelivery{
		DeliveryID: "dlv-1", AgentID: socketAgent,
		MessageRef: protocol.MessageRef{
			MessageID: "01JB0000000000000000000011", FromAgent: "agent-2",
			Priority: protocol.PriorityNormal, ThreadID: "thread-1",
		},
		NoticeLine:             notice,
		OrchestratorDecisionID: "01JB0000000000000000000002",
		ExpectedStateVersion:   1, ExpectedInputGuardVersion: guard.InputGuardVersion,
		ExpectedDraftGeneration: guard.DraftGeneration,
		ControlVersions:         protocol.ControlVersions{"global": 18},
	}))
	require.Equal(t, protocol.DeliveryResultKindPrepared, r.Result)
	r = result(send(protocol.AgentOpCommitDelivery, 0, protocol.CommitDelivery{DeliveryID: "dlv-1", WriterLeaseID: "lease-1"}))
	require.Equal(t, protocol.DeliveryResultKindCommitted, r.Result)
	require.Equal(t, protocol.OpaqueID("lease-1"), r.WriterLeaseID)
	require.Equal(t, protocol.InjectModeTyped, r.InjectMode, "the codex profile types")
	readUntil(t, outer, "got:"+notice)

	_, err = outer.Write([]byte("\n"))
	require.NoError(t, err)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(15 * time.Second):
		t.Fatal("run never ended after the child read its line")
	}
}
