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

// socketAgent is the agent of the socket test runs.
const socketAgent = "agent-1"

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
	body, err := json.Marshal(protocol.RegisterAgentResult{AgentID: socketAgent, SessionID: "sess-1"})
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
