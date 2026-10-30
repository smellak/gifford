//go:build unix

package run

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/creack/pty"
	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/cli/exitcode"
	hubcmd "github.com/smellak/gifford/internal/cli/hub"
	"github.com/smellak/gifford/internal/endpoint"
	"github.com/smellak/gifford/internal/profiles"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy"
	"github.com/smellak/gifford/internal/proxy/session"
)

// harnessFlag selects the harness profile in the run tests.
const harnessFlag = "--harness"

// codexHarness is the profile the agent tests measure with.
const codexHarness = "codex"

const (
	testEnvironmentUnset = "unset"
	testEnvironmentAgent = "agent-env"
	testEnvironmentTrue  = "true"
	testClaudeHarness    = string(protocol.HarnessClaudeCode)
)

// TestHelperProcess is the fake harness for the run tests. It runs as
// os.Args[0] -test.run=^TestHelperProcess$ -- <status|kill>: a status exits
// with it, kill SIGKILLs itself. With no argument it is a normal test run
// and skips.
func TestHelperProcess(t *testing.T) {
	if flag.NArg() == 0 {
		t.Skip("not a helper process")
	}
	// The kill mode needs SIGKILL, so it lives in the unix test file.
	if flag.Arg(0) == "kill" {
		proc, err := os.FindProcess(os.Getpid())
		require.NoError(t, err)
		require.NoError(t, proc.Kill())
		os.Exit(127)
	}
	if flag.Arg(0) == "say" {
		_, _ = fmt.Fprintln(os.Stdout, "hello-agent-say")
		os.Exit(0)
	}
	if flag.Arg(0) == "environment" {
		value, ok := os.LookupEnv(claudePromptSuggestionEnv)
		if !ok {
			value = testEnvironmentUnset
		}
		_, _ = fmt.Fprintf(os.Stdout, "<prompt-suggestion %s>\n", value)
		os.Exit(0)
	}
	if flag.Arg(0) == "whoami" {
		if flag.NArg() < 2 {
			os.Exit(2)
		}
		os.Exit(runHelperWhoami(flag.Arg(1)))
	}
	code, err := strconv.Atoi(flag.Arg(0))
	require.NoError(t, err)
	os.Exit(code)
}

// cliHelperArgv re-runs this test binary as a helper with the given mode.
func cliHelperArgv(mode string) []string {
	return []string{os.Args[0], "-test.run=^TestHelperProcess$", "--", mode}
}

// cliHelperArgvExtra re-runs this test binary as a helper with the
// given mode and extra arguments.
func cliHelperArgvExtra(mode string, extra ...string) []string {
	return append(cliHelperArgv(mode), extra...)
}

// runHelperWhoami asks hub.sock for the connection's agent and prints
// whoami=<agent>: exit 0 bound, 1 otherwise.
func runHelperWhoami(sockPath string) int {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	hub, err := endpoint.DialHub(ctx, sockPath)
	if err != nil {
		return 1
	}
	defer func() { _ = hub.Close() }()
	agent, err := hub.WhoAmI(ctx)
	if err != nil {
		return 1
	}
	fmt.Printf("whoami=%s\n", agent)
	return 0
}

// TestRunPassesExitStatusUnix runs the command on a test PTY through NewCmd:
// status 0 passes nil, 7 passes ExitError{7}, SIGKILL passes 137, stderr
// carries exactly the daemon notice, and a non-file stdin refuses with
// ErrNotTerminal. It is serial: it sets process environment.
func TestRunPassesExitStatusUnix(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	t.Setenv("GORACE", "atexit_sleep_ms=0")
	outer, slave, err := pty.Open()
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = outer.Close()
		_ = slave.Close()
	})
	require.NoError(t, pty.Setsize(outer, &pty.Winsize{Rows: 24, Cols: 80}))
	runOnPTY := func(t *testing.T, stdin io.Reader, args []string) (string, error) {
		t.Helper()
		cmd := NewCmd(nil)
		cmd.SilenceErrors = true
		cmd.SilenceUsage = true
		cmd.SetIn(stdin)
		cmd.SetOut(slave)
		var stderr strings.Builder
		cmd.SetErr(&stderr)
		cmd.SetArgs(args)
		err := cmd.Execute()
		return stderr.String(), err
	}
	t.Run("status 0", func(t *testing.T) {
		stderr, err := runOnPTY(t, slave, append([]string{"--"}, cliHelperArgv("0")...))
		require.NoError(t, err)
		require.Equal(t, proxy.DaemonNotice+"\n", stderr)
	})
	t.Run("status 7", func(t *testing.T) {
		stderr, err := runOnPTY(t, slave, append([]string{"--"}, cliHelperArgv("7")...))
		var ec *deps.ExitError
		require.ErrorAs(t, err, &ec)
		require.Equal(t, 7, ec.Code)
		require.Equal(t, proxy.DaemonNotice+"\n", stderr)
	})
	t.Run("killed", func(t *testing.T) {
		stderr, err := runOnPTY(t, slave, append([]string{"--"}, cliHelperArgv("kill")...))
		var ec *deps.ExitError
		require.ErrorAs(t, err, &ec)
		require.Equal(t, 137, ec.Code)
		require.Equal(t, proxy.DaemonNotice+"\n", stderr)
	})
	t.Run("non-terminal stdin", func(t *testing.T) {
		stderr, err := runOnPTY(t, strings.NewReader(""), append([]string{"--"}, cliHelperArgv("0")...))
		var ec *deps.ExitError
		require.ErrorAs(t, err, &ec)
		require.Equal(t, exitcode.Error, ec.Code)
		require.ErrorIs(t, ec.Err, proxy.ErrNotTerminal)
		require.Empty(t, stderr)
	})
}

// TestRunRecordsSession runs the harness through the session wrapper: the
// --harness profile is accepted, recordings/<id>.raw and
// <id>.events.jsonl (0600, pty.opened first) stay behind with an
// opaque-safe name, and an unknown harness fails instead.
func TestRunRecordsSession(t *testing.T) {
	dataHome := t.TempDir()
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
	t.Setenv("XDG_DATA_HOME", dataHome)
	t.Setenv("GORACE", "atexit_sleep_ms=0")
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
	cmd.SetArgs(append([]string{harnessFlag, codexHarness, "--"}, cliHelperArgv("0")...))
	require.NoError(t, cmd.Execute())
	require.Equal(t, proxy.DaemonNotice+"\n", stderr.String())

	recDir := filepath.Join(dataHome, "gifford", "recordings")
	entries, err := os.ReadDir(recDir)
	require.NoError(t, err)
	require.Len(t, entries, 2, "the recording and its events, side by side")
	ids, err := session.ListSessions(filepath.Join(dataHome, "gifford"))
	require.NoError(t, err)
	require.Len(t, ids, 1)
	name := ids[0]
	require.True(t, protocol.OpaqueID(name).Valid(), "recording name %q is not an opaque id", name)

	raw, err := os.ReadFile(filepath.Join(recDir, name+session.RecordingSuffix))
	require.NoError(t, err)
	require.NoError(t, session.Replay(bytes.NewReader(raw), func(session.Chunk) error {
		return nil
	}))

	evPath := filepath.Join(recDir, name+session.EventsSuffix)
	fi, err := os.Stat(evPath)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), fi.Mode().Perm())
	f, err := os.Open(evPath)
	require.NoError(t, err)
	evs, err := session.ReadEventLog(f, name)
	require.NoError(t, f.Close())
	require.NoError(t, err, "a whole session: ordered, nothing dropped, end line last")
	require.Equal(t, protocol.EventTypePTYOpened, evs[0].Type)
	require.JSONEq(t, `{"cols":80,"rows":24,"harness":"codex","harness_version":"unknown"}`, string(evs[0].Payload))
	require.Equal(t, protocol.EventTypePTYClosed, evs[len(evs)-1].Type)

	bad := NewCmd(nil)
	bad.SilenceErrors = true
	bad.SilenceUsage = true
	bad.SetIn(slave)
	bad.SetOut(slave)
	bad.SetErr(io.Discard)
	bad.SetArgs(append([]string{harnessFlag, "bogus", "--"}, cliHelperArgv("0")...))
	var ec *deps.ExitError
	require.ErrorAs(t, bad.Execute(), &ec)
	require.Equal(t, exitcode.Error, ec.Code)
}

// fakeHub is a scripted hub.sock: it answers register_agent with a
// fixed session id, records every registration, and holds connections
// open until the test ends, like the real Hub.
type fakeHub struct {
	mu       sync.Mutex
	received []protocol.RegisterAgent
	rejected []string
	open     int
	conns    []net.Conn
	listener net.Listener
}

// startFakeHub binds sockPath and serves it in the background.
func startFakeHub(t *testing.T, sockPath string) *fakeHub {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(sockPath), 0o700))
	_ = os.Remove(sockPath)
	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "unix", sockPath)
	require.NoError(t, err)
	fake := &fakeHub{listener: listener}
	t.Cleanup(func() {
		_ = listener.Close()
		fake.mu.Lock()
		defer fake.mu.Unlock()
		for _, conn := range fake.conns {
			_ = conn.Close()
		}
	})
	go fake.serve()
	return fake
}

// serve accepts connections until the listener closes.
func (f *fakeHub) serve() {
	for {
		conn, err := f.listener.Accept()
		if err != nil {
			return
		}
		f.mu.Lock()
		f.conns = append(f.conns, conn)
		f.open++
		f.mu.Unlock()
		go f.handle(conn)
	}
}

// fakeRequest is the JSON-RPC subset the fake understands.
type fakeRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
	ID      int64           `json:"id"`
}

// handle answers register_agent calls on one connection and keeps it
// open: the client closing ends the session.
func (f *fakeHub) handle(conn net.Conn) {
	defer func() {
		_ = conn.Close()
		f.mu.Lock()
		f.open--
		f.mu.Unlock()
	}()
	reader := bufio.NewReader(conn)
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			return
		}
		var req fakeRequest
		if err := json.Unmarshal(line, &req); err != nil {
			return
		}
		if req.Method != "register_agent" {
			f.reply(conn, req.ID, "", fmt.Sprintf(`{"code":%d}`, -32601))
			continue
		}
		var reg protocol.RegisterAgent
		if err := json.Unmarshal(req.Params, &reg); err != nil {
			f.reply(conn, req.ID, "", fmt.Sprintf(`{"code":%d}`, -32602))
			continue
		}
		if err := reg.Validate(); err != nil {
			f.mu.Lock()
			f.rejected = append(f.rejected, err.Error())
			f.mu.Unlock()
			f.reply(conn, req.ID, "", fmt.Sprintf(`{"code":%d}`, -32602))
			continue
		}
		f.mu.Lock()
		f.received = append(f.received, reg)
		f.mu.Unlock()
		result, _ := json.Marshal(protocol.RegisterAgentResult{
			AgentID:   reg.Manifest.AgentID,
			SessionID: "sess-1",
		})
		f.reply(conn, req.ID, string(result), "")
	}
}

// reply writes one JSON-RPC answer line: either a result or an error.
func (f *fakeHub) reply(conn net.Conn, id int64, result, failure string) {
	var line string
	if failure == "" {
		line = fmt.Sprintf(`{"jsonrpc":"2.0","result":%s,"id":%d}`+"\n", result, id)
	} else {
		line = fmt.Sprintf(`{"jsonrpc":"2.0","error":%s,"id":%d}`+"\n", failure, id)
	}
	_, _ = conn.Write([]byte(line))
}

// openConns reports how many client connections the fake still holds.
func (f *fakeHub) openConns() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.open
}

// registrations returns the register_agent bodies seen so far.
func (f *fakeHub) registrations() []protocol.RegisterAgent {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]protocol.RegisterAgent(nil), f.received...)
}

// rejections returns the registration validation failures so far.
func (f *fakeHub) rejections() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.rejected...)
}

// writeTestExecutable keeps forks out of the interval where the executable is
// open for writing. Otherwise a concurrent child can inherit the descriptor
// before close-on-exec takes effect and then fail to execute it with ETXTBSY.
func writeTestExecutable(t *testing.T, path string, body []byte) {
	t.Helper()
	syscall.ForkLock.RLock()
	defer syscall.ForkLock.RUnlock()
	require.NoError(t, os.WriteFile(path, body, 0o700))
}

// writeGateScript drops an executable child that answers --version,
// prints ready, then waits for one input line before exiting: the test
// releases it by writing to the terminal.
func writeGateScript(t *testing.T, version string) (script, gate string) {
	t.Helper()
	dir := t.TempDir()
	script = filepath.Join(dir, "fakeharness")
	gate = filepath.Join(dir, "gate")
	body := "#!/bin/sh\n" +
		"if [ \"$1\" = \"--version\" ]; then echo \"" + version + "\"; exit 0; fi\n" +
		"echo ready\n" +
		"read -r gate\n"
	writeTestExecutable(t, script, []byte(body))
	return script, gate
}

// versionProbeFixture returns a stable executable fixture through a uniquely
// named symlink. The fixture branches on that name, so the probe tests do not
// create and execute shell programs concurrently in the system temp directory.
func versionProbeFixture(t *testing.T, behavior string) string {
	t.Helper()
	target, err := filepath.Abs(filepath.Join("testdata", "version_harness.sh"))
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), behavior)
	require.NoError(t, os.Symlink(target, path))
	return path
}

// syncBuffer is a goroutine-safe stderr for background commands.
type syncBuffer struct {
	mu sync.Mutex
	sb strings.Builder
}

// Write appends to the buffer.
func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.sb.Write(p)
}

// String returns the buffer contents.
func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.sb.String()
}

// termTimeout bounds every terminal wait in the run tests.
const termTimeout = 15 * time.Second

// readUntil collects terminal bytes until substr appears or termTimeout
// elapses.
func readUntil(t *testing.T, f *os.File, substr string) []byte {
	t.Helper()
	var collected []byte
	deadline := time.Now().Add(termTimeout)
	for time.Now().Before(deadline) {
		_ = f.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
		chunk := make([]byte, 4096)
		n, err := f.Read(chunk)
		if n > 0 {
			collected = append(collected, chunk[:n]...)
		}
		if strings.Contains(string(collected), substr) {
			return collected
		}
		if err != nil && !os.IsTimeout(err) {
			t.Fatalf("terminal read: %v", err)
		}
	}
	t.Fatalf("terminal never showed %q (got %q)", substr, string(collected))
	return nil
}

// TestRunAgentRegistersAndHolds runs a gated child with --agent
// against the fake hub: the manifest and process arrive as specified,
// the connection stays open while the child lives, and closing the
// run closes it. stderr carries only the daemon notice.
func TestRunAgentRegistersAndHolds(t *testing.T) {
	runtimeHome := t.TempDir()
	dataHome := t.TempDir()
	t.Setenv("XDG_RUNTIME_DIR", runtimeHome)
	t.Setenv("XDG_DATA_HOME", dataHome)
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
	var stderr strings.Builder
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{agentFlag, "agent-1", harnessFlag, codexHarness, "--", script, gate})
	done := make(chan error, 1)
	go func() { done <- cmd.Execute() }()

	readUntil(t, outer, "ready")
	require.Eventually(t, func() bool {
		return len(fake.registrations()) == 1
	}, 10*time.Second, 10*time.Millisecond, "hub never saw register_agent")

	regs := fake.registrations()
	require.Empty(t, fake.rejections(), "fake must accept the run manifest")
	manifest := regs[0].Manifest
	require.Equal(t, int64(1), manifest.SchemaVersion)
	require.Equal(t, protocol.AgentID("agent-1"), manifest.AgentID)
	require.Equal(t, protocol.Harness(codexHarness), manifest.Harness)
	require.Equal(t, "fakeharness 1.2.3", manifest.HarnessVersion)
	require.Equal(t, protocol.AdapterTypeGenericPTY, manifest.AdapterType)
	require.Equal(t, protocol.InputGuardProviderPTYProxy, manifest.InputGuardProvider)
	require.Equal(t, protocol.CaptureModePTYProxy, manifest.CaptureMode)
	require.True(t, manifest.Deliverable)
	require.True(t, manifest.Capabilities.MCP)
	require.NotNil(t, regs[0].Process)
	require.Equal(t, int64(os.Getpid()), regs[0].Process.PID)
	require.Equal(t, 1, fake.openConns(), "session connection stays open while the child lives")

	_, err = outer.Write([]byte("\n"))
	require.NoError(t, err)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(15 * time.Second):
		t.Fatal("run never ended after releasing the child")
	}
	require.Eventually(t, func() bool {
		return fake.openConns() == 0
	}, 10*time.Second, 10*time.Millisecond, "closing the run must close the session")
	require.Equal(t, proxy.DaemonNotice+"\n", stderr.String())
}

// TestRunAgentWithoutHubRunsTransparent runs the same child with and
// without --agent and no hub.sock: the terminal bytes are identical,
// the run succeeds, and stderr gains exactly the one no-session line.
func TestRunAgentWithoutHubRunsTransparent(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	t.Setenv("GORACE", "atexit_sleep_ms=0")

	runSay := func(t *testing.T, agent string) ([]byte, string) {
		t.Helper()
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
		var args []string
		if agent != "" {
			args = append(args, agentFlag, agent)
		}
		args = append(args, append([]string{"--"}, cliHelperArgv("say")...)...)
		cmd.SetArgs(args)
		require.NoError(t, cmd.Execute())
		return readUntil(t, outer, "hello-agent-say"), stderr.String()
	}

	withAgentOut, withAgentErr := runSay(t, "agent-9")
	plainOut, plainErr := runSay(t, "")
	require.Equal(t, string(plainOut), string(withAgentOut), "no hub: --agent must not change terminal bytes")
	require.Equal(t, proxy.DaemonNotice+"\n", plainErr)
	require.Equal(t, "gifford: no hub session for this run: dial hub: hub unavailable\n"+
		proxy.DaemonNotice+"\n", withAgentErr)
}

// TestRunAgentRealHubBindsChild runs --agent against a real gifford
// hub: the harnessed child asking whoami is bound to the agent, and
// after the run ends a fresh whoami binds to nothing. Skipped until
// core/2.5-register teaches the hub register_agent: until then the
// hub answers -32601 and the run degrades to no-session.
func TestRunAgentRealHubBindsChild(t *testing.T) {
	configHome := t.TempDir()
	runtimeHome := t.TempDir()
	dataHome := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", configHome)
	t.Setenv("XDG_RUNTIME_DIR", runtimeHome)
	t.Setenv("XDG_DATA_HOME", dataHome)
	t.Setenv("GORACE", "atexit_sleep_ms=0")
	require.NoError(t, os.MkdirAll(filepath.Join(configHome, "gifford"), 0o700))
	require.NoError(t, os.WriteFile(
		filepath.Join(configHome, "gifford", "config.yaml"),
		[]byte("schema_version: 1\nnode_id: node-a\n"), 0o600))
	sockPath := filepath.Join(runtimeHome, "gifford", "hub.sock")

	hubCtx, stopHub := context.WithCancel(context.Background())
	hubDone := make(chan error, 1)
	hub := hubcmd.NewCmd(&deps.Deps{})
	hub.SilenceErrors = true
	hub.SilenceUsage = true
	hub.SetOut(io.Discard)
	hubErr := &syncBuffer{}
	hub.SetErr(hubErr)
	hub.SetArgs([]string{})
	go func() { hubDone <- hub.ExecuteContext(hubCtx) }()
	require.Eventually(t, func() bool {
		if _, err := os.Stat(sockPath); err != nil {
			return false
		}
		return strings.Contains(hubErr.String(), "ready")
	}, 15*time.Second, 20*time.Millisecond, "real hub never reported ready")

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
	cmd.SetArgs(append([]string{agentFlag, "agent-real", harnessFlag, codexHarness, "--"},
		cliHelperArgvExtra("whoami", sockPath)...))
	done := make(chan error, 1)
	go func() { done <- cmd.Execute() }()

	out := readUntil(t, outer, "whoami=agent-real")
	require.Contains(t, string(out), "whoami=agent-real")
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(15 * time.Second):
		t.Fatal("run never ended after the whoami child exited")
	}
	// The daemon is running and registration succeeded. This helper cannot
	// answer --version, so only the explicit unmeasured-profile notice appears.
	require.Equal(t, fmt.Sprintf(
		"gifford: profile %s version %q sin medir; using generic\n",
		codexHarness, unknownVersion,
	), stderr.String())

	probeCtx, cancelProbe := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancelProbe()
	probeArgv := cliHelperArgvExtra("whoami", sockPath)
	probe := exec.CommandContext(probeCtx, probeArgv[0], probeArgv[1:]...)
	probeOut, err := probe.CombinedOutput()
	require.Error(t, err, "a fresh whoami after the run must not bind")
	require.NotContains(t, string(probeOut), "whoami=agent-real")

	stopHub()
	select {
	case err := <-hubDone:
		require.NoError(t, err)
	case <-time.After(15 * time.Second):
		t.Fatal("real hub never stopped")
	}
}

// writeCountingScript drops an executable child that counts every
// invocation in counterPath, answers --version, then prints ready and
// exits: the count proves how many processes a run spawns.
func writeCountingScript(t *testing.T, version, counterPath string) string {
	t.Helper()
	script := filepath.Join(t.TempDir(), "countingharness")
	body := "#!/bin/sh\n" +
		"echo invoked >> \"$COUNT_FILE\"\n" +
		"if [ \"$1\" = \"--version\" ]; then echo \"" + version + "\"; exit 0; fi\n" +
		"echo ready\n"
	writeTestExecutable(t, script, []byte(body))
	t.Setenv("COUNT_FILE", counterPath)
	return script
}

// countInvocations returns the invocation lines in counterPath.
func countInvocations(t *testing.T, counterPath string) int {
	t.Helper()
	raw, err := os.ReadFile(counterPath)
	if os.IsNotExist(err) {
		return 0
	}
	require.NoError(t, err)
	return len(strings.Split(strings.TrimSpace(string(raw)), "\n"))
}

// recordedVersion reads the session's pty.opened harness version from
// its events file.
func recordedVersion(t *testing.T, dataHome string) string {
	t.Helper()
	ids, err := session.ListSessions(filepath.Join(dataHome, "gifford"))
	require.NoError(t, err)
	require.Len(t, ids, 1)
	raw, err := os.ReadFile(filepath.Join(dataHome, "gifford", "recordings", ids[0]+session.EventsSuffix))
	require.NoError(t, err)
	evs, err := session.ReadEventLog(bytes.NewReader(raw), ids[0])
	require.NoError(t, err)
	var opened session.PTYOpened
	require.NoError(t, json.Unmarshal(evs[0].Payload, &opened))
	return opened.Version()
}

// TestRunAgentRecordsHarnessVersion runs --agent against the fake hub
// with a counting harness: pty.opened carries the probed version,
// the manifest carries the same one, and the harness ran exactly
// twice (one probe, one session).
func TestRunAgentRecordsHarnessVersion(t *testing.T) {
	runtimeHome := t.TempDir()
	dataHome := t.TempDir()
	t.Setenv("XDG_RUNTIME_DIR", runtimeHome)
	t.Setenv("XDG_DATA_HOME", dataHome)
	t.Setenv("GORACE", "atexit_sleep_ms=0")
	fake := startFakeHub(t, filepath.Join(runtimeHome, "gifford", "hub.sock"))
	counter := filepath.Join(t.TempDir(), "count")
	script := writeCountingScript(t, "fakeharness 4.5.6", counter)

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
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{agentFlag, "agent-v", harnessFlag, codexHarness, "--", script})
	done := make(chan error, 1)
	go func() { done <- cmd.Execute() }()

	readUntil(t, outer, "ready")
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(15 * time.Second):
		t.Fatal("run never ended")
	}
	require.Equal(t, "fakeharness 4.5.6", recordedVersion(t, dataHome))
	regs := fake.registrations()
	require.Len(t, regs, 1)
	require.Equal(t, "fakeharness 4.5.6", regs[0].Manifest.HarnessVersion)
	require.Equal(t, 2, countInvocations(t, counter), "one probe plus one session run")
}

// TestRunWithoutAgentRecordsUnknownVersion runs the counting harness
// without --agent: pty.opened reads unknown and the harness ran
// exactly once, so no probe spawned.
func TestRunWithoutAgentRecordsUnknownVersion(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
	dataHome := t.TempDir()
	t.Setenv("XDG_DATA_HOME", dataHome)
	t.Setenv("GORACE", "atexit_sleep_ms=0")
	counter := filepath.Join(t.TempDir(), "count")
	script := writeCountingScript(t, "fakeharness 4.5.6", counter)

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
	cmd.SetErr(io.Discard)
	cmd.SetArgs(append([]string{"--"}, script))
	require.NoError(t, cmd.Execute())
	require.Equal(t, session.UnknownHarnessVersion, recordedVersion(t, dataHome))
	require.Equal(t, 1, countInvocations(t, counter), "no probe without --agent")
}

// TestRunClaudeChildEnvironment observes the final harness process: only a
// Claude --agent run defaults prompt suggestions off, while an explicit value,
// another harness and the transparent no-agent path keep the user environment.
func TestRunClaudeChildEnvironment(t *testing.T) {
	t.Setenv("GORACE", "atexit_sleep_ms=0")
	cases := []struct {
		name      string
		harness   string
		agent     string
		explicit  bool
		value     string
		wantValue string
	}{
		{name: "claude agent defaults false", harness: testClaudeHarness, agent: testEnvironmentAgent, wantValue: "false"},
		{name: "claude agent preserves true", harness: testClaudeHarness, agent: testEnvironmentAgent, explicit: true, value: testEnvironmentTrue, wantValue: testEnvironmentTrue},
		{name: "other harness does not add", harness: codexHarness, agent: testEnvironmentAgent, wantValue: testEnvironmentUnset},
		{name: "without agent does not add", harness: testClaudeHarness, wantValue: testEnvironmentUnset},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Let t.Setenv remember and restore any caller value, then make the
			// unset cases genuinely absent rather than explicitly empty.
			t.Setenv(claudePromptSuggestionEnv, tc.value)
			if !tc.explicit {
				require.NoError(t, os.Unsetenv(claudePromptSuggestionEnv))
			}
			t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
			t.Setenv("XDG_DATA_HOME", t.TempDir())

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
			cmd.SetErr(io.Discard)
			args := []string{harnessFlag, tc.harness}
			if tc.agent != "" {
				args = append([]string{agentFlag, tc.agent}, args...)
			}
			args = append(args, "--")
			args = append(args, cliHelperArgv("environment")...)
			cmd.SetArgs(args)

			done := make(chan error, 1)
			go func() { done <- cmd.Execute() }()
			marker := "<prompt-suggestion " + tc.wantValue + ">"
			require.Contains(t, string(readUntil(t, outer, marker)), marker)
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(termTimeout):
				t.Fatal("run never ended after the environment helper exited")
			}
			parentValue, exists := os.LookupEnv(claudePromptSuggestionEnv)
			require.Equal(t, tc.explicit, exists, "the run must not mutate the process environment")
			if tc.explicit {
				require.Equal(t, tc.value, parentValue)
			}
		})
	}
}

func TestRunProfileLookupDefaultsToEmbedded(t *testing.T) {
	t.Parallel()

	lookup, err := resolveRunProfileLookup(sessionParams{})
	require.NoError(t, err)
	got, err := lookup(codexHarness, "0.154.0")
	require.NoError(t, err)

	embedded, err := profiles.Load()
	require.NoError(t, err)
	want, err := embedded.Profile(codexHarness, "0.154.0")
	require.NoError(t, err)
	require.Equal(t, want.Harness, got.Harness)
	require.Equal(t, want.Measured, got.Measured)
	require.Equal(t, want.VersionRange, got.VersionRange)
	require.Equal(t, want.InputLinePattern, got.InputLinePattern)
}

func TestCommandUsesInstanceProfileLookup(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	t.Setenv("GORACE", "atexit_sleep_ms=0")

	fixture, err := profiles.ParseFile(codexHarness, []byte(`harness: codex
measured: true
version_range: "0.154.0"
submit: "\r"
insert_newline: "\n"
clear_line: "\u0015"
cancel: "\u0003"
bracketed_paste: false
extended_keyboard: none
input_line: "^› (.*)$"
`))
	require.NoError(t, err)

	var calls int
	lookup := func(name, version string) (profiles.Profile, error) {
		calls++
		require.Equal(t, codexHarness, name)
		require.Empty(t, version, "a run without --agent remains unprobed")
		return fixture, nil
	}
	outer, slave, err := pty.Open()
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = outer.Close()
		_ = slave.Close()
	})
	require.NoError(t, pty.Setsize(outer, &pty.Winsize{Rows: 24, Cols: 80}))

	cmd := newCmdWithProfileLookup(nil, lookup)
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	cmd.SetIn(slave)
	cmd.SetOut(slave)
	cmd.SetErr(io.Discard)
	cmd.SetArgs(append([]string{harnessFlag, codexHarness, "--"}, cliHelperArgv("0")...))
	require.NoError(t, cmd.Execute())
	require.Equal(t, 1, calls)
}

func TestSelectRunProfileUsesSharedMeasuredVersion(t *testing.T) {
	t.Parallel()

	measured := profiles.Profile{Harness: "claude-code", Measured: true, VersionRange: "2.1.280"}
	generic := profiles.Profile{Harness: genericHarness, Measured: true, VersionRange: "*"}
	selectProfile := func(t *testing.T, line string, agent bool) (profiles.Profile, string, string) {
		t.Helper()
		var selectedVersion string
		lookup := func(name, version string) (profiles.Profile, error) {
			selectedVersion = version
			if name == genericHarness {
				return generic, nil
			}
			require.Equal(t, "claude-code", name)
			if version == measured.VersionRange {
				return measured, nil
			}
			return generic, nil
		}
		var notice strings.Builder
		profile, err := selectRunProfile(lookup, "claude-code", line, agent, &notice)
		require.NoError(t, err)
		return profile, selectedVersion, notice.String()
	}

	exact, selected, notice := selectProfile(t, "2.1.280 (Claude Code)", true)
	require.Equal(t, "2.1.280", selected)
	require.Equal(t, measured, exact)
	require.Empty(t, notice)

	foreign, selected, notice := selectProfile(t, "2.1.281 (Claude Code)", true)
	require.Equal(t, "2.1.281", selected)
	require.Equal(t, genericHarness, foreign.Harness)
	require.False(t, foreign.Measured)
	require.Contains(t, notice, `profile claude-code version "2.1.281" sin medir`)

	prerelease, selected, notice := selectProfile(t, "2.1.280-beta.1", true)
	require.Equal(t, "2.1.280-beta.1", selected)
	require.Equal(t, genericHarness, prerelease.Harness)
	require.False(t, prerelease.Measured)
	require.Contains(t, notice, `profile claude-code version "2.1.280-beta.1" sin medir`)

	ambiguous, selected, notice := selectProfile(t, "2.1.280 protocol 1.0", true)
	require.Empty(t, selected)
	require.Equal(t, genericHarness, ambiguous.Harness)
	require.False(t, ambiguous.Measured)
	require.Contains(t, notice, `profile claude-code version "unknown" sin medir`)

	failedProbe := probeHarnessVersion(context.Background(), []string{filepath.Join(t.TempDir(), "missing")})
	require.Equal(t, unknownVersion, failedProbe)
	unknown, selected, notice := selectProfile(t, failedProbe, true)
	require.Empty(t, selected)
	require.Equal(t, genericHarness, unknown.Harness)
	require.False(t, unknown.Measured)
	require.Contains(t, notice, `profile claude-code version "unknown" sin medir`)

	transparent, selected, notice := selectProfile(t, "2.1.280 (Claude Code)", false)
	require.Empty(t, selected)
	require.Equal(t, genericHarness, transparent.Harness)
	require.True(t, transparent.Measured, "without --agent selection stays unchanged")
	require.Empty(t, notice)

	t.Run("unknown bypasses a measured wildcard profile", func(t *testing.T) {
		t.Parallel()
		var lookedUp []string
		lookup := func(name, version string) (profiles.Profile, error) {
			lookedUp = append(lookedUp, name+":"+version)
			if name == genericHarness {
				return generic, nil
			}
			return profiles.Profile{Harness: name, Measured: true, VersionRange: "*"}, nil
		}
		var fallbackNotice strings.Builder
		profile, err := selectRunProfile(lookup, "claude-code", unknownVersion, true, &fallbackNotice)
		require.NoError(t, err)
		require.Equal(t, []string{"claude-code:", genericHarness + ":"}, lookedUp)
		require.Equal(t, genericHarness, profile.Harness)
		require.False(t, profile.Measured)
		require.Contains(t, fallbackNotice.String(), `version "unknown" sin medir`)
	})
}

// TestProbeHarnessVersion checks the --version probe: a version line,
// nothing useful, a missing binary, and a slow answer.
func TestProbeHarnessVersion(t *testing.T) {
	t.Parallel()

	require.Equal(t, "fakeharness 9.9.9",
		probeHarnessVersion(context.Background(), []string{versionProbeFixture(t, "version"), "--version-ignored"}))
	require.Equal(t, unknownVersion, probeHarnessVersion(context.Background(), nil))
	require.Equal(t, unknownVersion,
		probeHarnessVersion(context.Background(), []string{filepath.Join(t.TempDir(), "nope")}))
	require.Equal(t, unknownVersion,
		probeHarnessVersion(context.Background(), []string{versionProbeFixture(t, "blank")}))
	started := time.Now()
	require.Equal(t, unknownVersion,
		probeHarnessVersion(context.Background(), []string{versionProbeFixture(t, "slow")}))
	require.Less(t, time.Since(started), 10*time.Second, "slow probe must time out")
}

// TestProbeHarnessVersionNormalizes checks the probe boundary: an
// internal control character falls back to unknown, 64 printable
// runes (even multibyte) pass through whole, and 65 runes fall back.
// Whatever leaves the probe always satisfies the event-log reader.
func TestProbeHarnessVersionNormalizes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	require.Equal(t, unknownVersion,
		probeHarnessVersion(ctx, []string{versionProbeFixture(t, "control")}))
	wide64 := strings.Repeat("é", 64)
	require.Equal(t, wide64,
		probeHarnessVersion(ctx, []string{versionProbeFixture(t, "wide64")}))
	require.Equal(t, unknownVersion,
		probeHarnessVersion(ctx, []string{versionProbeFixture(t, "wide65")}))
	require.True(t, session.ValidHarnessVersion(wide64))
}
