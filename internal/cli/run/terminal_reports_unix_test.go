//go:build unix

package run

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/creack/pty"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/smellak/gifford/internal/agentsock"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy/session"
)

const (
	terminalReportHelperEnv = "GIFFORD_TEST_TERMINAL_REPORT_HELPER"
	terminalReportHarness   = "GIFFORD_TEST_TERMINAL_REPORT_HARNESS"
	terminalReportGate      = "GIFFORD_TEST_TERMINAL_REPORT_GATE"
	terminalReportAgent     = "agent-terminal"
	terminalReportSession   = "sess-1"
)

// TestTerminalReportRunHelper is the process a real tmux pane starts. It runs
// the production gifford run command against the synthetic harness, using the
// pane's real terminal as stdin/stdout. A normal package test skips it.
func TestTerminalReportRunHelper(t *testing.T) {
	if os.Getenv(terminalReportHelperEnv) != "1" {
		t.Skip("not the terminal-report helper process")
	}
	cmd := NewCmd(nil)
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	cmd.SetIn(os.Stdin)
	cmd.SetOut(os.Stdout)
	cmd.SetErr(os.Stderr)
	cmd.SetArgs([]string{
		agentFlag, terminalReportAgent, harnessFlag, "generic", "--",
		os.Getenv(terminalReportHarness), os.Getenv(terminalReportGate),
	})
	require.NoError(t, cmd.ExecuteContext(t.Context()))
}

const terminalReportHarnessBody = `#!/usr/bin/env bash
set -eu
if [[ "${1-}" == "--version" ]]; then
	printf 'terminal-report 1.0\n'
	exit 0
fi
exec 3<>"$1"
stty raw -echo
printf 'terminal-report-ready\r\n'
IFS= read -r _ <&3

read_terminal_reply() {
	local reply="" byte=""
	while IFS= read -r -N 1 byte; do
		reply+="$byte"
		if [[ "$byte" == $'\007' || "$reply" == *$'\033\\' ]]; then
			break
		fi
		if (( ${#reply} > 4096 )); then
			return 1
		fi
	done
	printf '%s' "$reply" | od -An -tx1 | tr -d ' \n'
}

printf '\033[>0q'
xtversion="$(read_terminal_reply)"
printf '\033]11;?\033\\'
osc11="$(read_terminal_reply)"
printf '\r\nterminal-report-done xt=%s osc=%s\r\n' "$xtversion" "$osc11"
IFS= read -r _ <&3
`

type terminalQuery struct {
	query    []byte
	response []byte
}

var terminalQueries = []terminalQuery{
	{query: []byte("\x1b[c"), response: []byte("\x1b[?1;2c")},
	{query: []byte("\x1b[>c"), response: []byte("\x1b[>0;95;0c")},
	{query: []byte("\x1b[>q"), response: []byte("\x1bP>|xterm 390\x1b\\")},
	{query: []byte("\x1b[>0q"), response: []byte("\x1bP>|xterm 390\x1b\\")},
	{query: []byte("\x1b]10;?\x1b\\"), response: []byte("\x1b]10;rgb:eeee/eeee/eeee\x1b\\")},
	{query: []byte("\x1b]10;?\a"), response: []byte("\x1b]10;rgb:eeee/eeee/eeee\x1b\\")},
	{query: []byte("\x1b]11;?\x1b\\"), response: []byte("\x1b]11;rgb:1111/2222/3333\x1b\\")},
	{query: []byte("\x1b]11;?\a"), response: []byte("\x1b]11;rgb:1111/2222/3333\x1b\\")},
}

// syntheticTerminal is the only peer attached to the private tmux server. It
// drains all display bytes and answers terminal queries, but sends no key.
type syntheticTerminal struct {
	mu     sync.Mutex
	output []byte
	seen   []int
	pty    *os.File
	cmd    *exec.Cmd
	done   chan error
}

func startSyntheticTerminal(t *testing.T, command string, env []string, socket, target string) *syntheticTerminal {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), command, "-S", socket, "attach-session", "-t", target)
	cmd.Env = env
	terminal, err := pty.Start(cmd)
	require.NoError(t, err)
	require.NoError(t, pty.Setsize(terminal, &pty.Winsize{Rows: 24, Cols: 80}))
	s := &syntheticTerminal{pty: terminal, cmd: cmd, done: make(chan error, 1), seen: make([]int, len(terminalQueries))}
	go s.serve()
	t.Cleanup(func() {
		_ = terminal.Close()
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		select {
		case serveErr := <-s.done:
			require.NoError(t, serveErr)
		case <-time.After(2 * time.Second):
		}
	})
	return s
}

func (s *syntheticTerminal) serve() {
	buf := make([]byte, 32*1024)
	for {
		n, err := s.pty.Read(buf)
		if n > 0 {
			s.mu.Lock()
			s.output = append(s.output, buf[:n]...)
			var replies [][]byte
			for i, q := range terminalQueries {
				count := bytes.Count(s.output, q.query)
				for s.seen[i] < count {
					replies = append(replies, q.response)
					s.seen[i]++
				}
			}
			s.mu.Unlock()
			for _, reply := range replies {
				if _, writeErr := s.pty.Write(reply); writeErr != nil {
					s.done <- writeErr
					return
				}
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, os.ErrClosed) || errors.Is(err, unix.EIO) {
				err = nil
			}
			s.done <- err
			return
		}
	}
}

func (s *syntheticTerminal) snapshot() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]byte(nil), s.output...)
}

func (s *syntheticTerminal) sawStartupQueries() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.seen[0] > 0 && s.seen[1] > 0 && s.seen[2] > 0 && s.seen[6] > 0
}

func terminalTMUXEnvironment() []string {
	environment := make([]string, 0, len(os.Environ())+1)
	for _, variable := range os.Environ() {
		if strings.HasPrefix(variable, "TMUX=") || strings.HasPrefix(variable, "TMUX_PANE=") ||
			strings.HasPrefix(variable, "TERM=") {
			continue
		}
		environment = append(environment, variable)
	}
	return append(environment, "TERM=xterm-256color")
}

func runTerminalTMUX(t *testing.T, command string, env []string, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, command, args...)
	cmd.Env = env
	cmd.WaitDelay = 500 * time.Millisecond
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, string(output))
	return string(output)
}

func terminalGuardHandshake(t *testing.T, runtimeHome string) (net.Conn, protocol.InputGuardReport) {
	t.Helper()
	path := filepath.Join(runtimeHome, "gifford", "agents", terminalReportAgent+".sock")
	require.Eventually(t, func() bool {
		_, err := os.Lstat(path)
		return err == nil
	}, 10*time.Second, 10*time.Millisecond, "gifford run never opened the agent socket")
	conn, err := (&net.Dialer{}).DialContext(t.Context(), "unix", path)
	require.NoError(t, err)
	require.NoError(t, conn.SetDeadline(time.Now().Add(20*time.Second)))
	hello, err := agentsock.ReadFrame(conn)
	require.NoError(t, err)
	require.Equal(t, protocol.AgentOpRegisterAgent, hello.Op)
	replyTo := hello.FrameID
	body, err := json.Marshal(protocol.RegisterAgentResult{AgentID: terminalReportAgent, SessionID: terminalReportSession})
	require.NoError(t, err)
	require.NoError(t, agentsock.WriteFrame(conn, protocol.AgentFrame{
		Op: protocol.AgentOpRegisterAgentResult, FrameID: 1, ReplyTo: &replyTo,
		SentAt: time.Date(2026, 10, 31, 0, 0, 0, 0, time.UTC), Body: body,
	}))
	frame, err := agentsock.ReadFrame(conn)
	require.NoError(t, err)
	require.Equal(t, protocol.AgentOpReportInputGuard, frame.Op)
	var report protocol.InputGuardReport
	require.NoError(t, json.Unmarshal(frame.Body, &report))
	return conn, report
}

func resyncTerminalGuard(t *testing.T, conn net.Conn) protocol.InputGuardReport {
	t.Helper()
	body, err := json.Marshal(protocol.AgentTarget{AgentID: terminalReportAgent})
	require.NoError(t, err)
	const frameID = 2
	require.NoError(t, agentsock.WriteFrame(conn, protocol.AgentFrame{
		Op: protocol.AgentOpResync, FrameID: frameID,
		SentAt: time.Date(2026, 10, 31, 0, 0, 1, 0, time.UTC), Body: body,
	}))
	frame := readAnswer(t, conn, frameID)
	require.Equal(t, protocol.AgentOpReportInputGuard, frame.Op)
	var report protocol.InputGuardReport
	require.NoError(t, json.Unmarshal(frame.Body, &report))
	return report
}

func terminalRunEvents(t *testing.T, dataHome string) []session.LoggedEvent {
	t.Helper()
	dataDir := filepath.Join(dataHome, "gifford")
	ids, err := session.ListSessions(dataDir)
	require.NoError(t, err)
	require.Len(t, ids, 1)
	file, err := os.Open(filepath.Join(dataDir, "recordings", ids[0]+session.EventsSuffix))
	require.NoError(t, err)
	events, readErr := session.ReadEventLog(file, ids[0])
	require.NoError(t, errors.Join(readErr, file.Close()))
	return events
}

// TestRunIgnoresRealTMUXTerminalResponses reproduces the production failure
// without a model or a human key. A synthetic harness under a real private
// tmux server emits XTVERSION and OSC 11 queries. The attached terminal answers
// tmux's queries; both replies reach the child intact, while the guard report
// and semantic input-event counters remain byte-for-byte unchanged.
func TestRunIgnoresRealTMUXTerminalResponses(t *testing.T) {
	tmux, err := exec.LookPath("tmux")
	if err != nil {
		t.Skip("tmux is not installed")
	}
	runtimeHome := t.TempDir()
	dataHome := t.TempDir()
	configHome := t.TempDir()
	t.Setenv("XDG_RUNTIME_DIR", runtimeHome)
	t.Setenv("XDG_DATA_HOME", dataHome)
	t.Setenv("XDG_CONFIG_HOME", configHome)
	t.Setenv("GORACE", "atexit_sleep_ms=0")
	startFakeHub(t, filepath.Join(runtimeHome, "gifford", "hub.sock"))

	testDir := t.TempDir()
	harness := filepath.Join(testDir, "terminal-report-harness")
	writeTestExecutable(t, harness, []byte(terminalReportHarnessBody))
	gate := filepath.Join(testDir, "gate")
	require.NoError(t, unix.Mkfifo(gate, 0o600))
	gateFile, err := os.OpenFile(gate, os.O_RDWR, 0o600)
	require.NoError(t, err)
	t.Cleanup(func() { _ = gateFile.Close() })
	wrapper := filepath.Join(testDir, "gifford-run-helper")
	writeTestExecutable(t, wrapper, []byte("#!/bin/sh\nexec "+strconv.Quote(os.Args[0])+" -test.run=^TestTerminalReportRunHelper$\n"))
	t.Setenv(terminalReportHelperEnv, "1")
	t.Setenv(terminalReportHarness, harness)
	t.Setenv(terminalReportGate, gate)

	env := terminalTMUXEnvironment()
	tmuxVersion := strings.TrimSpace(runTerminalTMUX(t, tmux, env, "-V"))
	socket := filepath.Join(testDir, "tmux.sock")
	const target = "terminal-report"
	runTerminalTMUX(t, tmux, env, "-S", socket, "new-session", "-d", "-s", target, "-x", "80", "-y", "24", wrapper)
	stopped := false
	stop := func(requireSuccess bool) {
		if stopped {
			return
		}
		stopped = true
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, tmux, "-S", socket, "kill-server")
		cmd.Env = env
		err := cmd.Run()
		if requireSuccess {
			require.NoError(t, err)
		}
	}
	t.Cleanup(func() { stop(false) })
	runTerminalTMUX(t, tmux, env, "-S", socket, "set-option", "-t", target, "remain-on-exit", "on")
	terminal := startSyntheticTerminal(t, tmux, env, socket, target)
	require.Eventually(t, terminal.sawStartupQueries, 5*time.Second, 10*time.Millisecond, "synthetic terminal never answered tmux startup")
	require.Eventually(t, func() bool {
		return bytes.Contains(terminal.snapshot(), []byte("terminal-report-ready"))
	}, 10*time.Second, 10*time.Millisecond, "harness never became ready")

	conn, before := terminalGuardHandshake(t, runtimeHome)
	t.Cleanup(func() { _ = conn.Close() })
	require.Equal(t, protocol.DraftStateClosed, before.DraftState)
	require.True(t, before.LastHumanKeyAt.IsZero())
	_, err = gateFile.WriteString("go\n")
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return bytes.Contains(terminal.snapshot(), []byte("terminal-report-done"))
	}, 10*time.Second, 10*time.Millisecond, "harness did not receive both terminal replies")
	output := string(terminal.snapshot())
	expectedXTVERSION := hex.EncodeToString([]byte("\x1bP>|" + tmuxVersion + "\x1b\\"))
	expectedOSC11 := hex.EncodeToString([]byte("\x1b]11;rgb:1111/2222/3333\x1b\\"))
	require.Contains(t, output, "terminal-report-done xt="+expectedXTVERSION+" osc="+expectedOSC11,
		"the child must receive both terminal replies byte-for-byte")

	after := resyncTerminalGuard(t, conn)
	require.Equal(t, before, after, "terminal responses must not move human guard state or counters")
	_, err = gateFile.WriteString("quit\n")
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return strings.TrimSpace(runTerminalTMUX(t, tmux, env,
			"-S", socket, "display-message", "-p", "-t", target, "#{pane_dead}")) == "1"
	}, 10*time.Second, 20*time.Millisecond, "gifford run helper never exited")

	semanticInputEvents := map[protocol.EventType]bool{
		protocol.EventTypeInputControlKey:     true,
		protocol.EventTypeInputKeyChunk:       true,
		protocol.EventTypeInputSubmitDetected: true,
		protocol.EventTypeInputPasteStarted:   true,
		protocol.EventTypeInputPasteChunk:     true,
		protocol.EventTypeInputPasteCompleted: true,
		protocol.EventTypeInputActivityPaused: true,
		protocol.EventTypeInputDraftOpened:    true,
		protocol.EventTypeInputDraftUpdated:   true,
		protocol.EventTypeInputDraftUnknown:   true,
		protocol.EventTypeInputDraftSubmitted: true,
		protocol.EventTypeInputDraftCancelled: true,
		protocol.EventTypeInputDraftCleared:   true,
	}
	for _, event := range terminalRunEvents(t, dataHome) {
		if semanticInputEvents[event.Type] {
			t.Fatalf("terminal response created semantic input event %s", event.Type)
		}
	}
	stop(true)
}
