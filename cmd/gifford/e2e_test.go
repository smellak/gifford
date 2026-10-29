package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/hub"
	"github.com/smellak/gifford/internal/protocol"
)

const (
	e2eHarnessEnv      = "GIFFORD_E2E_FAKE_HARNESS"
	e2eBinaryEnv       = "GIFFORD_E2E_GIFFORD_BINARY"
	e2eJSONRPCVersion  = "2.0"
	e2eNameField       = "name"
	e2eToField         = "to"
	e2eSubjectField    = "subject"
	e2eBodyField       = "body"
	e2eClientIDField   = "client_msg_id"
	e2eGenerationField = "delivery_generation"
	e2eBeta            = "beta"
	e2eRPCTimeout      = 10 * time.Second
)

var e2eGiffordBinary string

func TestMain(m *testing.M) {
	for _, arg := range os.Args[1:] {
		if arg == "--version" {
			_, _ = fmt.Fprintln(os.Stdout, "gifford-e2e-harness 1.0")
			os.Exit(0)
		}
	}
	if os.Getenv(e2eHarnessEnv) == "1" {
		os.Exit(runE2EHarness())
	}
	var buildDir string
	if runtime.GOOS == "linux" {
		var err error
		buildDir, err = os.MkdirTemp("", "gifford-e2e-bin-*")
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "create e2e build dir: %v\n", err)
			os.Exit(2)
		}
		e2eGiffordBinary = filepath.Join(buildDir, "gifford")
		cmd := exec.Command("go", "build", "-race", "-o", e2eGiffordBinary, ".") //nolint:noctx // TestMain has no context.
		cmd.Stdout = os.Stderr
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "build e2e gifford: %v\n", err)
			_ = os.RemoveAll(buildDir)
			os.Exit(2)
		}
	}
	code := m.Run()
	if buildDir != "" {
		_ = os.RemoveAll(buildDir)
	}
	os.Exit(code)
}

type e2eHarnessCommand struct {
	Method string          `json:"method"`
	Params json.RawMessage `json:"params,omitempty"`
}

type e2eRPCRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      int64           `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type e2eRPCError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

type e2eRPCResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      int64           `json:"id"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *e2eRPCError    `json:"error,omitempty"`
}

func runE2EHarness() int {
	binary := os.Getenv(e2eBinaryEnv)
	if binary == "" {
		return 2
	}
	mcp := exec.Command(binary, "mcp") //nolint:noctx // the harness lifetime owns the child.
	stdin, err := mcp.StdinPipe()
	if err != nil {
		return 2
	}
	stdout, err := mcp.StdoutPipe()
	if err != nil {
		return 2
	}
	mcp.Stderr = os.Stderr
	if err := mcp.Start(); err != nil {
		return 2
	}
	reader := bufio.NewReader(stdout)
	nextID := int64(1)
	initialize, _ := json.Marshal(map[string]any{
		"protocolVersion": "2025-03-26",
		"capabilities":    map[string]any{},
		"clientInfo":      map[string]string{e2eNameField: "gifford-e2e", "version": "1"},
	})
	if _, err := e2eMCPCall(stdin, reader, nextID, "initialize", initialize); err != nil {
		_ = stdin.Close()
		_ = mcp.Wait()
		return 2
	}
	nextID++
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 64*1024), 1<<20)
	for scanner.Scan() {
		var command e2eHarnessCommand
		if err := json.Unmarshal(scanner.Bytes(), &command); err != nil {
			_, _ = fmt.Fprintf(os.Stdout, `{"harness_error":%q}`+"\n", err.Error())
			continue
		}
		if command.Method == "exit" {
			_ = stdin.Close()
			if err := mcp.Wait(); err != nil {
				return 2
			}
			_, _ = fmt.Fprintln(os.Stdout, `{"stopped":true}`)
			return 0
		}
		response, err := e2eMCPCall(stdin, reader, nextID, command.Method, command.Params)
		if err != nil {
			_ = stdin.Close()
			_ = mcp.Wait()
			return 2
		}
		nextID++
		_, _ = os.Stdout.Write(response)
		_, _ = os.Stdout.Write([]byte{'\n'})
	}
	_ = stdin.Close()
	if err := mcp.Wait(); err != nil {
		return 2
	}
	return 0
}

func e2eMCPCall(
	stdin io.Writer,
	reader *bufio.Reader,
	id int64,
	method string,
	params json.RawMessage,
) ([]byte, error) {
	request := e2eRPCRequest{JSONRPC: e2eJSONRPCVersion, ID: id, Method: method, Params: params}
	if err := json.NewEncoder(stdin).Encode(request); err != nil {
		return nil, err
	}
	line, err := reader.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	return bytes.TrimSpace(line), nil
}

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

type e2eProcess struct {
	cmd     *exec.Cmd
	done    chan struct{}
	mu      sync.Mutex
	waitErr error
}

func startE2EProcess(cmd *exec.Cmd) (*e2eProcess, error) {
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	process := &e2eProcess{cmd: cmd, done: make(chan struct{})}
	go func() {
		err := cmd.Wait()
		process.mu.Lock()
		process.waitErr = err
		process.mu.Unlock()
		close(process.done)
	}()
	return process, nil
}

func (p *e2eProcess) wait(timeout time.Duration) error {
	select {
	case <-p.done:
		return p.result()
	case <-time.After(timeout):
		_ = p.cmd.Process.Kill()
		<-p.done
		return fmt.Errorf("process %d did not stop", p.cmd.Process.Pid)
	}
}

func (p *e2eProcess) result() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.waitErr
}

func (p *e2eProcess) terminateAndWait() error {
	select {
	case <-p.done:
		return p.result()
	default:
	}
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return err
	}
	return p.wait(e2eRPCTimeout)
}

type e2eAgent struct {
	process *e2eProcess
	outer   *os.File
	reader  *bufio.Reader
	stderr  *lockedBuffer
}

func startE2EAgent(
	ctx context.Context,
	t *testing.T,
	env []string,
	agent protocol.AgentID,
) *e2eAgent {
	t.Helper()
	outer, slave, err := openE2EPTY()
	require.NoError(t, err)
	cmd := exec.CommandContext(ctx, e2eGiffordBinary,
		"run", "--agent", string(agent), "--harness", "generic", "--", os.Args[0])
	cmd.Env = append(append([]string{}, env...),
		e2eHarnessEnv+"=1", e2eBinaryEnv+"="+e2eGiffordBinary, "TERM=xterm-256color")
	cmd.Stdin = slave
	cmd.Stdout = slave
	stderr := &lockedBuffer{}
	cmd.Stderr = stderr
	process, err := startE2EProcess(cmd)
	_ = slave.Close()
	if err != nil {
		_ = outer.Close()
	}
	require.NoError(t, err)
	return &e2eAgent{process: process, outer: outer, reader: bufio.NewReader(outer), stderr: stderr}
}

func (a *e2eAgent) rpc(t *testing.T, method string, params any) e2eRPCResponse {
	t.Helper()
	rawParams, err := json.Marshal(params)
	require.NoError(t, err)
	rawCommand, err := json.Marshal(e2eHarnessCommand{Method: method, Params: rawParams})
	require.NoError(t, err)
	_ = a.outer.SetReadDeadline(time.Now().Add(e2eRPCTimeout))
	_, err = a.outer.Write(append(rawCommand, '\n'))
	require.NoError(t, err)
	for {
		line, readErr := a.reader.ReadString('\n')
		require.NoError(t, readErr, "agent stderr: %s", a.stderr.String())
		line = strings.TrimSpace(line)
		start, end := strings.IndexByte(line, '{'), strings.LastIndexByte(line, '}')
		if start < 0 || end < start {
			continue
		}
		var response e2eRPCResponse
		if json.Unmarshal([]byte(line[start:end+1]), &response) != nil || response.JSONRPC != "2.0" {
			continue
		}
		return response
	}
}

func (a *e2eAgent) callTool(t *testing.T, name string, args any, result any) {
	t.Helper()
	response := a.rpc(t, "tools/call", map[string]any{e2eNameField: name, "arguments": args})
	require.Nil(t, response.Error, "agent stderr: %s", a.stderr.String())
	decodeE2EToolResult(t, response.Result, result)
}

func (a *e2eAgent) stop(t *testing.T) {
	t.Helper()
	select {
	case <-a.process.done:
		require.NoError(t, a.process.result(), "agent stderr: %s", a.stderr.String())
		_ = a.outer.Close()
		return
	default:
	}
	raw, err := json.Marshal(e2eHarnessCommand{Method: "exit"})
	require.NoError(t, err)
	_, err = a.outer.Write(append(raw, '\n'))
	require.NoError(t, err)
	require.NoError(t, a.process.wait(e2eRPCTimeout), "agent stderr: %s", a.stderr.String())
	require.NoError(t, a.outer.Close())
}

type e2eToolResult struct {
	Content []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"content"`
}

func decodeE2EToolResult(t *testing.T, raw json.RawMessage, result any) {
	t.Helper()
	var tool e2eToolResult
	require.NoError(t, json.Unmarshal(raw, &tool))
	require.Len(t, tool.Content, 1)
	require.Equal(t, "text", tool.Content[0].Type)
	require.NoError(t, json.Unmarshal([]byte(tool.Content[0].Text), result))
}

type e2eMCPClient struct {
	process *e2eProcess
	stdin   io.WriteCloser
	reader  *bufio.Reader
	stderr  *lockedBuffer
	nextID  int64
}

func startE2EMCP(ctx context.Context, t *testing.T, env []string) *e2eMCPClient {
	t.Helper()
	cmd := exec.CommandContext(ctx, e2eGiffordBinary, "mcp")
	cmd.Env = env
	stdin, err := cmd.StdinPipe()
	require.NoError(t, err)
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	stderr := &lockedBuffer{}
	cmd.Stderr = stderr
	process, err := startE2EProcess(cmd)
	require.NoError(t, err)
	return &e2eMCPClient{
		process: process, stdin: stdin, reader: bufio.NewReader(stdout), stderr: stderr, nextID: 1,
	}
}

func (c *e2eMCPClient) call(t *testing.T, method string, params any) e2eRPCResponse {
	t.Helper()
	rawParams, err := json.Marshal(params)
	require.NoError(t, err)
	request := e2eRPCRequest{JSONRPC: e2eJSONRPCVersion, ID: c.nextID, Method: method, Params: rawParams}
	c.nextID++
	require.NoError(t, json.NewEncoder(c.stdin).Encode(request))
	line, err := c.reader.ReadBytes('\n')
	require.NoError(t, err, "mcp stderr: %s", c.stderr.String())
	var response e2eRPCResponse
	require.NoError(t, json.Unmarshal(line, &response))
	return response
}

func (c *e2eMCPClient) stop(t *testing.T) {
	t.Helper()
	require.NoError(t, c.stdin.Close())
	require.NoError(t, c.process.wait(e2eRPCTimeout), "mcp stderr: %s", c.stderr.String())
}

type e2eEnvironment struct {
	env        []string
	runtimeDir string
}

func newE2EEnvironment(t *testing.T) e2eEnvironment {
	t.Helper()
	root := t.TempDir()
	configHome := filepath.Join(root, "config")
	dataHome := filepath.Join(root, "data")
	runtimeHome := filepath.Join(root, "runtime")
	for _, dir := range []string{configHome, dataHome, runtimeHome, filepath.Join(configHome, "gifford")} {
		require.NoError(t, os.MkdirAll(dir, 0o700))
		require.NoError(t, os.Chmod(dir, 0o700))
	}
	require.NoError(t, os.WriteFile(filepath.Join(configHome, "gifford", "config.yaml"),
		[]byte("schema_version: 1\nnode_id: node-a\n"), 0o600))
	env := append(os.Environ(),
		"XDG_CONFIG_HOME="+configHome,
		"XDG_DATA_HOME="+dataHome,
		"XDG_RUNTIME_DIR="+runtimeHome,
		"GORACE=atexit_sleep_ms=0",
	)
	return e2eEnvironment{env: env, runtimeDir: filepath.Join(runtimeHome, "gifford")}
}

func startE2EHub(ctx context.Context, t *testing.T, environment e2eEnvironment) (*e2eProcess, *lockedBuffer) {
	t.Helper()
	cmd := exec.CommandContext(ctx, e2eGiffordBinary, "hub")
	cmd.Env = environment.env
	stderrPipe, err := cmd.StderrPipe()
	require.NoError(t, err)
	stdout := &lockedBuffer{}
	cmd.Stdout = stdout
	process, err := startE2EProcess(cmd)
	require.NoError(t, err)
	ready := make(chan struct{})
	lines := &lockedBuffer{}
	go func() {
		scanner := bufio.NewScanner(stderrPipe)
		found := false
		for scanner.Scan() {
			line := scanner.Text()
			_, _ = fmt.Fprintln(lines, line)
			if !found && strings.Contains(line, "ready: serving") {
				found = true
				close(ready)
			}
		}
	}()
	select {
	case <-ready:
	case <-process.done:
		require.FailNow(t, "hub stopped before ready", "stderr: %s", lines.String())
	case <-time.After(e2eRPCTimeout):
		require.FailNow(t, "hub did not become ready", "stderr: %s", lines.String())
	}
	require.Empty(t, stdout.String())
	return process, lines
}

func runE2ECLI(ctx context.Context, t *testing.T, env []string, result any, args ...string) {
	t.Helper()
	cmd := exec.CommandContext(ctx, e2eGiffordBinary, args...)
	cmd.Env = env
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	output, err := cmd.Output()
	require.NoError(t, err, "stderr: %s", stderr.String())
	require.NoError(t, json.Unmarshal(output, result), "output: %s", output)
}

func requireNoE2ESockets(t *testing.T, runtimeDir string) {
	t.Helper()
	require.Eventually(t, func() bool {
		for _, name := range []string{"hub.sock", "daemon.sock"} {
			if _, err := os.Lstat(filepath.Join(runtimeDir, name)); !errors.Is(err, os.ErrNotExist) {
				return false
			}
		}
		return true
	}, 2*time.Second, 20*time.Millisecond)
}

func TestRealProcessMessageLifecycle(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("requires Linux PTYs and SO_PEERCRED")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 55*time.Second)
	defer cancel()
	environment := newE2EEnvironment(t)
	hubProcess, hubStderr := startE2EHub(ctx, t, environment)
	t.Cleanup(func() {
		_ = hubProcess.terminateAndWait()
	})

	alpha := startE2EAgent(ctx, t, environment.env, "alpha")
	t.Cleanup(func() {
		select {
		case <-alpha.process.done:
		default:
			_ = alpha.process.terminateAndWait()
		}
		_ = alpha.outer.Close()
	})
	beta := startE2EAgent(ctx, t, environment.env, e2eBeta)
	t.Cleanup(func() {
		select {
		case <-beta.process.done:
		default:
			_ = beta.process.terminateAndWait()
		}
		_ = beta.outer.Close()
	})

	var pending hub.ListPendingMessagesResult
	beta.callTool(t, "list_pending_messages", map[string]any{}, &pending)
	require.Empty(t, pending.Messages)

	const firstBody = "first end-to-end body"
	var first protocol.AdmissionResult
	alpha.callTool(t, "send_message", map[string]any{
		e2eToField: e2eBeta, e2eSubjectField: "first", e2eBodyField: firstBody,
		e2eClientIDField: "e2e-message-one",
	}, &first)
	require.Equal(t, protocol.AdmissionStatusAccepted, first.Result)
	require.True(t, first.MessageID.Valid())

	beta.callTool(t, "list_pending_messages", map[string]any{}, &pending)
	require.Len(t, pending.Messages, 1)
	require.Equal(t, first.MessageID, pending.Messages[0].MessageID)
	firstGeneration := pending.Messages[0].DeliveryGeneration
	var readFirst protocol.LogicalMessage
	beta.callTool(t, "read_message", map[string]any{
		"id": first.MessageID, e2eGenerationField: firstGeneration,
	}, &readFirst)
	require.Equal(t, firstBody, readFirst.Body)
	var acked map[string]bool
	beta.callTool(t, "ack_message", map[string]any{
		"id": first.MessageID, e2eGenerationField: firstGeneration, "status": "ACKNOWLEDGED",
	}, &acked)
	require.True(t, acked["acked"])

	var firstStatus hub.GetDeliveryStatusResult
	alpha.callTool(t, "get_delivery_status", map[string]any{"message_id": first.MessageID}, &firstStatus)
	require.Len(t, firstStatus.Recipients, 1)
	require.Equal(t, hub.AckMessageAcknowledged, firstStatus.Recipients[0].AckStatus)

	var links protocol.LinkList
	runE2ECLI(ctx, t, environment.env, &links, "ctl", "link", "set", "alpha", e2eBeta, "hold")
	const (
		secondBody = "held end-to-end body"
		secondID   = "e2e-message-two"
	)
	var second protocol.AdmissionResult
	alpha.callTool(t, "send_message", map[string]any{
		e2eToField: e2eBeta, e2eSubjectField: "held", e2eBodyField: secondBody,
		e2eClientIDField: secondID,
	}, &second)
	require.Equal(t, protocol.AdmissionStatusAcceptedHeld, second.Result)
	require.Equal(t, protocol.DecisionReasonLinkHold, second.ControlReason)
	beta.callTool(t, "list_pending_messages", map[string]any{}, &pending)
	require.Empty(t, pending.Messages)

	var held protocol.Explanation
	runE2ECLI(ctx, t, environment.env, &held,
		"ctl", "channel", "explain", "--message", string(second.MessageID))
	require.Equal(t, protocol.DecisionOutcomeHold, held.Outcome)
	require.Equal(t, protocol.DecisionReasonLinkHold, held.Reason)
	require.Equal(t, protocol.ExplanationScopeLink, held.Blocker)

	runE2ECLI(ctx, t, environment.env, &links, "ctl", "link", "set", "alpha", e2eBeta, "allow")
	beta.callTool(t, "list_pending_messages", map[string]any{}, &pending)
	require.Len(t, pending.Messages, 1)
	require.Equal(t, second.MessageID, pending.Messages[0].MessageID)
	secondGeneration := pending.Messages[0].DeliveryGeneration
	var readSecond protocol.LogicalMessage
	beta.callTool(t, "read_message", map[string]any{
		"id": second.MessageID, e2eGenerationField: secondGeneration,
	}, &readSecond)
	require.Equal(t, secondBody, readSecond.Body)
	beta.callTool(t, "ack_message", map[string]any{
		"id": second.MessageID, e2eGenerationField: secondGeneration, "status": "ACKNOWLEDGED",
	}, &acked)
	require.True(t, acked["acked"])

	var released protocol.Explanation
	runE2ECLI(ctx, t, environment.env, &released,
		"ctl", "channel", "explain", "--message", string(second.MessageID))
	require.Equal(t, protocol.DecisionOutcomeAllow, released.Outcome)
	require.Equal(t, protocol.DecisionReasonHoldReleased, released.Reason)

	var retried protocol.AdmissionResult
	alpha.callTool(t, "send_message", map[string]any{
		e2eToField: e2eBeta, e2eSubjectField: "held", e2eBodyField: secondBody,
		e2eClientIDField: secondID,
	}, &retried)
	require.Equal(t, second.MessageID, retried.MessageID)
	beta.callTool(t, "list_pending_messages", map[string]any{}, &pending)
	require.Empty(t, pending.Messages)

	outside := startE2EMCP(ctx, t, environment.env)
	t.Cleanup(func() {
		_ = outside.stdin.Close()
		select {
		case <-outside.process.done:
		default:
			_ = outside.process.terminateAndWait()
		}
	})
	tools := outside.call(t, "tools/list", map[string]any{})
	require.Nil(t, tools.Error, "mcp stderr: %s", outside.stderr.String())
	var listed struct {
		Tools []json.RawMessage `json:"tools"`
	}
	require.NoError(t, json.Unmarshal(tools.Result, &listed))
	require.Empty(t, listed.Tools)
	failed := outside.call(t, "tools/call", map[string]any{
		e2eNameField: "send_message", "arguments": map[string]any{
			e2eToField: e2eBeta, e2eSubjectField: "outside", e2eBodyField: "must fail",
		},
	})
	require.NotNil(t, failed.Error)
	require.Equal(t, -32001, failed.Error.Code)
	require.Equal(t, "No verified identity", failed.Error.Message)
	outside.stop(t)

	alpha.stop(t)
	beta.stop(t)
	require.NoError(t, hubProcess.terminateAndWait(),
		"hub stderr: %s", hubStderr.String())
	requireNoE2ESockets(t, environment.runtimeDir)
}
