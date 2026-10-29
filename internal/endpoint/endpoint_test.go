package endpoint

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"

	giffordcrypto "github.com/smellak/gifford/internal/crypto"
	"github.com/smellak/gifford/internal/hub"
	"github.com/smellak/gifford/internal/protocol"
)

// This file holds the maintained verification of the Endpoint: identity
// through a fake Hub on a scratch socket, envelope signing, and
// idempotent acks. Nothing leaves scratch directories.

// testBody is the message body every send test uses.
const testBody = "b"

// fakeHub is a scripted hub.sock server: the Endpoint under test dials
// it with its real socket client.
type fakeHub struct {
	sock        string
	listener    *net.UnixListener
	closeWhoami bool
	whoamiAgent string
	errorMethod string
	errorCode   protocol.ErrorCode
	keyActive   bool

	mu    sync.Mutex
	calls map[string]int
}

// script reads the fake's behaviour under its mutex.
func (f *fakeHub) script() (closeWhoami bool, agent, errorMethod string, errorCode protocol.ErrorCode) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closeWhoami, f.whoamiAgent, f.errorMethod, f.errorCode
}

// recordCall counts one method under the mutex.
func (f *fakeHub) recordCall(method string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls[method]++
}

// configure scripts the fake under its mutex, so late changes cannot
// race the serving loop.
func (f *fakeHub) configure(closeWhoami bool, agent string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closeWhoami = closeWhoami
	f.whoamiAgent = agent
	f.errorMethod = ""
	f.errorCode = ""
	f.keyActive = true
}

func (f *fakeHub) suspendEndpointKey() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.keyActive = false
}

func (f *fakeHub) configureDomainError(method string, code protocol.ErrorCode) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.errorMethod = method
	f.errorCode = code
}

// startFakeHub serves the script on a scratch socket.
func startFakeHub(t *testing.T) *fakeHub {
	t.Helper()
	sock := filepath.Join(t.TempDir(), "hub.sock")
	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "unix", sock)
	require.NoError(t, err)
	fake := &fakeHub{
		sock:     sock,
		listener: listener.(*net.UnixListener),
		calls:    make(map[string]int),
	}
	go fake.serve()
	t.Cleanup(func() { require.NoError(t, listener.Close()) })
	return fake
}

// serve answers one line per line until the listener closes.
func (f *fakeHub) serve() {
	for {
		raw, err := f.listener.Accept()
		if err != nil {
			return
		}
		conn, ok := raw.(*net.UnixConn)
		if !ok {
			_ = raw.Close()
			continue
		}
		go f.serveConn(conn)
	}
}

// serveConn answers one connection line by line.
func (f *fakeHub) serveConn(conn *net.UnixConn) {
	defer func() { _ = conn.Close() }()
	scanner := bufio.NewScanner(conn)
	scanner.Buffer(make([]byte, 64*1024), 1<<20)
	for scanner.Scan() {
		var req struct {
			Method string          `json:"method"`
			ID     json.RawMessage `json:"id"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &req); err != nil {
			return
		}
		f.recordCall(req.Method)
		closeWhoami, agent, errorMethod, errorCode := f.script()
		switch req.Method {
		case "whoami":
			if closeWhoami {
				return
			}
			f.answer(conn, req.ID, map[string]string{"agent_id": agent}, nil)
		case hub.MethodRegisterEndpoint:
			f.mu.Lock()
			active := f.keyActive
			f.mu.Unlock()
			outcome := "registered"
			if !active {
				outcome = "suspended"
			}
			f.answer(conn, req.ID, map[string]any{
				"key_id": endpointKeyID(protocol.AgentID(agent)), "outcome": outcome, "active": active,
			}, nil)
		default:
			if req.Method == errorMethod {
				f.answerDomain(conn, req.ID, errorCode)
				continue
			}
			f.answer(conn, req.ID, map[string]any{"ok": true}, nil)
		}
	}
}

func (f *fakeHub) answerDomain(conn *net.UnixConn, id json.RawMessage, code protocol.ErrorCode) {
	type wireError struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Data    struct {
			ErrorCode protocol.ErrorCode `json:"error_code"`
		} `json:"data"`
	}
	type wire struct {
		JSONRPC string          `json:"jsonrpc"`
		Error   wireError       `json:"error"`
		ID      json.RawMessage `json:"id"`
	}
	out := wire{JSONRPC: jsonRPCVersion, ID: id}
	out.Error.Code = -32000
	out.Error.Message = "Request rejected"
	out.Error.Data.ErrorCode = code
	raw, err := json.Marshal(out)
	if err == nil {
		_, _ = conn.Write(append(raw, '\n'))
	}
}

// answer writes one Hub answer line.
func (f *fakeHub) answer(conn *net.UnixConn, id json.RawMessage, result any, code *int) {
	type wireError struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}
	type wire struct {
		JSONRPC string          `json:"jsonrpc"`
		Result  any             `json:"result,omitempty"`
		Error   *wireError      `json:"error,omitempty"`
		ID      json.RawMessage `json:"id"`
	}
	out := wire{JSONRPC: jsonRPCVersion, Result: result, ID: id}
	if code != nil {
		out.Result = nil
		out.Error = &wireError{Code: *code, Message: "fake"}
	}
	raw, err := json.Marshal(out)
	if err != nil {
		return
	}
	_, _ = conn.Write(append(raw, '\n'))
}

// endpointProc drives the Endpoint through a synthetic child process, so
// the server runs over real stdio pipes.
type endpointProc struct {
	stdin  io.WriteCloser
	reader *bufio.Reader
	cmd    *exec.Cmd
	done   chan error
}

// startEndpoint spawns the Endpoint child wired to the fake Hub.
func startEndpoint(t *testing.T, dataDir, sock string) *endpointProc {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=TestHelperEndpoint")
	cmd.Env = append(os.Environ(),
		"GIFFORD_ENDPOINT=1",
		"GIFFORD_HUB_SOCK="+sock,
		"GIFFORD_DATA_DIR="+dataDir,
	)
	stdin, err := cmd.StdinPipe()
	require.NoError(t, err)
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	cmd.Stderr = io.Discard
	require.NoError(t, cmd.Start())
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	return &endpointProc{stdin: stdin, reader: bufio.NewReader(stdout), cmd: cmd, done: done}
}

// TestHelperEndpoint is not a test: it runs the Endpoint over stdio for
// the tests below.
func TestHelperEndpoint(_ *testing.T) {
	if os.Getenv("GIFFORD_ENDPOINT") != "1" {
		return
	}
	if err := runHelperEndpoint(); err != nil {
		os.Exit(2)
	}
}

// runHelperEndpoint serves one stdio session against the Hub socket.
func runHelperEndpoint() error {
	ctx := context.Background()
	hub, err := DialHub(ctx, os.Getenv("GIFFORD_HUB_SOCK"))
	if err != nil {
		return err
	}
	defer func() { _ = hub.Close() }()
	server, err := New(ctx, os.Getenv("GIFFORD_DATA_DIR"), os.Stdin, os.Stdout, hub)
	if err != nil {
		return err
	}
	return server.Run(context.Background())
}

// request sends one MCP request and returns the raw frame plus the
// decoded response.
func (e *endpointProc) request(t *testing.T, method string, params any, id int) ([]byte, map[string]any) {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "method": method, "params": params, "id": id,
	})
	require.NoError(t, err)
	_, err = e.stdin.Write(append(body, '\n'))
	require.NoError(t, err)
	return e.readOne(t)
}

// readOne reads one framed response.
func (e *endpointProc) readOne(t *testing.T) ([]byte, map[string]any) {
	t.Helper()
	type outcome struct {
		raw []byte
		err error
	}
	arrived := make(chan outcome, 1)
	go func() {
		raw, err := readFrame(e.reader)
		arrived <- outcome{raw: raw, err: err}
	}()
	select {
	case got := <-arrived:
		require.NoError(t, got.err)
		var decoded map[string]any
		require.NoError(t, json.Unmarshal(got.raw, &decoded))
		return got.raw, decoded
	case <-time.After(10 * time.Second):
		t.Fatal("no MCP response within 10s")
		return nil, nil
	}
}

// close ends the session and requires the child gone.
func (e *endpointProc) close(t *testing.T) {
	t.Helper()
	require.NoError(t, e.stdin.Close())
	select {
	case err := <-e.done:
		require.NoError(t, err)
	case <-time.After(15 * time.Second):
		_ = e.cmd.Process.Kill()
		t.Fatal("endpoint child did not exit within 15s")
	}
}

// initialize performs the MCP handshake.
func (e *endpointProc) initialize(t *testing.T) {
	t.Helper()
	_, resp := e.request(t, "initialize",
		map[string]any{"protocolVersion": "2024-11-05", "capabilities": map[string]any{}}, 1)
	require.Equal(t, "2024-11-05", resp["result"].(map[string]any)["protocolVersion"])
}

// call invokes one tool and returns its result member.
func (e *endpointProc) call(t *testing.T, id int, name string, args map[string]any) any {
	t.Helper()
	_, resp := e.request(t, "tools/call",
		map[string]any{argName: name, argArguments: args}, id)
	require.NotContains(t, resp, "error")
	result, ok := resp["result"].(map[string]any)
	require.True(t, ok, "tools/call result is not an object: %v", resp)
	content, ok := result["content"].([]any)
	require.True(t, ok, "tools/call result has no content: %v", result)
	require.Len(t, content, 1)
	item, ok := content[0].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "text", item["type"])
	text, ok := item["text"].(string)
	require.True(t, ok)
	var value any
	require.NoError(t, json.Unmarshal([]byte(text), &value))
	return value
}

// callError invokes one tool and returns the raw frame plus the error
// member.
func (e *endpointProc) callError(t *testing.T, id int, name string, args map[string]any) ([]byte, map[string]any) {
	t.Helper()
	raw, resp := e.request(t, "tools/call",
		map[string]any{argName: name, argArguments: args}, id)
	require.NotContains(t, resp, "result")
	failure, ok := resp["error"].(map[string]any)
	require.True(t, ok, "call has no error member: %v", resp)
	require.NotContains(t, failure, "data")
	return raw, failure
}

// prepareIdentity creates the agent key the child endpoint will load.
func prepareIdentity(t *testing.T, dataDir string) ed25519.PublicKey {
	t.Helper()
	pub, err := EnsureAgentKey(dataDir, "agent-ep")
	require.NoError(t, err)
	return pub
}

// Common argument names.
const (
	argName               = "name"
	argArguments          = "arguments"
	argTo                 = "to"
	argSubject            = "subject"
	argBody               = "body"
	argDeliveryGeneration = "delivery_generation"
	testReplyBody         = "answer"
	toolReadMessage       = "read_message"
)

// sendArgs builds send_message arguments.
func sendArgs(to, subject, body string) map[string]any {
	return map[string]any{argTo: to, argSubject: subject, argBody: body}
}

// TestIdentityToolsList proves the handshake and the table: a verified
// agent gets the ten tools with object schemas.
func TestIdentityToolsList(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	require.NoError(t, os.Chmod(dataDir, 0o700))
	prepareIdentity(t, dataDir)
	fake := startFakeHub(t)
	fake.configure(false, "agent-ep")
	child := startEndpoint(t, dataDir, fake.sock)
	child.initialize(t)

	_, resp := child.request(t, "tools/list", nil, 2)
	tools, ok := resp["result"].(map[string]any)["tools"].([]any)
	require.True(t, ok, "tools/list has no tool array: %v", resp)
	require.Len(t, tools, len(toolNames()))
	seen := make(map[string]bool)
	for _, tool := range tools {
		entry := tool.(map[string]any)
		name := entry[argName].(string)
		seen[name] = true
		schema, ok := entry["inputSchema"].(map[string]any)
		require.True(t, ok, "tool %v has no schema", entry[argName])
		require.Equal(t, "object", schema["type"])
		if name == "ack_message" {
			properties := schema["properties"].(map[string]any)
			status := properties["status"].(map[string]any)
			require.Equal(t, []any{"PROCESSING", "ACKNOWLEDGED"}, status["enum"])
			require.Contains(t, status["description"], "ACKNOWLEDGED when the message has been handled")
		}
	}
	for _, name := range toolNames() {
		require.True(t, seen[name], "tool %s not served", name)
	}
	require.Equal(t, 1, fakeHubCalls(t, fake, "whoami"))
	require.Equal(t, 1, fakeHubCalls(t, fake, "register_endpoint"))
	child.close(t)
}

func TestSuspendedEndpointKeyBlocksSendBeforeAdmission(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	require.NoError(t, os.Chmod(dataDir, 0o700))
	fake := startFakeHub(t)
	fake.configure(false, "agent-ep")
	fake.suspendEndpointKey()
	child := startEndpoint(t, dataDir, fake.sock)
	child.initialize(t)
	raw, response := child.request(t, "tools/call", map[string]any{
		argName: toolSendMessage, argArguments: sendArgs("agent-other", "subject", "body"),
	}, 2)
	failure, ok := response["error"].(map[string]any)
	require.True(t, ok, "call has no error member: %v", response)
	require.Equal(t, -32000, mcpCode(t, failure))
	require.Equal(t, "Request rejected", failure["message"])
	data := failure["data"].(map[string]any)
	require.Equal(t, string(protocol.ErrorCodeKeyUnavailable), data["error_code"])
	require.NotContains(t, string(raw), "body")
	require.Zero(t, fakeHubCalls(t, fake, "admit_message"))
	child.close(t)
}

// TestNoIdentityNoTools proves an unverified connection gets nothing:
// the Hub closes on whoami, the list is empty, and every call fails
// without content.
func TestNoIdentityNoTools(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	require.NoError(t, os.Chmod(dataDir, 0o700))
	fake := startFakeHub(t)
	fake.configure(true, "")
	child := startEndpoint(t, dataDir, fake.sock)
	child.initialize(t)

	_, resp := child.request(t, "tools/list", nil, 2)
	tools := resp["result"].(map[string]any)["tools"].([]any)
	require.Empty(t, tools)

	raw, failure := child.callError(t, 3, toolSendMessage,
		map[string]any{"to": "a", "subject": "s", "body": testBody})
	require.Equal(t, -32001, mcpCode(t, failure))
	require.Equal(t, "No verified identity", failure["message"])
	require.NotContains(t, string(raw), "b")
	child.close(t)
}

// TestNewEnsuresAndRegistersKey proves a bound Endpoint creates its signing
// key and registers it before exposing tools.
func TestNewEnsuresAndRegistersKey(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	require.NoError(t, os.Chmod(dataDir, 0o700))
	fake := startFakeHub(t)
	fake.configure(false, "agent-nokey")
	child := startEndpoint(t, dataDir, fake.sock)
	child.initialize(t)

	_, resp := child.request(t, "tools/list", nil, 2)
	tools := resp["result"].(map[string]any)["tools"].([]any)
	require.Len(t, tools, len(toolNames()))
	require.Equal(t, 1, fakeHubCalls(t, fake, "register_endpoint"))
	require.Equal(t, os.FileMode(0o600), dirMode(t,
		filepath.Join(dataDir, agentKeysDir, "agent-nokey", agentSeedFile)))
	child.close(t)
}

// TestSendMessageRejectsSpoofedSender proves identity is never accepted from
// tool arguments and plaintext never reaches the Hub on invalid params.
func TestSendMessageRejectsSpoofedSender(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	require.NoError(t, os.Chmod(dataDir, 0o700))
	prepareIdentity(t, dataDir)
	fake := startFakeHub(t)
	fake.configure(false, "agent-ep")
	child := startEndpoint(t, dataDir, fake.sock)
	child.initialize(t)

	args := sendArgs("agent-other", "s", "hello")
	args["from"] = "evil"
	raw, failure := child.callError(t, 2, toolSendMessage, args)
	require.Equal(t, -32602, mcpCode(t, failure))
	require.Equal(t, "Invalid params", failure["message"])
	require.NotContains(t, string(raw), "hello")
	require.Zero(t, fakeHubCalls(t, fake, "admit_message"))
	child.close(t)
}

// TestReplyInvalidID proves malformed reply identity fails before admission.
func TestReplyInvalidID(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	require.NoError(t, os.Chmod(dataDir, 0o700))
	prepareIdentity(t, dataDir)
	fake := startFakeHub(t)
	fake.configure(false, "agent-ep")
	child := startEndpoint(t, dataDir, fake.sock)
	child.initialize(t)

	raw, failure := child.callError(t, 2, "reply_to_message",
		map[string]any{"id": "m1", argBody: testReplyBody})
	require.Equal(t, -32602, mcpCode(t, failure))
	require.NotContains(t, string(raw), testReplyBody)
	require.Zero(t, fakeHubCalls(t, fake, "admit_message"))
	child.close(t)
}

func TestReplyUnavailableBeforeHub(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	require.NoError(t, os.Chmod(dataDir, 0o700))
	prepareIdentity(t, dataDir)
	fake := startFakeHub(t)
	fake.configure(false, "agent-ep")
	child := startEndpoint(t, dataDir, fake.sock)
	child.initialize(t)

	raw, failure := child.callError(t, 2, "reply_to_message", map[string]any{
		"id": "01ARZ3NDEKTSV4RRFFQ69G5FAV", argBody: testReplyBody,
	})
	require.Equal(t, -32002, mcpCode(t, failure))
	require.Equal(t, "Capability unavailable", failure["message"])
	require.NotContains(t, string(raw), testReplyBody)
	require.Zero(t, fakeHubCalls(t, fake, "admit_message"))
	child.close(t)
}

// TestAckIdempotent proves each ACK state has at most one effect and the
// PROCESSING-to-ACKNOWLEDGED transition reaches the Hub.
func TestAckIdempotent(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	require.NoError(t, os.Chmod(dataDir, 0o700))
	prepareIdentity(t, dataDir)
	fake := startFakeHub(t)
	fake.configure(false, "agent-ep")
	child := startEndpoint(t, dataDir, fake.sock)
	child.initialize(t)
	const status = "status"

	const messageID = "01ARZ3NDEKTSV4RRFFQ69G5FAV"
	for _, id := range []int{2, 3} {
		result := child.call(t, id, "ack_message",
			map[string]any{"id": messageID, argDeliveryGeneration: 1, status: "PROCESSING"})
		require.True(t, result.(map[string]any)["acked"].(bool))
	}
	for _, id := range []int{4, 5} {
		result := child.call(t, id, "ack_message",
			map[string]any{"id": messageID, argDeliveryGeneration: 1, status: "ACKNOWLEDGED"})
		require.True(t, result.(map[string]any)["acked"].(bool))
	}
	require.Equal(t, 2, fakeHubCalls(t, fake, "ack_message"))
	for id, invalid := range []string{"done", "acknowledged"} {
		_, failure := child.callError(t, 6+id, "ack_message",
			map[string]any{"id": messageID, argDeliveryGeneration: 1, status: invalid})
		require.Equal(t, -32602, mcpCode(t, failure))
		require.Equal(t, "Invalid params", failure["message"])
	}
	require.Equal(t, 2, fakeHubCalls(t, fake, "ack_message"))
	child.close(t)
}

// TestTypedErrors proves failures are typed and content-free: unknown
// tools, misshapen arguments, and Hub failures never echo caller text and
// never carry data.
func TestTypedErrors(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	require.NoError(t, os.Chmod(dataDir, 0o700))
	prepareIdentity(t, dataDir)
	fake := startFakeHub(t)
	fake.configure(false, "agent-ep")
	child := startEndpoint(t, dataDir, fake.sock)
	child.initialize(t)

	_, failure := child.callError(t, 2, "nope", map[string]any{})
	require.Equal(t, -32602, mcpCode(t, failure))
	require.Equal(t, "Unknown tool", failure["message"])

	_, failure = child.callError(t, 3, toolSendMessage, map[string]any{"to": "a"})
	require.Equal(t, -32602, mcpCode(t, failure))

	_, failure = child.callError(t, 4, "get_thread", map[string]any{"thread_id": "thread-a"})
	require.Equal(t, -32002, mcpCode(t, failure))
	require.Equal(t, "Capability unavailable", failure["message"])
	require.Zero(t, fakeHubCalls(t, fake, "get_thread"))

	secret := "top-secret-body"
	raw, failure := child.callError(t, 5, toolSendMessage, sendArgs("a", "s", secret))
	require.Equal(t, -32603, mcpCode(t, failure))
	require.Equal(t, "Hub unavailable", failure["message"])
	require.NotContains(t, string(raw), secret)
	child.close(t)
}

func TestHubDomainErrorsCrossSocketAndMCP(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	require.NoError(t, os.Chmod(dataDir, 0o700))
	prepareIdentity(t, dataDir)
	fake := startFakeHub(t)
	fake.configure(false, "agent-ep")
	child := startEndpoint(t, dataDir, fake.sock)
	child.initialize(t)

	assertDomain := func(id int, tool, hubMethod string, args map[string]any, code protocol.ErrorCode) {
		t.Helper()
		fake.configureDomainError(hubMethod, code)
		raw, response := child.request(t, "tools/call",
			map[string]any{argName: tool, argArguments: args}, id)
		failure, ok := response["error"].(map[string]any)
		require.True(t, ok, "call has no error member: %v", response)
		require.Equal(t, -32000, mcpCode(t, failure), "failure: %v", failure)
		require.Equal(t, "Request rejected", failure["message"])
		data, ok := failure["data"].(map[string]any)
		require.True(t, ok)
		require.Equal(t, string(code), data["error_code"])
		require.NotContains(t, string(raw), "private backend detail")
	}
	assertDomain(2, "list_pending_messages", "list_pending_messages", map[string]any{}, protocol.ErrorCodeNotFound)
	assertDomain(3, toolSendMessage, "admit_message", sendArgs("agent-other", "subject", "body"),
		protocol.ErrorCodePolicyDenied)
	child.close(t)
}

type typedFailureHub struct {
	code protocol.ErrorCode
}

func (h typedFailureHub) WhoAmI(context.Context) (protocol.AgentID, error) {
	return "agent-ep", nil
}

func (h typedFailureHub) Call(_ context.Context, method string, _ any) (json.RawMessage, error) {
	if method == hub.MethodRegisterEndpoint {
		return json.Marshal(map[string]any{
			"key_id": endpointKeyID("agent-ep"), "outcome": "registered", "active": true,
		})
	}
	return nil, fmt.Errorf("private backend detail: %w", &HubError{Code: -32000, ErrorCode: h.code})
}

func TestHubDomainErrorsReachMCPAsStableCodes(t *testing.T) {
	t.Parallel()
	const messageID = "01ARZ3NDEKTSV4RRFFQ69G5FAV"
	tests := []struct {
		name string
		code protocol.ErrorCode
		tool string
		args map[string]any
	}{
		{"not found", protocol.ErrorCodeNotFound, toolReadMessage, map[string]any{
			"id": messageID, argDeliveryGeneration: 1,
		}},
		{"not recipient", protocol.ErrorCodeNotRecipient, toolReadMessage, map[string]any{
			"id": messageID, argDeliveryGeneration: 1,
		}},
		{"expired", protocol.ErrorCodeExpired, toolReadMessage, map[string]any{
			"id": messageID, argDeliveryGeneration: 1,
		}},
		{
			"policy denied", protocol.ErrorCodePolicyDenied, toolSendMessage,
			sendArgs("agent-other", "subject", "body"),
		},
		{"key unavailable", protocol.ErrorCodeKeyUnavailable, toolReadMessage, map[string]any{
			"id": messageID, argDeliveryGeneration: 1,
		}},
		{
			"recipient not deliverable", protocol.ErrorCodeRecipientNotDeliverable, toolSendMessage,
			sendArgs("agent-other", "subject", "body"),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			dataDir := t.TempDir()
			require.NoError(t, os.Chmod(dataDir, 0o700))
			prepareIdentity(t, dataDir)
			var output bytes.Buffer
			server, err := New(t.Context(), dataDir, bytes.NewReader(nil), &output,
				typedFailureHub{code: test.code})
			require.NoError(t, err)
			params, err := json.Marshal(map[string]any{argName: test.tool, argArguments: test.args})
			require.NoError(t, err)
			request, err := json.Marshal(mcpRequest{
				JSONRPC: jsonRPCVersion, Method: "tools/call", Params: params, ID: json.RawMessage("1"),
			})
			require.NoError(t, err)
			require.NoError(t, server.serveOne(t.Context(), request))
			var response mcpResponse
			require.NoError(t, json.Unmarshal(output.Bytes(), &response))
			require.NotNil(t, response.Error)
			require.Equal(t, -32000, response.Error.Code)
			require.Equal(t, "Request rejected", response.Error.Message)
			require.NotNil(t, response.Error.Data)
			require.Equal(t, test.code, response.Error.Data.ErrorCode)
			require.NotContains(t, output.String(), "private backend detail")
		})
	}
}

// TestAgentKeys proves key ownership: creation with 0700/0600 modes,
// stability across calls, and the XDG data directory when none is given.
func TestAgentKeys(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	require.NoError(t, os.Chmod(dataDir, 0o700))

	first, err := EnsureAgentKey(dataDir, "agent-keys")
	require.NoError(t, err)
	second, err := EnsureAgentKey(dataDir, "agent-keys")
	require.NoError(t, err)
	require.Equal(t, []byte(first), []byte(second))

	require.Equal(t, os.FileMode(0o700), dirMode(t, filepath.Join(dataDir, "keys", "agent-keys")))
	require.Equal(t, os.FileMode(0o600), dirMode(t, filepath.Join(dataDir, "keys", "agent-keys", "signing.seed")))

	priv, err := LoadAgentPrivate(dataDir, "agent-keys")
	require.NoError(t, err)
	envelope := protocol.WireEnvelope{ClientMsgID: "client-key-test"}
	require.NoError(t, giffordcrypto.SignWireEnvelope(priv, &envelope))
	require.NoError(t, giffordcrypto.VerifyWireEnvelope(first, envelope))

	_, err = EnsureAgentKey(dataDir, "UPPER")
	require.ErrorIs(t, err, ErrInvalidArgument)
	_, err = LoadAgentPrivate(dataDir, "agent-missing")
	require.ErrorIs(t, err, ErrNoIdentity)

	for _, parent := range []struct {
		name string
		path string
	}{
		{"data directory", dataDir},
		{"keys directory", filepath.Join(dataDir, "keys")},
		{"agent directory", filepath.Join(dataDir, "keys", "agent-keys")},
	} {
		require.NoError(t, os.Chmod(parent.path, 0o755), parent.name)
		_, err := LoadAgentPrivate(dataDir, "agent-keys")
		require.ErrorIs(t, err, ErrInvalidArgument, parent.name)
		require.NoError(t, os.Chmod(parent.path, 0o700), parent.name)
	}

	agentDir := filepath.Join(dataDir, "keys", "agent-keys")
	realDir := agentDir + ".real"
	require.NoError(t, os.Rename(agentDir, realDir))
	require.NoError(t, os.Symlink(filepath.Base(realDir), agentDir))
	_, err = LoadAgentPrivate(dataDir, "agent-keys")
	require.ErrorIs(t, err, ErrInvalidArgument)
	_, err = EnsureAgentKey(dataDir, "agent-keys")
	require.ErrorIs(t, err, ErrInvalidArgument)
	require.NoError(t, os.Remove(agentDir))
	require.NoError(t, os.Rename(realDir, agentDir))

	seed := filepath.Join(agentDir, agentSeedFile)
	realSeed := seed + ".real"
	require.NoError(t, os.Rename(seed, realSeed))
	require.NoError(t, os.Symlink(filepath.Base(realSeed), seed))
	_, err = LoadAgentPrivate(dataDir, "agent-keys")
	require.ErrorIs(t, err, ErrInvalidArgument)
	_, err = EnsureAgentKey(dataDir, "agent-keys")
	require.ErrorIs(t, err, ErrInvalidArgument)
}

func TestInitializeNegotiatesSupportedVersion(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	require.NoError(t, os.Chmod(dataDir, 0o700))
	fake := startFakeHub(t)
	fake.configure(false, "")
	child := startEndpoint(t, dataDir, fake.sock)
	_, resp := child.request(t, "initialize", map[string]any{
		"protocolVersion": "bogus", "capabilities": map[string]any{},
		"clientInfo": map[string]any{argName: "review", "version": "1"},
	}, 1)
	result := resp["result"].(map[string]any)
	require.Equal(t, mcpProtocolVersion, result["protocolVersion"])
	serverInfo := result["serverInfo"].(map[string]any)
	require.Equal(t, "gifford-endpoint", serverInfo[argName])
	require.NotEmpty(t, serverInfo["version"])
	child.close(t)
}

func TestMCPConformanceWithOfficialClient(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	require.NoError(t, os.Chmod(dataDir, 0o700))
	prepareIdentity(t, dataDir)
	fake := startFakeHub(t)
	fake.configure(false, "agent-ep")
	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=TestHelperEndpoint")
	cmd.Env = append(os.Environ(),
		"GIFFORD_ENDPOINT=1",
		"GIFFORD_HUB_SOCK="+fake.sock,
		"GIFFORD_DATA_DIR="+dataDir,
	)
	cmd.Stderr = io.Discard
	client := mcp.NewClient(&mcp.Implementation{Name: "gifford-review", Version: "v1.0.0"}, nil)
	session, err := client.Connect(t.Context(), &mcp.CommandTransport{Command: cmd, TerminateDuration: 3 * time.Second}, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, session.Close()) }()
	initialized := session.InitializeResult()
	require.NotNil(t, initialized)
	require.Equal(t, mcpProtocolVersion, initialized.ProtocolVersion)
	require.Equal(t, "gifford-endpoint", initialized.ServerInfo.Name)
	require.NotEmpty(t, initialized.ServerInfo.Version)
	listed, err := session.ListTools(t.Context(), nil)
	require.NoError(t, err)
	require.Len(t, listed.Tools, len(Tools()))
}

type contextBlockingHub struct{}

func (contextBlockingHub) WhoAmI(ctx context.Context) (protocol.AgentID, error) {
	<-ctx.Done()
	return "", ctx.Err()
}

func (contextBlockingHub) Call(ctx context.Context, _ string, _ any) (json.RawMessage, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestStartupPropagatesContext(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	require.NoError(t, os.Chmod(dataDir, 0o700))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := New(ctx, dataDir, bytes.NewReader(nil), io.Discard, contextBlockingHub{})
	require.ErrorIs(t, err, context.Canceled)
	_, err = DialHub(ctx, filepath.Join(dataDir, "missing.sock"))
	require.ErrorIs(t, err, context.Canceled)
}

func TestMCPLineFraming(t *testing.T) {
	t.Parallel()
	var wire bytes.Buffer
	require.NoError(t, writeFrame(&wire, mcpResponse{JSONRPC: jsonRPCVersion, Result: map[string]any{}, ID: json.RawMessage("1")}))
	require.NotContains(t, wire.String(), "Content-Length")
	require.Equal(t, 1, bytes.Count(wire.Bytes(), []byte{'\n'}))
	frame, err := readFrame(bufio.NewReader(bytes.NewReader(wire.Bytes())))
	require.NoError(t, err)
	require.True(t, json.Valid(frame))
}

func TestSocketHubCallHonoursCancellation(t *testing.T) {
	t.Parallel()
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
	hub := &SocketHub{slot: make(chan struct{}, 1), conn: client, reader: bufio.NewReader(client)}
	received := make(chan struct{})
	go func() {
		_, _ = bufio.NewReader(server).ReadBytes('\n')
		close(received)
	}()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { _, err := hub.Call(ctx, "blocked", nil); done <- err }()
	<-received
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("SocketHub.Call ignored context cancellation")
	}
	_, err := hub.Call(t.Context(), "after-cancel", nil)
	require.ErrorIs(t, err, ErrHubUnavailable)
}

func TestSocketHubRejectsMismatchedResponse(t *testing.T) {
	t.Parallel()
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
	hub := &SocketHub{slot: make(chan struct{}, 1), conn: client, reader: bufio.NewReader(client)}
	go func() {
		_, _ = bufio.NewReader(server).ReadBytes('\n')
		_, _ = io.WriteString(server, `{"jsonrpc":"2.0","id":999,"result":{"wrong":true}}`+"\n")
	}()
	_, err := hub.Call(t.Context(), "first", nil)
	require.ErrorIs(t, err, ErrHubUnavailable)
	_, err = hub.Call(t.Context(), "second", nil)
	require.ErrorIs(t, err, ErrHubUnavailable)
}

func TestSocketHubPreservesDomainErrorCode(t *testing.T) {
	t.Parallel()
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
	hubClient := &SocketHub{slot: make(chan struct{}, 1), conn: client, reader: bufio.NewReader(client)}
	go func() {
		raw, err := bufio.NewReader(server).ReadBytes('\n')
		if err != nil {
			return
		}
		var request hubRequest
		if json.Unmarshal(raw, &request) != nil {
			return
		}
		_, _ = fmt.Fprintf(server,
			`{"jsonrpc":"2.0","id":%d,"error":{"code":-32000,"message":"hidden","data":{"error_code":"NOT_FOUND"}}}`+"\n",
			request.ID)
	}()
	_, err := hubClient.Call(t.Context(), "typed", nil)
	var failure *HubError
	require.ErrorAs(t, err, &failure)
	require.Equal(t, protocol.ErrorCodeNotFound, failure.ErrorCode)
	require.NotErrorIs(t, err, ErrHubUnavailable)
}

// TestAgentKeysXDG proves an empty data directory resolves through
// internal/xdg, honouring XDG_DATA_HOME.
func TestAgentKeysXDG(t *testing.T) {
	home := t.TempDir()
	t.Setenv("XDG_DATA_HOME", home)
	pub, err := EnsureAgentKey("", "agent-xdg")
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600),
		dirMode(t, filepath.Join(home, "gifford", "keys", "agent-xdg", "signing.seed")))
	again, err := EnsureAgentKey("", "agent-xdg")
	require.NoError(t, err)
	require.Equal(t, []byte(pub), []byte(again))
}

// dirMode returns the permission bits of one path.
func dirMode(t *testing.T, path string) os.FileMode {
	t.Helper()
	info, err := os.Stat(path)
	require.NoError(t, err)
	return info.Mode().Perm()
}

// fakeHubCalls reads how often the fake saw a method.
func fakeHubCalls(t *testing.T, fake *fakeHub, method string) int {
	t.Helper()
	fake.mu.Lock()
	defer fake.mu.Unlock()
	return fake.calls[method]
}

// mcpCode reads an integer MCP error code.
func mcpCode(t *testing.T, failure map[string]any) int {
	t.Helper()
	code, ok := failure["code"].(float64)
	require.True(t, ok, "error has no numeric code: %v", failure)
	return int(code)
}
