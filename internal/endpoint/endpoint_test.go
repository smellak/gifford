package endpoint

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// This file holds the maintained verification of the Endpoint: identity
// through a fake Hub on a scratch socket, envelope signing, and
// idempotent acks. Nothing leaves scratch directories.

// testBody is the message body every send test uses.
const testBody = "b"

// cannedVerdict is the admission verdict the fake Hub returns. It must
// satisfy protocol.AdmissionResult.Validate.
const cannedVerdict = `{"result":"ACCEPTED","decision_id":"chd_01ARZ3NDEKTSV4RRFFQ69G5FAV",` +
	`"message_id":"01ARZ3NDEKTSV4RRFFQ69G5FAW","thread_id":"thread-1","control_version":0}`

// fakeHub is a scripted hub.sock server: the Endpoint under test dials
// it with its real socket client.
type fakeHub struct {
	sock        string
	listener    *net.UnixListener
	closeWhoami bool
	whoamiAgent string
	admitResult json.RawMessage
	admitCode   *int

	mu          sync.Mutex
	calls       map[string]int
	admitParams []json.RawMessage
}

// script reads the fake's behaviour under its mutex.
func (f *fakeHub) script() (closeWhoami bool, agent string, admit json.RawMessage, code *int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closeWhoami, f.whoamiAgent, f.admitResult, f.admitCode
}

// recordCall counts one method and stores admit params under the mutex.
func (f *fakeHub) recordCall(method string, params json.RawMessage) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls[method]++
	if method == "admit_message" {
		f.admitParams = append(f.admitParams, append(json.RawMessage(nil), params...))
	}
}

// configure scripts the fake under its mutex, so late changes cannot
// race the serving loop.
func (f *fakeHub) configure(closeWhoami bool, agent string, code *int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closeWhoami = closeWhoami
	f.whoamiAgent = agent
	f.admitCode = code
}

// admitParamsCopy returns the recorded admit params under the mutex.
func (f *fakeHub) admitParamsCopy() []json.RawMessage {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]json.RawMessage(nil), f.admitParams...)
}

// startFakeHub serves the script on a scratch socket. A nil admitResult
// with a nil admitCode answers admit_message with the canned verdict.
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
			Params json.RawMessage `json:"params"`
			ID     json.RawMessage `json:"id"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &req); err != nil {
			return
		}
		f.recordCall(req.Method, req.Params)
		closeWhoami, agent, admit, code := f.script()
		switch req.Method {
		case "whoami":
			if closeWhoami {
				return
			}
			f.answer(conn, req.ID, map[string]string{"agent_id": agent}, nil)
		case "admit_message":
			if code != nil {
				f.answer(conn, req.ID, nil, code)
			} else {
				if admit == nil {
					admit = json.RawMessage(cannedVerdict)
				}
				f.answer(conn, req.ID, admit, nil)
			}
		default:
			f.answer(conn, req.ID, map[string]any{"ok": true}, nil)
		}
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
	out := wire{JSONRPC: "2.0", Result: result, ID: id}
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
	hub, err := DialHub(os.Getenv("GIFFORD_HUB_SOCK"))
	if err != nil {
		return err
	}
	defer func() { _ = hub.Close() }()
	server, err := New(os.Getenv("GIFFORD_DATA_DIR"), os.Stdin, os.Stdout, hub)
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
		map[string]any{"name": name, "arguments": args}, id)
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
		map[string]any{"name": name, "arguments": args}, id)
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
	argTo      = "to"
	argSubject = "subject"
	argBody    = "body"
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
	fake.configure(false, "agent-ep", nil)
	child := startEndpoint(t, dataDir, fake.sock)
	child.initialize(t)

	_, resp := child.request(t, "tools/list", nil, 2)
	tools, ok := resp["result"].(map[string]any)["tools"].([]any)
	require.True(t, ok, "tools/list has no tool array: %v", resp)
	require.Len(t, tools, len(toolNames()))
	seen := make(map[string]bool)
	for _, tool := range tools {
		entry := tool.(map[string]any)
		seen[entry["name"].(string)] = true
		schema, ok := entry["inputSchema"].(map[string]any)
		require.True(t, ok, "tool %v has no schema", entry["name"])
		require.Equal(t, "object", schema["type"])
	}
	for _, name := range toolNames() {
		require.True(t, seen[name], "tool %s not served", name)
	}
	require.Equal(t, 1, fakeHubCalls(t, fake, "whoami"))
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
	fake.configure(true, "", nil)
	child := startEndpoint(t, dataDir, fake.sock)
	child.initialize(t)

	_, resp := child.request(t, "tools/list", nil, 2)
	tools := resp["result"].(map[string]any)["tools"].([]any)
	require.Empty(t, tools)

	raw, failure := child.callError(t, 3, "send_message",
		map[string]any{"to": "a", "subject": "s", "body": testBody})
	require.Equal(t, -32001, mcpCode(t, failure))
	require.Equal(t, "No verified identity", failure["message"])
	require.NotContains(t, string(raw), "b")
	child.close(t)
}

// TestNoKeyNoTools proves a verified agent without a key still gets no
// tools: identity alone mints nothing.
func TestNoKeyNoTools(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	require.NoError(t, os.Chmod(dataDir, 0o700))
	fake := startFakeHub(t)
	fake.configure(false, "agent-nokey", nil)
	child := startEndpoint(t, dataDir, fake.sock)
	child.initialize(t)

	_, resp := child.request(t, "tools/list", nil, 2)
	tools := resp["result"].(map[string]any)["tools"].([]any)
	require.Empty(t, tools)
	child.close(t)
}

// TestSendMessageAdmits proves the send path: admission carries the connection
// identity (a spoofed sender is ignored) and routing metadata, but no message
// plaintext, and the typed verdict comes back.
func TestSendMessageAdmits(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	require.NoError(t, os.Chmod(dataDir, 0o700))
	prepareIdentity(t, dataDir)
	fake := startFakeHub(t)
	fake.configure(false, "agent-ep", nil)
	child := startEndpoint(t, dataDir, fake.sock)
	child.initialize(t)

	args := sendArgs("agent-other", "s", "hello")
	args["from"] = "evil"
	result := child.call(t, 2, "send_message", args)
	require.Equal(t, "01ARZ3NDEKTSV4RRFFQ69G5FAW", result.(map[string]any)["message_id"])

	params := fake.admitParamsCopy()
	require.Len(t, params, 1)
	var metadata map[string]any
	require.NoError(t, json.Unmarshal(params[0], &metadata))
	require.Equal(t, "agent-ep", metadata["from"])
	require.Equal(t, "agent-other", metadata["to"])
	require.NotContains(t, metadata, "subject")
	require.NotContains(t, metadata, "body")
	require.NotContains(t, string(params[0]), "hello")
	child.close(t)
}

// TestReplyAdmits proves replies ride the same plaintext-free admission path.
func TestReplyAdmits(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	require.NoError(t, os.Chmod(dataDir, 0o700))
	prepareIdentity(t, dataDir)
	fake := startFakeHub(t)
	fake.configure(false, "agent-ep", nil)
	child := startEndpoint(t, dataDir, fake.sock)
	child.initialize(t)

	result := child.call(t, 2, "reply_to_message",
		map[string]any{"id": "m1", argBody: "answer"})
	require.Equal(t, "01ARZ3NDEKTSV4RRFFQ69G5FAW", result.(map[string]any)["message_id"])

	params := fake.admitParamsCopy()
	require.Len(t, params, 1)
	var metadata map[string]any
	require.NoError(t, json.Unmarshal(params[0], &metadata))
	require.Equal(t, "m1", metadata["in_reply_to"])
	require.NotContains(t, metadata, "body")
	require.NotContains(t, string(params[0]), "answer")
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
	fake.configure(false, "agent-ep", nil)
	child := startEndpoint(t, dataDir, fake.sock)
	child.initialize(t)

	for _, id := range []int{2, 3} {
		result := child.call(t, id, "ack_message",
			map[string]any{"id": "m9", "status": "PROCESSING"})
		require.True(t, result.(map[string]any)["acked"].(bool))
	}
	for _, id := range []int{4, 5} {
		result := child.call(t, id, "ack_message",
			map[string]any{"id": "m9", "status": "ACKNOWLEDGED"})
		require.True(t, result.(map[string]any)["acked"].(bool))
	}
	require.Equal(t, 2, fakeHubCalls(t, fake, "ack_message"))
	_, failure := child.callError(t, 6, "ack_message",
		map[string]any{"id": "m10", "status": "READ"})
	require.Equal(t, -32602, mcpCode(t, failure))
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
	fake.configure(false, "agent-ep", nil)
	denied := -32603
	fake.configure(false, "agent-ep", &denied)
	child := startEndpoint(t, dataDir, fake.sock)
	child.initialize(t)

	_, failure := child.callError(t, 2, "nope", map[string]any{})
	require.Equal(t, -32602, mcpCode(t, failure))
	require.Equal(t, "Unknown tool", failure["message"])

	_, failure = child.callError(t, 3, "send_message", map[string]any{"to": "a"})
	require.Equal(t, -32602, mcpCode(t, failure))

	secret := "top-secret-body"
	raw, failure := child.callError(t, 4, "send_message", sendArgs("a", "s", secret))
	require.Equal(t, -32603, mcpCode(t, failure))
	require.NotContains(t, string(raw), secret)
	child.close(t)
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
	signed, err := SignEnvelope(priv, Envelope{From: "agent-keys", To: "b", Subject: "s", Body: "x"})
	require.NoError(t, err)
	require.True(t, VerifyEnvelope(first, signed))

	_, err = EnsureAgentKey(dataDir, "UPPER")
	require.ErrorIs(t, err, ErrInvalidArgument)
	_, err = LoadAgentPrivate(dataDir, "agent-missing")
	require.ErrorIs(t, err, ErrNoIdentity)

	for name, path := range map[string]string{
		"data directory":  dataDir,
		"keys directory":  filepath.Join(dataDir, "keys"),
		"agent directory": filepath.Join(dataDir, "keys", "agent-keys"),
	} {
		t.Run(name, func(t *testing.T) {
			require.NoError(t, os.Chmod(path, 0o755))
			_, err := LoadAgentPrivate(dataDir, "agent-keys")
			require.ErrorIs(t, err, ErrInvalidArgument)
			require.NoError(t, os.Chmod(path, 0o700))
		})
	}
}

func TestMCPLineFraming(t *testing.T) {
	t.Parallel()
	var wire bytes.Buffer
	require.NoError(t, writeFrame(&wire, mcpResponse{JSONRPC: "2.0", Result: map[string]any{}, ID: json.RawMessage("1")}))
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
