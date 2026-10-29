package hub

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
)

// This file holds the maintained verification of the switchboard: the
// socket layout, agent binding through a synthetic child process, the
// stubbed method table, and every error shape. Nothing is written outside
// scratch directories.

// TestHelperHubClient is not a test: it is the synthetic child the tests
// below spawn. It dials the socket and pipes stdin to it and it to stdout,
// so the parent drives the protocol through the child's pid and SO_PEERCRED
// reports the child.
func TestHelperHubClient(_ *testing.T) {
	if os.Getenv("GIFFORD_HUB_CLIENT") != "1" {
		return
	}
	conn, err := (&net.Dialer{}).DialContext(context.Background(), "unix", os.Getenv("GIFFORD_HUB_SOCK"))
	if err != nil {
		os.Exit(2)
	}
	uc, ok := conn.(*net.UnixConn)
	if !ok {
		os.Exit(2)
	}
	go func() {
		_, _ = io.Copy(uc, os.Stdin)
		_ = uc.CloseWrite()
	}()
	_, _ = io.Copy(os.Stdout, uc)
}

// hubClient drives the protocol through a synthetic child process.
type hubClient struct {
	stdin  io.WriteCloser
	lines  *bufio.Reader
	stderr *bytes.Buffer
	cmd    *exec.Cmd
	done   chan error
}

// dialChild spawns the synthetic child connected to sock.
func dialChild(t *testing.T, sock string) *hubClient {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=TestHelperHubClient")
	cmd.Env = append(os.Environ(), "GIFFORD_HUB_CLIENT=1", "GIFFORD_HUB_SOCK="+sock)
	stdin, err := cmd.StdinPipe()
	require.NoError(t, err)
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	require.NoError(t, cmd.Start())
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	return &hubClient{stdin: stdin, lines: bufio.NewReader(stdout), stderr: &stderr, cmd: cmd, done: done}
}

// send writes one request line through the child.
func (c *hubClient) send(t *testing.T, line string) {
	t.Helper()
	_, err := io.WriteString(c.stdin, line+"\n")
	require.NoError(t, err)
}

// recv reads one response line, failing after a short wait instead of
// hanging when the server stays silent.
func (c *hubClient) recv(t *testing.T) map[string]any {
	t.Helper()
	type outcome struct {
		line string
		err  error
	}
	arrived := make(chan outcome, 1)
	go func() {
		line, err := c.lines.ReadString('\n')
		arrived <- outcome{line: line, err: err}
	}()
	select {
	case got := <-arrived:
		require.NoError(t, got.err)
		var decoded map[string]any
		require.NoError(t, json.Unmarshal([]byte(got.line), &decoded))
		return decoded
	case <-time.After(10 * time.Second):
		t.Fatalf("no response within 10s (child stderr: %s)", c.stderr.String())
		return nil
	}
}

// readAll drains the child's stdout until it exits, returning everything
// the connection delivered.
func (c *hubClient) readAll(t *testing.T) string {
	t.Helper()
	type outcome struct {
		raw string
		err error
	}
	arrived := make(chan outcome, 1)
	go func() {
		raw, err := io.ReadAll(c.lines)
		arrived <- outcome{raw: string(raw), err: err}
	}()
	select {
	case got := <-arrived:
		require.NoError(t, got.err)
		return got.raw
	case <-time.After(15 * time.Second):
		_ = c.cmd.Process.Kill()
		t.Fatalf("child did not exit within 15s (child stderr: %s)", c.stderr.String())
		return ""
	}
}

// close ends the session and requires the child gone.
func (c *hubClient) close(t *testing.T) {
	t.Helper()
	require.NoError(t, c.stdin.Close())
	select {
	case err := <-c.done:
		require.NoError(t, err)
	case <-time.After(15 * time.Second):
		_ = c.cmd.Process.Kill()
		t.Fatal("child did not exit within 15s")
	}
}

// testHub starts a server on a scratch socket over a scratch store. With
// bindSelf it registers a live session anchored in the calling test
// process, so the synthetic child (its descendant) binds to agent-hub.
func testHub(t *testing.T, bindSelf bool, handlers Handlers) string {
	t.Helper()
	dataDir := t.TempDir()
	store, err := eventstore.Open(t.Context(), filepath.Join(t.TempDir(), "gifford.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	if bindSelf {
		self, err := registry.CurrentProcess()
		require.NoError(t, err)
		claim, err := json.Marshal(self)
		require.NoError(t, err)
		testHubSession(t.Context(), t, store.DB(), "agent-hub", claim)
	}
	server, err := New(dataDir, store.DB(), handlers)
	require.NoError(t, err)
	sock, err := server.Listen()
	require.NoError(t, err)
	go func() { _ = server.Serve(context.Background()) }()
	t.Cleanup(func() { require.NoError(t, server.Close()) })
	return sock
}

// testHubSession inserts the agent and one live session carrying claim.
func testHubSession(ctx context.Context, t *testing.T, db *sql.DB, agent string, claim []byte) {
	t.Helper()
	_, err := db.ExecContext(ctx, `INSERT INTO agents
		(agent_id, node_id, harness, created_at, updated_at)
		VALUES (?, 'test-node', 'test', '2026-01-01T00:00:00.000000000Z', '2026-01-01T00:00:00.000000000Z')`, agent)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `INSERT INTO sessions
		(session_id, agent_id, state, capture_mode, deliverable, manifest_json,
		 process_json, started_at, ended_at, updated_at)
		VALUES ('session-hub', ?, 'CONNECTED', 'pty_proxy', 1, '{}', ?, '2026-01-01T00:00:00.000000000Z', NULL,
		 '2026-01-01T00:00:00.000000000Z')`, agent, string(claim))
	require.NoError(t, err)
}

// errorOf extracts the error member of a response.
func errorOf(t *testing.T, resp map[string]any) map[string]any {
	t.Helper()
	raw, ok := resp["error"].(map[string]any)
	require.True(t, ok, "response has no error member: %v", resp)
	require.NotContains(t, resp, "result")
	require.NotContains(t, raw, "data")
	return raw
}

// errorCode reads the integer code of an error member. Wire numbers
// decode as float64; the conversion keeps integer comparisons exact for
// these small codes.
func errorCode(t *testing.T, resp map[string]any) int {
	t.Helper()
	code, ok := errorOf(t, resp)["code"].(float64)
	require.True(t, ok, "error member has no numeric code: %v", resp)
	return int(code)
}

// responseID reads a numeric response id as an integer.
func responseID(t *testing.T, resp map[string]any) int {
	t.Helper()
	id, ok := resp["id"].(float64)
	require.True(t, ok, "response has no numeric id: %v", resp)
	return int(id)
}

// TestSocketPermissions proves the layout: directory at 0700 and socket
// file at 0600.
func TestSocketPermissions(t *testing.T) {
	t.Parallel()
	sock := testHub(t, false, Handlers{})
	dirInfo, err := os.Stat(filepath.Dir(sock))
	require.NoError(t, err)
	require.True(t, dirInfo.IsDir())
	require.Equal(t, os.FileMode(0o700), dirInfo.Mode().Perm())
	sockInfo, err := os.Stat(sock)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), sockInfo.Mode().Perm())
}

// TestStubsAnswerNotReady proves the four-method table through the bound
// child: every local method answers -32603 with no data while its handler
// is nil. The answer itself proves the binding: an unbound connection
// would receive nothing.
func TestStubsAnswerNotReady(t *testing.T) {
	t.Parallel()
	sock := testHub(t, true, Handlers{})
	client := dialChild(t, sock)
	for i, method := range []string{
		MethodAdmitMessage, MethodSubmitMessageEnvelope, MethodClaimMessage, MethodAckMessage,
	} {
		client.send(t, fmt.Sprintf(`{"jsonrpc":"2.0","method":"%s","params":{"text":"the hub never reads this"},"id":%d}`,
			method, i+1))
		resp := client.recv(t)
		require.Equal(t, "2.0", resp["jsonrpc"])
		require.Equal(t, i+1, responseID(t, resp))
		require.Equal(t, codeNotReady, errorCode(t, resp))
	}
	client.close(t)
}

// TestUnknownMethod proves an unknown method is -32601 with the id
// mirrored and no data.
func TestUnknownMethod(t *testing.T) {
	t.Parallel()
	sock := testHub(t, true, Handlers{})
	client := dialChild(t, sock)
	client.send(t, `{"jsonrpc":"2.0","method":"exfiltrate","params":{},"id":"a"}`)
	resp := client.recv(t)
	require.Equal(t, "a", resp["id"])
	require.Equal(t, codeMethodNotFound, errorCode(t, resp))
	client.close(t)
}

// TestMalformed proves anything outside the local subset is -32602 with
// no data: garbage, batches, and envelopes with a missing, mistyped, or
// versionless method.
func TestMalformed(t *testing.T) {
	t.Parallel()
	sock := testHub(t, true, Handlers{})
	client := dialChild(t, sock)
	for _, line := range []struct {
		body string
		id   any
	}{
		{`not json`, nil},
		{`[1,2]`, nil},
		{`42`, nil},
		{`{"jsonrpc":"2.0","id":5}`, nil},
		{`{"jsonrpc":"2.0","method":42,"id":6}`, nil},
		{`{"jsonrpc":"1.0","method":"admit_message","id":7}`, 7},
		{`{"jsonrpc":"2.0","method":"","id":8}`, nil},
	} {
		client.send(t, line.body)
		resp := client.recv(t)
		require.Equal(t, codeInvalidRequest, errorCode(t, resp))
		if line.id == nil {
			require.Nil(t, resp["id"])
		} else {
			require.Equal(t, line.id, responseID(t, resp))
		}
	}
	client.close(t)
}

// TestEmptyLinesSkipped proves blank lines earn no response while the
// connection keeps serving.
func TestEmptyLinesSkipped(t *testing.T) {
	t.Parallel()
	sock := testHub(t, true, Handlers{})
	client := dialChild(t, sock)
	client.send(t, ``)
	client.send(t, `   `)
	client.send(t, `{"jsonrpc":"2.0","method":"ack_message","id":1}`)
	resp := client.recv(t)
	require.Equal(t, 1, responseID(t, resp))
	require.Equal(t, codeNotReady, errorCode(t, resp))
	client.close(t)
}

// TestClosesUnbound proves a peer with no registered agent receives
// nothing: the store holds no sessions, the server sends zero bytes, and
// the connection drops. The assertion looks for protocol lines (every
// response starts with "{") because the synthetic child shares its stdout
// with the test framework trailer.
func TestClosesUnbound(t *testing.T) {
	t.Parallel()
	sock := testHub(t, false, Handlers{})
	client := dialChild(t, sock)
	require.NotContains(t, client.readAll(t), "{")
	select {
	case err := <-client.done:
		require.NoError(t, err)
	case <-time.After(15 * time.Second):
		t.Fatal("child did not exit after the server closed the connection")
	}
}

// fakeAdmitter is the seam test double: it records what the Hub passes
// through and answers a fixed result.
type fakeAdmitter struct {
	agent  protocol.AgentID
	params json.RawMessage
	calls  int
}

// AdmitMessage implements Admitter.
func (f *fakeAdmitter) AdmitMessage(_ context.Context, agent protocol.AgentID, params json.RawMessage) (any, error) {
	f.agent = agent
	f.params = append(json.RawMessage(nil), params...)
	f.calls++
	return map[string]string{"status": "accepted"}, nil
}

// TestHandlerSeam proves each handler sits behind its small interface: a
// filled Admitter runs with the bound agent and opaque params, and its
// result is mirrored back.
func TestHandlerSeam(t *testing.T) {
	t.Parallel()
	admit := &fakeAdmitter{}
	sock := testHub(t, true, Handlers{Admit: admit})
	client := dialChild(t, sock)
	client.send(t, `{"jsonrpc":"2.0","method":"admit_message","params":{"opaque":[1,2,3]},"id":1}`)
	resp := client.recv(t)
	require.Equal(t, map[string]any{"status": "accepted"}, resp["result"])
	require.Equal(t, 1, admit.calls)
	require.Equal(t, protocol.AgentID("agent-hub"), admit.agent)
	require.JSONEq(t, `{"opaque":[1,2,3]}`, string(admit.params))
	client.close(t)
}

// TestNewValidation proves the constructor rejects empty arguments.
func TestNewValidation(t *testing.T) {
	t.Parallel()
	store, err := eventstore.Open(t.Context(), filepath.Join(t.TempDir(), "gifford.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	_, err = New("", store.DB(), Handlers{})
	require.Error(t, err)
	_, err = New(t.TempDir(), nil, Handlers{})
	require.Error(t, err)
}
