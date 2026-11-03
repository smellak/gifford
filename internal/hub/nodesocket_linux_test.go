//go:build linux

package hub

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
)

const nodeSocketPath = "/run/gifford/node.sock"

// The node socket's classes are decided by uid, and a test runs as one uid.
// Each test therefore picks which class the test's own uid falls in by
// choosing the bridge and node uids around it.

type recordingBridge struct {
	mu    sync.Mutex
	calls []string
}

// testBridgeDispositionField names the disposition key in stub receipts.
const testBridgeDispositionField = "disposition"

func (b *recordingBridge) MirrorMessage(context.Context, json.RawMessage) (any, error) {
	b.record(MethodBridgeMirrorMessage)
	return map[string]string{"disposition": "MIRRORED"}, nil
}

func (b *recordingBridge) MarkRead(context.Context, json.RawMessage) (any, error) {
	b.record(MethodBridgeMarkRead)
	return nil, domainError(protocol.ErrorCodeNotFound, errors.New("no mirrored message"))
}

func (b *recordingBridge) PutReminder(context.Context, json.RawMessage) (any, error) {
	b.record(MethodBridgePutReminder)
	return map[string]string{testBridgeDispositionField: "CREATED"}, nil
}

func (b *recordingBridge) GetReminder(context.Context, json.RawMessage) (any, error) {
	b.record(MethodBridgeGetReminder)
	return nil, domainError(protocol.ErrorCodeNotFound, errors.New("no reminder"))
}

func (b *recordingBridge) CancelReminder(context.Context, json.RawMessage) (any, error) {
	b.record(MethodBridgeCancelReminder)
	return map[string]string{"state": "CANCELLED"}, nil
}

func (b *recordingBridge) record(method string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.calls = append(b.calls, method)
}

func (b *recordingBridge) recorded() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.calls...)
}

// testNodeSocket serves a node socket over a scratch store. A non-nil claim
// registers a live session for agent-hub anchored in the calling test
// process, so the synthetic child descends from it.
func testNodeSocket(
	t *testing.T,
	bridgeUID, nodeUID int64,
	handlers Handlers,
	claim *protocol.ProcessIdentity,
) string {
	t.Helper()
	store, err := eventstore.Open(t.Context(), filepath.Join(t.TempDir(), "gifford.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	if claim != nil {
		raw, err := json.Marshal(claim)
		require.NoError(t, err)
		testHubSession(t.Context(), t, store.DB(), "agent-hub", raw)
	}
	server, err := New(t.TempDir(), store.DB(), handlers)
	require.NoError(t, err)
	dir := filepath.Join(t.TempDir(), "run")
	require.NoError(t, os.Mkdir(dir, 0o755))
	node, err := newNodeSocket(server, filepath.Join(dir, "node.sock"), bridgeUID, nodeUID)
	require.NoError(t, err)
	sock, err := node.Listen()
	require.NoError(t, err)
	go func() { _ = node.Serve(context.Background()) }()
	t.Cleanup(func() { require.NoError(t, node.Close()) })
	return sock
}

func selfProcess(t *testing.T) *protocol.ProcessIdentity {
	t.Helper()
	self, err := registry.CurrentProcess()
	require.NoError(t, err)
	return &self
}

// requireClosed proves the server closed the connection on its own: the
// child exits only once the server hangs up, and it received nothing.
func requireClosed(t *testing.T, client *hubClient) {
	t.Helper()
	require.NotContains(t, client.readAll(t), "{")
	select {
	case err := <-client.done:
		require.NoError(t, err, "the child connected; a failed dial exits 2")
	case <-time.After(15 * time.Second):
		t.Fatal("child did not exit after the server closed the connection")
	}
}

func TestNodeSocketLayout(t *testing.T) {
	t.Parallel()
	me := int64(os.Getuid())
	sock := testNodeSocket(t, me+1, me+2, Handlers{}, nil)
	info, err := os.Stat(sock)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o666), info.Mode().Perm(), "every local user may connect")

	server := &Server{}
	open := filepath.Join(t.TempDir(), "open")
	require.NoError(t, os.Mkdir(open, 0o755))
	require.NoError(t, os.Chmod(open, 0o775))
	link := filepath.Join(t.TempDir(), "link")
	require.NoError(t, os.Symlink(filepath.Dir(sock), link))
	for name, dir := range map[string]string{
		"a group-writable directory": open,
		"a symlinked directory":      link,
		"a missing directory":        filepath.Join(t.TempDir(), "missing"),
	} {
		node, err := newNodeSocket(server, filepath.Join(dir, "node.sock"), me+1, me+2)
		require.NoError(t, err)
		_, err = node.Listen()
		require.Error(t, err, name)
	}
}

func TestNewNodeSocketRefuses(t *testing.T) {
	t.Parallel()
	server := &Server{}
	for name, args := range map[string]struct {
		server            *Server
		path              string
		bridgeUID, nodeID int64
	}{
		"root as the bridge":           {server, nodeSocketPath, 0, 900},
		"the node's uid as the bridge": {server, nodeSocketPath, 900, 900},
		"a relative path":              {server, "run/node.sock", 901, 900},
		"an unclean path":              {server, "/run/gifford/../node.sock", 901, 900},
		"no server":                    {nil, nodeSocketPath, 901, 900},
	} {
		_, err := newNodeSocket(args.server, args.path, args.bridgeUID, args.nodeID)
		require.ErrorIs(t, err, errInvalidArgument, name)
	}
}

func TestNodeSocketBridgeReachesOnlyTheBridgeMethods(t *testing.T) {
	t.Parallel()
	me := int64(os.Getuid())
	bridge := &recordingBridge{}
	sock := testNodeSocket(t, me, me+1, Handlers{Bridge: bridge}, selfProcess(t))
	client := dialChild(t, sock)

	client.send(t, `{"jsonrpc":"2.0","id":1,"method":"bridge_mirror_message","params":{}}`)
	resp := client.recv(t)
	require.Equal(t, 1, responseID(t, resp))
	require.Equal(t, map[string]any{"disposition": "MIRRORED"}, resp["result"])

	client.send(t, `{"jsonrpc":"2.0","id":2,"method":"bridge_mark_read","params":{}}`)
	resp = client.recv(t)
	failure, ok := resp["error"].(map[string]any)
	require.True(t, ok, "%v", resp)
	require.InDelta(t, codeDomainError, failure["code"], 0)
	require.Equal(t, map[string]any{"error_code": string(protocol.ErrorCodeNotFound)}, failure["data"])

	// Even with a live session above it, the bridge's uid is never an agent.
	for i, method := range []string{
		MethodWhoAmI, MethodAdmitMessage, MethodRegisterAgent, MethodRegisterEndpoint, MethodAckMessage,
	} {
		client.send(t, `{"jsonrpc":"2.0","id":`+jsonID(10+i)+`,"method":"`+method+`","params":{}}`)
		resp := client.recv(t)
		require.Equal(t, codeMethodNotFound, errorCode(t, resp), method)
	}
	client.send(t, `not json`)
	require.NotEqual(t, codeMethodNotFound, errorCode(t, client.recv(t)), "a malformed line is a parse error")
	client.close(t)
	require.Equal(t, []string{MethodBridgeMirrorMessage, MethodBridgeMarkRead}, bridge.recorded())
}

func jsonID(i int) string {
	raw, _ := json.Marshal(i)
	return string(raw)
}

func TestNodeSocketClosesTheBridgeWithoutAHandler(t *testing.T) {
	t.Parallel()
	me := int64(os.Getuid())
	requireClosed(t, dialChild(t, testNodeSocket(t, me, me+1, Handlers{}, nil)))
}

func TestNodeSocketClosesTheNodeUID(t *testing.T) {
	t.Parallel()
	me := int64(os.Getuid())
	sock := testNodeSocket(t, me+1, me, Handlers{Bridge: &recordingBridge{}}, selfProcess(t))
	requireClosed(t, dialChild(t, sock))
}

func TestNodeSocketAgentReachesOnlyAgentMethods(t *testing.T) {
	t.Parallel()
	me := int64(os.Getuid())
	bridge := &recordingBridge{}
	sock := testNodeSocket(t, me+1, me+2, Handlers{Bridge: bridge}, selfProcess(t))
	client := dialChild(t, sock)

	client.send(t, `{"jsonrpc":"2.0","id":1,"method":"whoami"}`)
	resp := client.recv(t)
	require.Equal(t, map[string]any{"agent_id": "agent-hub"}, resp["result"], "bound to the session it descends from")

	client.send(t, `{"jsonrpc":"2.0","id":2,"method":"bridge_mirror_message","params":{}}`)
	require.Equal(t, codeMethodNotFound, errorCode(t, client.recv(t)))
	client.send(t, `{"jsonrpc":"2.0","id":3,"method":"bridge_mark_read","params":{}}`)
	require.Equal(t, codeMethodNotFound, errorCode(t, client.recv(t)))
	client.send(t, `{"jsonrpc":"2.0","id":4,"method":"register_agent","params":{}}`)
	require.Equal(t, codeInvalidRequest, errorCode(t, client.recv(t)), "no registration on the node socket")
	client.close(t)
	require.Empty(t, bridge.recorded())
}

func TestNodeSocketRefusesASessionOfAnotherUID(t *testing.T) {
	t.Parallel()
	me := int64(os.Getuid())
	claim := selfProcess(t)
	claim.UID = me + 3
	sock := testNodeSocket(t, me+1, me+2, Handlers{Bridge: &recordingBridge{}}, claim)
	requireClosed(t, dialChild(t, sock))
}

func TestNodeSocketClosesUnknownPeers(t *testing.T) {
	t.Parallel()
	me := int64(os.Getuid())
	sock := testNodeSocket(t, me+1, me+2, Handlers{Bridge: &recordingBridge{}}, nil)
	requireClosed(t, dialChild(t, sock))
}
