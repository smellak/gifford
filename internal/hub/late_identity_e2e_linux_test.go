//go:build linux

package hub_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/endpoint"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/hub"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
)

const lateNode = "node-late"

type lateClock struct{ now time.Time }

func (c lateClock) Now() time.Time { return c.now }

// TestEndpointRedialsNodeSocketUntilItsSessionRegisters drives the real
// endpoint over node.sock as an agent of another user. node.sock closes the
// connection while no session exists; the endpoint dials it again on each
// call, binds once the session registers, and never dials again after
// that, even when its session ends and another agent's session takes over
// the same lineage.
func TestEndpointRedialsNodeSocketUntilItsSessionRegisters(t *testing.T) {
	store, err := eventstore.Open(t.Context(), filepath.Join(t.TempDir(), "gifford.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	clk := lateClock{now: time.Date(2026, time.November, 3, 1, 0, 0, 0, time.UTC)}
	engine, err := channel.New(channel.Options{Clock: clk})
	require.NoError(t, err)
	controller, err := channel.NewController(channel.ControllerOptions{
		DB: store.DB(), Engine: engine, Clock: clk, NodeID: lateNode,
	})
	require.NoError(t, err)
	require.NoError(t, controller.Reload(t.Context()))
	registryDir := t.TempDir()
	require.NoError(t, os.Chmod(registryDir, 0o700))
	keyRegistry, err := registry.New(lateNode, registryDir, clk)
	require.NoError(t, err)
	service, err := hub.NewService(hub.ServiceOptions{
		DB: store.DB(), NodeID: lateNode, Engine: engine, Approvals: controller,
		Registry: keyRegistry, Clock: clk,
	})
	require.NoError(t, err)
	server, err := hub.New(t.TempDir(), store.DB(), service.Handlers())
	require.NoError(t, err)
	me := int64(os.Getuid())
	runDir := filepath.Join(t.TempDir(), "run")
	require.NoError(t, os.Mkdir(runDir, 0o755))
	node, err := hub.NewNodeSocketForNode(server, filepath.Join(runDir, "node.sock"), me+1, me+2)
	require.NoError(t, err)
	sock, err := node.Listen()
	require.NoError(t, err)
	go func() { _ = node.Serve(context.Background()) }()
	t.Cleanup(func() { require.NoError(t, node.Close()) })

	dataDir := t.TempDir()
	require.NoError(t, os.Chmod(dataDir, 0o700))
	link, err := endpoint.DialHub(t.Context(), sock)
	require.NoError(t, err)
	t.Cleanup(func() { _ = link.Close() })
	stdin, toEndpoint := io.Pipe()
	fromEndpoint, stdout := io.Pipe()
	mcp, err := endpoint.New(t.Context(), dataDir, stdin, stdout, link)
	require.NoError(t, err, "an endpoint the node closed still starts")
	served := make(chan error, 1)
	go func() {
		served <- mcp.Run(context.Background())
		_ = stdout.Close()
	}()
	t.Cleanup(func() {
		_ = toEndpoint.Close()
		require.NoError(t, <-served)
	})
	frames := bufio.NewReader(fromEndpoint)
	listPending := func(id int) map[string]any {
		t.Helper()
		_, err := fmt.Fprintf(toEndpoint, `{"jsonrpc":"2.0","id":%d,"method":"tools/call",`+
			`"params":{"name":"list_pending_messages","arguments":{}}}`+"\n", id)
		require.NoError(t, err)
		line, err := frames.ReadBytes('\n')
		require.NoError(t, err)
		var resp map[string]any
		require.NoError(t, json.Unmarshal(line, &resp))
		return resp
	}

	failure, ok := listPending(1)["error"].(map[string]any)
	require.True(t, ok, "no session yet, no identity")
	require.Equal(t, "No verified identity", failure["message"])
	entries, err := os.ReadDir(dataDir)
	require.NoError(t, err)
	require.Empty(t, entries, "no key material without identity")

	self, err := registry.CurrentProcess()
	require.NoError(t, err)
	stamp := eventstore.FormatTime(clk.now)
	insertNodeSession(t, store, "agent-node-a", self, stamp)
	listed := listPending(2)
	require.NotContains(t, listed, "error", "the new connection binds once the session exists")
	active, err := keyRegistry.ActiveSigningKey(t.Context(), store.DB(), "agent-node-a")
	require.NoError(t, err)
	require.Equal(t, "ed25519:agent-node-a:v1", active.KeyID)

	_, err = store.DB().ExecContext(t.Context(),
		"UPDATE sessions SET state = 'DETACHED' WHERE session_id = 'session-agent-node-a'")
	require.NoError(t, err)
	insertNodeSession(t, store, "agent-node-b", self, stamp)
	for id := 3; id <= 4; id++ {
		require.Contains(t, listPending(id), "error", "a bound endpoint never dials again")
	}
	_, err = keyRegistry.ActiveSigningKey(t.Context(), store.DB(), "agent-node-b")
	require.Error(t, err, "the other agent registered nothing")
	require.NoError(t, filepath.WalkDir(dataDir, func(path string, _ fs.DirEntry, err error) error {
		require.NotContains(t, path, "agent-node-b")
		return err
	}))
}

func insertNodeSession(
	t *testing.T,
	store *eventstore.Store,
	agent protocol.AgentID,
	process protocol.ProcessIdentity,
	stamp string,
) {
	t.Helper()
	_, err := store.DB().ExecContext(t.Context(), `INSERT INTO agents
		(agent_id, node_id, harness, created_at, updated_at) VALUES (?, ?, 'test', ?, ?)`,
		agent, lateNode, stamp, stamp)
	require.NoError(t, err)
	processJSON, err := json.Marshal(process)
	require.NoError(t, err)
	_, err = store.DB().ExecContext(t.Context(), `INSERT INTO sessions
		(session_id, agent_id, state, capture_mode, deliverable, manifest_json,
		 process_json, started_at, updated_at)
		VALUES (?, ?, 'CONNECTED', 'pty_proxy', 1, '{}', ?, ?, ?)`,
		"session-"+string(agent), agent, string(processJSON), stamp, stamp)
	require.NoError(t, err)
}
