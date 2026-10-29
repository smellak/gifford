package controlapi

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
)

func testServer(t *testing.T, runtimeDir string) (*Server, *eventstore.Store) {
	t.Helper()
	store, err := eventstore.Open(t.Context(), filepath.Join(t.TempDir(), "gifford.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	engine, err := channel.New(channel.Options{Clock: clock.System{}})
	require.NoError(t, err)
	controller, err := channel.NewController(channel.ControllerOptions{DB: store.DB(), Engine: engine, Clock: clock.System{}, NodeID: "node-a", Entropy: bytes.NewReader(make([]byte, 1024))})
	require.NoError(t, err)
	require.NoError(t, controller.Reload(t.Context()))
	server, err := New(Options{RuntimeDir: runtimeDir, DB: store.DB(), Controller: controller})
	require.NoError(t, err)
	return server, store
}

func rpcRequest(t *testing.T, method protocol.ControlMethod, params any) []byte {
	t.Helper()
	raw, err := json.Marshal(params)
	require.NoError(t, err)
	request := protocol.RPCRequest{JSONRPC: protocol.JSONRPCVersion, ID: 1, Method: method, Params: raw}
	raw, err = json.Marshal(request)
	require.NoError(t, err)
	return raw
}

func TestServerCASConflictAndUnsupported(t *testing.T) {
	t.Parallel()
	server, _ := testServer(t, filepath.Join(t.TempDir(), "runtime"))
	peer := Peer{PID: os.Getpid(), UID: uint32(os.Getuid()), GID: uint32(os.Getgid()), Verified: true}
	set := protocol.SetGlobalParams{Mode: protocol.ChannelModePaused, ExpectedVersion: 0}
	first := server.Handle(t.Context(), peer, rpcRequest(t, protocol.ControlMethodChannelSetGlobal, set))
	require.Nil(t, first.Error)
	stale := server.Handle(t.Context(), peer, rpcRequest(t, protocol.ControlMethodChannelSetGlobal, set))
	require.Equal(t, int64(rpcDomainError), stale.Error.Code)
	require.Equal(t, protocol.ErrorCodeControlVersionConflict, stale.Error.Data.ErrorCode)
	require.Equal(t, int64(1), *stale.Error.Data.CurrentVersion)

	unsupported := server.Handle(t.Context(), peer, rpcRequest(t, protocol.ControlMethodLoopGet, protocol.LoopParams{LoopID: "loop_01JB0000000000000000000003"}))
	require.Equal(t, int64(rpcInternalError), unsupported.Error.Code)
	require.Nil(t, unsupported.Error.Data)
}

func TestServerUnverifiedPeerIsReadOnly(t *testing.T) {
	t.Parallel()
	server, _ := testServer(t, filepath.Join(t.TempDir(), "runtime"))
	peer := Peer{}
	read := server.Handle(t.Context(), peer, rpcRequest(t, protocol.ControlMethodChannelGetGlobal, protocol.EmptyParams{}))
	require.Nil(t, read.Error)
	mutation := server.Handle(t.Context(), peer, rpcRequest(t, protocol.ControlMethodChannelSetGlobal, protocol.SetGlobalParams{Mode: protocol.ChannelModePaused, ExpectedVersion: 0}))
	require.Equal(t, int64(rpcDomainError), mutation.Error.Code)
	require.Equal(t, protocol.ErrorCodePolicyDenied, mutation.Error.Data.ErrorCode)
}

func TestServerRejectsRegisteredAgentMutationButAllowsRead(t *testing.T) {
	t.Parallel()
	server, store := testServer(t, filepath.Join(t.TempDir(), "runtime"))
	process, err := registry.CurrentProcess()
	require.NoError(t, err)
	processJSON, err := json.Marshal(process)
	require.NoError(t, err)
	now := eventstore.FormatTime(time.Now().UTC())
	_, err = store.DB().ExecContext(t.Context(), `INSERT INTO agents (agent_id,node_id,harness,created_at,updated_at) VALUES (?,?,?,?,?)`, "agent-1", "node-a", "codex", now, now)
	require.NoError(t, err)
	_, err = store.DB().ExecContext(t.Context(), `INSERT INTO sessions (session_id,agent_id,state,capture_mode,deliverable,manifest_json,process_json,started_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?)`, "session-1", "agent-1", "CONNECTED", "pty_proxy", 1, `{}`, string(processJSON), now, now)
	require.NoError(t, err)
	peer := Peer{PID: os.Getpid(), UID: uint32(os.Getuid()), GID: uint32(os.Getgid()), Verified: true}

	read := server.Handle(t.Context(), peer, rpcRequest(t, protocol.ControlMethodChannelGetGlobal, protocol.EmptyParams{}))
	require.Nil(t, read.Error)
	mutation := server.Handle(t.Context(), peer, rpcRequest(t, protocol.ControlMethodChannelSetGlobal, protocol.SetGlobalParams{Mode: protocol.ChannelModeOff, ExpectedVersion: 0}))
	require.Equal(t, int64(rpcDomainError), mutation.Error.Code)
	require.Equal(t, protocol.ErrorCodePolicyDenied, mutation.Error.Data.ErrorCode)
}

func TestServerSocketModesAndNewlineFraming(t *testing.T) {
	t.Parallel()
	runtimeDir := filepath.Join(t.TempDir(), "runtime")
	server, _ := testServer(t, runtimeDir)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := os.Stat(server.Path()); err == nil {
			break
		}
		require.True(t, time.Now().Before(deadline), "control socket was not created")
		time.Sleep(5 * time.Millisecond)
	}
	dirInfo, err := os.Stat(runtimeDir)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o700), dirInfo.Mode().Perm())
	socketInfo, err := os.Stat(server.Path())
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), socketInfo.Mode().Perm())
	conn, err := (&net.Dialer{}).DialContext(t.Context(), "unix", server.Path())
	require.NoError(t, err)
	request := append(rpcRequest(t, protocol.ControlMethodChannelGetGlobal, protocol.EmptyParams{}), '\n')
	_, err = conn.Write(request)
	require.NoError(t, err)
	line, err := bufio.NewReader(conn).ReadBytes('\n')
	require.NoError(t, err)
	require.NotEmpty(t, line)
	require.NoError(t, conn.Close())
	cancel()
	require.NoError(t, <-done)
}
