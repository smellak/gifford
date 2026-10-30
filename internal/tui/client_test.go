package tui

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/controlapi"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

func TestClientReadsGlobalAndNodeState(t *testing.T) {
	t.Parallel()

	client, _ := startControlServer(t)
	var global protocol.ChannelState
	require.NoError(t, client.Call(t.Context(), protocol.ControlMethodChannelGetGlobal, protocol.EmptyParams{}, &global))
	require.Equal(t, protocol.ScopeTypeGlobal, global.ScopeType)
	require.Equal(t, protocol.ChannelModeActive, global.Mode)

	var node protocol.ChannelState
	require.NoError(t, client.Call(t.Context(), protocol.ControlMethodChannelGetNode, protocol.NodeParams{NodeID: "node-a"}, &node))
	require.Equal(t, protocol.ScopeTypeNode, node.ScopeType)
	require.Equal(t, "node-a", node.ScopeKey)
}

func TestClientRejectsMutationBeforeDial(t *testing.T) {
	t.Parallel()

	client, err := NewClient(ClientOptions{SocketPath: filepath.Join(t.TempDir(), "absent.sock")})
	require.NoError(t, err)
	err = client.Call(t.Context(), protocol.ControlMethodChannelSetGlobal,
		protocol.SetGlobalParams{Mode: protocol.ChannelModeOff}, &protocol.ChannelState{})
	require.ErrorIs(t, err, ErrMutationForbidden)
	require.NotErrorIs(t, err, os.ErrNotExist)
}

func TestClientRejectsReadOutsideAllowlist(t *testing.T) {
	t.Parallel()

	client, err := NewClient(ClientOptions{SocketPath: filepath.Join(t.TempDir(), "absent.sock")})
	require.NoError(t, err)
	err = client.Call(t.Context(), protocol.ControlMethodNodePeers,
		protocol.EmptyParams{}, &protocol.NodePeersResult{})
	require.ErrorIs(t, err, ErrMethodNotAllowed)
}

func TestClientSilentServerTimesOut(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "daemon.sock")
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "unix", path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	accepted := make(chan struct{})
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		close(accepted)
		_, _ = bufio.NewReader(conn).ReadBytes('\n')
		<-t.Context().Done()
	}()

	client, err := NewClient(ClientOptions{SocketPath: path, Timeout: 40 * time.Millisecond})
	require.NoError(t, err)
	started := time.Now()
	var state protocol.ChannelState
	err = client.Call(t.Context(), protocol.ControlMethodChannelGetGlobal, protocol.EmptyParams{}, &state)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, time.Since(started), time.Second)
	<-accepted
}

func TestClientCancellationInterruptsRead(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "daemon.sock")
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "unix", path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	received := make(chan struct{})
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		_, _ = bufio.NewReader(conn).ReadBytes('\n')
		close(received)
		<-t.Context().Done()
	}()

	client, err := NewClient(ClientOptions{SocketPath: path, Timeout: time.Second})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		var state protocol.ChannelState
		done <- client.Call(ctx, protocol.ControlMethodChannelGetGlobal, protocol.EmptyParams{}, &state)
	}()
	<-received
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func startControlServer(t *testing.T) (*Client, *channel.Controller) {
	t.Helper()

	store, err := eventstore.Open(t.Context(), filepath.Join(t.TempDir(), "gifford.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	engine, err := channel.New(channel.Options{Clock: clock.System{}})
	require.NoError(t, err)
	controller, err := channel.NewController(channel.ControllerOptions{
		DB: store.DB(), Engine: engine, Clock: clock.System{}, NodeID: "node-a",
		Entropy: bytes.NewReader(make([]byte, 1024)),
	})
	require.NoError(t, err)
	require.NoError(t, controller.Reload(t.Context()))

	server, err := controlapi.New(controlapi.Options{
		RuntimeDir: filepath.Join(t.TempDir(), "runtime"),
		DB:         store.DB(),
		Controller: controller,
	})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	waitForSocket(t, server.Path())
	t.Cleanup(func() {
		cancel()
		require.NoError(t, <-done)
	})

	client, err := NewClient(ClientOptions{SocketPath: server.Path(), Timeout: time.Second})
	require.NoError(t, err)
	return client, controller
}

func waitForSocket(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := os.Stat(path); err == nil {
			return
		} else if !errors.Is(err, os.ErrNotExist) {
			require.NoError(t, err)
		}
		require.True(t, time.Now().Before(deadline), "control socket was not created")
		time.Sleep(5 * time.Millisecond)
	}
}
