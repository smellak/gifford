package adapter

import (
	"context"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/nativeevents"
	"github.com/smellak/gifford/internal/protocol"
)

func emitArgs() []string {
	return []string{
		"emit", "--harness", "codex", "--harness-version", "1.2.3",
		"--emitter-type", "hook", "--emitter-event-id", "event-1",
		"--type", "turn.started", "--observed-at", "1793509200123",
	}
}

func executeEmit(t *testing.T) time.Duration {
	t.Helper()
	cmd := NewCmd(nil)
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs(emitArgs())
	started := time.Now()
	require.NoError(t, cmd.Execute())
	return time.Since(started)
}

func shortSocket(t *testing.T) (string, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "gifford-emit-") //nolint:usetesting // keep the Unix socket path below its platform limit
	require.NoError(t, err)
	return filepath.Join(dir, "entry.sock"), func() { require.NoError(t, os.RemoveAll(dir)) }
}

func listenEmitTest(t *testing.T, serve func(net.Conn)) string {
	t.Helper()
	path, cleanup := shortSocket(t)
	t.Cleanup(cleanup)
	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "unix", path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		serve(conn)
	}()
	return path
}

func TestEmitAlwaysExitsZeroWhenSocketIsMissing(t *testing.T) {
	t.Setenv(SocketEnvironment, filepath.Join(t.TempDir(), "missing.sock"))
	require.Less(t, executeEmit(t), time.Second)
}

func TestEmitAlwaysExitsZeroWhenHelloOrAckTimesOut(t *testing.T) {
	t.Run("hello", func(t *testing.T) {
		path := listenEmitTest(t, func(conn net.Conn) {
			_, _ = nativeevents.ReadMessage(conn)
			<-time.After(time.Second)
		})
		t.Setenv(SocketEnvironment, path)
		require.Less(t, executeEmit(t), time.Second)
	})
	t.Run("ack", func(t *testing.T) {
		path := listenEmitTest(t, func(conn net.Conn) {
			_, _ = nativeevents.ReadMessage(conn)
			_ = nativeevents.WriteMessage(conn, nativeevents.HelloResult{
				Operation: nativeevents.OperationHelloResult, Accepted: true,
			})
			_, _ = nativeevents.ReadMessage(conn)
			<-time.After(time.Second)
		})
		t.Setenv(SocketEnvironment, path)
		require.Less(t, executeEmit(t), time.Second)
	})
}

func TestEmitSendsHelloEventAndWaitsForMatchingAck(t *testing.T) {
	received := make(chan nativeevents.Event, 1)
	path := listenEmitTest(t, func(conn net.Conn) {
		message, err := nativeevents.ReadMessage(conn)
		if err != nil {
			return
		}
		hello, ok := message.(nativeevents.Hello)
		if !ok || hello.Harness != protocol.HarnessCodex || hello.SchemaVersion != nativeevents.SchemaVersion {
			return
		}
		_ = nativeevents.WriteMessage(conn, nativeevents.HelloResult{
			Operation: nativeevents.OperationHelloResult, Accepted: true,
		})
		message, err = nativeevents.ReadMessage(conn)
		if err != nil {
			return
		}
		event, ok := message.(nativeevents.Event)
		if !ok {
			return
		}
		received <- event
		_ = nativeevents.WriteMessage(conn, nativeevents.Ack{
			Operation: nativeevents.OperationAck, Sequence: event.Sequence,
			EmitterEventID: event.EmitterEventID,
		})
	})
	t.Setenv(SocketEnvironment, path)
	require.Less(t, executeEmit(t), time.Second)
	select {
	case event := <-received:
		require.Equal(t, protocol.EventTypeTurnStarted, event.Type)
		require.Equal(t, protocol.OpaqueID("event-1"), event.EmitterEventID)
	case <-time.After(time.Second):
		t.Fatal("emit did not send its event")
	}
}
