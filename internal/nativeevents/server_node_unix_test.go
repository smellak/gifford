//go:build unix

package nativeevents

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/orchestrator"
	"github.com/smellak/gifford/internal/protocol"
)

type stubNodeSessions struct {
	binding SessionBinding
	err     error
}

func (s stubNodeSessions) LookupNodeSession(context.Context, protocol.AgentID, protocol.OpaqueID) (SessionBinding, error) {
	return s.binding, s.err
}

func nodeTestBinding(queue *Queue) SessionBinding {
	return SessionBinding{
		Process: protocol.ProcessIdentity{
			PID: 100, ProcessStartTime: 10,
			UID: int64(os.Getuid()), GID: int64(os.Getgid()),
		},
		Queue: queue,
	}
}

func nodeTestInspector() testInspector {
	return testInspector{tree: orchestrator.ProcessTree{
		Root: orchestrator.ProcessToken{PID: 100, StartTime: 10},
		Descendants: []orchestrator.ProcessNode{
			{
				Token:  orchestrator.ProcessToken{PID: 200, StartTime: 20},
				Parent: orchestrator.ProcessToken{PID: 100, StartTime: 10},
			},
			{
				Token:  orchestrator.ProcessToken{PID: 300, StartTime: 30},
				Parent: orchestrator.ProcessToken{PID: 200, StartTime: 20},
			},
		},
	}}
}

const (
	nodeTestAgentID   = "agent-1"
	nodeTestSessionID = "session-1"
)

func nodeTestSocketDir(t *testing.T) string {
	t.Helper()
	//nolint:usetesting // keep the Unix socket path below its platform limit
	dir, err := os.MkdirTemp("", "node-test-")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(dir)) })
	return dir
}

func listenNodeTestServer(t *testing.T, inspector orchestrator.PeerProcessInspector, sessions NodeSessionLookup) *NodeServer {
	t.Helper()
	dir := nodeTestSocketDir(t)
	server, err := ListenNode(context.Background(), NodeOptions{
		SocketDir: dir,
		Resolve: func(context.Context, int) (protocol.AgentID, protocol.OpaqueID, error) {
			return nodeTestAgentID, nodeTestSessionID, nil
		},
		Sessions:  sessions,
		Inspector: inspector,
		peer: func(net.Conn) (peerCredentials, error) {
			return peerCredentials{uid: int64(os.Getuid()), pid: 300}, nil
		},
		startTime: func(int64) (int64, error) { return 30, nil },
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, server.Close()) })
	return server
}

func dialNodeTestServer(t *testing.T, server *NodeServer) *net.UnixConn {
	t.Helper()
	address, err := net.ResolveUnixAddr("unix", server.Path())
	require.NoError(t, err)
	conn, err := net.DialUnix("unix", nil, address)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func greetNodeTestServer(t *testing.T, conn *net.UnixConn, mutate func(*Hello)) HelloResult {
	t.Helper()
	hello := wireHello()
	if mutate != nil {
		mutate(&hello)
	}
	require.NoError(t, WriteMessage(conn, hello))
	message, err := ReadMessage(conn)
	require.NoError(t, err)
	result, ok := message.(HelloResult)
	require.True(t, ok)
	return result
}

func TestNodeServerAcceptsAndAcksAfterDurability(t *testing.T) {
	t.Parallel()
	queue := newTestQueue(t)
	server := listenNodeTestServer(t, nodeTestInspector(), stubNodeSessions{binding: nodeTestBinding(queue)})
	info, err := os.Stat(server.Path())
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o666), info.Mode().Perm())

	conn := dialNodeTestServer(t, server)
	require.True(t, greetNodeTestServer(t, conn, nil).Accepted)
	event := wireEvent()
	require.NoError(t, WriteMessage(conn, event))
	head, ok := queue.Head()
	require.Eventually(t, func() bool {
		head, ok = queue.Head()
		return ok
	}, time.Second, time.Millisecond)

	require.NoError(t, conn.SetReadDeadline(time.Now().Add(20*time.Millisecond)))
	_, err = ReadMessage(conn)
	require.Error(t, err, "ack must wait for durable storage")
	require.NoError(t, conn.SetReadDeadline(time.Time{}))
	queue.Settle(protocol.EmitEventResult{
		EventID: head.EventID, Status: protocol.EmitEventStatusStored,
	})
	message, err := ReadMessage(conn)
	require.NoError(t, err)
	require.Equal(t, Ack{
		Operation: OperationAck, Sequence: event.Sequence,
		EmitterEventID: event.EmitterEventID,
	}, message)
}

func listenNodeTestServerWithOptions(
	t *testing.T,
	inspector orchestrator.PeerProcessInspector,
	mutate func(*NodeOptions),
) *NodeServer {
	t.Helper()
	queue := newTestQueue(t)
	opts := NodeOptions{
		SocketDir: nodeTestSocketDir(t),
		Resolve: func(context.Context, int) (protocol.AgentID, protocol.OpaqueID, error) {
			return nodeTestAgentID, nodeTestSessionID, nil
		},
		Sessions:  stubNodeSessions{binding: nodeTestBinding(queue)},
		Inspector: inspector,
		peer: func(net.Conn) (peerCredentials, error) {
			return peerCredentials{uid: int64(os.Getuid()), pid: 300}, nil
		},
		startTime: func(int64) (int64, error) { return 30, nil },
	}
	if mutate != nil {
		mutate(&opts)
	}
	server, err := ListenNode(context.Background(), opts)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, server.Close()) })
	return server
}

func TestNodeServerRejectsWithoutValidResolution(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		mutate func(*NodeOptions)
	}{
		{"resolve fails", func(opts *NodeOptions) {
			opts.Resolve = func(context.Context, int) (protocol.AgentID, protocol.OpaqueID, error) {
				return "", "", errors.New("no session owns the peer")
			}
		}},
		{"no binding for session", func(opts *NodeOptions) {
			opts.Sessions = stubNodeSessions{err: errors.New("unknown session")}
		}},
		{"peer uid differs from session owner", func(opts *NodeOptions) {
			opts.peer = func(net.Conn) (peerCredentials, error) {
				return peerCredentials{uid: int64(os.Getuid()) + 1, pid: 300}, nil
			}
		}},
		{"peer outside session process tree", func(opts *NodeOptions) {
			opts.startTime = func(int64) (int64, error) { return 999, nil }
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			server := listenNodeTestServerWithOptions(t, nodeTestInspector(), test.mutate)
			conn := dialNodeTestServer(t, server)
			result := greetNodeTestServer(t, conn, nil)
			require.False(t, result.Accepted)
			require.Contains(t, result.Reason, "peer_not_in_tree")
		})
	}
}

func TestNodeServerRejectsHarnessMismatch(t *testing.T) {
	t.Parallel()
	lifecycle := newRecordingEmitterLifecycle()
	server := listenNodeTestServerWithOptions(t, nodeTestInspector(), func(opts *NodeOptions) {
		opts.Lifecycle = func(protocol.AgentID, protocol.OpaqueID) EmitterLifecycle {
			return lifecycle
		}
	})
	conn := dialNodeTestServer(t, server)
	result := greetNodeTestServer(t, conn, func(hello *Hello) {
		hello.Harness = protocol.HarnessOpencode
	})
	require.False(t, result.Accepted)
	require.Contains(t, result.Reason, "harness_mismatch")
	select {
	case <-lifecycle.failed:
	case <-time.After(time.Second):
		t.Fatal("rejected peer must report through the emitter lifecycle")
	}
}

func TestNodeServerPinsHarnessPerSession(t *testing.T) {
	t.Parallel()
	inspector := &sequenceInspector{trees: []orchestrator.ProcessTree{
		{
			Root: orchestrator.ProcessToken{PID: 100, StartTime: 10},
			Descendants: []orchestrator.ProcessNode{
				{
					Token:  orchestrator.ProcessToken{PID: 200, StartTime: 20},
					Parent: orchestrator.ProcessToken{PID: 100, StartTime: 10},
				},
				{
					Token:  orchestrator.ProcessToken{PID: 300, StartTime: 30},
					Parent: orchestrator.ProcessToken{PID: 200, StartTime: 20},
				},
			},
		},
		{
			Root: orchestrator.ProcessToken{PID: 100, StartTime: 10},
			Descendants: []orchestrator.ProcessNode{
				{
					Token:  orchestrator.ProcessToken{PID: 202, StartTime: 22},
					Parent: orchestrator.ProcessToken{PID: 100, StartTime: 10},
				},
				{
					Token:  orchestrator.ProcessToken{PID: 300, StartTime: 30},
					Parent: orchestrator.ProcessToken{PID: 202, StartTime: 22},
				},
			},
		},
	}}
	server := listenNodeTestServer(t, inspector, stubNodeSessions{binding: nodeTestBinding(newTestQueue(t))})
	first := dialNodeTestServer(t, server)
	require.True(t, greetNodeTestServer(t, first, nil).Accepted)

	second := dialNodeTestServer(t, server)
	result := greetNodeTestServer(t, second, nil)
	require.False(t, result.Accepted)
	require.Contains(t, result.Reason, "peer_not_in_tree")
}

func TestNodeServerStopsRejectingVocabulary(t *testing.T) {
	t.Parallel()
	queue := newTestQueue(t)
	server := listenNodeTestServer(t, nodeTestInspector(), stubNodeSessions{binding: nodeTestBinding(queue)})
	conn := dialNodeTestServer(t, server)
	require.True(t, greetNodeTestServer(t, conn, nil).Accepted)
	require.NoError(t, WriteMessage(conn, wireHello()))
	_, err := ReadMessage(conn)
	require.Error(t, err, "out-of-vocabulary traffic must terminate the connection")
}

func TestListenNodeRejectsMissingSocketDir(t *testing.T) {
	t.Parallel()
	_, err := ListenNode(context.Background(), NodeOptions{
		SocketDir: filepath.Join(t.TempDir(), "missing"),
	})
	require.Error(t, err)
}
