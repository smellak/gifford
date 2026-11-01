//go:build linux

package nativeevents

import (
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/orchestrator"
	"github.com/smellak/gifford/internal/protocol"
)

type testInspector struct {
	tree orchestrator.ProcessTree
	err  error
}

type recordingEmitterLifecycle struct {
	activated chan EmitterType
	failed    chan EmitterType
}

func newRecordingEmitterLifecycle() *recordingEmitterLifecycle {
	return &recordingEmitterLifecycle{
		activated: make(chan EmitterType, 4), failed: make(chan EmitterType, 4),
	}
}

func (l *recordingEmitterLifecycle) NativeEmitterActivated(emitter EmitterType) error {
	l.activated <- emitter
	return nil
}

func (l *recordingEmitterLifecycle) NativeEmitterFailed(emitter EmitterType) {
	l.failed <- emitter
}

func (i testInspector) Inspect(protocol.ProcessIdentity) (orchestrator.ProcessTree, error) {
	return i.tree, i.err
}

func listenTestServer(t *testing.T, inspector orchestrator.ProcessInspector) (*Server, *Queue) {
	return listenTestServerLifecycle(t, inspector, nil)
}

func listenTestServerLifecycle(
	t *testing.T,
	inspector orchestrator.ProcessInspector,
	lifecycle EmitterLifecycle,
) (*Server, *Queue) {
	t.Helper()
	queue := newTestQueue(t)
	runtimeDir, err := os.MkdirTemp("", "nativeevents-test-") //nolint:usetesting // keep the Unix socket path below its platform limit
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(runtimeDir)) })
	root := protocol.ProcessIdentity{
		PID: 100, ProcessStartTime: 10, UID: int64(os.Getuid()), GID: int64(os.Getgid()),
	}
	server, err := Listen(t.Context(), Options{
		RuntimeDir: runtimeDir, Root: root, Inspector: inspector, Queue: queue,
		Lifecycle: lifecycle,
		peer: func(net.Conn) (peerCredentials, error) {
			return peerCredentials{uid: root.UID, pid: 200}, nil
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, server.Close()) })
	return server, queue
}

func greetTestServerAs(t *testing.T, conn *net.UnixConn, emitter EmitterType) HelloResult {
	t.Helper()
	hello := wireHello()
	hello.EmitterType = emitter
	require.NoError(t, WriteMessage(conn, hello))
	message, err := ReadMessage(conn)
	require.NoError(t, err)
	result, ok := message.(HelloResult)
	require.True(t, ok)
	return result
}

func acceptedInspector() testInspector {
	return testInspector{tree: orchestrator.ProcessTree{
		Root: orchestrator.ProcessToken{PID: 100, StartTime: 10},
		Descendants: []orchestrator.ProcessNode{{
			Token:  orchestrator.ProcessToken{PID: 200, StartTime: 20},
			Parent: orchestrator.ProcessToken{PID: 100, StartTime: 10},
		}},
	}}
}

func dialTestServer(t *testing.T, server *Server) *net.UnixConn {
	t.Helper()
	address, err := net.ResolveUnixAddr("unix", server.Path())
	require.NoError(t, err)
	conn, err := net.DialUnix("unix", nil, address)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func greetTestServer(t *testing.T, conn *net.UnixConn) HelloResult {
	return greetTestServerAs(t, conn, EmitterTypePlugin)
}

func TestEmitterLifecycleDegradesPersistentButNotCleanHookEOF(t *testing.T) {
	for _, test := range []struct {
		name       string
		emitter    EmitterType
		wantFailed bool
	}{
		{name: "persistent plugin", emitter: EmitterTypePlugin, wantFailed: true},
		{name: "clean hook", emitter: EmitterTypeHook},
	} {
		t.Run(test.name, func(t *testing.T) {
			lifecycle := newRecordingEmitterLifecycle()
			server, queue := listenTestServerLifecycle(t, acceptedInspector(), lifecycle)
			conn := dialTestServer(t, server)
			require.True(t, greetTestServerAs(t, conn, test.emitter).Accepted)
			event := wireEvent()
			require.NoError(t, WriteMessage(conn, event))
			select {
			case got := <-lifecycle.activated:
				require.Equal(t, test.emitter, got)
			case <-time.After(time.Second):
				t.Fatal("first event did not activate emitter")
			}
			require.Eventually(t, func() bool {
				_, ok := queue.Head()
				return ok
			}, time.Second, time.Millisecond)
			head, _ := queue.Head()
			queue.Settle(protocol.EmitEventResult{
				EventID: head.EventID, Status: protocol.EmitEventStatusStored,
			})
			_, err := ReadMessage(conn)
			require.NoError(t, err)
			require.NoError(t, conn.Close())
			if test.wantFailed {
				select {
				case got := <-lifecycle.failed:
					require.Equal(t, test.emitter, got)
				case <-time.After(time.Second):
					t.Fatal("persistent disconnect did not degrade emitter")
				}
				return
			}
			select {
			case got := <-lifecycle.failed:
				t.Fatalf("clean hook EOF degraded %s", got)
			case <-time.After(50 * time.Millisecond):
			}
		})
	}
}

func TestServerEntryPermissionsAndAckAfterDurability(t *testing.T) {
	t.Parallel()
	server, queue := listenTestServer(t, acceptedInspector())
	dirInfo, err := os.Stat(filepath.Dir(server.Path()))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o700), dirInfo.Mode().Perm())
	socketInfo, err := os.Stat(server.Path())
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), socketInfo.Mode().Perm())

	conn := dialTestServer(t, server)
	require.True(t, greetTestServer(t, conn).Accepted)
	event := wireEvent()
	require.NoError(t, WriteMessage(conn, event))
	head, ok := queue.Head()
	require.Eventually(t, func() bool {
		head, ok = queue.Head()
		return ok
	}, time.Second, time.Millisecond)

	require.NoError(t, conn.SetReadDeadline(time.Now().Add(20*time.Millisecond)))
	_, err = ReadMessage(conn)
	var netErr net.Error
	require.ErrorAs(t, err, &netErr)
	require.True(t, netErr.Timeout(), "ack must wait for durable storage")
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

func TestServerRejectsOutsideAndIncompleteProcessObservations(t *testing.T) {
	t.Parallel()
	for name, test := range map[string]struct {
		inspector testInspector
		reason    HelloRejectReason
	}{
		"outside tree": {
			inspector: testInspector{tree: orchestrator.ProcessTree{
				Root: orchestrator.ProcessToken{PID: 100, StartTime: 10},
			}},
			reason: HelloRejectPeerNotInTree,
		},
		"incomplete": {
			inspector: testInspector{err: orchestrator.ErrProcessObservationIncomplete},
			reason:    HelloRejectObservationIncomplete,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			server, _ := listenTestServer(t, test.inspector)
			result := greetTestServer(t, dialTestServer(t, server))
			require.False(t, result.Accepted)
			require.Equal(t, test.reason, result.Reason)
		})
	}
}

func TestServerDeduplicatesExactSequenceAndClosesOnConflict(t *testing.T) {
	t.Parallel()
	server, queue := listenTestServer(t, acceptedInspector())
	conn := dialTestServer(t, server)
	require.True(t, greetTestServer(t, conn).Accepted)
	event := wireEvent()
	require.NoError(t, WriteMessage(conn, event))
	require.Eventually(t, func() bool {
		_, ok := queue.Head()
		return ok
	}, time.Second, time.Millisecond)
	head, _ := queue.Head()
	queue.Settle(protocol.EmitEventResult{EventID: head.EventID, Status: protocol.EmitEventStatusStored})
	_, err := ReadMessage(conn)
	require.NoError(t, err)

	// An exact seq/id replay gets the already-durable ack.
	require.NoError(t, WriteMessage(conn, event))
	_, err = ReadMessage(conn)
	require.NoError(t, err)

	conflict := event
	conflict.Result = protocol.NativeEventResultFailed
	require.NoError(t, WriteMessage(conn, conflict))
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(time.Second)))
	_, err = ReadMessage(conn)
	require.Error(t, err)
}

func TestServerAuthenticatedHelloMismatchDegradesEmitter(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		mutate func(*Hello)
		reason HelloRejectReason
	}{
		{
			name: "schema", reason: HelloRejectUnsupportedSchema,
			mutate: func(hello *Hello) { hello.SchemaVersion++ },
		},
		{
			name: "harness version", reason: HelloRejectHarnessMismatch,
			mutate: func(hello *Hello) { hello.HarnessVersion = "9.9.9" },
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			lifecycle := newRecordingEmitterLifecycle()
			server, _ := listenTestServerLifecycle(t, acceptedInspector(), lifecycle)
			conn := dialTestServer(t, server)
			hello := wireHello()
			test.mutate(&hello)
			require.NoError(t, WriteMessage(conn, hello))
			message, err := ReadMessage(conn)
			require.NoError(t, err)
			require.Equal(t, HelloResult{
				Operation: OperationHelloResult, Reason: test.reason,
			}, message)
			select {
			case got := <-lifecycle.failed:
				require.Equal(t, hello.EmitterType, got)
			case <-time.After(time.Second):
				t.Fatal("authenticated version mismatch did not degrade emitter")
			}
		})
	}
}
