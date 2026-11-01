//go:build linux

package nativeevents

import (
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/orchestrator"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
)

type testInspector struct {
	tree orchestrator.ProcessTree
	err  error
}

func (i testInspector) Inspect(protocol.ProcessIdentity) (orchestrator.ProcessTree, error) {
	return i.tree, i.err
}

func listenTestServer(t *testing.T, inspector orchestrator.ProcessInspector) (*Server, *Queue) {
	t.Helper()
	return listenTestServerWithOptions(t, inspector, nil)
}

func listenTestServerWithOptions(
	t *testing.T,
	inspector orchestrator.ProcessInspector,
	configure func(*Options),
) (*Server, *Queue) {
	t.Helper()
	queue := newTestQueue(t)
	runtimeDir, err := os.MkdirTemp("", "nativeevents-test-") //nolint:usetesting // keep the Unix socket path below its platform limit
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(runtimeDir)) })
	root := protocol.ProcessIdentity{
		PID: 100, ProcessStartTime: 10, UID: int64(os.Getuid()), GID: int64(os.Getgid()),
	}
	opts := Options{
		RuntimeDir: runtimeDir, Root: root, Inspector: inspector, Queue: queue,
		peer: func(net.Conn) (peerCredentials, error) {
			return peerCredentials{uid: root.UID, pid: 200}, nil
		},
		startTime: func(int64) (int64, error) { return 20, nil },
	}
	if configure != nil {
		configure(&opts)
	}
	server, err := Listen(t.Context(), opts)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, server.Close()) })
	return server, opts.Queue
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

type sequenceInspector struct {
	mu    sync.Mutex
	trees []orchestrator.ProcessTree
	next  int
}

func (i *sequenceInspector) Inspect(protocol.ProcessIdentity) (orchestrator.ProcessTree, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.next >= len(i.trees) {
		return orchestrator.ProcessTree{}, errors.New("unexpected process inspection")
	}
	tree := i.trees[i.next]
	i.next++
	return tree, nil
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
	t.Helper()
	require.NoError(t, WriteMessage(conn, wireHello()))
	message, err := ReadMessage(conn)
	require.NoError(t, err)
	result, ok := message.(HelloResult)
	require.True(t, ok)
	return result
}

func TestReadProcessStartTimeMatchesRegisteredIdentity(t *testing.T) {
	t.Parallel()
	process, err := registry.CurrentProcess()
	require.NoError(t, err)
	startTime, err := readProcessStartTime(process.PID)
	require.NoError(t, err)
	require.Equal(t, process.ProcessStartTime, startTime)
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

func TestServerReadsPeerStartTimeBeforeHello(t *testing.T) {
	t.Parallel()
	read := make(chan struct{}, 1)
	server, _ := listenTestServerWithOptions(t, acceptedInspector(), func(opts *Options) {
		opts.startTime = func(_ int64) (int64, error) {
			read <- struct{}{}
			return 20, nil
		}
	})
	_ = dialTestServer(t, server)
	select {
	case <-read:
	case <-time.After(time.Second):
		t.Fatal("peer start time was not read before the hello")
	}
}

func TestServerHelloDeadlineIsLimitedToHello(t *testing.T) {
	t.Parallel()
	t.Run("silent connection", func(t *testing.T) {
		t.Parallel()
		server, _ := listenTestServerWithOptions(t, acceptedInspector(), func(opts *Options) {
			opts.helloTimeout = 30 * time.Millisecond
		})
		conn := dialTestServer(t, server)
		require.NoError(t, conn.SetReadDeadline(time.Now().Add(time.Second)))
		started := time.Now()
		_, err := ReadMessage(conn)
		require.Error(t, err)
		var netErr net.Error
		if errors.As(err, &netErr) {
			require.False(t, netErr.Timeout(), "the server must close the silent connection first")
		}
		require.Less(t, time.Since(started), 500*time.Millisecond)
	})
	t.Run("accepted emitter", func(t *testing.T) {
		t.Parallel()
		const timeout = 20 * time.Millisecond
		server, queue := listenTestServerWithOptions(t, acceptedInspector(), func(opts *Options) {
			opts.helloTimeout = timeout
		})
		conn := dialTestServer(t, server)
		require.True(t, greetTestServer(t, conn).Accepted)
		<-time.After(2 * timeout)
		require.NoError(t, WriteMessage(conn, wireEvent()))
		require.Eventually(t, func() bool {
			_, ok := queue.Head()
			return ok
		}, time.Second, time.Millisecond, "the hello deadline must be cleared")
	})
}

func TestServerBindsExactPeerToPinnedHarnessSubtree(t *testing.T) {
	t.Parallel()
	root := orchestrator.ProcessToken{PID: 100, StartTime: 10}
	harness := orchestrator.ProcessToken{PID: 200, StartTime: 20}

	t.Run("peer start time differs", func(t *testing.T) {
		t.Parallel()
		server, _ := listenTestServerWithOptions(t, acceptedInspector(), func(opts *Options) {
			opts.startTime = func(int64) (int64, error) { return 21, nil }
		})
		result := greetTestServer(t, dialTestServer(t, server))
		require.False(t, result.Accepted)
		require.Equal(t, HelloRejectPeerNotInTree, result.Reason)
	})

	t.Run("peer start time unavailable", func(t *testing.T) {
		t.Parallel()
		server, _ := listenTestServerWithOptions(t, acceptedInspector(), func(opts *Options) {
			opts.startTime = func(int64) (int64, error) {
				return 0, errors.New("gone")
			}
		})
		result := greetTestServer(t, dialTestServer(t, server))
		require.False(t, result.Accepted)
		require.Equal(t, HelloRejectPeerNotInTree, result.Reason)
	})

	t.Run("descendant of harness", func(t *testing.T) {
		t.Parallel()
		descendant := orchestrator.ProcessToken{PID: 201, StartTime: 21}
		inspector := testInspector{tree: orchestrator.ProcessTree{
			Root: root,
			Descendants: []orchestrator.ProcessNode{
				{Token: harness, Parent: root},
				{Token: descendant, Parent: harness},
			},
		}}
		server, _ := listenTestServerWithOptions(t, inspector, func(opts *Options) {
			opts.peer = func(net.Conn) (peerCredentials, error) {
				return peerCredentials{uid: opts.Root.UID, pid: descendant.PID}, nil
			}
			opts.startTime = func(int64) (int64, error) { return descendant.StartTime, nil }
		})
		require.True(t, greetTestServer(t, dialTestServer(t, server)).Accepted)
	})

	t.Run("sibling of harness", func(t *testing.T) {
		t.Parallel()
		sibling := orchestrator.ProcessToken{PID: 201, StartTime: 21}
		inspector := testInspector{tree: orchestrator.ProcessTree{
			Root: root,
			Descendants: []orchestrator.ProcessNode{
				{Token: harness, Parent: root},
				{Token: sibling, Parent: root},
			},
		}}
		server, _ := listenTestServerWithOptions(t, inspector, func(opts *Options) {
			opts.peer = func(net.Conn) (peerCredentials, error) {
				return peerCredentials{uid: opts.Root.UID, pid: sibling.PID}, nil
			}
			opts.startTime = func(int64) (int64, error) { return sibling.StartTime, nil }
		})
		result := greetTestServer(t, dialTestServer(t, server))
		require.False(t, result.Accepted)
		require.Equal(t, HelloRejectPeerNotInTree, result.Reason)
	})
}

func TestServerRejectsChangedHarnessChild(t *testing.T) {
	t.Parallel()
	root := orchestrator.ProcessToken{PID: 100, StartTime: 10}
	first := orchestrator.ProcessToken{PID: 200, StartTime: 20}
	second := orchestrator.ProcessToken{PID: 201, StartTime: 21}
	inspector := &sequenceInspector{trees: []orchestrator.ProcessTree{
		{Root: root, Descendants: []orchestrator.ProcessNode{{Token: first, Parent: root}}},
		{Root: root, Descendants: []orchestrator.ProcessNode{{Token: second, Parent: root}}},
	}}
	var (
		peerMu sync.Mutex
		peers  = []orchestrator.ProcessToken{first, second}
	)
	server, _ := listenTestServerWithOptions(t, inspector, func(opts *Options) {
		opts.peer = func(net.Conn) (peerCredentials, error) {
			peerMu.Lock()
			defer peerMu.Unlock()
			peer := peers[0]
			peers = peers[1:]
			return peerCredentials{uid: opts.Root.UID, pid: peer.PID}, nil
		}
		opts.startTime = func(pid int64) (int64, error) {
			switch pid {
			case first.PID:
				return first.StartTime, nil
			case second.PID:
				return second.StartTime, nil
			default:
				return 0, errors.New("unknown peer")
			}
		}
	})
	firstConn := dialTestServer(t, server)
	require.True(t, greetTestServer(t, firstConn).Accepted)
	require.NoError(t, firstConn.Close())
	result := greetTestServer(t, dialTestServer(t, server))
	require.False(t, result.Accepted)
	require.Equal(t, HelloRejectPeerNotInTree, result.Reason)
}

func TestServerRejectsMultipleHarnessChildrenWithoutPinningOne(t *testing.T) {
	t.Parallel()
	root := orchestrator.ProcessToken{PID: 100, StartTime: 10}
	harness := orchestrator.ProcessToken{PID: 200, StartTime: 20}
	peer := orchestrator.ProcessToken{PID: 201, StartTime: 21}
	other := orchestrator.ProcessToken{PID: 202, StartTime: 22}
	inspector := &sequenceInspector{trees: []orchestrator.ProcessTree{
		{
			Root: root,
			Descendants: []orchestrator.ProcessNode{
				{Token: harness, Parent: root},
				{Token: peer, Parent: harness},
				{Token: other, Parent: root},
			},
		},
		{
			Root: root,
			Descendants: []orchestrator.ProcessNode{
				{Token: harness, Parent: root},
				{Token: peer, Parent: harness},
			},
		},
	}}
	server, _ := listenTestServerWithOptions(t, inspector, func(opts *Options) {
		opts.peer = func(net.Conn) (peerCredentials, error) {
			return peerCredentials{uid: opts.Root.UID, pid: peer.PID}, nil
		}
		opts.startTime = func(int64) (int64, error) { return peer.StartTime, nil }
	})

	result := greetTestServer(t, dialTestServer(t, server))
	require.False(t, result.Accepted)
	require.Equal(t, HelloRejectPeerNotInTree, result.Reason)
	server.bindMu.Lock()
	require.Equal(t, orchestrator.ProcessToken{}, server.harnessChild)
	server.bindMu.Unlock()

	require.True(t, greetTestServer(t, dialTestServer(t, server)).Accepted)
	server.bindMu.Lock()
	require.Equal(t, harness, server.harnessChild)
	server.bindMu.Unlock()
}

func TestServerNormalizesHarnessVersions(t *testing.T) {
	t.Parallel()
	for name, test := range map[string]struct {
		harness protocol.Harness
		stored  string
		hello   string
		accept  bool
	}{
		"codex version line": {
			harness: protocol.HarnessCodex, stored: "codex-cli 0.154.0",
			hello: "0.154.0", accept: true,
		},
		"claude version line": {
			harness: protocol.HarnessClaudeCode, stored: "2.1.280 (Claude Code)",
			hello: "2.1.280", accept: true,
		},
		"unparseable versions": {
			harness: protocol.HarnessCodex, stored: "unknown", hello: "unknown",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			lineage := testLineage()
			lineage.Harness = test.harness
			lineage.HarnessVersion = test.stored
			queue, err := NewQueue(lineage)
			require.NoError(t, err)
			server, _ := listenTestServerWithOptions(t, acceptedInspector(), func(opts *Options) {
				opts.Queue = queue
			})
			hello := wireHello()
			hello.Harness = test.harness
			hello.HarnessVersion = test.hello
			conn := dialTestServer(t, server)
			require.NoError(t, WriteMessage(conn, hello))
			message, err := ReadMessage(conn)
			require.NoError(t, err)
			result, ok := message.(HelloResult)
			require.True(t, ok)
			require.Equal(t, test.accept, result.Accepted)
			if !test.accept {
				require.Equal(t, HelloRejectHarnessMismatch, result.Reason)
			}
		})
	}
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

func TestServerRejectsUnsupportedHelloBeforeBinding(t *testing.T) {
	t.Parallel()
	server, _ := listenTestServer(t, testInspector{err: errors.New("must not matter")})
	conn := dialTestServer(t, server)
	hello := wireHello()
	hello.SchemaVersion++
	require.NoError(t, WriteMessage(conn, hello))
	message, err := ReadMessage(conn)
	require.NoError(t, err)
	require.Equal(t, HelloResult{
		Operation: OperationHelloResult, Reason: HelloRejectUnsupportedSchema,
	}, message)
}
