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

	"github.com/smellak/gifford/internal/analyst"
	"github.com/smellak/gifford/internal/orchestrator"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
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
	return listenTestServerWithOptions(t, inspector, func(opts *Options) {
		opts.Lifecycle = lifecycle
	})
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

func TestHookNoWithoutClosureFallsBackAfterNativeTTL(t *testing.T) {
	// A denial in a one-shot permission hook produces no resolved callback:
	// the real emitter sends only approval.requested, receives its durable ACK,
	// and exits cleanly. The missing closure must not become an endless veto.
	lifecycle := newRecordingEmitterLifecycle()
	server, queue := listenTestServerLifecycle(t, acceptedInspector(), lifecycle)
	receiptAt := time.Date(2026, 11, 1, 8, 30, 0, 0, time.UTC)
	var receiptMu sync.RWMutex
	queue.now = func() time.Time {
		receiptMu.RLock()
		defer receiptMu.RUnlock()
		return receiptAt
	}
	setReceiptAt := func(next time.Time) {
		receiptMu.Lock()
		receiptAt = next
		receiptMu.Unlock()
	}
	conn := dialTestServer(t, server)
	require.True(t, greetTestServerAs(t, conn, EmitterTypeHook).Accepted)
	event := wireEvent()
	event.Type = protocol.EventTypeApprovalRequested
	event.RequestRef = "request-hook-no"
	require.NoError(t, WriteMessage(conn, event))
	require.Eventually(t, func() bool {
		_, ok := queue.Head()
		return ok
	}, time.Second, time.Millisecond)
	head, _ := queue.Head()
	queue.Settle(protocol.EmitEventResult{
		EventID: head.EventID, Status: protocol.EmitEventStatusStored,
	})
	message, err := ReadMessage(conn)
	require.NoError(t, err)
	require.IsType(t, Ack{}, message)
	require.NoError(t, conn.Close())
	select {
	case got := <-lifecycle.activated:
		require.Equal(t, EmitterTypeHook, got)
	case <-time.After(time.Second):
		t.Fatal("denial hook did not establish its verified binding")
	}
	select {
	case got := <-lifecycle.failed:
		t.Fatalf("clean denial hook EOF degraded %s", got)
	case <-time.After(50 * time.Millisecond):
	}

	native := analyst.NativeEvent{
		EventID: head.EventID, Type: head.EventType, At: head.WallTime,
		Sequence: head.Sequence, RequestRef: head.CorrelationID,
	}
	live := analyst.ReduceNative(
		[]analyst.NativeEvent{native}, head.WallTime.Add(analyst.NativeOperationTTL-time.Millisecond),
	)
	require.True(t, live.Blocks)
	require.Equal(t, protocol.AgentPhaseWaitingApproval, live.Phase)
	expired := analyst.ReduceNative(
		[]analyst.NativeEvent{native}, head.WallTime.Add(analyst.NativeOperationTTL),
	)
	require.False(t, expired.Blocks)
	require.Equal(t, protocol.AgentPhaseUnknown, expired.Phase)

	// A later hook establishes a real new binding. Its fresh operation may
	// become authoritative, but it cannot renew the expired request.
	setReceiptAt(head.WallTime.Add(analyst.NativeOperationTTL + time.Second))
	freshConn := dialTestServer(t, server)
	require.True(t, greetTestServerAs(t, freshConn, EmitterTypeHook).Accepted)
	freshEvent := wireEvent()
	freshEvent.EmitterEventID = "event-hook-after-expiry"
	freshEvent.Type = protocol.EventTypeApprovalRequested
	freshEvent.RequestRef = "request-hook-after-expiry"
	require.NoError(t, WriteMessage(freshConn, freshEvent))
	var freshHead protocol.EventEnvelope
	require.Eventually(t, func() bool {
		var ok bool
		freshHead, ok = queue.Head()
		return ok
	}, time.Second, time.Millisecond)
	require.Equal(t, head.Sequence+1, freshHead.Sequence)
	require.Equal(t, protocol.EventTypeApprovalRequested, freshHead.EventType)
	queue.Settle(protocol.EmitEventResult{
		EventID: freshHead.EventID, Status: protocol.EmitEventStatusStored,
	})
	message, err = ReadMessage(freshConn)
	require.NoError(t, err)
	require.IsType(t, Ack{}, message)
	require.NoError(t, freshConn.Close())
	select {
	case got := <-lifecycle.activated:
		require.Equal(t, EmitterTypeHook, got)
	case <-time.After(time.Second):
		t.Fatal("fresh hook event did not establish a new verified binding")
	}
	select {
	case got := <-lifecycle.failed:
		t.Fatalf("clean rebound hook EOF degraded %s", got)
	case <-time.After(50 * time.Millisecond):
	}
	freshNative := analyst.NativeEvent{
		EventID: freshHead.EventID, Type: freshHead.EventType, At: freshHead.WallTime,
		Sequence: freshHead.Sequence, RequestRef: freshHead.CorrelationID,
		TurnRef: freshHead.TurnID,
	}
	rebound := analyst.ReduceNative(
		[]analyst.NativeEvent{native, freshNative}, freshHead.WallTime,
	)
	require.True(t, rebound.Blocks)
	require.Equal(t, protocol.AgentPhaseWaitingApproval, rebound.Phase)
	require.Equal(t, []protocol.ULID{freshHead.EventID}, rebound.Evidence,
		"the fresh binding cannot resurrect the expired request")
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

func TestServerUnauthenticatedHelloMismatchDoesNotDegradeEmitter(t *testing.T) {
	t.Parallel()
	outside := testInspector{tree: orchestrator.ProcessTree{
		Root: orchestrator.ProcessToken{PID: 100, StartTime: 10},
	}}
	for _, test := range []struct {
		name   string
		mutate func(*Hello)
	}{
		{name: "schema", mutate: func(hello *Hello) { hello.SchemaVersion++ }},
		{name: "harness version", mutate: func(hello *Hello) { hello.HarnessVersion = "9.9.9" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			lifecycle := newRecordingEmitterLifecycle()
			server, _ := listenTestServerLifecycle(t, outside, lifecycle)
			conn := dialTestServer(t, server)
			hello := wireHello()
			test.mutate(&hello)
			require.NoError(t, WriteMessage(conn, hello))
			message, err := ReadMessage(conn)
			require.NoError(t, err)
			require.Equal(t, HelloResult{
				Operation: OperationHelloResult, Reason: HelloRejectPeerNotInTree,
			}, message)
			select {
			case got := <-lifecycle.failed:
				t.Fatalf("unauthenticated mismatch degraded %s", got)
			case <-time.After(50 * time.Millisecond):
			}
		})
	}
}
