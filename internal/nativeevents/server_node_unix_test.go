//go:build unix

package nativeevents

import (
	"context"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/orchestrator"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/socketpath"
)

const (
	nodeTestAgentID   = "agent-1"
	nodeTestSessionID = "session-1"
)

type stubNodeInstances struct {
	mu   sync.Mutex
	inst NodeInstance
	err  error
}

func (s *stubNodeInstances) LookupNodeInstance(context.Context, protocol.AgentID) (NodeInstance, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inst, s.err
}

func (s *stubNodeInstances) set(inst NodeInstance, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.inst, s.err = inst, err
}

func nodeTestInstance() NodeInstance {
	return NodeInstance{
		Agent:   nodeTestAgentID,
		Session: nodeTestSessionID,
		Harness: protocol.ProcessIdentity{
			PID: 100, ProcessStartTime: 10, UID: int64(os.Getuid()),
		},
		HarnessType: protocol.HarnessCodex, HarnessVersion: testHarnessVersion,
		Active: true,
	}
}

// nodeAnchorTree roots the fake ascent at the registered harness anchor:
// the hook (200) hangs off the anchor (100), the peer (300) off the hook.
// It mirrors the tmux shape where the session process already is the
// harness, so the pin must hold the anchor, never the hook child.
func nodeAnchorTree(rootPID, rootStart, hookPID, hookStart, peerPID, peerStart int64) orchestrator.ProcessTree {
	root := orchestrator.ProcessToken{PID: rootPID, StartTime: rootStart}
	hook := orchestrator.ProcessToken{PID: hookPID, StartTime: hookStart}
	return orchestrator.ProcessTree{
		Root: root,
		Descendants: []orchestrator.ProcessNode{
			{Token: hook, Parent: root},
			{Token: orchestrator.ProcessToken{PID: peerPID, StartTime: peerStart}, Parent: hook},
		},
	}
}

type stubNodeQueues struct {
	mu     sync.Mutex
	queues map[protocol.OpaqueID]*Queue
	err    error
}

func (s *stubNodeQueues) QueueForSession(_ context.Context, agent protocol.AgentID, session protocol.OpaqueID, _ NodeInstance) (*Queue, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return nil, s.err
	}
	if queue, ok := s.queues[session]; ok {
		return queue, nil
	}
	queue, err := NewQueue(Lineage{
		NodeID: "node-a", SessionID: session, AgentID: agent,
		Harness: protocol.HarnessCodex, HarnessVersion: testHarnessVersion,
		AdapterType: protocol.AdapterTypeNativeHook,
	})
	if err != nil {
		return nil, err
	}
	if s.queues == nil {
		s.queues = make(map[protocol.OpaqueID]*Queue)
	}
	s.queues[session] = queue
	return queue, nil
}

type stubNodeStore struct {
	mu       sync.Mutex
	appended []protocol.EventEnvelope
	err      error
}

func (s *stubNodeStore) AppendEvents(_ context.Context, events []protocol.EventEnvelope) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	s.appended = append(s.appended, events...)
	return nil
}

func (s *stubNodeStore) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.appended)
}

type nodeTestSetup struct {
	server    *NodeServer
	instances *stubNodeInstances
	queues    *stubNodeQueues
	store     *stubNodeStore
}

func listenNodeTestServer(
	t *testing.T,
	mutate func(opts *NodeOptions, instances *stubNodeInstances, queues *stubNodeQueues, store *stubNodeStore),
) nodeTestSetup {
	t.Helper()
	instances := &stubNodeInstances{inst: nodeTestInstance()}
	queues := &stubNodeQueues{}
	store := &stubNodeStore{}
	//nolint:usetesting // keep the Unix socket path below its platform limit
	dir, err := os.MkdirTemp("", "node-test-")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(dir)) })
	opts := NodeOptions{
		SocketDir: dir,
		Resolve: func(context.Context, int) (protocol.AgentID, protocol.OpaqueID, error) {
			return nodeTestAgentID, nodeTestSessionID, nil
		},
		Instances: instances,
		Queues:    queues,
		Store:     store,
		Inspector: testInspector{tree: nodeAnchorTree(100, 10, 200, 20, 300, 30)},
		peer: func(net.Conn) (peerCredentials, error) {
			return peerCredentials{uid: int64(os.Getuid()), pid: 300}, nil
		},
		startTime: func(int64) (int64, error) { return 30, nil },
	}
	if mutate != nil {
		mutate(&opts, instances, queues, store)
	}
	server, err := ListenNode(context.Background(), opts)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, server.Close()) })
	return nodeTestSetup{server: server, instances: instances, queues: queues, store: store}
}

// nodeQueue is the session queue the stub provider built for the test.
func (s nodeTestSetup) nodeQueue(t *testing.T, session protocol.OpaqueID) *Queue {
	t.Helper()
	s.queues.mu.Lock()
	defer s.queues.mu.Unlock()
	queue, ok := s.queues.queues[session]
	require.True(t, ok, "the hello must have created the session queue")
	return queue
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
	setup := listenNodeTestServer(t, nil)
	info, err := os.Stat(setup.server.Path())
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o666), info.Mode().Perm())

	conn := dialNodeTestServer(t, setup.server)
	require.True(t, greetNodeTestServer(t, conn, nil).Accepted)
	event := wireEvent()
	require.NoError(t, WriteMessage(conn, event))
	message, err := ReadMessage(conn)
	require.NoError(t, err)
	require.Equal(t, Ack{
		Operation: OperationAck, Sequence: event.Sequence,
		EmitterEventID: event.EmitterEventID,
	}, message, "the drain commits the head before the ack leaves the entry")
	require.Equal(t, 1, setup.store.count())
	require.Empty(t, func() []protocol.EventEnvelope {
		head, ok := setup.nodeQueue(t, nodeTestSessionID).Head()
		if ok {
			return []protocol.EventEnvelope{head}
		}
		return nil
	}(), "a stored head leaves the queue")
}

func TestNodeServerFailsClosedWhenTheStoreRefuses(t *testing.T) {
	t.Parallel()
	setup := listenNodeTestServer(t, func(_ *NodeOptions, _ *stubNodeInstances, _ *stubNodeQueues, store *stubNodeStore) {
		store.err = errors.New("store unavailable")
	})
	conn := dialNodeTestServer(t, setup.server)
	require.True(t, greetNodeTestServer(t, conn, nil).Accepted)
	require.NoError(t, WriteMessage(conn, wireEvent()))
	_, err := ReadMessage(conn)
	require.Error(t, err, "a refused store must close the connection without an ack")
}

func TestNodeServerRejectsWithoutValidIdentity(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		reason HelloRejectReason
		mutate func(opts *NodeOptions, instances *stubNodeInstances, queues *stubNodeQueues)
	}{
		{"resolve fails", HelloRejectPeerNotInTree, func(opts *NodeOptions, _ *stubNodeInstances, _ *stubNodeQueues) {
			opts.Resolve = func(context.Context, int) (protocol.AgentID, protocol.OpaqueID, error) {
				return "", "", errors.New("no session owns the peer")
			}
		}},
		{"unknown instance", HelloRejectPeerNotInTree, func(_ *NodeOptions, instances *stubNodeInstances, _ *stubNodeQueues) {
			instances.set(NodeInstance{}, errors.New("no active instance"))
		}},
		{"invalidated instance", HelloRejectPeerNotInTree, func(_ *NodeOptions, instances *stubNodeInstances, _ *stubNodeQueues) {
			inst := nodeTestInstance()
			inst.Active = false
			instances.set(inst, nil)
		}},
		{"instance bound to another session", HelloRejectPeerNotInTree, func(_ *NodeOptions, instances *stubNodeInstances, _ *stubNodeQueues) {
			inst := nodeTestInstance()
			inst.Session = "session-9"
			instances.set(inst, nil)
		}},
		{"missing harness anchor", HelloRejectPeerNotInTree, func(_ *NodeOptions, instances *stubNodeInstances, _ *stubNodeQueues) {
			inst := nodeTestInstance()
			inst.Harness = protocol.ProcessIdentity{}
			instances.set(inst, nil)
		}},
		{"peer uid differs from registered owner", HelloRejectPeerNotInTree, func(opts *NodeOptions, _ *stubNodeInstances, _ *stubNodeQueues) {
			opts.peer = func(net.Conn) (peerCredentials, error) {
				return peerCredentials{uid: int64(os.Getuid()) + 1, pid: 300}, nil
			}
		}},
		{"peer outside the registered harness", HelloRejectPeerNotInTree, func(opts *NodeOptions, _ *stubNodeInstances, _ *stubNodeQueues) {
			opts.startTime = func(int64) (int64, error) { return 999, nil }
		}},
		{"reused pid with a different start", HelloRejectPeerNotInTree, func(_ *NodeOptions, instances *stubNodeInstances, _ *stubNodeQueues) {
			inst := nodeTestInstance()
			inst.Harness.ProcessStartTime = 11
			instances.set(inst, nil)
		}},
		{"no queue for session", HelloRejectPeerNotInTree, func(_ *NodeOptions, _ *stubNodeInstances, queues *stubNodeQueues) {
			queues.err = errors.New("no session queue")
		}},
		{"observation incomplete", HelloRejectObservationIncomplete, func(opts *NodeOptions, _ *stubNodeInstances, _ *stubNodeQueues) {
			opts.Inspector = testInspector{err: orchestrator.ErrProcessObservationIncomplete}
		}},
		{"unsupported schema", HelloRejectUnsupportedSchema, func(_ *NodeOptions, _ *stubNodeInstances, _ *stubNodeQueues) {
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			setup := listenNodeTestServer(t, func(opts *NodeOptions, instances *stubNodeInstances, queues *stubNodeQueues, _ *stubNodeStore) {
				test.mutate(opts, instances, queues)
			})
			conn := dialNodeTestServer(t, setup.server)
			mutate := func(*Hello) {}
			if test.name == "unsupported schema" {
				mutate = func(hello *Hello) { hello.SchemaVersion = SchemaVersion + 1 }
			}
			result := greetNodeTestServer(t, conn, mutate)
			require.False(t, result.Accepted)
			require.Equal(t, test.reason, result.Reason)
		})
	}
}

func TestNodeServerRejectsHarnessMismatch(t *testing.T) {
	t.Parallel()
	lifecycle := newRecordingEmitterLifecycle()
	setup := listenNodeTestServer(t, func(opts *NodeOptions, _ *stubNodeInstances, _ *stubNodeQueues, _ *stubNodeStore) {
		opts.Lifecycle = func(protocol.AgentID, protocol.OpaqueID) EmitterLifecycle {
			return lifecycle
		}
	})
	conn := dialNodeTestServer(t, setup.server)
	result := greetNodeTestServer(t, conn, func(hello *Hello) {
		hello.Harness = protocol.HarnessOpencode
	})
	require.False(t, result.Accepted)
	require.Equal(t, HelloRejectHarnessMismatch, result.Reason)
	select {
	case <-lifecycle.failed:
	case <-time.After(time.Second):
		t.Fatal("rejected peer must report through the emitter lifecycle")
	}
}

func TestNodeServerRelineOnHarnessRotationAndVetoesTheOld(t *testing.T) {
	t.Parallel()
	inspector := &sequenceInspector{trees: []orchestrator.ProcessTree{
		nodeAnchorTree(100, 10, 200, 20, 300, 30),
		nodeAnchorTree(101, 11, 201, 21, 300, 30),
	}}
	setup := listenNodeTestServer(t, func(opts *NodeOptions, _ *stubNodeInstances, _ *stubNodeQueues, _ *stubNodeStore) {
		opts.Inspector = inspector
	})
	first := dialNodeTestServer(t, setup.server)
	require.True(t, greetNodeTestServer(t, first, nil).Accepted)

	rotated := nodeTestInstance()
	rotated.Harness.PID = 101
	rotated.Harness.ProcessStartTime = 11
	setup.instances.set(rotated, nil)
	second := dialNodeTestServer(t, setup.server)
	require.True(t, greetNodeTestServer(t, second, nil).Accepted,
		"a re-registered harness keeps emitting against the new anchor")

	require.NoError(t, WriteMessage(first, wireEvent()))
	_, err := ReadMessage(first)
	require.Error(t, err, "events after a harness rotation are vetoed")

	event := wireEvent()
	event.Sequence = 1
	require.NoError(t, WriteMessage(second, event))
	ack, err := ReadMessage(second)
	require.NoError(t, err)
	require.Equal(t, Ack{Operation: OperationAck, Sequence: 1, EmitterEventID: event.EmitterEventID}, ack)
	require.Equal(t, 1, setup.store.count(), "only the re-pinned session stores")
}

func TestNodeServerRoutesEventsByBindingNeverBySessionRef(t *testing.T) {
	t.Parallel()
	setup := listenNodeTestServer(t, nil)
	conn := dialNodeTestServer(t, setup.server)
	require.True(t, greetNodeTestServer(t, conn, nil).Accepted)

	event := wireEvent()
	event.SessionRef = "session-2"
	require.NoError(t, WriteMessage(conn, event))
	ack, err := ReadMessage(conn)
	require.NoError(t, err)
	require.IsType(t, Ack{}, ack)
	require.Equal(t, 1, setup.store.count())
	setup.queues.mu.Lock()
	other, otherExists := setup.queues.queues["session-2"]
	setup.queues.mu.Unlock()
	if otherExists {
		_, otherHas := other.Head()
		require.False(t, otherHas, "a forged session reference must not open delivery elsewhere")
	}
}

func TestNodeServerAnswersStatusWithoutDisturbingPins(t *testing.T) {
	t.Parallel()
	setup := listenNodeTestServer(t, nil)
	conn := dialNodeTestServer(t, setup.server)
	require.True(t, greetNodeTestServer(t, conn, nil).Accepted)

	statusConn := dialNodeTestServer(t, setup.server)
	require.NoError(t, WriteMessage(statusConn, StatusRequest{Operation: OperationStatus}))
	message, err := ReadMessage(statusConn)
	require.NoError(t, err)
	result, ok := message.(StatusResult)
	require.True(t, ok)
	require.True(t, result.Serving)
	require.Len(t, result.Receptions, 1)
	require.Equal(t, protocol.AgentID(nodeTestAgentID), result.Receptions[0].Agent)
	require.Equal(t, protocol.HarnessCodex, result.Receptions[0].Harness)
	require.Equal(t, testHarnessVersion, result.Receptions[0].HarnessVersion)
	require.Positive(t, result.Receptions[0].AcceptedAtUnixMs)

	again := dialNodeTestServer(t, setup.server)
	require.True(t, greetNodeTestServer(t, again, nil).Accepted,
		"a status request must not disturb the session pin")
}

func TestNodeServerStopsRejectingVocabulary(t *testing.T) {
	t.Parallel()
	setup := listenNodeTestServer(t, nil)
	conn := dialNodeTestServer(t, setup.server)
	require.True(t, greetNodeTestServer(t, conn, nil).Accepted)
	require.NoError(t, WriteMessage(conn, wireHello()))
	_, err := ReadMessage(conn)
	require.Error(t, err, "out-of-vocabulary traffic must terminate the connection")
}

// A node entry path one byte over the platform limit fails before the
// socket directory is inspected or any stale entry is claimed.
func TestListenNodeChecksSocketPathLength(t *testing.T) {
	t.Parallel()
	base, err := os.MkdirTemp("", "ln-") //nolint:usetesting // exact Unix socket lengths need a short stable base
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(base)) })
	padding := socketpath.MaxBytes + 1 - len(base) - len(NodeSocketName) - 2
	require.Positive(t, padding)
	dir := filepath.Join(base, strings.Repeat("d", padding))
	path := filepath.Join(dir, NodeSocketName)
	require.Len(t, path, socketpath.MaxBytes+1)

	_, err = ListenNode(context.Background(), NodeOptions{
		SocketDir: dir,
		Resolve: func(context.Context, int) (protocol.AgentID, protocol.OpaqueID, error) {
			return nodeTestAgentID, nodeTestSessionID, nil
		},
		Instances: &stubNodeInstances{inst: nodeTestInstance()},
		Queues:    &stubNodeQueues{},
		Store:     &stubNodeStore{},
		Inspector: testInspector{tree: nodeAnchorTree(100, 10, 200, 20, 300, 30)},
	})
	var lengthErr *socketpath.LengthError
	require.ErrorAs(t, err, &lengthErr)
	require.Equal(t, path, lengthErr.Path)
	require.Equal(t, socketpath.MaxBytes, lengthErr.Limit)
	_, statErr := os.Stat(dir)
	require.ErrorIs(t, statErr, os.ErrNotExist, "validation must precede any filesystem step")
}

func TestListenNodeRejectsMissingSocketDir(t *testing.T) {
	t.Parallel()
	_, err := ListenNode(context.Background(), NodeOptions{
		SocketDir: filepath.Join(t.TempDir(), "missing"),
	})
	require.Error(t, err)
}

// TestNodeEmitterChild is the emitter side of the real-process end to
// end test below. It never runs without its environment marker.
func TestNodeEmitterChild(t *testing.T) {
	if os.Getenv("GO_TEST_NODE_EMITTER") != "1" {
		t.Skip("node emitter child only")
	}
	fatal := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	path := os.Getenv("GO_TEST_NODE_SOCKET")
	conn, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: path, Net: "unix"})
	fatal(err)
	defer func() { _ = conn.Close() }()
	fatal(conn.SetDeadline(time.Now().Add(10 * time.Second)))
	fatal(WriteMessage(conn, wireHello()))
	message, err := ReadMessage(conn)
	fatal(err)
	result, ok := message.(HelloResult)
	fatal(func() error {
		if !ok || !result.Accepted {
			return errors.New("node entry refused the real peer hello")
		}
		return nil
	}())
	fatal(WriteMessage(conn, wireEvent()))
	ack, err := ReadMessage(conn)
	fatal(err)
	if _, ok := ack.(Ack); !ok {
		t.Fatal("node entry did not acknowledge durably")
	}
}

// TestNodeHarnessChild is the registered harness of the database end to
// end test: it waits for the test's go-ahead, spawns exactly one emitter
// child, waits for it and exits with its code.
func TestNodeHarnessChild(t *testing.T) {
	if os.Getenv("GO_TEST_NODE_HARNESS") != "1" {
		t.Skip("node harness child only")
	}
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for range 300 {
		if _, err := os.Stat(os.Getenv("GO_TEST_NODE_GO")); err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if _, err := os.Stat(os.Getenv("GO_TEST_NODE_GO")); err != nil {
		t.Fatal("harness go-ahead never arrived")
	}
	status := os.Getenv("GO_TEST_NODE_STATUS")
	cmd := exec.CommandContext(t.Context(), self, "-test.run=TestNodeEmitterChild$")
	cmd.Env = append(os.Environ(), "GO_TEST_NODE_EMITTER=1")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	outcome := "emitter-ok"
	if err := cmd.Run(); err != nil {
		outcome = "emitter-fail: " + err.Error()
	}
	if status != "" {
		_ = os.WriteFile(status, []byte(outcome), 0o600)
	}
	if outcome != "emitter-ok" {
		t.Fatalf("emitter failed: %s", outcome)
	}
}
