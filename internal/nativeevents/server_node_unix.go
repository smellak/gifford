//go:build unix

package nativeevents

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/orchestrator"
	"github.com/smellak/gifford/internal/profiles"
	"github.com/smellak/gifford/internal/protocol"
)

// NodeInstance is the registered identity one node hello must prove: the
// harness pid and start time recorded at registration, its version, the
// owner uid, the bound session, and whether the row is still ACTIVE.
// Neither the hello, the environment nor the event arguments carry
// identity; this row does.
type NodeInstance struct {
	Agent          protocol.AgentID
	Session        protocol.OpaqueID
	Harness        protocol.ProcessIdentity
	HarnessType    protocol.Harness
	HarnessVersion string
	Active         bool
}

// InstanceLookup resolves the registered instance of an agent. An unknown
// agent, an INVALID row and a missing row all refuse the hello with the
// same closed reason: the wire never distinguishes them.
type InstanceLookup interface {
	LookupNodeInstance(ctx context.Context, agent protocol.AgentID) (NodeInstance, error)
}

// SessionQueues resolves the durable queue of one CONNECTED session.
// Implementations share one *Queue per session so sequences and emitter
// dedup stay stable across the session's connections. A missing session
// or queue refuses the hello: the entry never stores for a session nobody
// observes.
type SessionQueues interface {
	QueueForSession(ctx context.Context, agent protocol.AgentID, session protocol.OpaqueID, instance NodeInstance) (*Queue, error)
}

// DurableStore appends validated envelopes in one atomic transaction.
// The node entry acks an event only after the store commits it.
type DurableStore interface {
	AppendEvents(ctx context.Context, events []protocol.EventEnvelope) error
}

// NodeOptions binds one node-shared native-event entry. Unlike Options,
// there is no fixed run root: every connection authenticates its own peer
// against the session registry and the registered instance row.
type NodeOptions struct {
	// SocketDir holds the entry; it must already exist, belong to this
	// uid and be writable by nobody else, mirroring the hub node socket.
	SocketDir string
	// Resolve maps a peer pid to the CONNECTED session that anchors it,
	// typically registry.ResolveAgentSessionPID.
	Resolve func(ctx context.Context, pid int) (protocol.AgentID, protocol.OpaqueID, error)
	// Instances maps the resolved agent to its registered instance row.
	Instances InstanceLookup
	// Queues maps the resolved session to its durable queue.
	Queues SessionQueues
	// Store commits every queued head before its ack leaves the entry.
	Store DurableStore
	// Inspector proves the peer descends from the registered harness pid.
	Inspector orchestrator.PeerProcessInspector
	// Lifecycle, when set, reports emitter state per accepted session.
	Lifecycle func(agent protocol.AgentID, session protocol.OpaqueID) EmitterLifecycle

	peer         peerCredentialReader
	startTime    processStartTimeReader
	helloTimeout time.Duration
	now          func() time.Time
}

// NodeServer owns one node-shared native-event Unix socket. Each accepted
// emitter is bound to exactly one CONNECTED session by SO_PEERCRED, the
// session registry and the registered instance row; anything else is
// closed, and every rejection uses the closed wire vocabulary.
type NodeServer struct {
	ctx    context.Context
	cancel context.CancelFunc
	opts   NodeOptions
	path   string
	ln     *net.UnixListener
	own    os.FileInfo

	mu        sync.Mutex
	conns     map[*net.UnixConn]struct{}
	closed    bool
	closeOnce sync.Once
	wg        sync.WaitGroup

	rejectMu   sync.Mutex
	rejections map[HelloRejectReason]uint64

	helloMu sync.Mutex
	hellos  map[protocol.OpaqueID]InstanceReception

	pinMu sync.Mutex
	pins  map[protocol.OpaqueID]orchestrator.ProcessToken

	queueMu   sync.Mutex
	queues    map[*Queue]struct{}
	queueWake chan struct{}
}

// ListenNode creates the node-shared entry and starts accepting emitters.
func ListenNode(ctx context.Context, opts NodeOptions) (*NodeServer, error) {
	if ctx == nil || !filepath.IsAbs(opts.SocketDir) || opts.Resolve == nil ||
		opts.Instances == nil || opts.Queues == nil || opts.Store == nil || opts.Inspector == nil {
		return nil, ErrInvalidOptions
	}
	if opts.peer == nil {
		opts.peer = readPeerCredentials
	}
	if opts.startTime == nil {
		opts.startTime = readProcessStartTime
	}
	if opts.helloTimeout <= 0 {
		opts.helloTimeout = helloReadTimeout
	}
	if opts.now == nil {
		opts.now = clock.System{}.Now
	}
	path := filepath.Join(filepath.Clean(opts.SocketDir), NodeSocketName)
	if err := nodeSocketDir(path); err != nil {
		return nil, err
	}
	if err := claimNodeSocket(ctx, path); err != nil {
		return nil, err
	}
	listener, err := (&net.ListenConfig{}).Listen(ctx, "unix", path)
	if err != nil {
		return nil, fmt.Errorf("nativeevents: listen node: %w", err)
	}
	ln, ok := listener.(*net.UnixListener)
	if !ok {
		_ = listener.Close()
		return nil, fmt.Errorf("nativeevents: node listen: not a unix listener")
	}
	ln.SetUnlinkOnClose(false)
	own, err := os.Lstat(path)
	if err == nil {
		//nolint:gosec // every local user may connect; each peer is classified by SO_PEERCRED.
		err = os.Chmod(path, 0o666)
	}
	if err != nil {
		_ = ln.Close()
		if own != nil {
			removeIfSame(path, own)
		}
		return nil, fmt.Errorf("nativeevents: node socket mode: %w", err)
	}
	serverCtx, cancel := context.WithCancel(ctx)
	server := &NodeServer{
		ctx: serverCtx, cancel: cancel, opts: opts, path: path, ln: ln, own: own,
		conns: make(map[*net.UnixConn]struct{}), rejections: make(map[HelloRejectReason]uint64),
		hellos: make(map[protocol.OpaqueID]InstanceReception),
		pins:   make(map[protocol.OpaqueID]orchestrator.ProcessToken),
		queues: make(map[*Queue]struct{}), queueWake: make(chan struct{}, 1),
	}
	server.wg.Add(1)
	go func() {
		defer server.wg.Done()
		server.serve()
	}()
	server.wg.Add(1)
	go func() {
		defer server.wg.Done()
		server.drain()
	}()
	go func() {
		select {
		case <-ctx.Done():
			_ = server.Close()
		case <-serverCtx.Done():
		}
	}()
	return server, nil
}

// Path returns the node entry path.
func (s *NodeServer) Path() string { return s.path }

// HelloRejections returns a copy of the closed rejection counters. The
// diagnostic intentionally retains no emitter ids, paths or payload.
func (s *NodeServer) HelloRejections() map[HelloRejectReason]uint64 {
	s.rejectMu.Lock()
	defer s.rejectMu.Unlock()
	counts := make(map[HelloRejectReason]uint64, len(s.rejections))
	for reason, count := range s.rejections {
		counts[reason] = count
	}
	return counts
}

// Close stops the listener and every emitter connection, then removes only
// the socket inode this server created.
func (s *NodeServer) Close() error {
	s.closeOnce.Do(func() {
		s.cancel()
		_ = s.ln.Close()
		s.mu.Lock()
		s.closed = true
		connections := make([]*net.UnixConn, 0, len(s.conns))
		for conn := range s.conns {
			connections = append(connections, conn)
		}
		s.mu.Unlock()
		for _, conn := range connections {
			_ = conn.Close()
		}
		s.wg.Wait()
		removeIfSame(s.path, s.own)
	})
	return nil
}

func (s *NodeServer) serve() {
	for {
		conn, err := s.ln.AcceptUnix()
		if err != nil {
			return
		}
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			_ = conn.Close()
			return
		}
		s.conns[conn] = struct{}{}
		s.wg.Add(1)
		s.mu.Unlock()
		go func() {
			defer s.wg.Done()
			defer func() {
				s.mu.Lock()
				delete(s.conns, conn)
				s.mu.Unlock()
				_ = conn.Close()
			}()
			s.handle(conn)
		}()
	}
}

func (s *NodeServer) handle(conn *net.UnixConn) {
	peer, peerErr := s.readPeer(conn)
	if conn.SetReadDeadline(s.opts.now().Add(s.opts.helloTimeout)) != nil {
		return
	}
	message, err := ReadMessage(conn)
	if err != nil {
		return
	}
	if conn.SetReadDeadline(time.Time{}) != nil {
		return
	}
	switch first := message.(type) {
	case StatusRequest:
		_ = WriteMessage(conn, s.snapshot())
		return
	case Hello:
		s.serveHello(conn, peer, peerErr, first)
	default:
		return
	}
}

func (s *NodeServer) serveHello(conn *net.UnixConn, peer peerCredentials, peerErr error, hello Hello) {
	queue, lifecycle, anchor, reason, peerBound := s.bind(peer, peerErr, hello)
	if reason != "" {
		s.rejectMu.Lock()
		s.rejections[reason]++
		s.rejectMu.Unlock()
	}
	result := HelloResult{Operation: OperationHelloResult, Accepted: reason == "", Reason: reason}
	if reason != "" && peerBound && lifecycle != nil {
		lifecycle.NativeEmitterFailed(hello.EmitterType)
	}
	if WriteMessage(conn, result) != nil || reason != "" {
		return
	}
	s.recordHello(anchor.session, anchor.agent, hello)
	s.watch(queue)
	pumpEvents(s.ctx, conn, queue, lifecycle, hello, s.revalidate(anchor))
}

// nodeAnchor is the identity a hello proved: the registered anchor token
// plus the session and agent it was bound to.
type nodeAnchor struct {
	agent   protocol.AgentID
	session protocol.OpaqueID
	peerUID int64
	token   orchestrator.ProcessToken
}

// bind attributes one hello to exactly one CONNECTED session. Identity
// comes only from SO_PEERCRED and the ascent to the registered harness
// pid with its registered start time and owner uid; the hello, the
// environment and the event arguments never contribute. Every check fails
// closed onto the wire's closed rejection vocabulary.
func (s *NodeServer) bind(
	peer peerCredentials,
	peerErr error,
	hello Hello,
) (queue *Queue, lifecycle EmitterLifecycle, anchor nodeAnchor, reason HelloRejectReason, peerBound bool) {
	if peerErr != nil || peer.pid <= 0 || peer.startTime <= 0 {
		return nil, nil, nodeAnchor{}, HelloRejectPeerNotInTree, false
	}
	agent, session, err := s.opts.Resolve(s.ctx, int(peer.pid))
	if err != nil || !agent.Valid() || !session.Valid() {
		return nil, nil, nodeAnchor{}, HelloRejectPeerNotInTree, false
	}
	instance, err := s.opts.Instances.LookupNodeInstance(s.ctx, agent)
	if err != nil || !instance.Active {
		return nil, nil, nodeAnchor{}, HelloRejectPeerNotInTree, false
	}
	if instance.Session != "" && instance.Session != session {
		return nil, nil, nodeAnchor{}, HelloRejectPeerNotInTree, false
	}
	if instance.Harness.PID <= 0 || instance.Harness.ProcessStartTime <= 0 {
		return nil, nil, nodeAnchor{}, HelloRejectPeerNotInTree, false
	}
	if instance.Harness.UID != peer.uid {
		return nil, nil, nodeAnchor{}, HelloRejectPeerNotInTree, false
	}
	var boundLifecycle EmitterLifecycle
	if s.opts.Lifecycle != nil {
		boundLifecycle = s.opts.Lifecycle(agent, session)
	}
	proven := orchestrator.ProcessToken{
		PID: instance.Harness.PID, StartTime: instance.Harness.ProcessStartTime,
	}
	lineage, err := s.opts.Inspector.InspectPeer(protocol.ProcessIdentity{
		PID: instance.Harness.PID, ProcessStartTime: instance.Harness.ProcessStartTime,
		UID: instance.Harness.UID,
	}, orchestrator.ProcessToken{PID: peer.pid, StartTime: peer.startTime})
	if orchestrator.IsProcessObservationIncomplete(err) {
		return nil, boundLifecycle, nodeAnchor{}, HelloRejectObservationIncomplete, false
	}
	if err != nil {
		return nil, boundLifecycle, nodeAnchor{}, HelloRejectPeerNotInTree, false
	}
	peerToken := orchestrator.ProcessToken{PID: peer.pid, StartTime: peer.startTime}
	if lineage.Peer != peerToken || lineage.Root != proven {
		return nil, boundLifecycle, nodeAnchor{}, HelloRejectPeerNotInTree, false
	}
	s.pin(session, proven)
	if hello.SchemaVersion != SchemaVersion {
		return nil, boundLifecycle, nodeAnchor{}, HelloRejectUnsupportedSchema, true
	}
	bound, err := s.opts.Queues.QueueForSession(s.ctx, agent, session, instance)
	if err != nil || bound == nil {
		return nil, boundLifecycle, nodeAnchor{}, HelloRejectPeerNotInTree, false
	}
	queueLineage := bound.Lineage()
	helloVersion := profiles.ParseVersion(hello.HarnessVersion)
	lineageVersion := profiles.ParseVersion(queueLineage.HarnessVersion)
	if hello.Harness != queueLineage.Harness || helloVersion == "" || lineageVersion == "" ||
		helloVersion != lineageVersion {
		return nil, boundLifecycle, nodeAnchor{}, HelloRejectHarnessMismatch, true
	}
	return bound, boundLifecycle, nodeAnchor{
		agent: agent, session: session, peerUID: peer.uid, token: proven,
	}, "", true
}

// watch registers a session queue with the durable drain. The drain is the
// only Settler: every head is committed to the store before its ack leaves
// the entry, and a store failure fails the event closed without an ack.
func (s *NodeServer) watch(queue *Queue) {
	if queue == nil {
		return
	}
	s.queueMu.Lock()
	defer s.queueMu.Unlock()
	if _, known := s.queues[queue]; known {
		return
	}
	s.queues[queue] = struct{}{}
	select {
	case s.queueWake <- struct{}{}:
	default:
	}
}

// drain commits every queued head of every watched session queue and
// settles it stored, or rejected when the store refuses. One goroutine
// serves all queues so idle sessions never strand a drain each.
func (s *NodeServer) drain() {
	for {
		s.queueMu.Lock()
		cases := make([]reflect.SelectCase, 0, len(s.queues)+2)
		cases = append(cases, reflect.SelectCase{
			Dir: reflect.SelectRecv, Chan: reflect.ValueOf(s.ctx.Done()),
		})
		cases = append(cases, reflect.SelectCase{
			Dir: reflect.SelectRecv, Chan: reflect.ValueOf(s.queueWake),
		})
		queues := make([]*Queue, 0, len(s.queues))
		for queue := range s.queues {
			queues = append(queues, queue)
			cases = append(cases, reflect.SelectCase{
				Dir: reflect.SelectRecv, Chan: reflect.ValueOf(queue.HeadChanged()),
			})
		}
		s.queueMu.Unlock()
		chosen, _, _ := reflect.Select(cases)
		if chosen == 0 {
			return
		}
		for _, queue := range queues {
			s.drainHeads(queue)
		}
	}
}

func (s *NodeServer) drainHeads(queue *Queue) {
	for {
		head, ok := queue.Head()
		if !ok {
			return
		}
		result := protocol.EmitEventResult{
			EventID: head.EventID, Status: protocol.EmitEventStatusStored,
		}
		if err := s.opts.Store.AppendEvents(s.ctx, []protocol.EventEnvelope{head}); err != nil {
			result.Status = protocol.EmitEventStatusRejected
		}
		queue.Settle(result)
	}
}

// pin keeps one harness anchor per session. The registry stays the
// authority: a hello that ascends to the current ACTIVE anchor re-pins it,
// so a re-registered harness keeps emitting while a replaced anchor
// vetoes the older connection on its next event.
func (s *NodeServer) pin(session protocol.OpaqueID, anchor orchestrator.ProcessToken) {
	s.pinMu.Lock()
	defer s.pinMu.Unlock()
	s.pins[session] = anchor
}

// revalidate vetoes single events before durable storage when the instance
// row no longer backs the hello: not ACTIVE, rebound to another session,
// a changed owner uid, or a rotated harness anchor. A vetoed event ends the
// connection without an ack, and never authorizes delivery anywhere else.
func (s *NodeServer) revalidate(anchor nodeAnchor) func(Event) error {
	return func(Event) error {
		instance, err := s.opts.Instances.LookupNodeInstance(s.ctx, anchor.agent)
		if err != nil || !instance.Active {
			return errors.New("nativeevents: instance no longer active")
		}
		if instance.Session != "" && instance.Session != anchor.session {
			return errors.New("nativeevents: instance rebound to another session")
		}
		if instance.Harness.UID != anchor.peerUID {
			return errors.New("nativeevents: instance owner changed")
		}
		if instance.Harness.PID != anchor.token.PID ||
			instance.Harness.ProcessStartTime != anchor.token.StartTime {
			return errors.New("nativeevents: harness anchor rotated")
		}
		return nil
	}
}

// recordHello remembers the last accepted hello per session for the status
// answer. Only the hello's public attributes are retained.
func (s *NodeServer) recordHello(session protocol.OpaqueID, agent protocol.AgentID, hello Hello) {
	s.helloMu.Lock()
	defer s.helloMu.Unlock()
	s.hellos[session] = InstanceReception{
		Agent: agent, Harness: hello.Harness, HarnessVersion: hello.HarnessVersion,
		AcceptedAtUnixMs: s.opts.now().UnixMilli(),
	}
}

// snapshot answers a status request. It carries no session ids, emitter
// ids, paths or event content: only the last accepted hello per instance.
func (s *NodeServer) snapshot() StatusResult {
	s.helloMu.Lock()
	defer s.helloMu.Unlock()
	result := StatusResult{Operation: OperationStatusResult, Serving: true}
	for _, reception := range s.hellos {
		result.Receptions = append(result.Receptions, reception)
	}
	sort.Slice(result.Receptions, func(i, j int) bool {
		if result.Receptions[i].Agent != result.Receptions[j].Agent {
			return result.Receptions[i].Agent < result.Receptions[j].Agent
		}
		return result.Receptions[i].AcceptedAtUnixMs < result.Receptions[j].AcceptedAtUnixMs
	})
	return result
}

func (s *NodeServer) readPeer(conn net.Conn) (peerCredentials, error) {
	peer, err := s.opts.peer(conn)
	if err != nil {
		return peerCredentials{}, err
	}
	startTime, err := s.opts.startTime(peer.pid)
	if err != nil {
		return peerCredentials{}, err
	}
	peer.startTime = startTime
	return peer, nil
}

// nodeSocketDir mirrors the hub node socket discipline: the directory must
// already exist, belong to this uid and be writable by nobody else, so no
// other user can replace the entry.
func nodeSocketDir(path string) error {
	dir := filepath.Dir(path)
	fi, err := os.Lstat(dir)
	if err != nil {
		return fmt.Errorf("nativeevents: node socket directory %s: %w", dir, err)
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok || !fi.IsDir() || fi.Mode()&fs.ModeSymlink != 0 ||
		int64(st.Uid) != int64(os.Geteuid()) || fi.Mode().Perm()&0o022 != 0 {
		return fmt.Errorf("nativeevents: node socket directory %s: must be this uid's own directory, writable only by it: %w",
			dir, ErrPrivateDir)
	}
	return nil
}

// claimNodeSocket prepares path for binding. A missing path is free. A
// socket file with a live server behind it refuses: only a socket proven
// stale by a failed dial may be removed. Anything else refuses, so Listen
// never deletes foreign files.
func claimNodeSocket(ctx context.Context, path string) error {
	fi, err := os.Lstat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("nativeevents: stat node socket: %w", err)
	}
	if fi.Mode().Type() != os.ModeSocket {
		return fmt.Errorf("nativeevents: %s is not a socket: %w", path, ErrNotOurSocket)
	}
	probe, err := (&net.Dialer{Timeout: probeTimeout}).DialContext(ctx, "unix", path)
	if err == nil {
		_ = probe.Close()
		return fmt.Errorf("nativeevents: live server on %s: %w", path, ErrSocketInUse)
	}
	if !errors.Is(err, syscall.ECONNREFUSED) {
		return fmt.Errorf("nativeevents: probe node socket %s: %w", path, err)
	}
	if rerr := os.Remove(path); rerr != nil {
		return fmt.Errorf("nativeevents: remove stale node socket: %w", rerr)
	}
	return nil
}
