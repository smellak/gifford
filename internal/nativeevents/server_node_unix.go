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
	"sync"
	"syscall"
	"time"

	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/orchestrator"
	"github.com/smellak/gifford/internal/profiles"
	"github.com/smellak/gifford/internal/protocol"
)

// NodeSocketName is the well-known leaf of the node-shared native-event
// entry inside the node directory. Resumed tmux sessions never run
// gifford run, so their emitters cannot learn a per-run path from
// GIFFORD_ADAPTER_SOCKET; the node entry is the only stable address.
const NodeSocketName = "native.sock"

// SessionBinding is everything the node-shared entry needs about one live
// session. The session owner registers its durable queue; the node entry
// never invents one, so events always land where the observer reads them.
type SessionBinding struct {
	Process protocol.ProcessIdentity
	Queue   *Queue
}

// NodeSessionLookup resolves a registry-authenticated session to its
// binding. A missing session or queue refuses the hello: the entry
// never stores for a session nobody observes.
type NodeSessionLookup interface {
	LookupNodeSession(ctx context.Context, agent protocol.AgentID, session protocol.OpaqueID) (SessionBinding, error)
}

// NodeOptions binds one node-shared native-event entry. Unlike Options,
// there is no fixed run root: every connection authenticates its own peer
// against the session registry.
type NodeOptions struct {
	// SocketDir holds the entry; it must already exist, belong to this
	// uid and be writable by nobody else, mirroring the hub node socket.
	SocketDir string
	// Resolve maps a peer pid to the CONNECTED session that anchors it,
	// typically registry.ResolveAgentSessionPID.
	Resolve func(ctx context.Context, pid int) (protocol.AgentID, protocol.OpaqueID, error)
	// Sessions maps the resolved session to its binding.
	Sessions NodeSessionLookup
	// Inspector checks the peer descends from the session process.
	Inspector orchestrator.PeerProcessInspector
	// Lifecycle, when set, reports emitter state per accepted session.
	Lifecycle func(agent protocol.AgentID, session protocol.OpaqueID) EmitterLifecycle

	peer         peerCredentialReader
	startTime    processStartTimeReader
	helloTimeout time.Duration
	now          func() time.Time
}

// NodeServer owns one node-shared native-event Unix socket. Each accepted
// emitter is bound to exactly one CONNECTED session by SO_PEERCRED plus
// the session registry; anything else is closed before a byte is read,
// and every rejection uses the closed wire vocabulary.
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

	pinMu sync.Mutex
	pins  map[protocol.OpaqueID]orchestrator.ProcessToken
}

// ListenNode creates the node-shared entry and starts accepting emitters.
func ListenNode(ctx context.Context, opts NodeOptions) (*NodeServer, error) {
	if ctx == nil || !filepath.IsAbs(opts.SocketDir) || opts.Resolve == nil ||
		opts.Sessions == nil || opts.Inspector == nil {
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
		pins: make(map[protocol.OpaqueID]orchestrator.ProcessToken),
	}
	server.wg.Add(1)
	go func() {
		defer server.wg.Done()
		server.serve()
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
	hello, ok := message.(Hello)
	if !ok {
		return
	}
	queue, lifecycle, reason, peerBound := s.bind(peer, peerErr, hello)
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
	pumpEvents(s.ctx, conn, queue, lifecycle, hello)
}

// bind attributes one hello to exactly one CONNECTED session. Every check
// fails closed onto the wire's closed rejection vocabulary.
func (s *NodeServer) bind(
	peer peerCredentials,
	peerErr error,
	hello Hello,
) (queue *Queue, lifecycle EmitterLifecycle, reason HelloRejectReason, peerBound bool) {
	if peerErr != nil || peer.pid <= 0 || peer.startTime <= 0 {
		return nil, nil, HelloRejectPeerNotInTree, false
	}
	agent, session, err := s.opts.Resolve(s.ctx, int(peer.pid))
	if err != nil || !agent.Valid() || !session.Valid() {
		return nil, nil, HelloRejectPeerNotInTree, false
	}
	binding, err := s.opts.Sessions.LookupNodeSession(s.ctx, agent, session)
	if err != nil || binding.Queue == nil || binding.Process.PID <= 0 || binding.Process.ProcessStartTime < 0 {
		return nil, nil, HelloRejectPeerNotInTree, false
	}
	if peer.uid != binding.Process.UID {
		return nil, nil, HelloRejectPeerNotInTree, false
	}
	if s.opts.Lifecycle != nil {
		lifecycle = s.opts.Lifecycle(agent, session)
	}
	peerToken := orchestrator.ProcessToken{PID: peer.pid, StartTime: peer.startTime}
	lineageObservation, err := s.opts.Inspector.InspectPeer(binding.Process, peerToken)
	if orchestrator.IsProcessObservationIncomplete(err) {
		return nil, lifecycle, HelloRejectObservationIncomplete, false
	}
	if err != nil {
		return nil, lifecycle, HelloRejectPeerNotInTree, false
	}
	if !s.pinHarness(session, lineageObservation, peerToken) {
		return nil, lifecycle, HelloRejectPeerNotInTree, false
	}
	if hello.SchemaVersion != SchemaVersion {
		return nil, lifecycle, HelloRejectUnsupportedSchema, true
	}
	lineage := binding.Queue.Lineage()
	helloVersion := profiles.ParseVersion(hello.HarnessVersion)
	lineageVersion := profiles.ParseVersion(lineage.HarnessVersion)
	if hello.Harness != lineage.Harness || helloVersion == "" || lineageVersion == "" ||
		helloVersion != lineageVersion {
		return nil, lifecycle, HelloRejectHarnessMismatch, true
	}
	return binding.Queue, lifecycle, "", true
}

// pinHarness keeps one harness child per session, mirroring the run-bound
// server's single-harness pin across its own sessions.
func (s *NodeServer) pinHarness(session protocol.OpaqueID, lineage orchestrator.PeerLineage, peer orchestrator.ProcessToken) bool {
	if lineage.Peer != peer || lineage.Harness.PID <= 0 || lineage.Harness.StartTime <= 0 {
		return false
	}
	s.pinMu.Lock()
	defer s.pinMu.Unlock()
	if pinned, found := s.pins[session]; found && pinned != lineage.Harness {
		return false
	}
	s.pins[session] = lineage.Harness
	return true
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
