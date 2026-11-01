//go:build unix

package nativeevents

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/smellak/gifford/internal/orchestrator"
	"github.com/smellak/gifford/internal/protocol"
)

const (
	nativeEventsDir = "native-events"
	probeTimeout    = 100 * time.Millisecond
)

var (
	// ErrInvalidOptions means the listener cannot bind events to one run.
	ErrInvalidOptions = errors.New("nativeevents: invalid listener options")
	// ErrPrivateDir means the socket parent is not a private real directory.
	ErrPrivateDir = errors.New("nativeevents: socket directory is not private")
	// ErrSocketInUse means another live listener owns the requested entry.
	ErrSocketInUse = errors.New("nativeevents: socket is in use")
	// ErrNotOurSocket means the existing entry is not this user's Unix socket.
	ErrNotOurSocket = errors.New("nativeevents: existing entry is not our socket")
)

type peerCredentials struct {
	uid int64
	pid int64
}

type peerCredentialReader func(net.Conn) (peerCredentials, error)

// EmitterLifecycle receives only state changes of an authenticated native
// emitter connection. Implementations must not block on durable storage.
type EmitterLifecycle interface {
	NativeEmitterActivated(EmitterType) error
	NativeEmitterFailed(EmitterType)
}

// Options binds a listener to one authenticated run and its durable queue.
type Options struct {
	RuntimeDir string
	Root       protocol.ProcessIdentity
	Inspector  orchestrator.ProcessInspector
	Queue      *Queue
	Lifecycle  EmitterLifecycle

	peer peerCredentialReader
}

// Server owns one native-event Unix socket.
type Server struct {
	ctx    context.Context
	cancel context.CancelFunc
	opts   Options
	path   string
	ln     *net.UnixListener
	own    os.FileInfo

	mu     sync.Mutex
	conns  map[*net.UnixConn]struct{}
	closed bool
	wg     sync.WaitGroup
	once   sync.Once
}

// SocketPath returns the native-event entry of agent under runtimeDir.
func SocketPath(runtimeDir string, agent protocol.AgentID) string {
	return filepath.Join(runtimeDir, nativeEventsDir, string(agent)+".sock")
}

// Listen creates the private entry and starts accepting emitters.
func Listen(ctx context.Context, opts Options) (*Server, error) {
	if ctx == nil || !filepath.IsAbs(opts.RuntimeDir) || opts.Queue == nil ||
		opts.Root.Validate() != nil {
		return nil, ErrInvalidOptions
	}
	if opts.Inspector == nil {
		opts.Inspector = orchestrator.NewProcessInspector("")
	}
	if opts.peer == nil {
		opts.peer = readPeerCredentials
	}
	agent := opts.Queue.lineage.AgentID
	path := SocketPath(filepath.Clean(opts.RuntimeDir), agent)
	if err := privateDir(filepath.Dir(path)); err != nil {
		return nil, err
	}
	if err := clearDead(ctx, path); err != nil {
		return nil, err
	}
	listener, err := (&net.ListenConfig{}).Listen(ctx, "unix", path)
	if err != nil {
		return nil, fmt.Errorf("nativeevents: listen: %w", err)
	}
	ln, ok := listener.(*net.UnixListener)
	if !ok {
		_ = listener.Close()
		return nil, fmt.Errorf("nativeevents: listen: not a unix listener")
	}
	ln.SetUnlinkOnClose(false)
	own, err := os.Lstat(path)
	if err == nil {
		err = os.Chmod(path, 0o600)
	}
	if err != nil {
		_ = ln.Close()
		if own != nil {
			removeIfSame(path, own)
		}
		return nil, fmt.Errorf("nativeevents: socket mode: %w", err)
	}
	serverCtx, cancel := context.WithCancel(ctx)
	server := &Server{
		ctx: serverCtx, cancel: cancel, opts: opts, path: path, ln: ln, own: own,
		conns: make(map[*net.UnixConn]struct{}),
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

// Path returns the socket path created by this server.
func (s *Server) Path() string { return s.path }

// Queue returns the durable queue served by this entry.
func (s *Server) Queue() *Queue { return s.opts.Queue }

// Close stops the listener and every emitter connection, then removes only
// the socket inode this server created.
func (s *Server) Close() error {
	s.once.Do(func() {
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

func (s *Server) serve() {
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

func (s *Server) handle(conn *net.UnixConn) {
	message, err := ReadMessage(conn)
	if err != nil {
		return
	}
	hello, ok := message.(Hello)
	if !ok {
		return
	}
	reason, peerBound := s.bind(conn, hello)
	result := HelloResult{Operation: OperationHelloResult, Accepted: reason == "", Reason: reason}
	if reason != "" && peerBound && s.opts.Lifecycle != nil {
		s.opts.Lifecycle.NativeEmitterFailed(hello.EmitterType)
	}
	if WriteMessage(conn, result) != nil || reason != "" {
		return
	}
	lifecycleBound := s.opts.Lifecycle != nil
	activated := false
	lastAcked := false
	cleanHookEOF := false
	defer func() {
		if !lifecycleBound || cleanHookEOF || s.ctx.Err() != nil {
			return
		}
		s.opts.Lifecycle.NativeEmitterFailed(hello.EmitterType)
	}()
	lastSequence := int64(0)
	seen := make(map[int64]Event)
	for {
		message, err = ReadMessage(conn)
		if err != nil {
			cleanHookEOF = hello.EmitterType == EmitterTypeHook && lastAcked && errors.Is(err, io.EOF)
			return
		}
		event, ok := message.(Event)
		if !ok {
			return
		}
		if prior, exists := seen[event.Sequence]; exists {
			if prior != event {
				return
			}
		} else {
			if event.Sequence != lastSequence+1 {
				return
			}
			seen[event.Sequence] = event
			lastSequence = event.Sequence
		}
		if !activated && s.opts.Lifecycle != nil {
			if err := s.opts.Lifecycle.NativeEmitterActivated(hello.EmitterType); err != nil {
				return
			}
			activated = true
		}
		receipt, err := s.opts.Queue.Submit(hello.EmitterType, event)
		if err != nil || !receipt.Accepted() || !receipt.Wait(s.ctx) {
			return
		}
		if WriteMessage(conn, Ack{
			Operation: OperationAck, Sequence: event.Sequence,
			EmitterEventID: event.EmitterEventID,
		}) != nil {
			return
		}
		lastAcked = true
	}
}

func (s *Server) bind(conn net.Conn, hello Hello) (HelloRejectReason, bool) {
	lineage := s.opts.Queue.lineage
	peer, err := s.opts.peer(conn)
	if err != nil || peer.uid != s.opts.Root.UID || peer.pid <= 0 {
		return HelloRejectPeerNotInTree, false
	}
	tree, err := s.opts.Inspector.Inspect(s.opts.Root)
	if orchestrator.IsProcessObservationIncomplete(err) {
		return HelloRejectObservationIncomplete, false
	}
	if err != nil {
		return HelloRejectPeerNotInTree, false
	}
	for _, node := range tree.Descendants {
		if node.Token.PID == peer.pid && node.Token.StartTime > 0 {
			// Retaining the observed token for this call binds both PID and
			// starttime; only this already-authenticated connection proceeds.
			switch {
			case hello.SchemaVersion != SchemaVersion:
				return HelloRejectUnsupportedSchema, true
			case hello.Harness != lineage.Harness || hello.HarnessVersion != lineage.HarnessVersion:
				return HelloRejectHarnessMismatch, true
			default:
				return "", true
			}
		}
	}
	return HelloRejectPeerNotInTree, false
}

func privateDir(dir string) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("%w: %w", ErrPrivateDir, err)
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrPrivateDir, err)
	}
	if !info.IsDir() || info.Mode().Perm() != 0o700 || !ownedByUser(info) {
		return ErrPrivateDir
	}
	return nil
}

func clearDead(ctx context.Context, path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("nativeevents: stat socket: %w", err)
	}
	if info.Mode().Type() != os.ModeSocket || !ownedByUser(info) {
		return ErrNotOurSocket
	}
	probe, err := (&net.Dialer{Timeout: probeTimeout}).DialContext(ctx, "unix", path)
	if err == nil {
		_ = probe.Close()
		return ErrSocketInUse
	}
	if !errors.Is(err, syscall.ECONNREFUSED) {
		return fmt.Errorf("%w: %w", ErrSocketInUse, err)
	}
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("nativeevents: remove dead socket: %w", err)
	}
	return nil
}

func ownedByUser(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && int64(stat.Uid) == int64(os.Getuid())
}

func removeIfSame(path string, own os.FileInfo) {
	info, err := os.Lstat(path)
	if err == nil && os.SameFile(info, own) {
		_ = os.Remove(path)
	}
}
