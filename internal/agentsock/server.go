//go:build unix

package agentsock

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/smellak/gifford/internal/protocol"
)

const (
	// GreetingTimeout bounds the wait for register_agent_result.
	GreetingTimeout = 5 * time.Second
	// HeartbeatEvery spaces the heartbeats of a registered connection.
	HeartbeatEvery = 3 * time.Second
	// agentsDir holds the sockets under the runtime directory.
	agentsDir = "agents"
	// probeTimeout bounds the dial that tells a live socket from a
	// dead one.
	probeTimeout = time.Second
)

var (
	// ErrInvalidOptions means the options are missing or out of range.
	ErrInvalidOptions = errors.New("agentsock: invalid options")
	// ErrPrivateDir means the socket directory is not a directory of
	// this user with mode 0700, so no socket opens in it.
	ErrPrivateDir = errors.New("agentsock: socket directory is not private")
	// ErrSocketInUse means a live socket for this agent already
	// listens: another session holds it, and it is left alone.
	ErrSocketInUse = errors.New("agentsock: another session holds the agent socket")
	// ErrNotOurSocket means the socket path holds something other
	// than a socket of this user, and it is left alone.
	ErrNotOurSocket = errors.New("agentsock: the socket path is not this user's socket")
)

// Guard is the session's input guard as the socket reads it.
type Guard interface {
	// InputGuard is the guard's current reading. The socket fills in
	// the agent and session ids and the detection source.
	InputGuard() protocol.InputGuardReport
	// GuardChanged signals a new guard version. It never blocks the
	// session: one pending signal stands for any number of changes.
	GuardChanged() <-chan struct{}
}

// Options configures one agent socket.
type Options struct {
	// RuntimeDir is the absolute runtime directory; the socket is
	// agents/<agent_id>.sock under it.
	RuntimeDir string
	// Register is the greeting, the same register_agent as the hub
	// registration: its manifest names the agent.
	Register protocol.RegisterAgent
	// Guard is read for every report.
	Guard Guard
	// Delivery runs the delivery and automation orders; nil refuses
	// them with UNSUPPORTED_OP, as a socket that only reports.
	Delivery Delivery
	// Clock drives the greeting deadline and the heartbeat; nil is
	// SystemClock.
	Clock Clock
	// Peer admits a connection; nil admits only this user, by
	// SO_PEERCRED on Linux, and refuses every connection elsewhere.
	Peer func(net.Conn) error
}

// Server is one listening agent socket. Close stops it and removes the
// socket file it created, and nothing else.
type Server struct {
	opts  Options
	agent protocol.AgentID
	path  string
	ln    *net.UnixListener
	// own is the socket file this server created.
	own os.FileInfo

	mu     sync.Mutex
	active *conn
	closed bool
	wg     sync.WaitGroup
	once   sync.Once
}

// SocketPath is the socket of agent under runtimeDir.
func SocketPath(runtimeDir string, agent protocol.AgentID) string {
	return filepath.Join(runtimeDir, agentsDir, string(agent)+".sock")
}

// Listen opens the agent socket and starts accepting. The directory is
// created with mode 0700, and refused if it exists with another mode or
// owner. A live socket at the path belongs to another session of the
// same agent and is left alone; a dead socket of this user is replaced.
// The socket gets mode 0600.
func Listen(ctx context.Context, opts Options) (*Server, error) {
	agent := opts.Register.Manifest.AgentID
	if !filepath.IsAbs(opts.RuntimeDir) || !agent.Valid() || opts.Guard == nil {
		return nil, ErrInvalidOptions
	}
	if err := opts.Register.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidOptions, err)
	}
	if opts.Clock == nil {
		opts.Clock = SystemClock{}
	}
	if opts.Peer == nil {
		opts.Peer = samePeer
	}
	path := SocketPath(filepath.Clean(opts.RuntimeDir), agent)
	if err := privateDir(filepath.Dir(path)); err != nil {
		return nil, err
	}
	if err := clearDead(ctx, path); err != nil {
		return nil, err
	}
	l, err := (&net.ListenConfig{}).Listen(ctx, "unix", path)
	if err != nil {
		return nil, fmt.Errorf("agentsock: listen: %w", err)
	}
	ln, ok := l.(*net.UnixListener)
	if !ok {
		_ = l.Close()
		return nil, fmt.Errorf("agentsock: listen: not a unix listener")
	}
	// Close removes the file itself, and only if it is still this one.
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
		return nil, fmt.Errorf("agentsock: socket mode: %w", err)
	}
	s := &Server{opts: opts, agent: agent, path: path, ln: ln, own: own}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.serve()
	}()
	return s, nil
}

// Path returns the socket path.
func (s *Server) Path() string { return s.path }

// Close stops accepting, closes the connection, waits for its
// goroutines and removes the socket file if it is still the one this
// server created.
func (s *Server) Close() error {
	s.once.Do(func() {
		s.mu.Lock()
		s.closed = true
		active := s.active
		s.mu.Unlock()
		_ = s.ln.Close()
		if active != nil {
			active.close()
		}
		s.wg.Wait()
		removeIfSame(s.path, s.own)
	})
	return nil
}

// serve accepts until the listener closes.
func (s *Server) serve() {
	for {
		c, err := s.ln.AcceptUnix()
		if err != nil {
			// Closed, or the listener failed: either way no
			// connection is served any more.
			_ = s.ln.Close()
			return
		}
		s.admit(c)
	}
}

// admit serves c if it is the only connection and its peer passes.
func (s *Server) admit(c *net.UnixConn) {
	if s.opts.Peer(c) != nil {
		_ = c.Close()
		return
	}
	s.mu.Lock()
	if s.closed || s.active != nil {
		s.mu.Unlock()
		_ = c.Close()
		return
	}
	cn := newConn(c, &s.opts, s.agent)
	s.active = cn
	s.wg.Add(1)
	s.mu.Unlock()
	go func() {
		defer s.wg.Done()
		cn.run()
		s.mu.Lock()
		s.active = nil
		s.mu.Unlock()
	}()
}

// privateDir creates dir with mode 0700, or checks the one there: a
// directory, not a link, of this user, with mode 0700.
func privateDir(dir string) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("%w: %w", ErrPrivateDir, err)
	}
	fi, err := os.Lstat(dir)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrPrivateDir, err)
	}
	if !fi.IsDir() || fi.Mode().Perm() != 0o700 || !ownedByUser(fi) {
		return ErrPrivateDir
	}
	return nil
}

// clearDead leaves path free for a new socket: nothing there, or a dead
// socket of this user, which it removes. Anything else stays.
func clearDead(ctx context.Context, path string) error {
	fi, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("agentsock: stat socket: %w", err)
	}
	if fi.Mode().Type() != os.ModeSocket || !ownedByUser(fi) {
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
		return fmt.Errorf("agentsock: remove dead socket: %w", err)
	}
	return nil
}

// ownedByUser reports whether this user owns fi.
func ownedByUser(fi os.FileInfo) bool {
	st, ok := fi.Sys().(*syscall.Stat_t)
	return ok && int64(st.Uid) == int64(os.Getuid())
}

// removeIfSame removes path only while it is still the file own names,
// on the same device and inode.
func removeIfSame(path string, own os.FileInfo) {
	fi, err := os.Lstat(path)
	if err == nil && os.SameFile(fi, own) {
		_ = os.Remove(path)
	}
}
