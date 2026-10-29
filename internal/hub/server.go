package hub

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"

	"github.com/smellak/gifford/internal/registry"
)

// This file holds the socket lifecycle: listening on hub.sock in the data
// directory, binding every connection to its agent through SO_PEERCRED and
// the registry, and serving one JSON-RPC message per line.

// SocketName is the socket file inside the data directory.
const SocketName = "hub.sock"

// maxLineBytes caps one request line. Longer lines close the connection:
// the local subset is single small control calls, never bulk transfer.
const maxLineBytes = 4 << 20

// Server is the local switchboard of one node. It is safe for concurrent
// use. The zero value is not usable; build it with New.
type Server struct {
	dataDir  string
	query    registry.Querier
	handlers Handlers

	mu       sync.Mutex
	listener *net.UnixListener
	conns    map[*net.UnixConn]struct{}
	path     string
	wg       sync.WaitGroup
}

// New returns the switchboard serving dataDir, resolving peer pids
// against the store behind query. It performs no I/O: Listen opens the
// socket.
func New(dataDir string, query registry.Querier, handlers Handlers) (*Server, error) {
	if dataDir == "" || query == nil {
		return nil, fmt.Errorf("new hub server: %w", errInvalidArgument)
	}
	return &Server{dataDir: dataDir, query: query, handlers: handlers}, nil
}

// Listen creates the data directory (mode 0700) and binds hub.sock in it
// (mode 0600), replacing a stale file. It returns the socket path.
func (s *Server) Listen() (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener != nil {
		return "", fmt.Errorf("hub listen: already listening: %w", errInvalidArgument)
	}
	if err := os.MkdirAll(s.dataDir, 0o700); err != nil {
		return "", fmt.Errorf("hub listen: create data directory: %w", err)
	}
	//nolint:gosec // directories need the execute bit; 0700 is the repo policy.
	if err := os.Chmod(s.dataDir, 0o700); err != nil {
		return "", fmt.Errorf("hub listen: chmod data directory: %w", err)
	}
	path := filepath.Join(s.dataDir, SocketName)
	if err := claimSocketPath(path); err != nil {
		return "", err
	}
	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "unix", path)
	if err != nil {
		return "", fmt.Errorf("hub listen: %w", err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		_ = listener.Close()
		return "", fmt.Errorf("hub listen: chmod socket: %w", err)
	}
	unix, ok := listener.(*net.UnixListener)
	if !ok {
		_ = listener.Close()
		return "", fmt.Errorf("hub listen: not a unix listener: %w", errInvalidArgument)
	}
	s.listener = unix
	s.conns = make(map[*net.UnixConn]struct{})
	s.path = path
	return path, nil
}

// claimSocketPath prepares path for binding. A missing path is free. A
// socket file with a live server behind it refuses: only a socket proven
// stale by a failed dial may be removed. Anything else — a regular file,
// a directory, a symlink — refuses, so Listen never deletes foreign
// files.
func claimSocketPath(path string) error {
	fi, err := os.Lstat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("hub listen: stat socket: %w", err)
	}
	if fi.Mode().Type() != os.ModeSocket {
		return fmt.Errorf("hub listen: %s is not a socket: %w", path, errInvalidArgument)
	}
	probe, err := (&net.Dialer{}).DialContext(context.Background(), "unix", path)
	if err == nil {
		_ = probe.Close()
		return fmt.Errorf("hub listen: live server on %s: %w", path, errInvalidArgument)
	}
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("hub listen: remove stale socket: %w", err)
	}
	return nil
}

// Serve accepts connections until Close or context cancellation, binding
// each to its agent and serving its lines. Cancellation stops accepting
// and returns nil without closing open connections; Close still owns
// them. It returns nil on a deliberate stop and the accept error
// otherwise.
func (s *Server) Serve(ctx context.Context) error {
	for ctx.Err() == nil {
		type accepted struct {
			conn *net.UnixConn
			err  error
		}
		arrived := make(chan accepted, 1)
		go func() {
			conn, err := s.accept()
			arrived <- accepted{conn: conn, err: err}
		}()
		select {
		case <-ctx.Done():
			s.stopListening()
			return nil
		case got := <-arrived:
			if got.err != nil {
				if s.closed() {
					return nil
				}
				select {
				case <-ctx.Done():
					return nil
				default:
					return got.err
				}
			}
			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				s.serveConn(ctx, got.conn)
			}()
		}
	}
	return nil
}

// stopListening closes the listener so in-flight and future accepts end,
// without touching open connections. Close still owns them.
func (s *Server) stopListening() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener != nil {
		_ = s.listener.Close()
		s.listener = nil
	}
}

// Close stops accepting, drops every open connection, and waits for their
// handlers to finish.
func (s *Server) Close() error {
	s.mu.Lock()
	var listener *net.UnixListener
	if s.listener != nil {
		listener = s.listener
		s.listener = nil
	}
	conns := make([]*net.UnixConn, 0, len(s.conns))
	for conn := range s.conns {
		conns = append(conns, conn)
	}
	s.mu.Unlock()
	for _, conn := range conns {
		_ = conn.Close()
	}
	s.wg.Wait()
	if listener == nil {
		return nil
	}
	if err := listener.Close(); err != nil {
		return fmt.Errorf("hub close: %w", err)
	}
	return nil
}

// accept takes one connection and tracks it. A closed listener reports
// the server as closed.
func (s *Server) accept() (*net.UnixConn, error) {
	s.mu.Lock()
	listener := s.listener
	s.mu.Unlock()
	if listener == nil {
		return nil, fmt.Errorf("hub accept: %w", net.ErrClosed)
	}
	raw, err := listener.Accept()
	if err != nil {
		return nil, fmt.Errorf("hub accept: %w", err)
	}
	conn, ok := raw.(*net.UnixConn)
	if !ok {
		_ = raw.Close()
		return nil, fmt.Errorf("hub accept: not a unix connection: %w", errInvalidArgument)
	}
	s.mu.Lock()
	if s.listener == nil {
		s.mu.Unlock()
		_ = conn.Close()
		return nil, fmt.Errorf("hub accept: %w", net.ErrClosed)
	}
	if s.conns == nil {
		s.conns = make(map[*net.UnixConn]struct{})
	}
	s.conns[conn] = struct{}{}
	s.mu.Unlock()
	return conn, nil
}

// closed reports whether Close ran.
func (s *Server) closed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listener == nil
}

// forget untracks a finished connection.
func (s *Server) forget(conn *net.UnixConn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.conns, conn)
}

// serveConn binds one connection to its agent and serves its lines. A
// peer with no registered agent receives nothing: the connection closes
// silently.
func (s *Server) serveConn(ctx context.Context, conn *net.UnixConn) {
	defer s.forget(conn)
	defer func() { _ = conn.Close() }()
	pid, err := peerPID(conn)
	if err != nil {
		return
	}
	agent, err := registry.ResolveAgentPID(ctx, s.query, pid)
	if err != nil {
		return
	}
	scanner := bufio.NewScanner(conn)
	scanner.Buffer(make([]byte, 64*1024), maxLineBytes)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(trimSpace(line)) == 0 {
			continue
		}
		req, rpcErr := decodeLine(line)
		var resp rpcResponse
		if rpcErr != nil {
			resp = rpcResponse{JSONRPC: jsonRPCVersion, Error: rpcErr, ID: json.RawMessage("null")}
		} else {
			resp = dispatch(ctx, agent, s.handlers, req)
		}
		raw, err := encodeLine(resp)
		if err != nil {
			return
		}
		if _, err := conn.Write(append(raw, '\n')); err != nil {
			return
		}
	}
}

// peerPID returns the pid the kernel reports for the far end of conn. It
// trusts the kernel, not the peer. The lookup itself is per-OS: Linux
// answers from SO_PEERCRED, and platforms without peer credentials fail
// closed (the connection gets nothing).
func peerPID(conn *net.UnixConn) (int, error) {
	raw, err := conn.SyscallConn()
	if err != nil {
		return 0, fmt.Errorf("hub peer credentials: %w", err)
	}
	pid, err := peerCredPID(raw)
	if err != nil {
		return 0, fmt.Errorf("hub peer credentials: %w", err)
	}
	if pid <= 0 {
		return 0, fmt.Errorf("hub peer pid %d: %w", pid, errInvalidArgument)
	}
	return pid, nil
}

// trimSpace trims one line without allocating a strings import.
func trimSpace(line []byte) []byte {
	start := 0
	for start < len(line) && (line[start] == ' ' || line[start] == '\t' || line[start] == '\r') {
		start++
	}
	end := len(line)
	for end > start && (line[end-1] == ' ' || line[end-1] == '\t' || line[end-1] == '\r') {
		end--
	}
	return line[start:end]
}
