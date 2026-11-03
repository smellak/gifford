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

	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
)

const messageInvalidRequest = "Invalid request"

// NodeSocket serves the socket that every local user may connect to, next to
// the private hub.sock. It shares the Server's handlers and session
// bookkeeping; only its listener and its peer classes are its own.
//
// Each connection is classified once, when it is accepted, from SO_PEERCRED
// and never from anything the peer sends:
//   - the configured bridge uid reaches only the bridge methods;
//   - root and the node's own uid are closed: administrators use the private
//     control socket;
//   - any other uid must descend from the process of a live session, and
//     that process must run as the same uid. It reaches the ordinary agent
//     methods as that session's agent, and never register_agent;
//   - everything else is closed before a byte is read.
type NodeSocket struct {
	server    *Server
	path      string
	bridgeUID int64
	nodeUID   int64
	bridgeLog *bridgeRejectionLog

	mu       sync.Mutex
	listener *net.UnixListener
	conns    map[*net.UnixConn]struct{}
	wg       sync.WaitGroup
}

// NewNodeSocket returns the node socket at path for server's handlers. The
// bridge uid must be neither root nor the node's own uid.
func NewNodeSocket(server *Server, path string, bridgeUID uint32) (*NodeSocket, error) {
	return newNodeSocket(server, path, int64(bridgeUID), int64(os.Getuid()))
}

func newNodeSocket(server *Server, path string, bridgeUID, nodeUID int64) (*NodeSocket, error) {
	if server == nil || !filepath.IsAbs(path) || filepath.Clean(path) != path ||
		bridgeUID <= 0 || bridgeUID == nodeUID {
		return nil, fmt.Errorf("new node socket: %w", errInvalidArgument)
	}
	return &NodeSocket{
		server: server, path: path, bridgeUID: bridgeUID, nodeUID: nodeUID,
		bridgeLog: newBridgeRejectionLog(nil, nil),
	}, nil
}

// Listen binds the socket with mode 0666. Its directory must already exist,
// belong to this process's uid and be writable by nobody else, so no other
// user can replace the socket.
func (n *NodeSocket) Listen() (string, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.listener != nil {
		return "", fmt.Errorf("node socket listen: already listening: %w", errInvalidArgument)
	}
	if err := checkNodeSocketDir(filepath.Dir(n.path)); err != nil {
		return "", err
	}
	if err := claimSocketPath(n.path); err != nil {
		return "", err
	}
	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "unix", n.path)
	if err != nil {
		return "", fmt.Errorf("node socket listen: %w", err)
	}
	//nolint:gosec // every local user may connect; each peer is classified by SO_PEERCRED.
	if err := os.Chmod(n.path, 0o666); err != nil {
		_ = listener.Close()
		return "", fmt.Errorf("node socket listen: chmod socket: %w", err)
	}
	unix, ok := listener.(*net.UnixListener)
	if !ok {
		_ = listener.Close()
		return "", fmt.Errorf("node socket listen: not a unix listener: %w", errInvalidArgument)
	}
	n.listener = unix
	n.conns = make(map[*net.UnixConn]struct{})
	return n.path, nil
}

// Serve accepts connections until Close or context cancellation. It
// returns nil on a deliberate stop and the accept error otherwise.
func (n *NodeSocket) Serve(ctx context.Context) error {
	for ctx.Err() == nil {
		type accepted struct {
			conn *net.UnixConn
			err  error
		}
		arrived := make(chan accepted, 1)
		go func() {
			conn, err := n.accept()
			arrived <- accepted{conn: conn, err: err}
		}()
		select {
		case <-ctx.Done():
			n.stopListening()
			return nil
		case got := <-arrived:
			if got.err != nil {
				if n.closed() {
					return nil
				}
				select {
				case <-ctx.Done():
					return nil
				default:
					return got.err
				}
			}
			n.wg.Add(1)
			go func() {
				defer n.wg.Done()
				n.serveConn(ctx, got.conn)
			}()
		}
	}
	return nil
}

// Close stops accepting, drops every open connection, waits for their
// handlers and removes the socket file.
func (n *NodeSocket) Close() error {
	n.mu.Lock()
	listener := n.listener
	n.listener = nil
	conns := make([]*net.UnixConn, 0, len(n.conns))
	for conn := range n.conns {
		conns = append(conns, conn)
	}
	n.mu.Unlock()
	for _, conn := range conns {
		_ = conn.Close()
	}
	n.wg.Wait()
	if listener == nil {
		return nil
	}
	if err := listener.Close(); err != nil {
		return fmt.Errorf("node socket close: %w", err)
	}
	return nil
}

func (n *NodeSocket) stopListening() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.listener != nil {
		_ = n.listener.Close()
		n.listener = nil
	}
}

func (n *NodeSocket) accept() (*net.UnixConn, error) {
	n.mu.Lock()
	listener := n.listener
	n.mu.Unlock()
	if listener == nil {
		return nil, fmt.Errorf("node socket accept: %w", net.ErrClosed)
	}
	raw, err := listener.Accept()
	if err != nil {
		return nil, fmt.Errorf("node socket accept: %w", err)
	}
	conn, ok := raw.(*net.UnixConn)
	if !ok {
		_ = raw.Close()
		return nil, fmt.Errorf("node socket accept: not a unix connection: %w", errInvalidArgument)
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.listener == nil {
		_ = conn.Close()
		return nil, fmt.Errorf("node socket accept: %w", net.ErrClosed)
	}
	n.conns[conn] = struct{}{}
	return conn, nil
}

func (n *NodeSocket) closed() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.listener == nil
}

func (n *NodeSocket) forget(conn *net.UnixConn) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.conns, conn)
}

// serveConn classifies the peer and serves it, or closes the connection
// without reading from it.
func (n *NodeSocket) serveConn(ctx context.Context, conn *net.UnixConn) {
	defer n.forget(conn)
	defer func() { _ = conn.Close() }()
	peer, err := peerCredentials(conn)
	if err != nil {
		return
	}
	switch peer.UID {
	case n.bridgeUID:
		if bridge := n.server.handlers.Bridge; bridge != nil {
			n.serveBridge(ctx, conn, bridge)
		}
	case 0, n.nodeUID:
		// Administrators use the private control socket.
	default:
		n.serveAgent(ctx, conn, peer)
	}
}

// serveAgent binds the peer to the live session it descends from, as
// hub.sock does, and refuses a session whose process runs as another uid.
// The peer's pidfd proves its PID was not reused while /proc was read.
func (n *NodeSocket) serveAgent(ctx context.Context, conn *net.UnixConn, peer peerIdentity) {
	s := n.server
	pidfd, err := peerPidfd(conn)
	if err != nil {
		return
	}
	defer closePidfd(pidfd)
	s.lifecycleMu.Lock()
	agent, session, err := registry.ResolveAgentSessionPID(ctx, s.query, peer.PID)
	var revision string
	bound := err == nil
	if bound {
		var connected bool
		revision, connected = s.connectedSessionRevision(ctx, agent, session)
		bound = connected && s.sessionProcessUID(ctx, session) == peer.UID && pidfdAlive(pidfd)
	}
	s.lifecycleMu.Unlock()
	if !bound {
		return
	}
	var endpointLease uint64
	defer func() {
		s.lifecycleMu.Lock()
		s.endpoints.release(endpointLease)
		s.lifecycleMu.Unlock()
	}()
	n.serveLines(conn, func(req rpcRequest) (rpcResponse, bool) {
		s.lifecycleMu.Lock()
		defer s.lifecycleMu.Unlock()
		if !s.sessionRevisionCurrent(ctx, agent, session, revision) ||
			(endpointLease != 0 && !s.endpoints.current(endpointLease)) {
			return rpcResponse{}, false
		}
		resp := dispatch(ctx, agent, peer.PID, s.handlers, req)
		if req.Method == MethodRegisterEndpoint && resp.Error == nil {
			endpointLease = s.trackEndpointRegistration(ctx, agent, peer.PID, req.Params, resp.Result, endpointLease)
		}
		return resp, true
	})
}

func (n *NodeSocket) serveBridge(ctx context.Context, conn *net.UnixConn, bridge BridgeHandler) {
	n.serveLines(conn, func(req rpcRequest) (rpcResponse, bool) {
		return dispatchBridge(ctx, bridge, req, n.bridgeLog), true
	})
}

// serveLines answers one request per line until the peer leaves, a write
// fails, or serve asks to drop the connection.
func (n *NodeSocket) serveLines(conn *net.UnixConn, serve func(rpcRequest) (rpcResponse, bool)) {
	scanner := bufio.NewScanner(conn)
	scanner.Buffer(make([]byte, 64*1024), maxLineBytes)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(trimSpace(line)) == 0 {
			continue
		}
		req, rpcErr := decodeLine(line)
		resp := rpcResponse{JSONRPC: jsonRPCVersion, Error: rpcErr, ID: json.RawMessage("null")}
		if rpcErr == nil {
			var keep bool
			if resp, keep = serve(req); !keep {
				return
			}
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

// dispatchBridge offers the bridge methods and nothing else.
func dispatchBridge(
	ctx context.Context,
	bridge BridgeHandler,
	req rpcRequest,
	rejectionLog *bridgeRejectionLog,
) rpcResponse {
	answer := rpcResponse{JSONRPC: jsonRPCVersion, ID: req.ID}
	if answer.ID == nil {
		answer.ID = json.RawMessage("null")
	}
	if req.JSONRPC != jsonRPCVersion || req.Method == "" {
		answer.Error = &rpcError{Code: codeInvalidRequest, Message: messageInvalidRequest}
		return answer
	}
	var handle func(context.Context, json.RawMessage) (any, error)
	switch req.Method {
	case MethodBridgeMirrorMessage:
		handle = bridge.MirrorMessage
	case MethodBridgeMarkRead:
		handle = bridge.MarkRead
	case MethodBridgePutReminder:
		handle = bridge.PutReminder
	case MethodBridgeGetReminder:
		handle = bridge.GetReminder
	case MethodBridgeCancelReminder:
		handle = bridge.CancelReminder
	case MethodBridgeTombstoneReminder:
		handle = bridge.TombstoneReminder
	default:
		answer.Error = &rpcError{Code: codeMethodNotFound, Message: "Method not found"}
		return answer
	}
	result, err := handle(ctx, req.Params)
	if err != nil {
		var callErr *callError
		if errors.As(err, &callErr) {
			answer.Error = callErr.rpcError()
		} else {
			answer.Error = &rpcError{Code: codeNotReady, Message: messageInternal}
		}
	} else {
		answer.Result = result
	}
	if rejectionLog != nil {
		rejectionLog.Record(ctx, req, answer)
	}
	return answer
}

// sessionProcessUID is the uid recorded for the session's process, or -1.
func (s *Server) sessionProcessUID(ctx context.Context, session protocol.OpaqueID) int64 {
	var raw string
	if err := s.query.QueryRowContext(ctx,
		"SELECT process_json FROM sessions WHERE session_id = ?", session).Scan(&raw); err != nil {
		return -1
	}
	var process protocol.ProcessIdentity
	if json.Unmarshal([]byte(raw), &process) != nil || process.Validate() != nil {
		return -1
	}
	return process.UID
}
