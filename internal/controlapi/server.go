package controlapi

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/sys/unix"

	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
)

const (
	rpcParseError     = -32700
	rpcInvalidRequest = -32600
	rpcMethodNotFound = -32601
	rpcInvalidParams  = -32602
	rpcInternalError  = -32603
	rpcDomainError    = -32000
	maxFrameBytes     = 1 << 20
)

// Options configures the local control server.
type Options struct {
	RuntimeDir string
	DB         *sql.DB
	Controller *channel.Controller
}

// Server serves newline-delimited JSON-RPC 2.0 on daemon.sock.
type Server struct {
	runtimeDir string
	path       string
	db         *sql.DB
	controller *channel.Controller
}

// New validates opts. Serve creates the socket and owns it until return.
func New(opts Options) (*Server, error) {
	if opts.RuntimeDir == "" || opts.DB == nil || opts.Controller == nil {
		return nil, fmt.Errorf("invalid control api options")
	}
	clean := filepath.Clean(opts.RuntimeDir)
	if !filepath.IsAbs(clean) {
		return nil, fmt.Errorf("runtime dir must be absolute")
	}
	return &Server{runtimeDir: clean, path: filepath.Join(clean, "daemon.sock"), db: opts.DB, controller: opts.Controller}, nil
}

// Path returns the configured socket path.
func (s *Server) Path() string { return s.path }

// Serve creates the private runtime directory and socket, then serves until
// ctx is cancelled. A pre-existing socket is never unlinked implicitly.
func (s *Server) Serve(ctx context.Context) error {
	if err := ensurePrivateDir(s.runtimeDir); err != nil {
		return err
	}
	listener, err := (&net.ListenConfig{}).Listen(ctx, "unix", s.path)
	if err != nil {
		return fmt.Errorf("listen on control socket: %w", err)
	}
	defer func() { _ = listener.Close() }()
	defer func() { _ = os.Remove(s.path) }()
	if err := os.Chmod(s.path, 0o600); err != nil {
		return fmt.Errorf("set control socket mode: %w", err)
	}

	var workers sync.WaitGroup
	defer workers.Wait()
	go func() {
		<-ctx.Done()
		_ = listener.Close()
	}()
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("accept control connection: %w", err)
		}
		workers.Add(1)
		go func() {
			defer workers.Done()
			defer func() { _ = conn.Close() }()
			s.serveConn(ctx, conn)
		}()
	}
}

func ensurePrivateDir(path string) error {
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		if err := os.MkdirAll(path, 0o700); err != nil {
			return fmt.Errorf("create control runtime dir: %w", err)
		}
		info, err = os.Stat(path)
	}
	if err != nil {
		return fmt.Errorf("stat control runtime dir: %w", err)
	}
	if !info.IsDir() || info.Mode().Perm() != 0o700 {
		return fmt.Errorf("control runtime dir must be a directory with mode 0700")
	}
	return nil
}

// Peer is the authenticated Unix-socket process identity.
type Peer struct {
	PID int
	UID uint32
	GID uint32
}

func (p Peer) actor() channel.Actor {
	return channel.Actor{Type: protocol.ActorTypeHuman, ID: "uid:" + strconv.FormatUint(uint64(p.UID), 10) + ":pid:" + strconv.Itoa(p.PID)}
}

func (s *Server) serveConn(ctx context.Context, conn net.Conn) {
	peer, err := peerCredentials(conn)
	if err != nil {
		return
	}
	scanner := bufio.NewScanner(conn)
	scanner.Buffer(make([]byte, 4096), maxFrameBytes)
	writer := bufio.NewWriter(conn)
	for scanner.Scan() {
		response := s.Handle(ctx, peer, scanner.Bytes())
		raw, err := json.Marshal(response)
		if err != nil {
			return
		}
		if _, err := writer.Write(append(raw, '\n')); err != nil {
			return
		}
		if err := writer.Flush(); err != nil {
			return
		}
	}
}

func peerCredentials(conn net.Conn) (Peer, error) {
	unixConn, ok := conn.(*net.UnixConn)
	if !ok {
		return Peer{}, fmt.Errorf("control connection is not unix")
	}
	raw, err := unixConn.SyscallConn()
	if err != nil {
		return Peer{}, fmt.Errorf("access unix control connection: %w", err)
	}
	var cred *unix.Ucred
	var controlErr error
	if err := raw.Control(func(fd uintptr) {
		cred, controlErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
	}); err != nil {
		return Peer{}, fmt.Errorf("read unix control credentials: %w", err)
	}
	if controlErr != nil {
		return Peer{}, fmt.Errorf("get unix control credentials: %w", controlErr)
	}
	return Peer{PID: int(cred.Pid), UID: cred.Uid, GID: cred.Gid}, nil
}

// Handle validates and executes one complete JSON-RPC frame. It is exported
// for transports and tests that already authenticated the peer.
func (s *Server) Handle(ctx context.Context, peer Peer, frame []byte) protocol.RPCResponse {
	id := int64(1)
	var rough struct {
		ID     int64                  `json:"id"`
		Method protocol.ControlMethod `json:"method"`
	}
	_ = json.Unmarshal(frame, &rough)
	if rough.ID > 0 {
		id = rough.ID
	}
	var req protocol.RPCRequest
	if !json.Valid(frame) {
		return errorResponse(id, rpcParseError, "Parse error", nil)
	}
	dec := json.NewDecoder(strings.NewReader(string(frame)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		return errorResponse(id, rpcInvalidRequest, "Invalid Request", nil)
	}
	if !req.Method.Valid() {
		return errorResponse(id, rpcMethodNotFound, "Method not found", nil)
	}
	if err := req.Validate(); err != nil {
		var violation *protocol.ValidationError
		if errors.As(err, &violation) && strings.HasPrefix(violation.Path, "params") {
			return errorResponse(id, rpcInvalidParams, "Invalid params", nil)
		}
		return errorResponse(id, rpcInvalidRequest, "Invalid Request", nil)
	}
	if methodMutates(req.Method) {
		agent, err := registry.ResolveAgentPID(ctx, s.db, peer.PID)
		switch {
		case err == nil && agent != "":
			data := &protocol.RPCErrorData{ErrorCode: protocol.ErrorCodePolicyDenied, Detail: "agent processes cannot mutate controls"}
			return errorResponse(req.ID, rpcDomainError, "Control mutation forbidden", data)
		case errors.Is(err, registry.ErrNoRegisteredAncestor):
		case err != nil:
			return errorResponse(req.ID, rpcInternalError, "Internal error", nil)
		}
	}
	result, err := s.dispatch(ctx, req, peer.actor())
	if err != nil {
		return mapError(req.ID, err)
	}
	raw, err := json.Marshal(result)
	if err != nil {
		return errorResponse(req.ID, rpcInternalError, "Internal error", nil)
	}
	response := protocol.RPCResponse{JSONRPC: protocol.JSONRPCVersion, ID: req.ID, Result: protocol.Payload(raw)}
	if err := response.Validate(); err != nil {
		return errorResponse(req.ID, rpcInternalError, "Internal error", nil)
	}
	return response
}

func methodMutates(method protocol.ControlMethod) bool {
	for _, spec := range protocol.ControlMethodSpecs() {
		if spec.Method == method {
			return spec.Mutates
		}
	}
	return false
}

func decodeParams(raw protocol.Payload, into any) error {
	if err := json.Unmarshal(raw, into); err != nil {
		return fmt.Errorf("decode validated params: %w", err)
	}
	return nil
}

//nolint:wrapcheck // mapError needs the backend's typed errors unchanged.
func (s *Server) dispatch(ctx context.Context, req protocol.RPCRequest, actor channel.Actor) (any, error) {
	switch req.Method {
	case protocol.ControlMethodChannelGetGlobal:
		return s.controller.Global(ctx)
	case protocol.ControlMethodChannelSetGlobal:
		var p protocol.SetGlobalParams
		_ = decodeParams(req.Params, &p)
		return s.controller.SetGlobal(ctx, p, actor)
	case protocol.ControlMethodChannelGetNode:
		var p protocol.NodeParams
		_ = decodeParams(req.Params, &p)
		return s.controller.Node(ctx, p.NodeID)
	case protocol.ControlMethodChannelSetNode:
		var p protocol.SetNodeParams
		_ = decodeParams(req.Params, &p)
		return s.controller.SetNode(ctx, p, actor)
	case protocol.ControlMethodChannelGetEffective:
		var p protocol.EffectiveParams
		_ = decodeParams(req.Params, &p)
		return s.controller.Effective(p)
	case protocol.ControlMethodChannelExplain:
		var p protocol.ExplainParams
		_ = decodeParams(req.Params, &p)
		return s.controller.ExplainPair(p)
	case protocol.ControlMethodAgentChannelGet:
		var p protocol.AgentParams
		_ = decodeParams(req.Params, &p)
		return s.controller.Agent(ctx, p.AgentID)
	case protocol.ControlMethodAgentChannelSet:
		var p protocol.SetAgentChannelParams
		_ = decodeParams(req.Params, &p)
		return s.controller.SetAgent(ctx, p, actor)
	case protocol.ControlMethodAgentChannelSetMode:
		var p protocol.SetAgentModeParams
		_ = decodeParams(req.Params, &p)
		return s.controller.SetAgentMode(ctx, p, actor)
	case protocol.ControlMethodLinkGet:
		var p protocol.LinkParams
		_ = decodeParams(req.Params, &p)
		return s.controller.Link(ctx, p.FromAgent, p.ToAgent)
	case protocol.ControlMethodLinkList:
		var p protocol.ListLinksParams
		_ = decodeParams(req.Params, &p)
		return s.controller.Links(ctx, p.AgentID)
	case protocol.ControlMethodLinkSet:
		var p protocol.SetLinkParams
		_ = decodeParams(req.Params, &p)
		return s.controller.SetLink(ctx, p, actor)
	case protocol.ControlMethodLinkDeleteOverride:
		var p protocol.DeleteLinkOverrideParams
		_ = decodeParams(req.Params, &p)
		return s.controller.DeleteLink(ctx, p, actor)
	default:
		return nil, errUnsupported
	}
}

var errUnsupported = errors.New("method not implemented")

func mapError(id int64, err error) protocol.RPCResponse {
	if errors.Is(err, errUnsupported) {
		return errorResponse(id, rpcInternalError, "Internal error", nil)
	}
	if conflict, ok := channel.VersionConflict(err); ok {
		data := &protocol.RPCErrorData{ErrorCode: protocol.ErrorCodeControlVersionConflict}
		if conflict.CurrentVersions != nil {
			data.CurrentVersions = protocol.ControlVersions(conflict.CurrentVersions)
		} else {
			version := conflict.CurrentVersion
			data.CurrentVersion = &version
		}
		return errorResponse(id, rpcDomainError, "Control version conflict", data)
	}
	if errors.Is(err, channel.ErrNotFound) {
		return errorResponse(id, rpcDomainError, "Not found", &protocol.RPCErrorData{ErrorCode: protocol.ErrorCodeNotFound})
	}
	if errors.Is(err, protocol.ErrInvalid) || errors.Is(err, channel.ErrInvalidQuery) {
		return errorResponse(id, rpcInvalidParams, "Invalid params", nil)
	}
	return errorResponse(id, rpcInternalError, "Internal error", nil)
}

func errorResponse(id, code int64, message string, data *protocol.RPCErrorData) protocol.RPCResponse {
	return protocol.RPCResponse{JSONRPC: protocol.JSONRPCVersion, ID: id, Error: &protocol.RPCError{Code: code, Message: message, Data: data}}
}
