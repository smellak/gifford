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

	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/orchestrator"
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

	deliveryDeferredDetail = "deliver_now deferred; the request is still pending"
)

// Options configures the local control server.
type Options struct {
	RuntimeDir string
	DB         *sql.DB
	// ReadDB is a separate pool configured with SQLite query_only on every
	// connection. Nil keeps the panel metadata methods unavailable.
	ReadDB     *sql.DB
	Controller *channel.Controller
	Approvals  ApprovalService
	Node       NodeService
}

// ApprovalService owns the manual delivery side effect while channel remains
// the sole approval_requests writer.
type ApprovalService interface {
	ResolveApproval(context.Context, protocol.ResolveApprovalParams, channel.Actor) (protocol.ApprovalRequest, error)
}

// NodeService owns federation enrollment operations inside the hub process,
// preserving the single-writer rule for node CLI commands.
type NodeService interface {
	EnrollToken(context.Context, protocol.NodeEnrollTokenParams) (protocol.NodeEnrollTokenResult, error)
	Join(context.Context, protocol.NodeJoinParams) (protocol.NodeJoinResult, error)
	Peers(context.Context) (protocol.NodePeersResult, error)
	Revoke(context.Context, protocol.NodeRevokeParams) (protocol.NodeRevokeResult, error)
}

// Server serves newline-delimited JSON-RPC 2.0 on daemon.sock.
type Server struct {
	runtimeDir string
	path       string
	db         *sql.DB
	readDB     *sql.DB
	controller *channel.Controller
	approvals  ApprovalService
	node       NodeService
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
	return &Server{
		runtimeDir: clean, path: filepath.Join(clean, "daemon.sock"),
		db: opts.DB, readDB: opts.ReadDB, controller: opts.Controller,
		approvals: opts.Approvals, node: opts.Node,
	}, nil
}

// Path returns the configured socket path.
func (s *Server) Path() string { return s.path }

// Serve creates the private runtime directory and socket, then serves until
// ctx is cancelled. A pre-existing socket is never unlinked implicitly.
func (s *Server) Serve(ctx context.Context) error {
	if err := ensurePrivateDir(s.runtimeDir); err != nil {
		return err
	}
	listener, err := listenPrivateUnix(ctx, s.path)
	if err != nil {
		return fmt.Errorf("listen on control socket: %w", err)
	}
	defer func() { _ = listener.Close() }()
	defer func() { _ = os.Remove(s.path) }()

	var workers sync.WaitGroup
	var activeMu sync.Mutex
	active := make(map[net.Conn]struct{})
	closeActive := func() {
		_ = listener.Close()
		activeMu.Lock()
		defer activeMu.Unlock()
		for conn := range active {
			_ = conn.Close()
		}
	}
	cancelDone := make(chan struct{})
	stopCancel := context.AfterFunc(ctx, func() {
		defer close(cancelDone)
		closeActive()
	})
	defer func() {
		if !stopCancel() {
			<-cancelDone
		}
		closeActive()
		workers.Wait()
	}()
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("accept control connection: %w", err)
		}
		activeMu.Lock()
		if ctx.Err() != nil {
			activeMu.Unlock()
			_ = conn.Close()
			return nil
		}
		active[conn] = struct{}{}
		activeMu.Unlock()
		workers.Add(1)
		go func() {
			defer workers.Done()
			defer func() {
				_ = conn.Close()
				activeMu.Lock()
				delete(active, conn)
				activeMu.Unlock()
			}()
			s.serveConn(ctx, conn)
		}()
	}
}

// listenPrivateUnix binds at an unpublished name, makes the socket private,
// then atomically publishes it at path. Callers can therefore treat path's
// existence as the readiness signal without observing the mode inherited from
// the process umask.
func listenPrivateUnix(ctx context.Context, path string) (net.Listener, error) {
	if _, err := os.Lstat(path); err == nil {
		return nil, fmt.Errorf("socket path already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("stat socket path: %w", err)
	}

	reservation, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+"-*")
	if err != nil {
		return nil, fmt.Errorf("reserve private socket name: %w", err)
	}
	temporaryPath := reservation.Name()
	if err := reservation.Close(); err != nil {
		_ = os.Remove(temporaryPath)
		return nil, fmt.Errorf("close private socket reservation: %w", err)
	}
	if err := os.Remove(temporaryPath); err != nil {
		return nil, fmt.Errorf("release private socket reservation: %w", err)
	}

	listener, err := (&net.ListenConfig{}).Listen(ctx, "unix", temporaryPath)
	if err != nil {
		return nil, fmt.Errorf("listen on private socket: %w", err)
	}
	unixListener, ok := listener.(*net.UnixListener)
	if !ok {
		_ = listener.Close()
		_ = os.Remove(temporaryPath)
		return nil, fmt.Errorf("private socket is not a Unix listener")
	}
	// Close must not unlink the now-free temporary name after publication.
	// Serve owns removal of the final path.
	unixListener.SetUnlinkOnClose(false)
	published := false
	defer func() {
		if !published {
			_ = listener.Close()
			_ = os.Remove(temporaryPath)
		}
	}()
	if err := os.Chmod(temporaryPath, 0o600); err != nil {
		return nil, fmt.Errorf("set control socket mode: %w", err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return nil, fmt.Errorf("publish control socket: %w", err)
	}
	published = true
	return listener, nil
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
	PID      int
	UID      uint32
	GID      uint32
	Verified bool
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
	if methodRequiresHuman(req.Method) {
		protectedRead := methodProtectsNodeReads(req.Method)
		if !peer.Verified {
			if protectedRead {
				data := &protocol.RPCErrorData{ErrorCode: protocol.ErrorCodePolicyDenied, Detail: "unverified socket peers cannot read node control data"}
				return errorResponse(req.ID, rpcDomainError, "Control read forbidden", data)
			}
			data := &protocol.RPCErrorData{ErrorCode: protocol.ErrorCodePolicyDenied, Detail: "unverified socket peers cannot mutate controls"}
			return errorResponse(req.ID, rpcDomainError, "Control mutation forbidden", data)
		}
		agent, err := registry.ResolveAgentPID(ctx, s.db, peer.PID)
		switch {
		case err == nil && agent != "":
			if protectedRead {
				data := &protocol.RPCErrorData{ErrorCode: protocol.ErrorCodePolicyDenied, Detail: "agent processes cannot read node control data"}
				return errorResponse(req.ID, rpcDomainError, "Control read forbidden", data)
			}
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

func methodProtectsNodeReads(method protocol.ControlMethod) bool {
	switch method {
	case protocol.ControlMethodAgentList,
		protocol.ControlMethodMessageList,
		protocol.ControlMethodNotificationList,
		protocol.ControlMethodDecisionList,
		protocol.ControlMethodDecisionGet,
		protocol.ControlMethodModelStatus:
		return true
	default:
		return false
	}
}

func methodRequiresHuman(method protocol.ControlMethod) bool {
	// The peer check excludes processes known to belong to a registered agent.
	// It is not isolation from every other process running under the same UID.
	return methodMutates(method) || methodProtectsNodeReads(method)
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
		return s.controller.Explain(ctx, p)
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
	case protocol.ControlMethodApprovalList:
		var p protocol.ListApprovalsParams
		_ = decodeParams(req.Params, &p)
		return s.controller.Approvals(ctx, p)
	case protocol.ControlMethodApprovalGet:
		var p protocol.ApprovalParams
		_ = decodeParams(req.Params, &p)
		return s.controller.Approval(ctx, p.ApprovalID)
	case protocol.ControlMethodApprovalResolve:
		var p protocol.ResolveApprovalParams
		_ = decodeParams(req.Params, &p)
		request, err := s.controller.Approval(ctx, p.ApprovalID)
		if err != nil {
			return nil, err
		}
		return s.resolveReadApproval(ctx, request, p, actor)
	case protocol.ControlMethodNodeEnrollToken:
		if s.node == nil {
			return nil, errUnsupported
		}
		var p protocol.NodeEnrollTokenParams
		_ = decodeParams(req.Params, &p)
		return s.node.EnrollToken(ctx, p)
	case protocol.ControlMethodNodeJoin:
		if s.node == nil {
			return nil, errUnsupported
		}
		var p protocol.NodeJoinParams
		_ = decodeParams(req.Params, &p)
		return s.node.Join(ctx, p)
	case protocol.ControlMethodNodePeers:
		if s.node == nil {
			return nil, errUnsupported
		}
		return s.node.Peers(ctx)
	case protocol.ControlMethodNodeRevoke:
		if s.node == nil {
			return nil, errUnsupported
		}
		var p protocol.NodeRevokeParams
		_ = decodeParams(req.Params, &p)
		return s.node.Revoke(ctx, p)
	case protocol.ControlMethodAgentList:
		var p protocol.AgentListParams
		_ = decodeParams(req.Params, &p)
		return s.readAgents(ctx, p)
	case protocol.ControlMethodMessageList:
		var p protocol.MessageListParams
		_ = decodeParams(req.Params, &p)
		return s.readMessages(ctx, p)
	case protocol.ControlMethodNotificationList:
		var p protocol.NotificationListParams
		_ = decodeParams(req.Params, &p)
		return s.readNotifications(ctx, p)
	case protocol.ControlMethodDecisionList:
		var p protocol.DecisionListParams
		_ = decodeParams(req.Params, &p)
		return s.readDecisions(ctx, p)
	case protocol.ControlMethodDecisionGet:
		var p protocol.DecisionGetParams
		_ = decodeParams(req.Params, &p)
		return s.readDecision(ctx, p)
	case protocol.ControlMethodModelStatus:
		return s.readModelStatus(ctx)
	default:
		return nil, errUnsupported
	}
}

// resolveReadApproval receives the request already read by dispatch, so source
// validation remains a side-effect-free boundary before the service runs.
func (s *Server) resolveReadApproval(
	ctx context.Context,
	request protocol.ApprovalRequest,
	params protocol.ResolveApprovalParams,
	actor channel.Actor,
) (protocol.ApprovalRequest, error) {
	if request.Source != protocol.ApprovalSourceOrchestrator || s.approvals == nil {
		return protocol.ApprovalRequest{}, errUnsupported
	}
	resolved, err := s.approvals.ResolveApproval(ctx, params, actor)
	if err != nil {
		return protocol.ApprovalRequest{}, fmt.Errorf("resolve read approval: %w", err)
	}
	return resolved, nil
}

var errUnsupported = errors.New("method not implemented")

func mapError(id int64, err error) protocol.RPCResponse {
	if errors.Is(err, errUnsupported) {
		return errorResponse(id, rpcInternalError, "Internal error", nil)
	}
	if errors.Is(err, orchestrator.ErrDeliveryDeferred) {
		return errorResponse(id, rpcDomainError, "Delivery deferred", &protocol.RPCErrorData{
			ErrorCode: protocol.ErrorCodeRecipientNotDeliverable,
			Detail:    deliveryDeferredDetail,
		})
	}
	for _, state := range []struct {
		err     error
		message string
	}{
		{registry.ErrNodeUnenrolled, "Node is not enrolled"},
		{registry.ErrNodeAlreadyEnrolled, "Node is already enrolled"},
		{registry.ErrFederationUnavailable, "Federation is not configured"},
	} {
		if errors.Is(err, state.err) {
			return errorResponse(id, rpcDomainError, state.message,
				&protocol.RPCErrorData{ErrorCode: protocol.ErrorCodeNodeOff})
		}
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
	if errors.Is(err, registry.ErrPeerNotFound) {
		return errorResponse(id, rpcDomainError, "Not found", &protocol.RPCErrorData{ErrorCode: protocol.ErrorCodeNotFound})
	}
	if errors.Is(err, channel.ErrUnsupported) {
		return errorResponse(id, rpcInternalError, "Loop explanation unavailable", nil)
	}
	if errors.Is(err, protocol.ErrInvalid) || errors.Is(err, channel.ErrInvalidQuery) ||
		errors.Is(err, registry.ErrInvalidArgument) {
		return errorResponse(id, rpcInvalidParams, "Invalid params", nil)
	}
	return errorResponse(id, rpcInternalError, "Internal error", nil)
}

func errorResponse(id, code int64, message string, data *protocol.RPCErrorData) protocol.RPCResponse {
	return protocol.RPCResponse{JSONRPC: protocol.JSONRPCVersion, ID: id, Error: &protocol.RPCError{Code: code, Message: message, Data: data}}
}
