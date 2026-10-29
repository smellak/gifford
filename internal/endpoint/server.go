package endpoint

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/version"
)

// This file holds the MCP server: JSON-RPC 2.0 over stdio with
// newline-delimited JSON framing, serving the tool table to one agent harness.
// Identity comes only from the Hub: without a verified agent the
// connection gets no tools. Only this package sees message plaintext —
// nothing is logged and errors carry no content.

// Server serves one harness over stdio. It is not safe for concurrent
// use; Run owns the framing loop.
type Server struct {
	hub      Hub
	dataDir  string
	stdin    io.Reader
	stdout   io.Writer
	agent    protocol.AgentID
	priv     ed25519.PrivateKey
	handlers map[string]func(context.Context, json.RawMessage) (any, error)

	mu       sync.Mutex
	acked    map[string]protocol.AckStatus
	outgoing map[protocol.OpaqueID]protocol.WireEnvelope
	requests map[protocol.OpaqueID]outboundMessage
	results  map[protocol.OpaqueID]protocol.AdmissionResult
	received map[protocol.ULID]protocol.LogicalMessage

	outboundStorePath string
	now               func() time.Time
}

// New dials the Hub through hub, learns the connection's agent with
// whoami, and loads its signing key. A connection the Hub does not bind
// still serves framing and an empty tool list, but every call fails with
// no identity: nothing is minted locally, and the agent id never comes
// from arguments or the environment.
func New(ctx context.Context, dataDir string, stdin io.Reader, stdout io.Writer, hub Hub) (*Server, error) {
	if stdin == nil || stdout == nil || hub == nil {
		return nil, fmt.Errorf("new endpoint server: %w", ErrInvalidArgument)
	}
	resolved, err := resolveDataDir(dataDir)
	if err != nil {
		return nil, err
	}
	server := &Server{
		hub:      hub,
		dataDir:  resolved,
		stdin:    stdin,
		stdout:   stdout,
		acked:    make(map[string]protocol.AckStatus),
		outgoing: make(map[protocol.OpaqueID]protocol.WireEnvelope),
		requests: make(map[protocol.OpaqueID]outboundMessage),
		results:  make(map[protocol.OpaqueID]protocol.AdmissionResult),
		received: make(map[protocol.ULID]protocol.LogicalMessage),
		now:      clock.System{}.Now,
	}
	server.install()
	if agent, err := hub.WhoAmI(ctx); err == nil {
		if priv, err := LoadAgentPrivate(resolved, agent); err == nil {
			server.agent = agent
			server.priv = priv
			if err := server.loadOutboundState(); err != nil {
				return nil, fmt.Errorf("load outbound attempts: %w", err)
			}
		}
	} else if ctx.Err() != nil {
		return nil, fmt.Errorf("hub identity: %w", ctx.Err())
	}
	return server, nil
}

// install wires the tool handlers.
func (s *Server) install() {
	s.handlers = map[string]func(context.Context, json.RawMessage) (any, error){
		"send_message":           s.sendMessage,
		"list_pending_messages":  s.listPendingMessages,
		"read_message":           s.readMessage,
		"ack_message":            s.ackMessage,
		"reply_to_message":       s.replyToMessage,
		"reject_message":         s.unavailable,
		"get_thread":             s.unavailable,
		"get_agent_presence":     s.unavailable,
		"get_delivery_status":    s.getDeliveryStatus,
		"request_loop_extension": s.unavailable,
	}
}

// Run serves MCP until EOF or a framing failure.
func (s *Server) Run(ctx context.Context) error {
	reader := bufio.NewReader(s.stdin)
	for {
		body, err := readFrame(reader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if err := s.serveOne(ctx, body); err != nil {
			return err
		}
	}
}

// mcpRequest is one decoded MCP message.
type mcpRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
	ID      json.RawMessage `json:"id,omitempty"`
}

// mcpResponse is one MCP answer. Typed Hub refusals carry only error_code.
type mcpResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	Result  any             `json:"result,omitempty"`
	Error   *mcpError       `json:"error,omitempty"`
	ID      json.RawMessage `json:"id"`
}

// mcpError is one bounded error member.
type mcpError struct {
	Code    int           `json:"code"`
	Message string        `json:"message"`
	Data    *mcpErrorData `json:"data,omitempty"`
}

type mcpErrorData struct {
	ErrorCode protocol.ErrorCode `json:"error_code"`
}

// CallToolResult is the MCP tools/call result shape. Tool values are JSON
// encoded in one text content item, so callers always receive the standard
// MCP envelope instead of a bare application object.
type CallToolResult struct {
	Content []ToolContent `json:"content"`
}

// ToolContent is one text content item in a CallToolResult.
type ToolContent struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

// serveOne answers one framed message. Notifications earn no answer.
func (s *Server) serveOne(ctx context.Context, body []byte) error {
	var req mcpRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return writeFrame(s.stdout, mcpResponse{
			JSONRPC: jsonRPCVersion,
			Error:   &mcpError{Code: -32700, Message: "Parse error"},
			ID:      json.RawMessage("null"),
		})
	}
	if req.ID == nil {
		s.serveNotification(req)
		return nil
	}
	resp := mcpResponse{JSONRPC: jsonRPCVersion, ID: req.ID}
	switch req.Method {
	case "initialize":
		resp.Result = s.initialize(req.Params)
	case "ping":
		resp.Result = map[string]any{}
	case "tools/list":
		list, err := toolsListJSON(s.agent != "")
		if err != nil {
			resp.Error = &mcpError{Code: -32603, Message: "Internal error"}
			break
		}
		resp.Result = list
	case "tools/call":
		result, err := s.callTool(ctx, req.Params)
		if err != nil {
			resp.Error = mcpFailure(err)
			break
		}
		wrapped, err := callToolResult(result)
		if err != nil {
			resp.Error = &mcpError{Code: -32603, Message: rpcMessage(ErrHubUnavailable)}
			break
		}
		resp.Result = wrapped
	default:
		resp.Error = &mcpError{Code: -32601, Message: "Method not found"}
	}
	return writeFrame(s.stdout, resp)
}

func callToolResult(value any) (CallToolResult, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return CallToolResult{}, fmt.Errorf("encode tool result: %w", err)
	}
	return CallToolResult{Content: []ToolContent{{Type: "text", Text: string(raw)}}}, nil
}

// serveNotification accepts server notifications, answering none.
func (s *Server) serveNotification(req mcpRequest) {
	switch req.Method {
	case "notifications/initialized":
	default:
	}
}

const mcpProtocolVersion = "2024-11-05"

// initialize answers the legacy MCP handshake with the version this server
// supports. A client offering another version can accept this negotiated
// fallback or disconnect.
func (s *Server) initialize(_ json.RawMessage) map[string]any {
	return map[string]any{
		"protocolVersion": mcpProtocolVersion,
		"capabilities":    map[string]any{"tools": map[string]any{}},
		"serverInfo":      map[string]any{"name": "gifford-endpoint", "version": version.String()},
	}
}

// callParams is the tools/call envelope.
type callParams struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments,omitempty"`
}

// callTool runs one tool. Without identity every call fails; unknown
// tools and misshapen arguments fail without touching the Hub.
func (s *Server) callTool(ctx context.Context, params json.RawMessage) (any, error) {
	if s.agent == "" {
		return nil, ErrNoIdentity
	}
	var call callParams
	decoder := json.NewDecoder(bytes.NewReader(params))
	if err := decoder.Decode(&call); err != nil {
		return nil, fmt.Errorf("call tool: %w", ErrBadArguments)
	}
	handle, ok := s.handlers[call.Name]
	if !ok || call.Name == "" {
		return nil, fmt.Errorf("call tool: %w", ErrUnknownTool)
	}
	args := call.Arguments
	if args == nil {
		args = json.RawMessage("{}")
	}
	return handle(ctx, args)
}

// unavailable fails closed for tools whose backing route lands later.
func (s *Server) unavailable(context.Context, json.RawMessage) (any, error) {
	return nil, ErrUnavailable
}

// decodeArgs decodes tool arguments, refusing non-objects.
func decodeArgs(args json.RawMessage, into any) error {
	decoder := json.NewDecoder(bytes.NewReader(args))
	var probe map[string]any
	if err := decoder.Decode(&probe); err != nil {
		return fmt.Errorf("decode arguments: %w", ErrBadArguments)
	}
	raw, err := json.Marshal(probe)
	if err != nil {
		return fmt.Errorf("decode arguments: %w", ErrBadArguments)
	}
	decoder = json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(into); err != nil {
		return fmt.Errorf("decode arguments: %w", ErrBadArguments)
	}
	return nil
}

// readFrame reads one newline-delimited JSON message.
func readFrame(reader *bufio.Reader) ([]byte, error) {
	line, err := reader.ReadBytes('\n')
	if err != nil {
		return nil, fmt.Errorf("read frame: %w", err)
	}
	if len(line) > 64<<20 {
		return nil, fmt.Errorf("frame too large: %w", ErrInvalidArgument)
	}
	return bytes.TrimSuffix(line, []byte{'\n'}), nil
}

// writeFrame writes one newline-delimited JSON message.
func writeFrame(writer io.Writer, resp mcpResponse) error {
	raw, err := json.Marshal(resp)
	if err != nil {
		return fmt.Errorf("encode frame: %w", err)
	}
	if _, err := writer.Write(append(raw, '\n')); err != nil {
		return fmt.Errorf("write frame: %w", err)
	}
	return nil
}
