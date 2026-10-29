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

	"github.com/smellak/gifford/internal/protocol"
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

	mu    sync.Mutex
	acked map[string]protocol.AckStatus
}

// New dials the Hub through hub, learns the connection's agent with
// whoami, and loads its signing key. A connection the Hub does not bind
// still serves framing and an empty tool list, but every call fails with
// no identity: nothing is minted locally, and the agent id never comes
// from arguments or the environment.
func New(dataDir string, stdin io.Reader, stdout io.Writer, hub Hub) (*Server, error) {
	if stdin == nil || stdout == nil || hub == nil {
		return nil, fmt.Errorf("new endpoint server: %w", ErrInvalidArgument)
	}
	resolved, err := resolveDataDir(dataDir)
	if err != nil {
		return nil, err
	}
	server := &Server{
		hub:     hub,
		dataDir: resolved,
		stdin:   stdin,
		stdout:  stdout,
		acked:   make(map[string]protocol.AckStatus),
	}
	server.install()
	if agent, err := hub.WhoAmI(context.Background()); err == nil {
		if priv, err := LoadAgentPrivate(resolved, agent); err == nil {
			server.agent = agent
			server.priv = priv
		}
	}
	return server, nil
}

// install wires the tool handlers.
func (s *Server) install() {
	s.handlers = map[string]func(context.Context, json.RawMessage) (any, error){
		"send_message":           s.sendMessage,
		"list_pending_messages":  s.passthrough("list_pending_messages"),
		"read_message":           s.passthrough("read_message"),
		"ack_message":            s.ackMessage,
		"reply_to_message":       s.replyToMessage,
		"reject_message":         s.passthrough("reject_message"),
		"get_thread":             s.passthrough("get_thread"),
		"get_agent_presence":     s.passthrough("get_agent_presence"),
		"get_delivery_status":    s.passthrough("get_delivery_status"),
		"request_loop_extension": s.passthrough("request_loop_extension"),
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

// mcpResponse is one MCP answer. Errors carry code and message only.
type mcpResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	Result  any             `json:"result,omitempty"`
	Error   *mcpError       `json:"error,omitempty"`
	ID      json.RawMessage `json:"id"`
}

// mcpError is one error member: never a data member.
type mcpError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
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
			resp.Error = &mcpError{Code: rpcCode(err), Message: rpcMessage(err)}
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

// initialize answers the MCP handshake, echoing the client's protocol
// version when one is offered.
func (s *Server) initialize(params json.RawMessage) map[string]any {
	version := "2024-11-05"
	var offered struct {
		ProtocolVersion string `json:"protocolVersion"`
	}
	if err := json.Unmarshal(params, &offered); err == nil && offered.ProtocolVersion != "" {
		version = offered.ProtocolVersion
	}
	return map[string]any{
		"protocolVersion": version,
		"capabilities":    map[string]any{"tools": map[string]any{}},
		"serverInfo":      map[string]any{"name": "gifford-endpoint"},
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

// sendParams decodes send_message arguments. From is filled from the
// connection identity, never from the client.
type sendParams struct {
	To                string  `json:"to"`
	Subject           string  `json:"subject"`
	Body              string  `json:"body"`
	ThreadID          *string `json:"thread_id"`
	Priority          *string `json:"priority"`
	TTL               *string `json:"ttl"`
	ClientMsgID       *string `json:"client_msg_id"`
	RequestedAutonomy *string `json:"requested_autonomy"`
	RequestedBudget   *string `json:"requested_budget"`
}

// sendMessage signs the envelope with the agent key and admits it,
// returning the typed admission verdict.
func (s *Server) sendMessage(ctx context.Context, args json.RawMessage) (any, error) {
	var params sendParams
	if err := decodeArgs(args, &params); err != nil {
		return nil, err
	}
	if params.To == "" || params.Subject == "" || params.Body == "" {
		return nil, fmt.Errorf("send message: %w", ErrBadArguments)
	}
	env := Envelope{From: s.agent, To: params.To, Subject: params.Subject, Body: params.Body}
	optional(&env.ThreadID, params.ThreadID)
	optional(&env.Priority, params.Priority)
	optional(&env.TTL, params.TTL)
	optional(&env.ClientMsgID, params.ClientMsgID)
	optional(&env.RequestedAutonomy, params.RequestedAutonomy)
	optional(&env.RequestedBudget, params.RequestedBudget)
	return s.admit(ctx, env)
}

// replyParams decodes reply_to_message arguments.
type replyParams struct {
	ID                string  `json:"id"`
	Body              string  `json:"body"`
	RequestedAutonomy *string `json:"requested_autonomy"`
}

// replyToMessage signs the reply envelope and admits it, returning the
// typed admission verdict.
func (s *Server) replyToMessage(ctx context.Context, args json.RawMessage) (any, error) {
	var params replyParams
	if err := decodeArgs(args, &params); err != nil {
		return nil, err
	}
	if params.ID == "" || params.Body == "" {
		return nil, fmt.Errorf("reply to message: %w", ErrBadArguments)
	}
	env := Envelope{From: s.agent, Body: params.Body, InReplyTo: params.ID}
	optional(&env.RequestedAutonomy, params.RequestedAutonomy)
	return s.admit(ctx, env)
}

// admit sends only routing metadata to admission and types the verdict. The
// Endpoint remains the sole plaintext holder throughout this call.
func (s *Server) admit(ctx context.Context, env Envelope) (any, error) {
	metadata := admissionMetadata{
		From: env.From, To: env.To, ThreadID: env.ThreadID, Priority: env.Priority,
		TTL: env.TTL, ClientMsgID: env.ClientMsgID,
		RequestedAutonomy: env.RequestedAutonomy, RequestedBudget: env.RequestedBudget,
		InReplyTo: env.InReplyTo,
	}
	raw, err := s.hub.Call(ctx, "admit_message", metadata)
	if err != nil {
		return nil, fmt.Errorf("admit message: %w", ErrHubUnavailable)
	}
	var verdict protocol.AdmissionResult
	if err := json.Unmarshal(raw, &verdict); err != nil {
		return nil, fmt.Errorf("admit message: %w", ErrHubUnavailable)
	}
	if err := verdict.Validate(); err != nil {
		return nil, fmt.Errorf("admit message: %w", ErrHubUnavailable)
	}
	return verdict, nil
}

// admissionMetadata is the only message information the Hub receives during
// admission. Subject and body deliberately have no fields here.
type admissionMetadata struct {
	From              protocol.AgentID `json:"from"`
	To                string           `json:"to,omitempty"`
	ThreadID          string           `json:"thread_id,omitempty"`
	Priority          string           `json:"priority,omitempty"`
	TTL               string           `json:"ttl,omitempty"`
	ClientMsgID       string           `json:"client_msg_id,omitempty"`
	RequestedAutonomy string           `json:"requested_autonomy,omitempty"`
	RequestedBudget   string           `json:"requested_budget,omitempty"`
	InReplyTo         string           `json:"in_reply_to,omitempty"`
}

// ackParams decodes ack_message arguments.
type ackParams struct {
	ID     string             `json:"id"`
	Status protocol.AckStatus `json:"status"`
}

// ackMessage acknowledges through the Hub. A repeated ack for the same
// id returns success without a second Hub call: it has no second effect.
func (s *Server) ackMessage(ctx context.Context, args json.RawMessage) (any, error) {
	var params ackParams
	if err := decodeArgs(args, &params); err != nil {
		return nil, err
	}
	if params.ID == "" || !params.Status.Valid() {
		return nil, fmt.Errorf("ack message: %w", ErrBadArguments)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	previous, seen := s.acked[params.ID]
	if seen && (previous == params.Status || previous == protocol.AckStatusAcknowledged) {
		return map[string]any{"acked": true}, nil
	}
	if _, err := s.hub.Call(ctx, "ack_message", params); err != nil {
		return nil, fmt.Errorf("ack message: %w", ErrHubUnavailable)
	}
	s.acked[params.ID] = params.Status
	return map[string]any{"acked": true}, nil
}

// passthrough forwards a tool's raw arguments to the same-named Hub
// method and returns its raw result. Later subphases type these.
func (s *Server) passthrough(method string) func(context.Context, json.RawMessage) (any, error) {
	return func(ctx context.Context, args json.RawMessage) (any, error) {
		var params any
		if len(args) > 0 {
			if err := json.Unmarshal(args, &params); err != nil {
				return nil, fmt.Errorf("call %s: %w", method, ErrBadArguments)
			}
		}
		raw, err := s.hub.Call(ctx, method, params)
		if err != nil {
			return nil, fmt.Errorf("call %s: %w", method, ErrHubUnavailable)
		}
		return raw, nil
	}
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
	if err := json.Unmarshal(raw, into); err != nil {
		return fmt.Errorf("decode arguments: %w", ErrBadArguments)
	}
	return nil
}

// optional copies a set optional argument.
func optional(into *string, from *string) {
	if from != nil {
		*into = *from
	}
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
