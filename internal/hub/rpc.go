package hub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/smellak/gifford/internal/protocol"
)

// This file holds the JSON-RPC 2.0 framing of the local socket: one
// message per line, a fixed four-method table, and bounded errors. Only a
// typed domain refusal carries its public error_code. The socket layer routes
// params opaquely; handlers inspect routing metadata but never plaintext.

// jsonRPCVersion is the only wire version the local subset speaks.
const jsonRPCVersion = "2.0"

// Local methods served on hub.sock. Anything else is unknown.
const (
	MethodAdmitMessage          = "admit_message"
	MethodSubmitMessageEnvelope = "submit_message_envelope"
	MethodClaimMessage          = "claim_message"
	MethodAckMessage            = "ack_message"
	MethodListPendingMessages   = "list_pending_messages"
	MethodGetDeliveryStatus     = "get_delivery_status"
	// MethodWhoAmI reports the agent the connection is bound to. It
	// takes no params and is answered locally, never delegated: the
	// Endpoint uses it to learn its identity. It is not an MCP tool.
	MethodWhoAmI = "whoami"
)

// JSON-RPC error codes served. Only codeDomainError carries a data member.
const (
	codeDomainError    = -32000
	codeInvalidRequest = -32602
	codeMethodNotFound = -32601
	codeNotReady       = -32603
)

// Admitter admits a message. A nil handler answers -32603.
type Admitter interface {
	AdmitMessage(ctx context.Context, agent protocol.AgentID, params json.RawMessage) (any, error)
}

// EnvelopeSubmitter stores an accepted envelope. A nil handler answers
// -32603.
type EnvelopeSubmitter interface {
	SubmitMessageEnvelope(ctx context.Context, agent protocol.AgentID, params json.RawMessage) (any, error)
}

// Claimer claims a queued message. A nil handler answers -32603.
type Claimer interface {
	ClaimMessage(ctx context.Context, agent protocol.AgentID, params json.RawMessage) (any, error)
}

// Acker acknowledges a claimed message. A nil handler answers -32603.
type Acker interface {
	AckMessage(ctx context.Context, agent protocol.AgentID, params json.RawMessage) (any, error)
}

// PendingLister lists delivery generations visible to the bound recipient.
type PendingLister interface {
	ListPendingMessages(ctx context.Context, agent protocol.AgentID, params json.RawMessage) (any, error)
}

// DeliveryStatusReader reports recipient delivery state to the bound sender.
type DeliveryStatusReader interface {
	GetDeliveryStatus(ctx context.Context, agent protocol.AgentID, params json.RawMessage) (any, error)
}

// Handlers holds one small interface per local method. Every nil member
// answers -32603 until its subphase fills it.
type Handlers struct {
	Admit          Admitter
	Submit         EnvelopeSubmitter
	Claim          Claimer
	Ack            Acker
	Pending        PendingLister
	DeliveryStatus DeliveryStatusReader
}

// rpcRequest is one decoded line. ID stays raw so the response mirrors it
// byte for byte; params stay raw so the Hub never decodes message content.
type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
	ID      json.RawMessage `json:"id,omitempty"`
}

// rpcError is one bounded error member.
type rpcError struct {
	Code    int           `json:"code"`
	Message string        `json:"message"`
	Data    *rpcErrorData `json:"data,omitempty"`
}

// rpcErrorData is present only for a typed domain refusal. Invalid requests
// and internal failures deliberately carry no data.
type rpcErrorData struct {
	ErrorCode protocol.ErrorCode `json:"error_code"`
}

// rpcResponse is one response line.
type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	Result  any             `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
	ID      json.RawMessage `json:"id"`
}

// dispatch routes one decoded request. Well-formed calls to known methods
// reach their handler (or -32603 while it is nil); anything outside the
// local subset is -32602 and unknown methods are -32601.
func dispatch(ctx context.Context, agent protocol.AgentID, handlers Handlers, req rpcRequest) rpcResponse {
	answer := rpcResponse{JSONRPC: jsonRPCVersion, ID: req.ID}
	if answer.ID == nil {
		answer.ID = json.RawMessage("null")
	}
	if req.JSONRPC != jsonRPCVersion || req.Method == "" {
		answer.Error = &rpcError{Code: codeInvalidRequest, Message: "Invalid request"}
		return answer
	}
	if req.Method == MethodWhoAmI {
		answer.Result = map[string]string{"agent_id": string(agent)}
		return answer
	}
	var handle func(context.Context, protocol.AgentID, json.RawMessage) (any, error)
	switch req.Method {
	case MethodAdmitMessage:
		if handlers.Admit != nil {
			admit := handlers.Admit
			handle = admit.AdmitMessage
		}
	case MethodSubmitMessageEnvelope:
		if handlers.Submit != nil {
			submit := handlers.Submit
			handle = submit.SubmitMessageEnvelope
		}
	case MethodClaimMessage:
		if handlers.Claim != nil {
			claim := handlers.Claim
			handle = claim.ClaimMessage
		}
	case MethodAckMessage:
		if handlers.Ack != nil {
			ack := handlers.Ack
			handle = ack.AckMessage
		}
	case MethodListPendingMessages:
		if handlers.Pending != nil {
			pending := handlers.Pending
			handle = pending.ListPendingMessages
		}
	case MethodGetDeliveryStatus:
		if handlers.DeliveryStatus != nil {
			status := handlers.DeliveryStatus
			handle = status.GetDeliveryStatus
		}
	default:
		answer.Error = &rpcError{Code: codeMethodNotFound, Message: "Method not found"}
		return answer
	}
	if handle == nil {
		answer.Error = &rpcError{Code: codeNotReady, Message: "Internal error"}
		return answer
	}
	result, err := handle(ctx, agent, req.Params)
	if err != nil {
		var callErr *callError
		if errors.As(err, &callErr) {
			answer.Error = callErr.rpcError()
			return answer
		}
		answer.Error = &rpcError{Code: codeNotReady, Message: "Internal error"}
		return answer
	}
	answer.Result = result
	return answer
}

// decodeLine parses one socket line into a request. Garbage, non-objects
// (batches included: the local subset is single calls), and envelopes
// with a missing or non-string method are -32602 with a null id.
func decodeLine(line []byte) (rpcRequest, *rpcError) {
	invalid := &rpcError{Code: codeInvalidRequest, Message: "Invalid request"}
	var req rpcRequest
	if err := json.Unmarshal(line, &req); err != nil {
		return rpcRequest{}, invalid
	}
	if req.Method == "" {
		return rpcRequest{}, invalid
	}
	return req, nil
}

// encodeLine renders one response line, without the newline itself.
func encodeLine(resp rpcResponse) ([]byte, error) {
	raw, err := json.Marshal(resp)
	if err != nil {
		return nil, fmt.Errorf("encode response: %w", err)
	}
	return raw, nil
}
