package endpoint

import "errors"

// Endpoint errors. Every MCP error the server emits maps from one of
// these: fixed codes with fixed messages, never carrying caller content.
var (
	// ErrInvalidArgument means a caller-supplied argument breaks its rules.
	ErrInvalidArgument = errors.New("invalid argument")
	// ErrNoIdentity means the Hub verified no agent for this connection:
	// it gets no tools and every call fails.
	ErrNoIdentity = errors.New("no verified agent identity")
	// ErrUnknownTool means the tool name is not served.
	ErrUnknownTool = errors.New("unknown tool")
	// ErrBadArguments means the tool arguments are not an object of the
	// tool's shape.
	ErrBadArguments = errors.New("bad tool arguments")
	// ErrHubUnavailable means the Hub link failed: unreachable, broken
	// framing, or a Hub-side error.
	ErrHubUnavailable = errors.New("hub unavailable")
	// ErrUnavailable means the requested tool has no route in this phase.
	ErrUnavailable = errors.New("capability unavailable")
	// ErrIntegrity means a claimed message failed signature or header binding.
	ErrIntegrity = errors.New("message integrity check failed")
	// ErrMetadataMismatch means Hub and signed recipient metadata disagreed.
	ErrMetadataMismatch = errors.New("message metadata mismatch")
)

// rpcCode maps an endpoint failure to its JSON-RPC code. Unknown tools
// and bad arguments are client mistakes; everything backend is internal.
// No error carries a data member.
func rpcCode(err error) int {
	switch {
	case errors.Is(err, ErrUnknownTool),
		errors.Is(err, ErrBadArguments):
		return -32602
	case errors.Is(err, ErrNoIdentity):
		return -32001
	case errors.Is(err, ErrUnavailable),
		errors.Is(err, ErrIntegrity), errors.Is(err, ErrMetadataMismatch):
		return -32002
	default:
		return -32603
	}
}

// rpcMessage maps an endpoint failure to its fixed message. Messages are
// constants: they never echo tool names, arguments, or Hub output.
func rpcMessage(err error) string {
	switch {
	case errors.Is(err, ErrUnknownTool):
		return "Unknown tool"
	case errors.Is(err, ErrBadArguments):
		return "Invalid params"
	case errors.Is(err, ErrNoIdentity):
		return "No verified identity"
	case errors.Is(err, ErrHubUnavailable):
		return "Hub unavailable"
	case errors.Is(err, ErrUnavailable):
		return "Capability unavailable"
	case errors.Is(err, ErrIntegrity):
		return "Message integrity check failed"
	case errors.Is(err, ErrMetadataMismatch):
		return "Message metadata mismatch"
	default:
		return "Internal error"
	}
}
