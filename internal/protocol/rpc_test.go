package protocol

import (
	"testing"

	"github.com/stretchr/testify/require"
)

const emptyParamsJSON = "{}"

func validRPCRequest() RPCRequest {
	return RPCRequest{
		JSONRPC: JSONRPCVersion,
		ID:      1,
		Method:  ControlMethodChannelGetGlobal,
		Params:  Payload(emptyParamsJSON),
	}
}

// TestRPCRules covers the JSON-RPC envelope of §6.8.
func TestRPCRules(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		valid bool
		patch func(*RPCRequest)
	}{
		{name: "empty params pass", valid: true, patch: func(*RPCRequest) {}},
		{name: "params decode strictly", valid: true, patch: func(r *RPCRequest) {
			r.Method = ControlMethodChannelGetNode
			r.Params = Payload(`{"node_id":"node-a"}`)
		}},
		{name: "unknown params rejected", patch: func(r *RPCRequest) {
			r.Params = Payload(`{"node_id":"node-a"}`)
		}},
		{name: "params validate", patch: func(r *RPCRequest) {
			r.Method = ControlMethodLinkGet
			r.Params = Payload(`{"from_agent":"agent-1","to_agent":"agent-1"}`)
		}},
		{name: "version is 2.0", patch: func(r *RPCRequest) {
			r.JSONRPC = "1.0"
		}},
		{name: "id starts at 1", patch: func(r *RPCRequest) {
			r.ID = 0
		}},
		{name: "method is known", patch: func(r *RPCRequest) {
			r.Method = "channel.bogus"
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			request := validRPCRequest()
			tc.patch(&request)
			if tc.valid {
				require.NoError(t, request.Validate())
				return
			}
			require.ErrorIs(t, request.Validate(), ErrInvalid)
		})
	}
}

// TestRPCResponseRules covers the exactly-one rule.
func TestRPCResponseRules(t *testing.T) {
	t.Parallel()

	response := RPCResponse{
		JSONRPC: JSONRPCVersion,
		ID:      1,
		Result:  Payload(`{"scope_type":"global"}`),
	}
	require.NoError(t, response.Validate())

	failed := RPCResponse{
		JSONRPC: JSONRPCVersion,
		ID:      1,
		Error: &RPCError{
			Code:    rpcCodeDomainError,
			Message: "link denied",
			Data:    &RPCErrorData{ErrorCode: ErrorCodeLinkDenied},
		},
	}
	require.NoError(t, failed.Validate())

	empty := RPCResponse{JSONRPC: JSONRPCVersion, ID: 1}
	require.ErrorIs(t, empty.Validate(), ErrInvalid)

	both := RPCResponse{
		JSONRPC: JSONRPCVersion,
		ID:      1,
		Result:  Payload(emptyParamsJSON),
		Error:   &RPCError{Code: rpcCodeInternalError, Message: "boom"},
	}
	require.ErrorIs(t, both.Validate(), ErrInvalid)

	plain := RPCError{Code: rpcCodeMethodNotFound, Message: "no such method"}
	require.NoError(t, plain.Validate())

	missing := RPCError{Code: rpcCodeDomainError, Message: "typed"}
	require.ErrorIs(t, missing.Validate(), ErrInvalid)

	extra := RPCError{
		Code:    rpcCodeInvalidParams,
		Message: "bad",
		Data:    &RPCErrorData{ErrorCode: ErrorCodeExpired},
	}
	require.ErrorIs(t, extra.Validate(), ErrInvalid)

	unknown := RPCError{Code: -32001, Message: "bad"}
	require.ErrorIs(t, unknown.Validate(), ErrInvalid)

	blank := RPCError{Code: rpcCodeInternalError}
	require.ErrorIs(t, blank.Validate(), ErrInvalid)
}
