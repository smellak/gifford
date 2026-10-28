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

// TestRPCParamsPresence requires params fields to be present and non-null
// before decoding, following the shape of the method's params type.
//
// Repeated expectation paths of the presence tables below.
const (
	paramsPresenceExpectedVersion = "params.expected_version"
	paramsPresenceUpperReason     = "params.REASON"
)

func TestRPCParamsPresence(t *testing.T) {
	t.Parallel()

	request := func(method ControlMethod, params string) RPCRequest {
		return RPCRequest{JSONRPC: JSONRPCVersion, ID: 1, Method: method, Params: Payload(params)}
	}
	setGlobal := func(params string) RPCRequest {
		return request(ControlMethodChannelSetGlobal, params)
	}
	loopCreate := func(params string) RPCRequest {
		return request(ControlMethodLoopCreate, params)
	}
	for _, tc := range []struct {
		name    string
		request RPCRequest
		path    string
		reason  string
	}{
		{"missing expected_version", setGlobal(`{"mode":"ACTIVE"}`), paramsPresenceExpectedVersion, "required"},
		{"null expected_version", setGlobal(`{"mode":"ACTIVE","expected_version":null}`), paramsPresenceExpectedVersion, reasonNullInvalid},
		{"null reason", setGlobal(`{"mode":"ACTIVE","expected_version":3,"reason":null}`), "params.reason", reasonNullInvalid},
		{"null ttl", setGlobal(`{"mode":"ACTIVE","expected_version":3,"ttl_ms":null}`), "params.ttl_ms", reasonNullInvalid},
		{"zero expected_version", setGlobal(`{"mode":"ACTIVE","expected_version":0}`), "", ""},
		{"missing loop generation", loopCreate(`{"thread_id":"thread-1","participants":["agent-1","agent-2"],"mode":"AUTO"}`), "params.expected_loop_generation", "required"},
		{"null inside budget", loopCreate(`{"thread_id":"thread-1","participants":["agent-1","agent-2"],"mode":"AUTO","expected_loop_generation":0,"budget":{"max_turns":null}}`), "params.budget.max_turns", reasonNullInvalid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := tc.request.Validate()
			if tc.path == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, ErrInvalid)
			var ve *ValidationError
			require.ErrorAs(t, err, &ve)
			require.Equal(t, tc.path, ve.Path)
			require.Equal(t, tc.reason, ve.Reason)
		})
	}
}

// TestRPCMapEntriesRejectNull requires null entries inside scalar maps
// such as expected_versions to fail: a null CAS version is not a zero.
func TestRPCMapEntriesRejectNull(t *testing.T) {
	t.Parallel()

	link := "link:agent-1→agent-2:message"
	set := func(versions string) RPCRequest {
		return RPCRequest{JSONRPC: JSONRPCVersion, ID: 1, Method: ControlMethodLinkSet, Params: Payload(
			`{"from_agent":"agent-1","to_agent":"agent-2","mode":"ALLOW","plane":"message","expected_versions":` + versions + `}`)}
	}
	del := func(versions string) RPCRequest {
		return RPCRequest{JSONRPC: JSONRPCVersion, ID: 1, Method: ControlMethodLinkDeleteOverride, Params: Payload(
			`{"from_agent":"agent-1","to_agent":"agent-2","plane":"message","expected_versions":` + versions + `}`)}
	}
	for _, tc := range []struct {
		name    string
		request RPCRequest
		path    string
	}{
		{"null set", set(`{"` + link + `":null}`), "params.expected_versions." + link},
		{"null delete", del(`{"` + link + `":null}`), "params.expected_versions." + link},
		{"null all bidirectional", RPCRequest{JSONRPC: JSONRPCVersion, ID: 1, Method: ControlMethodLinkSet, Params: Payload(
			`{"from_agent":"agent-1","to_agent":"agent-2","mode":"ALLOW","plane":"all","bidirectional":true,"expected_versions":{"link:agent-1→agent-2:message":0,"link:agent-1→agent-2:artifact":0,"link:agent-2→agent-1:message":0,"link:agent-2→agent-1:artifact":null}}`)}, "params.expected_versions.link:agent-2→agent-1:artifact"},
		{"null all first", RPCRequest{JSONRPC: JSONRPCVersion, ID: 1, Method: ControlMethodLinkSet, Params: Payload(
			`{"from_agent":"agent-1","to_agent":"agent-2","mode":"ALLOW","plane":"all","expected_versions":{"link:agent-1→agent-2:message":null,"link:agent-1→agent-2:artifact":0}}`)}, "params.expected_versions.link:agent-1→agent-2:message"},
		{"null all second", RPCRequest{JSONRPC: JSONRPCVersion, ID: 1, Method: ControlMethodLinkSet, Params: Payload(
			`{"from_agent":"agent-1","to_agent":"agent-2","mode":"ALLOW","plane":"all","expected_versions":{"link:agent-1→agent-2:message":1,"link:agent-1→agent-2:artifact":null}}`)}, "params.expected_versions.link:agent-1→agent-2:artifact"},
		{"null bidirectional reverse", RPCRequest{JSONRPC: JSONRPCVersion, ID: 1, Method: ControlMethodLinkSet, Params: Payload(
			`{"from_agent":"agent-1","to_agent":"agent-2","mode":"ALLOW","plane":"message","bidirectional":true,"expected_versions":{"link:agent-1→agent-2:message":0,"link:agent-2→agent-1:message":null}}`)}, "params.expected_versions.link:agent-2→agent-1:message"},
		{"null delete all", RPCRequest{JSONRPC: JSONRPCVersion, ID: 1, Method: ControlMethodLinkDeleteOverride, Params: Payload(
			`{"from_agent":"agent-1","to_agent":"agent-2","plane":"all","expected_versions":{"link:agent-1→agent-2:message":0,"link:agent-1→agent-2:artifact":null}}`)}, "params.expected_versions.link:agent-1→agent-2:artifact"},
		{"zero one all", RPCRequest{JSONRPC: JSONRPCVersion, ID: 1, Method: ControlMethodLinkSet, Params: Payload(
			`{"from_agent":"agent-1","to_agent":"agent-2","mode":"ALLOW","plane":"all","expected_versions":{"link:agent-1→agent-2:message":0,"link:agent-1→agent-2:artifact":1}}`)}, ""},
		{"zero delete all", RPCRequest{JSONRPC: JSONRPCVersion, ID: 1, Method: ControlMethodLinkDeleteOverride, Params: Payload(
			`{"from_agent":"agent-1","to_agent":"agent-2","plane":"all","expected_versions":{"link:agent-1→agent-2:message":0,"link:agent-1→agent-2:artifact":0}}`)}, ""},
		{"zero set", set(`{"` + link + `":0}`), ""},
		{"one set", set(`{"` + link + `":1}`), ""},
		{"zero delete", del(`{"` + link + `":0}`), ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := tc.request.Validate()
			if tc.path == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, ErrInvalid)
			var ve *ValidationError
			require.ErrorAs(t, err, &ve)
			require.Equal(t, tc.path, ve.Path)
			require.Equal(t, reasonNullInvalid, ve.Reason)
		})
	}
}

// TestRPCParamsExactKeys requires every key of a typed params object to be
// exactly the json name of one of its fields: any other key, including one
// that matches a field only case-insensitively, is an unknown field.
func TestRPCParamsExactKeys(t *testing.T) {
	t.Parallel()

	request := func(method ControlMethod, params string) RPCRequest {
		return RPCRequest{JSONRPC: JSONRPCVersion, ID: 1, Method: method, Params: Payload(params)}
	}
	setGlobal := func(params string) RPCRequest {
		return request(ControlMethodChannelSetGlobal, params)
	}
	for _, tc := range []struct {
		name    string
		request RPCRequest
		path    string
		reason  string
	}{
		{"uppercase reason null", setGlobal(`{"mode":"ACTIVE","expected_version":0,"REASON":null}`), paramsPresenceUpperReason, reasonUnknownField},
		{"mixed case ttl null", setGlobal(`{"mode":"ACTIVE","expected_version":0,"Ttl_Ms":null}`), "params.Ttl_Ms", reasonUnknownField},
		{"uppercase reason value", setGlobal(`{"mode":"ACTIVE","expected_version":0,"REASON":"hi"}`), paramsPresenceUpperReason, reasonUnknownField},
		{"truly unknown", setGlobal(`{"mode":"ACTIVE","expected_version":0,"colour":"red"}`), "params.colour", reasonUnknownField},
		{"byte order first", setGlobal(`{"mode":"ACTIVE","expected_version":0,"zeta":1,"REASON":null}`), paramsPresenceUpperReason, reasonUnknownField},
		{"uppercase ttl link", request(ControlMethodLinkSet, `{"from_agent":"agent-1","to_agent":"agent-2","mode":"ALLOW","plane":"message","TTL_MS":null,"expected_versions":{"link:agent-1→agent-2:message":0}}`), "params.TTL_MS", reasonUnknownField},
		{"uppercase budget", request(ControlMethodLoopCreate, `{"thread_id":"thread-1","participants":["agent-1","agent-2"],"mode":"AUTO","expected_loop_generation":0,"BUDGET":{"max_turns":null,"max_messages":5}}`), "params.BUDGET", reasonUnknownField},
		{"uppercase nested", request(ControlMethodLoopCreate, `{"thread_id":"thread-1","participants":["agent-1","agent-2"],"mode":"AUTO","expected_loop_generation":0,"budget":{"MAX_TURNS":null,"max_messages":5}}`), "params.budget.MAX_TURNS", reasonUnknownField},
		{"mixed case version", setGlobal(`{"mode":"ACTIVE","Expected_Version":0}`), "params.Expected_Version", reasonUnknownField},
		{"duplicate null last", setGlobal(`{"mode":"ACTIVE","expected_version":0,"expected_version":null}`), paramsPresenceExpectedVersion, reasonNullInvalid},
		{"duplicate zero last", setGlobal(`{"mode":"ACTIVE","expected_version":null,"expected_version":0}`), "", ""},
		{"canonical reason", setGlobal(`{"mode":"ACTIVE","expected_version":0,"reason":"hi"}`), "", ""},
		{"explicit false zero link", request(ControlMethodLinkSet, `{"from_agent":"agent-1","to_agent":"agent-2","mode":"ALLOW","plane":"message","bidirectional":false,"ttl_ms":0,"expected_versions":{"link:agent-1→agent-2:message":0}}`), "", ""},
		{"explicit false agent", request(ControlMethodAgentChannelSet, `{"agent_id":"agent-1","tx":false,"rx":true,"expected_version":0}`), "", ""},
		{"empty params", request(ControlMethodChannelGetGlobal, `{}`), "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := tc.request.Validate()
			if tc.path == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, ErrInvalid)
			var ve *ValidationError
			require.ErrorAs(t, err, &ve)
			require.Equal(t, tc.path, ve.Path)
			require.Equal(t, tc.reason, ve.Reason)
		})
	}
}
