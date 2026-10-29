package protocol

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNodeControlParamsValidateStrictly(t *testing.T) {
	t.Parallel()

	ttl := int64(900_000)
	fingerprint := SHA256Hex("abababababababababababababababababababababababababababababababab")
	valid := []struct {
		method ControlMethod
		params any
	}{
		{ControlMethodNodeEnrollToken, NodeEnrollTokenParams{OrganizationID: "org-a", TTLMS: &ttl}},
		{ControlMethodNodeJoin, NodeJoinParams{URL: "https://node-a.example.invalid", Token: "token", Fingerprint: fingerprint}},
		{ControlMethodNodePeers, EmptyParams{}},
		{ControlMethodNodeRevoke, NodeRevokeParams{NodeID: "node-b"}},
	}
	for _, test := range valid {
		raw, err := json.Marshal(test.params)
		require.NoError(t, err)
		require.NoError(t, (RPCRequest{
			JSONRPC: JSONRPCVersion, ID: 1, Method: test.method, Params: raw,
		}).Validate(), test.method)
	}

	tooLong := int64(900_001)
	for _, test := range []NodeEnrollTokenParams{
		{OrganizationID: "Org-A"},
		{TTLMS: &tooLong},
	} {
		require.Error(t, test.Validate())
	}
	require.Error(t, (NodeJoinParams{
		URL: "https://node-a.example.invalid", Token: "token", Fingerprint: "invalid-fingerprint",
	}).Validate())
}
