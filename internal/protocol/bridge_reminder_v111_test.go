package protocol

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBridgeReminderTombstoneContractsV111(t *testing.T) {
	t.Parallel()

	params := BridgeReminderTombstoneParams{
		Schema: BridgeReminderSchemaV1, SourceNodeID: "node-local",
		ExpectationID: "rsp-0123456789abcdef0123456789abcdef", Generation: 1,
		Reason: BridgeReminderCancelExpectationResolved,
	}
	require.NoError(t, params.Validate())

	receipt := BridgeReminderReceiptV111{
		ReminderID:       "01ARZ3NDEKTSV4RRFFQ69G5FAV",
		GiffordMessageID: "01ARZ3NDEKTSV4RRFFQ69G5FAW",
		Disposition:      BridgeReminderDispositionCreated,
		State:            BridgeReminderTerminalCancelled,
		AcceptedAt:       "2026-11-03T15:32:35Z", ExpiresAt: "9999-12-31T23:59:59.999999999Z",
		Tombstone: true,
	}
	require.NoError(t, receipt.Validate())
	receipt.Tombstone = false
	require.Error(t, receipt.Validate(), "the v1.11 terminal receipt always carries the tombstone marker")
	receipt.Tombstone = true
	receipt.State = BridgeReminderTerminalState("PENDING")
	require.Error(t, receipt.Validate(), "a tombstone receipt is always terminal")

	for _, name := range []string{
		"bridge_reminder_tombstone_params",
		"bridge_reminder_receipt_v1_11",
		"rpc_response_v1_11",
	} {
		require.Equal(t, MilestoneV111, bridgeReminderRoot(t, name).Since)
	}
}

func TestReminderRefusalsNeedV111ResponseSchema(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name      string
		code      ErrorCode
		retryable bool
	}{
		{name: "permanent tombstone", code: ErrorCodeReminderTombstoned},
		{name: "write contention", code: ErrorCodeReminderWriteBusy, retryable: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			raw, err := json.Marshal(RPCResponseV111{
				JSONRPC: JSONRPCVersion,
				ID:      1,
				Error: &RPCErrorV111{
					Code: rpcCodeDomainError, Message: "Request rejected",
					Data: &RPCErrorDataV111{ErrorCode: test.code, Retryable: test.retryable},
				},
			})
			require.NoError(t, err)
			document := mustDoc(t, raw)
			require.Error(t, compileSchema(t, bridgeReminderRoot(t, "rpc_response")).Validate(document),
				"the published response root must remain frozen")
			require.NoError(t, compileSchema(t, bridgeReminderRoot(t, "rpc_response_v1_11")).Validate(document))
		})
	}
}

func TestContractsV110RootsRemainFrozen(t *testing.T) {
	t.Parallel()

	want := map[string]string{
		"model_activation_result_v1_10": "3505c5d268582c9f48c4d09cb783ab61ca8376644a7ca6967878e43ee07cfc82",
		"model_selection_v1_10":         "4d607976776680f2187412e849da813d8126bdd628a3cc5ba993e4b00e5a77f0",
		"model_status_result_v1_10":     "9340adfd2ff0438d106030fe2797a7c110ccc6c358c65ca125b6fe5ba932c351",
		"rpc_request_v1_10":             "4742f4e35bc04d7fdbda949c192921280eb5bee9f3e0c804464d98ac0dad5e60",
		"rpc_response":                  "586fcb07cc4cee6d60f802f88ed961d63db8037e8b425e52366d047a943c8abe",
	}
	for name, digest := range want {
		raw, err := os.ReadFile(filepath.Join("..", "..", "schemas", "v1", name+".schema.json"))
		require.NoError(t, err)
		got := sha256.Sum256(raw)
		require.Equal(t, digest, hex.EncodeToString(got[:]), name)
	}
}

func bridgeReminderRoot(t *testing.T, name string) Root {
	t.Helper()
	for _, root := range Roots() {
		if root.Name == name {
			return root
		}
	}
	t.Fatalf("missing protocol root %q", name)
	return Root{}
}
