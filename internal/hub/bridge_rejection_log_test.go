package hub

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

func TestBridgeRejectionLogRateLimitsAndKeepsContentOut(t *testing.T) {
	t.Parallel()
	clock := &bridgeLogClock{now: time.Date(2026, 11, 3, 15, 0, 0, 0, time.UTC)}
	var output bytes.Buffer
	node := &NodeSocket{bridgeLog: newBridgeRejectionLog(
		slog.New(slog.NewJSONHandler(&output, nil)),
		clock,
	)}
	rejected := rpcResponse{Error: &rpcError{
		Code: codeDomainError,
		Data: &rpcErrorData{ErrorCode: protocol.ErrorCodeRecipientNotDeliverable},
	}}
	succeeded := rpcResponse{Result: struct{}{}}

	first := bridgeLogRequest(t, "agent-one", "73401", "body-one")
	node.bridgeLog.Record(t.Context(), first, rejected)

	clock.now = clock.now.Add(10 * time.Second)
	suppressed := bridgeLogRequest(t, "agent-one", "73402", "body-two")
	node.bridgeLog.Record(t.Context(), suppressed, rejected)

	clock.now = clock.now.Add(10 * time.Second)
	node.bridgeLog.Record(t.Context(), suppressed, succeeded)

	clock.now = clock.now.Add(10 * time.Second)
	other := bridgeLogRequest(t, "agent-two", "73403", "body-three")
	node.bridgeLog.Record(t.Context(), other, rejected)

	clock.now = clock.now.Add(30 * time.Second)
	boundary := bridgeLogRequest(t, "agent-one", "73404", "body-four")
	node.bridgeLog.Record(t.Context(), boundary, rejected)

	logs := bridgeLogLines(t, output.String())
	require.Len(t, logs, 3, "the repeated key emits first and once at the minute boundary")
	requireBridgeLog(t, logs[0], "agent-one", "73401", 1)
	requireBridgeLog(t, logs[1], "agent-two", "73403", 1)
	requireBridgeLog(t, logs[2], "agent-one", "73404", 2)
	for _, content := range []string{"body-one", "body-two", "body-three", "body-four"} {
		require.NotContains(t, output.String(), content)
	}
}

func TestBridgeRejectionLogReadsMarkReadRecipientAndNumericCode(t *testing.T) {
	t.Parallel()
	var output bytes.Buffer
	log := newBridgeRejectionLog(slog.New(slog.NewJSONHandler(&output, nil)), nil)
	params, err := json.Marshal(map[string]string{
		"recipient_agent":   "agent-reader",
		"source_message_id": "73501",
		"body":              "message-content-canary",
	})
	require.NoError(t, err)
	log.Record(t.Context(), rpcRequest{Method: MethodBridgeMarkRead, Params: params}, rpcResponse{
		Error: &rpcError{Code: codeInvalidRequest},
	})

	lines := bridgeLogLines(t, output.String())
	require.Len(t, lines, 1)
	require.Equal(t, MethodBridgeMarkRead, lines[0]["method"])
	require.Equal(t, "-32602", lines[0]["code"])
	require.Equal(t, "agent-reader", lines[0]["recipient_agent"])
	require.Equal(t, "73501", lines[0]["source_message_id"])
	require.NotContains(t, output.String(), "message-content-canary")
}

func TestDispatchBridgeLogsOnlyMethodsThatReachAHandler(t *testing.T) {
	t.Parallel()
	fixture := newBridgeFixture(t)
	params := testBridgeMirrorParams()
	params.SourceMessageID = "73601"
	params.ToAgent = testUnknownAgent
	var output bytes.Buffer
	rejectionLog := newBridgeRejectionLog(slog.New(slog.NewJSONHandler(&output, nil)), nil)

	response := dispatchBridge(t.Context(), fixture.service, rpcRequest{
		JSONRPC: jsonRPCVersion, Method: MethodBridgeMirrorMessage, Params: mustJSON(t, params),
	}, rejectionLog)
	require.NotNil(t, response.Error)
	require.Equal(t, protocol.ErrorCodeNotFound, response.Error.Data.ErrorCode)

	dispatchBridge(t.Context(), fixture.service, rpcRequest{
		JSONRPC: jsonRPCVersion, Method: MethodWhoAmI,
	}, rejectionLog)
	lines := bridgeLogLines(t, output.String())
	require.Len(t, lines, 1, "an unoffered method never reaches a bridge handler")
	require.Equal(t, MethodBridgeMirrorMessage, lines[0]["method"])
	require.Equal(t, string(protocol.ErrorCodeNotFound), lines[0]["code"])
}

func bridgeLogRequest(
	t *testing.T,
	recipient protocol.AgentID,
	sourceMessageID, body string,
) rpcRequest {
	t.Helper()
	params, err := json.Marshal(map[string]string{
		"to_agent": string(recipient), "source_message_id": sourceMessageID, "body": body,
	})
	require.NoError(t, err)
	return rpcRequest{Method: MethodBridgeMirrorMessage, Params: params}
}

type bridgeLogClock struct{ now time.Time }

func (c *bridgeLogClock) Now() time.Time { return c.now }

func bridgeLogLines(t *testing.T, raw string) []map[string]any {
	t.Helper()
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil
	}
	lines := strings.Split(trimmed, "\n")
	decoded := make([]map[string]any, len(lines))
	for index, line := range lines {
		require.NoError(t, json.Unmarshal([]byte(line), &decoded[index]))
	}
	return decoded
}

func requireBridgeLog(
	t *testing.T,
	line map[string]any,
	recipient, sourceMessageID string,
	rejections float64,
) {
	t.Helper()
	require.Equal(t, "Bridge method rejected", line["msg"])
	require.Equal(t, bridgeRejectionEvent, line["event"])
	require.Equal(t, MethodBridgeMirrorMessage, line["method"])
	require.Equal(t, string(protocol.ErrorCodeRecipientNotDeliverable), line["code"])
	require.InDelta(t, float64(codeDomainError), line["rpc_code"], 0)
	require.InDelta(t, rejections, line["rejections"], 0)
	require.Equal(t, recipient, line["recipient_agent"])
	require.Equal(t, sourceMessageID, line["source_message_id"])
}
