package ctl

import (
	"bytes"
	"encoding/json"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/cli/exitcode"
	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/tmuxlock"
)

const (
	ctlPaneCommand       = "pane"
	ctlPaneInvalidTarget = "INVALID_TARGET"
	ctlPaneSocket        = "/run/gifford-pane-test/node.sock"
	ctlPaneBootID        = "12345678-1234-1234-1234-123456789abc"
	ctlPaneExpectedHex   = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	ctlPaneOwner         = "capycom"
	ctlPanePeerWriter    = "notifier"
	ctlPaneHost          = "bench-node"
)

type allowPaneAuthorizer struct{}

func (allowPaneAuthorizer) AuthorizePane(tmuxlock.PanePeer) error { return nil }

func ctlPaneTargetArgs(extra ...string) []string {
	return append([]string{
		"--owner-uid", "1113",
		"--socket", ctlPaneSocket,
		"--pane-id", "%3",
		"--host", ctlPaneHost,
		"--boot-id", ctlPaneBootID,
		"--pane-pid", "424242",
		"--pane-start", "987654",
	}, extra...)
}

func ctlPaneTarget() protocol.PaneOperationTarget {
	return protocol.PaneOperationTarget{
		OwnerUID: 1113, Socket: ctlPaneSocket, PaneID: "%3",
		Host: ctlPaneHost, BootID: ctlPaneBootID, PanePID: 424242, PaneStart: 987654,
	}
}

// provisionPaneTree builds a synthetic lock tree with the exact target the
// pane commands observe, and points the pane observer at it.
func provisionPaneTree(t *testing.T) {
	t.Helper()
	root := t.TempDir()
	if err := os.Chmod(root, 0o711); err != nil {
		t.Fatalf("chmod pane test root: %v", err)
	}
	_, err := tmuxlock.Provision(tmuxlock.ProvisionSpec{
		Root:       root,
		Binding:    tmuxlock.PanelBinding{OwnerUID: 1113, Socket: ctlPaneSocket, PaneID: "%3"},
		PanePID:    424242,
		PaneStart:  987654,
		BootID:     ctlPaneBootID,
		Host:       ctlPaneHost,
		BindUID:    1113,
		Owner:      ctlPaneOwner,
		Policy:     tmuxlock.RecordPolicy{PeerOwner: ctlPaneOwner, PeerWriters: []string{ctlPanePeerWriter}},
		Clock:      clock.System{},
		Peer:       tmuxlock.PanePeer{PID: 424242, UID: 1113, Verified: true, Actor: "uid:1113:pid:424242"},
		Authorizer: allowPaneAuthorizer{},
	})
	require.NoError(t, err)
	old := paneLockRoot
	paneLockRoot = root
	t.Cleanup(func() { paneLockRoot = old })
}

// writePaneConfig writes the node configuration the status observer reads
// the record policy from.
func writePaneConfig(t *testing.T) {
	t.Helper()
	home := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", home)
	dir := filepath.Join(home, "gifford")
	require.NoError(t, os.MkdirAll(dir, 0o700))
	document := "tmux_locks:\n" +
		"  peer_owner: " + ctlPaneOwner + "\n" +
		"  peer_writers: [" + ctlPanePeerWriter + "]\n" +
		"  host: " + ctlPaneHost + "\n" +
		"  mode_path: /run/gifford-pane-test/mode\n" +
		"  admin_uids_path: /run/gifford-pane-test/admins\n" +
		"  operation_schema: pane-op/1\n" +
		"  initial_take_drain: 1m\n"
	require.NoError(t, os.WriteFile(filepath.Join(dir, "config.yaml"), []byte(document), 0o600))
}

func startPaneFakeServer(t *testing.T, handle func(protocol.RPCRequest) (any, *protocol.RPCError)) chan protocol.RPCRequest {
	t.Helper()
	base := t.TempDir()
	t.Setenv("XDG_RUNTIME_DIR", base)
	runtimeDir := filepath.Join(base, "gifford")
	require.NoError(t, os.Mkdir(runtimeDir, 0o700))
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "unix", filepath.Join(runtimeDir, "daemon.sock"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })

	requests := make(chan protocol.RPCRequest, 8)
	done := make(chan error, 1)
	go func() {
		defer close(done)
		defer func() { _ = listener.Close() }()
		for {
			conn, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}
			var request protocol.RPCRequest
			if decodeErr := json.NewDecoder(conn).Decode(&request); decodeErr != nil {
				_ = conn.Close()
				done <- decodeErr
				return
			}
			requests <- request
			result, rpcErr := handle(request)
			if rpcErr != nil {
				if encodeErr := json.NewEncoder(conn).Encode(protocol.RPCResponse{
					JSONRPC: protocol.JSONRPCVersion, ID: request.ID, Error: rpcErr,
				}); encodeErr != nil {
					_ = conn.Close()
					done <- encodeErr
					return
				}
				_ = conn.Close()
				continue
			}
			raw, encodeErr := json.Marshal(result)
			if encodeErr == nil {
				encodeErr = json.NewEncoder(conn).Encode(protocol.RPCResponse{
					JSONRPC: protocol.JSONRPCVersion, ID: request.ID, Result: raw,
				})
			}
			_ = conn.Close()
			if encodeErr != nil {
				done <- encodeErr
				return
			}
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		if err := <-done; err != nil {
			t.Errorf("pane fake server: %v", err)
		}
	})
	return requests
}

func runPaneCommand(t *testing.T, args []string) (string, error) {
	t.Helper()
	cmd := NewCmd(&deps.Deps{})
	var output bytes.Buffer
	cmd.SetOut(&output)
	cmd.SetErr(io.Discard)
	cmd.SetArgs(args)
	err := cmd.ExecuteContext(t.Context())
	return output.String(), err
}

func requireSingleJSONLine(t *testing.T, output string, into any) {
	t.Helper()
	require.Equal(t, 1, strings.Count(output, "\n"), "pane prints exactly one line")
	require.True(t, strings.HasSuffix(output, "\n"))
	require.NoError(t, json.Unmarshal([]byte(output), into))
}

func TestCtlPaneCommandsSendExactParams(t *testing.T) {
	provisionPaneTree(t)
	writePaneConfig(t)
	requests := startPaneFakeServer(t, func(protocol.RPCRequest) (any, *protocol.RPCError) {
		return protocol.PaneOperationResult{Changed: true}, nil
	})

	ops := []struct {
		name       string
		method     protocol.ControlMethod
		extraFlags []string
	}{
		{paneOpProvision, protocol.ControlMethodPaneProvision, nil},
		{paneOpTake, protocol.ControlMethodPaneTake, nil},
		{paneOpReturn, protocol.ControlMethodPaneReturn, nil},
		{paneOpReconcile, protocol.ControlMethodPaneReconcile, []string{"--expected-attempt", ctlPaneExpectedHex}},
		{paneOpRepair, protocol.ControlMethodPaneRepair, nil},
	}
	for _, op := range ops {
		args := append([]string{ctlPaneCommand, op.name}, ctlPaneTargetArgs(op.extraFlags...)...)
		output, err := runPaneCommand(t, args)
		require.NoError(t, err, "pane %s", op.name)
		var line paneOutcome
		requireSingleJSONLine(t, output, &line)
		require.Equal(t, op.name, line.Op, "pane %s", op.name)
		require.Equal(t, paneResultChanged, line.Result, "pane %s", op.name)
		require.Equal(t, ctlPaneOwner, line.OwnerBefore, "pane %s", op.name)
		require.Equal(t, ctlPaneOwner, line.OwnerAfter, "pane %s", op.name)
		require.NotNil(t, line.OwnerGen, "pane %s", op.name)
		require.Equal(t, uint32(0), *line.OwnerGen, "pane %s", op.name)
		require.Equal(t, "IDLE", line.Phase, "pane %s", op.name)
		require.Equal(t, "-", line.TxID, "pane %s", op.name)
	}

	wantTarget := ctlPaneTarget()
	for _, op := range ops {
		var request protocol.RPCRequest
		select {
		case request = <-requests:
		default:
			t.Fatalf("pane %s: no request reached the fake server", op.name)
		}
		require.Equal(t, op.method, request.Method, "pane %s", op.name)
		if op.name == "reconcile" {
			var params protocol.PaneAttemptParams
			require.NoError(t, json.Unmarshal(request.Params, &params), "pane %s", op.name)
			require.Equal(t, wantTarget, params.Target, "pane %s", op.name)
			require.Equal(t, protocol.PaneAttemptID(ctlPaneExpectedHex), params.ExpectedAttempt, "pane %s", op.name)
			continue
		}
		var params protocol.PaneOperationParams
		require.NoError(t, json.Unmarshal(request.Params, &params), "pane %s", op.name)
		require.Equal(t, wantTarget, params.Target, "pane %s", op.name)
	}
}

func TestCtlPaneNoopLine(t *testing.T) {
	provisionPaneTree(t)
	writePaneConfig(t)
	startPaneFakeServer(t, func(protocol.RPCRequest) (any, *protocol.RPCError) {
		return protocol.PaneOperationResult{Changed: false}, nil
	})

	output, err := runPaneCommand(t, append([]string{ctlPaneCommand, paneOpTake}, ctlPaneTargetArgs()...))
	require.NoError(t, err)
	var line paneOutcome
	requireSingleJSONLine(t, output, &line)
	require.Equal(t, "take", line.Op)
	require.Equal(t, paneResultNoop, line.Result)
	require.Equal(t, ctlPaneOwner, line.OwnerAfter)
}

func TestCtlPaneAPIErrorLineAndExit(t *testing.T) {
	provisionPaneTree(t)
	writePaneConfig(t)
	startPaneFakeServer(t, func(request protocol.RPCRequest) (any, *protocol.RPCError) {
		require.Equal(t, protocol.ControlMethodPaneTake, request.Method)
		return nil, &protocol.RPCError{
			Code: -32000, Message: "pane take refused",
			Data: &protocol.RPCErrorData{ErrorCode: protocol.ErrorCodePolicyDenied},
		}
	})

	output, err := runPaneCommand(t, append([]string{ctlPaneCommand, paneOpTake}, ctlPaneTargetArgs()...))
	var exit *deps.ExitError
	require.ErrorAs(t, err, &exit)
	require.Equal(t, exitcode.Refused, exit.Code)
	var line paneOutcome
	requireSingleJSONLine(t, output, &line)
	require.Equal(t, "take", line.Op)
	require.Equal(t, paneResultError, line.Result)
	require.Equal(t, string(protocol.ErrorCodePolicyDenied), line.Code)
	require.Equal(t, "pane take refused", line.Message)
}

func TestCtlPaneMissingFieldFailsBeforeRPC(t *testing.T) {
	without := func(drop string) []string {
		full := ctlPaneTargetArgs()
		var kept []string
		skipNext := false
		for _, arg := range full {
			if skipNext {
				skipNext = false
				continue
			}
			if arg == drop {
				skipNext = true
				continue
			}
			kept = append(kept, arg)
		}
		return kept
	}

	cases := map[string]struct {
		args []string
		code string
	}{
		"provision": {append([]string{ctlPaneCommand, paneOpProvision}, without("--socket")...), ctlPaneInvalidTarget},
		"take":      {append([]string{ctlPaneCommand, paneOpTake}, without("--owner-uid")...), ctlPaneInvalidTarget},
		"return":    {append([]string{ctlPaneCommand, paneOpReturn}, without("--pane-id")...), ctlPaneInvalidTarget},
		"repair":    {append([]string{ctlPaneCommand, paneOpRepair}, without("--boot-id")...), ctlPaneInvalidTarget},
		"reconcile": {append([]string{ctlPaneCommand, paneOpReconcile}, ctlPaneTargetArgs()...), "INVALID_ATTEMPT"},
		"attempt":   {append([]string{ctlPaneCommand, paneOpReconcile}, ctlPaneTargetArgs("--expected-attempt", "xyz")...), "INVALID_ATTEMPT"},
	}
	for name, test := range cases {
		t.Run(name, func(t *testing.T) {
			output, err := runPaneCommand(t, test.args)
			var exit *deps.ExitError
			require.ErrorAs(t, err, &exit)
			require.Equal(t, exitcode.Usage, exit.Code)
			var line paneOutcome
			requireSingleJSONLine(t, output, &line)
			require.Equal(t, paneResultError, line.Result)
			require.Equal(t, test.code, line.Code)
		})
	}
}

func TestCtlPaneReconcileDashAttempt(t *testing.T) {
	provisionPaneTree(t)
	writePaneConfig(t)
	requests := startPaneFakeServer(t, func(protocol.RPCRequest) (any, *protocol.RPCError) {
		return protocol.PaneOperationResult{Changed: true}, nil
	})

	args := append([]string{ctlPaneCommand, paneOpReconcile}, ctlPaneTargetArgs("--expected-attempt", "-")...)
	output, err := runPaneCommand(t, args)
	require.NoError(t, err)
	var line paneOutcome
	requireSingleJSONLine(t, output, &line)
	require.Equal(t, paneResultChanged, line.Result)

	var request protocol.RPCRequest
	select {
	case request = <-requests:
	default:
		t.Fatal("reconcile: no request reached the fake server")
	}
	var params protocol.PaneAttemptParams
	require.NoError(t, json.Unmarshal(request.Params, &params))
	require.Equal(t, protocol.PaneAttemptID("-"), params.ExpectedAttempt)
}

func TestCtlPaneStatusOverSyntheticTree(t *testing.T) {
	provisionPaneTree(t)
	writePaneConfig(t)

	output, err := runPaneCommand(t, append([]string{ctlPaneCommand, paneStatusUse}, ctlPaneTargetArgs()...))
	require.NoError(t, err)
	var receipt tmuxlock.PanelReceipt
	requireSingleJSONLine(t, output, &receipt)
	require.Equal(t, ctlPaneOwner, receipt.Owner)
	require.Equal(t, uint32(0), receipt.OwnerGen)
	require.Equal(t, uint32(0), receipt.Epoch)
	require.Nil(t, receipt.Pending)
	require.Equal(t, "IDLE", receipt.Phase)
	require.Equal(t, uint64(0), receipt.Gen)
	require.Equal(t, "-", receipt.TxID)
}

func TestCtlPaneStatusMissingTree(t *testing.T) {
	writePaneConfig(t)
	old := paneLockRoot
	paneLockRoot = t.TempDir()
	t.Cleanup(func() { paneLockRoot = old })

	output, err := runPaneCommand(t, append([]string{ctlPaneCommand, paneStatusUse}, ctlPaneTargetArgs()...))
	var exit *deps.ExitError
	require.ErrorAs(t, err, &exit)
	require.NotEqual(t, 0, exit.Code)
	var line paneOutcome
	requireSingleJSONLine(t, output, &line)
	require.Equal(t, "status", line.Op)
	require.Equal(t, paneResultError, line.Result)
	require.NotEmpty(t, line.Code)
}

func TestCtlPaneStatusMissingField(t *testing.T) {
	full := ctlPaneTargetArgs()
	args := append([]string{ctlPaneCommand, paneStatusUse}, full[:len(full)-2]...)
	output, err := runPaneCommand(t, args)
	var exit *deps.ExitError
	require.ErrorAs(t, err, &exit)
	require.Equal(t, exitcode.Usage, exit.Code)
	var line paneOutcome
	requireSingleJSONLine(t, output, &line)
	require.Equal(t, paneResultError, line.Result)
	require.Equal(t, ctlPaneInvalidTarget, line.Code)
}
