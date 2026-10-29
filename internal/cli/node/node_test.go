package node

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/cli/exitcode"
	"github.com/smellak/gifford/internal/protocol"
)

func TestNodeKeysNotImplemented(t *testing.T) {
	t.Parallel()

	cmd := NewCmd(&deps.Deps{})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"keys"})
	var exit *deps.ExitError
	require.ErrorAs(t, cmd.Execute(), &exit)
	require.Equal(t, exitcode.NotImplemented, exit.Code)
}

func TestNodeCommandsRequireRunningHub(t *testing.T) {
	base := t.TempDir()
	t.Setenv("XDG_RUNTIME_DIR", base)
	for _, arguments := range [][]string{
		{"peers"},
		{"join", "https://node-a.example.invalid", "--fingerprint", strings.Repeat("ab", 32)},
	} {
		cmd := NewCmd(&deps.Deps{})
		cmd.SetOut(io.Discard)
		cmd.SetErr(io.Discard)
		cmd.SetArgs(arguments)
		var exit *deps.ExitError
		require.ErrorAs(t, cmd.Execute(), &exit)
		require.Equal(t, exitcode.Error, exit.Code)
		require.ErrorIs(t, exit, errHubNotRunning)
		require.Equal(t, "gifford node: hub is not running", exit.Error())
	}
}

func TestNodeCommandsUseControlAPIAndPrintJSON(t *testing.T) {
	base := t.TempDir()
	t.Setenv("XDG_RUNTIME_DIR", base)
	runtimeDir := filepath.Join(base, "gifford")
	require.NoError(t, os.Mkdir(runtimeDir, 0o700))
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "unix", filepath.Join(runtimeDir, "daemon.sock"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })

	fullFingerprint := strings.Repeat("ab", 32)
	methods := make(chan protocol.ControlMethod, 4)
	serverErrors := make(chan error, 1)
	go func() {
		for range 4 {
			connection, err := listener.Accept()
			if err != nil {
				serverErrors <- err
				return
			}
			line, err := bufio.NewReader(connection).ReadBytes('\n')
			if err != nil {
				_ = connection.Close()
				serverErrors <- err
				return
			}
			var request protocol.RPCRequest
			if err := json.Unmarshal(line, &request); err != nil {
				_ = connection.Close()
				serverErrors <- err
				return
			}
			methods <- request.Method
			var value any
			switch request.Method {
			case protocol.ControlMethodNodeEnrollToken:
				value = protocol.NodeEnrollTokenResult{
					Token: "one-time-secret", NodeID: "node-a",
					Endpoint:    "https://node-a.example.invalid:7443",
					Fingerprint: protocol.SHA256Hex(fullFingerprint), ExpiresAt: "2026-10-29T21:15:00Z",
				}
			case protocol.ControlMethodNodeJoin:
				value = protocol.NodeJoinResult{Peer: testProtocolPeer(fullFingerprint)}
			case protocol.ControlMethodNodePeers:
				value = protocol.NodePeersResult{Peers: []protocol.NodePeer{testProtocolPeer(fullFingerprint)}}
			case protocol.ControlMethodNodeRevoke:
				peer := testProtocolPeer(fullFingerprint)
				peer.Status = "REVOKED"
				value = protocol.NodeRevokeResult{Peer: peer}
			default:
				_ = connection.Close()
				serverErrors <- fmt.Errorf("unexpected control method %s", request.Method)
				return
			}
			raw, err := json.Marshal(value)
			if err != nil {
				_ = connection.Close()
				serverErrors <- err
				return
			}
			response := protocol.RPCResponse{
				JSONRPC: protocol.JSONRPCVersion, ID: request.ID, Result: raw,
			}
			if err := json.NewEncoder(connection).Encode(response); err != nil {
				_ = connection.Close()
				serverErrors <- err
				return
			}
			_ = connection.Close()
		}
		serverErrors <- nil
	}()

	commands := []struct {
		arguments []string
		input     string
	}{
		{arguments: []string{"enroll-token", "--org", "org-b"}},
		{
			arguments: []string{"join", "https://node-b.example.invalid:7443", "--fingerprint", fullFingerprint},
			input:     "join-secret\n",
		},
		{arguments: []string{"peers"}},
		{arguments: []string{"revoke", "node-b"}},
	}
	outputs := make([]string, 0, len(commands))
	notices := make([]string, 0, len(commands))
	for _, command := range commands {
		var output, notice bytes.Buffer
		cmd := NewCmd(&deps.Deps{})
		cmd.SetOut(&output)
		cmd.SetErr(&notice)
		cmd.SetIn(strings.NewReader(command.input))
		cmd.SetArgs(command.arguments)
		require.NoError(t, cmd.ExecuteContext(t.Context()))
		outputs = append(outputs, output.String())
		notices = append(notices, notice.String())
	}
	require.NoError(t, <-serverErrors)
	close(methods)
	var gotMethods []protocol.ControlMethod
	for method := range methods {
		gotMethods = append(gotMethods, method)
	}
	require.Equal(t, []protocol.ControlMethod{
		protocol.ControlMethodNodeEnrollToken, protocol.ControlMethodNodeJoin,
		protocol.ControlMethodNodePeers, protocol.ControlMethodNodeRevoke,
	}, gotMethods)
	require.Equal(t, 1, strings.Count(outputs[0], "one-time-secret"))
	require.NotContains(t, outputs[1], "join-secret")
	require.Equal(t, joinRestartNotice+"\n", notices[1])
	require.Contains(t, outputs[2], `"fingerprint": "abababababab"`)
	require.NotContains(t, outputs[2], fullFingerprint)
	require.Contains(t, outputs[3], `"status": "REVOKED"`)
}

func TestJoinTokenFileMustBePrivate(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(path, []byte("one-time-secret\n"), 0o644))
	cmd := NewCmd(&deps.Deps{})
	_, err := readJoinToken(cmd, path, -1)
	require.ErrorContains(t, err, "mode 0600")
	require.NoError(t, os.Chmod(path, 0o600))
	token, err := readJoinToken(cmd, path, -1)
	require.NoError(t, err)
	require.Equal(t, "one-time-secret", token)
}

func TestJoinDoesNotAcceptTokenInArgv(t *testing.T) {
	t.Parallel()

	cmd := newJoinCmd()
	require.Nil(t, cmd.Flags().Lookup("token"))
	require.NotNil(t, cmd.Flags().Lookup("token-file"))
	require.NotNil(t, cmd.Flags().Lookup("token-fd"))
}

func testProtocolPeer(fingerprint string) protocol.NodePeer {
	return protocol.NodePeer{
		NodeID: "node-b", OrganizationID: "org-b", Status: "TRUSTED",
		Endpoint:      "https://node-b.example.invalid:7443",
		Fingerprint:   protocol.SHA256Hex(fingerprint),
		PolicyVersion: "policy-v1:000000000000",
	}
}

func TestNodeBareGroupShowsHelp(t *testing.T) {
	t.Parallel()

	cmd := NewCmd(&deps.Deps{})
	cmd.SetOut(io.Discard)
	cmd.SetArgs([]string{})
	require.NoError(t, cmd.Execute())
}
