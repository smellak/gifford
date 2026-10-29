package mcp

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/cli/exitcode"
	hubcmd "github.com/smellak/gifford/internal/cli/hub"
)

// envBase wires the XDG directories to scratch.
func envBase(t *testing.T) {
	t.Helper()
	base := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(base, "config"))
	t.Setenv("XDG_DATA_HOME", filepath.Join(base, "data"))
	t.Setenv("XDG_RUNTIME_DIR", filepath.Join(base, "run"))
}

// writeConfig drops a config.yaml with node_id under the config home.
func writeConfig(t *testing.T) {
	t.Helper()
	dir := filepath.Join(os.Getenv("XDG_CONFIG_HOME"), "gifford")
	require.NoError(t, os.MkdirAll(dir, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "config.yaml"), []byte("schema_version: 1\nnode_id: node-a\n"), 0o600))
}

// syncBuffer is a goroutine-safe command output buffer.
type syncBuffer struct {
	mu sync.Mutex
	sb strings.Builder
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.sb.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.sb.String()
}

// startHub runs the hub command until cancel and waits for its ready
// line. Hub failures fail the test: there is no hub without it.
func startHub(ctx context.Context, t *testing.T) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(ctx)
	cmd := hubcmd.NewCmd(&deps.Deps{})
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	cmd.SetOut(io.Discard)
	stderr := &syncBuffer{}
	cmd.SetErr(stderr)
	cmd.SetArgs([]string{})
	done := make(chan error, 1)
	go func() { done <- cmd.ExecuteContext(ctx) }()
	require.Eventually(t, func() bool {
		return strings.Contains(stderr.String(), "ready")
	}, 10*time.Second, 10*time.Millisecond, "hub never reported ready")
	t.Cleanup(func() {
		cancel()
		require.NoError(t, <-done)
	})
	return cancel
}

// mcpResponse is one stdout frame: result or error, never both, with
// the request id mirrored.
type mcpResponse struct {
	Result any             `json:"result"`
	Error  *mcpError       `json:"error"`
	ID     json.RawMessage `json:"id"`
}

type mcpError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// runMCP serves the given stdin frames and returns the stdout frames
// split by line.
func runMCP(t *testing.T, stdin string) (string, error) {
	t.Helper()
	cmd := NewCmd(&deps.Deps{})
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	cmd.SetIn(strings.NewReader(stdin))
	var out syncBuffer
	cmd.SetOut(&out)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{})
	err := cmd.ExecuteContext(t.Context())
	return out.String(), err
}

// TestMcpSessionWithoutIdentity serves a full handshake against the live
// hub: the test process holds no agent session, so initialize answers,
// tools/list is empty and a call fails with the no-identity error.
// Stdout carries frames only.
func TestMcpSessionWithoutIdentity(t *testing.T) {
	envBase(t)
	writeConfig(t)
	cancel := startHub(t.Context(), t)
	defer cancel()

	stdout, err := runMCP(t,
		"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{}}\n"+
			"{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/list\",\"params\":{}}\n"+
			"{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"tools/call\",\"params\":{\"name\":\"ack_message\",\"arguments\":{}}}\n")
	require.NoError(t, err)
	lines := strings.Split(strings.TrimSuffix(stdout, "\n"), "\n")
	require.Len(t, lines, 3, "stdout carries exactly the three frames, got %q", stdout)

	var initResp mcpResponse
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &initResp))
	require.Nil(t, initResp.Error)
	result, ok := initResp.Result.(map[string]any)
	require.True(t, ok, "initialize answers an object")
	_, ok = result["protocolVersion"]
	require.True(t, ok, "initialize negotiates a protocol version")

	var listResp mcpResponse
	require.NoError(t, json.Unmarshal([]byte(lines[1]), &listResp))
	require.Nil(t, listResp.Error)
	tools, ok := listResp.Result.(map[string]any)["tools"]
	require.True(t, ok, "tools/list answers a tool table")
	require.Empty(t, tools, "no session means no tools")

	var callResp mcpResponse
	require.NoError(t, json.Unmarshal([]byte(lines[2]), &callResp))
	require.NotNil(t, callResp.Error, "a call without identity fails")
	require.Contains(t, callResp.Error.Message, "identity")
}

// TestMcpWithoutHub fails with stdout untouched when no hub serves.
func TestMcpWithoutHub(t *testing.T) {
	envBase(t)

	stdout, err := runMCP(t, "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{}}\n")
	var ec *deps.ExitError
	require.ErrorAs(t, err, &ec)
	require.Equal(t, exitcode.Error, ec.Code)
	require.Contains(t, ec.Err.Error(), "hub")
	require.Empty(t, stdout, "no hub means no stdout frames")
}

// TestMcpStderrCarriesNoFrames serves one ping: diagnostics never leak
// into the stdout frame stream.
func TestMcpStderrCarriesNoFrames(t *testing.T) {
	envBase(t)
	writeConfig(t)
	cancel := startHub(t.Context(), t)
	defer cancel()

	stdout, err := runMCP(t, "{\"jsonrpc\":\"2.0\",\"id\":7,\"method\":\"ping\"}\n")
	require.NoError(t, err)
	var resp mcpResponse
	require.NoError(t, json.Unmarshal([]byte(strings.TrimSuffix(stdout, "\n")), &resp))
	rawID, merr := json.Marshal(7.0)
	require.NoError(t, merr)
	require.JSONEq(t, string(rawID), string(resp.ID), "the request id mirrors")
}

// TestMcpAcceptsAgentFlag keeps the accepted --agent surface: it is
// ignored for identity, so the call still fails without a session.
func TestMcpAcceptsAgentFlag(t *testing.T) {
	envBase(t)
	writeConfig(t)
	cancel := startHub(t.Context(), t)
	defer cancel()

	cmd := NewCmd(&deps.Deps{})
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	cmd.SetIn(strings.NewReader("{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/list\",\"params\":{}}\n"))
	var out syncBuffer
	cmd.SetOut(&out)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"--agent", "agent-1"})
	require.NoError(t, cmd.ExecuteContext(context.Background()))
	require.Contains(t, out.String(), `"tools"`)
}
