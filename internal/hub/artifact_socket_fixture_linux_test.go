//go:build linux

package hub_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/channel"
	artifactcli "github.com/smellak/gifford/internal/cli/artifact"
	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/endpoint"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/hub"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
	"github.com/smellak/gifford/internal/xdg"
)

const (
	artifactChildEnv = "GIFFORD_TEST_ARTIFACT_SOCKET_CHILD"
	artifactAgentA   = protocol.AgentID("agent-artifact-a")
	artifactAgentB   = protocol.AgentID("agent-artifact-b")
	artifactWhoAmI   = "whoami"
	artifactMCPStart = "mcp-start"
	artifactMCPList  = "mcp-list"
)

type artifactSocketFixture struct {
	store  *eventstore.Store
	server *hub.Server
	socket string
}

// The configured node and bridge uids differ from the actual test uid, as in
// TestEndpointRedialsNodeSocketUntilItsSessionRegisters. SO_PEERCRED and pidfd
// are real; no setuid or fake identity resolver is involved.
func newArtifactSocketFixture(t *testing.T) *artifactSocketFixture {
	t.Helper()
	if os.Getuid() == 0 {
		t.Skip("node.sock refuses root; run as an ordinary user")
	}
	store, err := eventstore.Open(t.Context(), filepath.Join(t.TempDir(), "gifford.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	clk := clock.System{}
	engine, err := channel.New(channel.Options{Clock: clk})
	require.NoError(t, err)
	controller, err := channel.NewController(channel.ControllerOptions{
		DB: store.DB(), Engine: engine, Clock: clk, NodeID: lateNode,
	})
	require.NoError(t, err)
	require.NoError(t, controller.Reload(t.Context()))
	registryDir := t.TempDir()
	require.NoError(t, os.Chmod(registryDir, 0o700))
	keys, err := registry.New(lateNode, registryDir, clk)
	require.NoError(t, err)
	service, err := hub.NewService(hub.ServiceOptions{
		DB: store.DB(), NodeID: lateNode, Engine: engine, Approvals: controller,
		Registry: keys, Clock: clk, DataDir: t.TempDir(),
	})
	require.NoError(t, err)
	server, err := hub.New(t.TempDir(), store.DB(), service.Handlers())
	require.NoError(t, err)
	me := int64(os.Getuid())
	nodeDir := t.TempDir()
	require.NoError(t, os.Chmod(nodeDir, 0o700))
	node, err := hub.NewNodeSocketForNode(server, filepath.Join(nodeDir, "node.sock"), me+1, me+2)
	require.NoError(t, err)
	socket, err := node.Listen()
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- node.Serve(context.Background()) }()
	t.Cleanup(func() {
		require.NoError(t, node.Close())
		require.NoError(t, <-done)
	})
	return &artifactSocketFixture{store: store, server: server, socket: socket}
}

func (f *artifactSocketFixture) agent(t *testing.T, agent protocol.AgentID) *artifactSocketChild {
	t.Helper()
	child := startArtifactSocketChild(t)
	insertNodeSession(t, f.store, agent, child.process, eventstore.FormatTime((clock.System{}).Now()))
	return child
}

type artifactChildRequest struct {
	Action string   `json:"action"`
	Socket string   `json:"socket,omitempty"`
	Args   []string `json:"args,omitempty"`
}

type artifactChildResponse struct {
	Output string `json:"output"`
	Error  string `json:"error,omitempty"`
}

// A and B are separate OS processes with private XDG paths. Each executes
// Cobra in-process while retaining its original MCP endpoint, when started.
type artifactSocketChild struct {
	process protocol.ProcessIdentity
	dataDir string
	input   *json.Encoder
	output  *json.Decoder
	cmd     *exec.Cmd
}

func startArtifactSocketChild(t *testing.T) *artifactSocketChild {
	t.Helper()
	base := t.TempDir()
	dataDir := filepath.Join(base, "data", "gifford")
	require.NoError(t, os.MkdirAll(dataDir, 0o700))
	executable, err := os.Executable()
	require.NoError(t, err)
	// t.Context is canceled before Cleanup. Keep the child alive until stdin
	// closes there, with an independent deadline to bound a broken helper.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 45*time.Second)
	t.Cleanup(cancel)
	cmd := exec.CommandContext(ctx, executable, "-test.run=^TestArtifactSocketChild$", "-test.timeout=45s")
	cmd.Env = append(os.Environ(), artifactChildEnv+"=1",
		"XDG_CONFIG_HOME="+filepath.Join(base, "config"),
		"XDG_DATA_HOME="+filepath.Join(base, "data"),
		"XDG_RUNTIME_DIR="+filepath.Join(base, "run"))
	cmd.WaitDelay = time.Second
	stdin, err := cmd.StdinPipe()
	require.NoError(t, err)
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	require.NoError(t, cmd.Start())
	t.Cleanup(func() {
		_ = stdin.Close()
		require.NoError(t, cmd.Wait(), "artifact child stderr: %s", stderr.String())
	})
	child := &artifactSocketChild{
		dataDir: dataDir, input: json.NewEncoder(stdin), output: json.NewDecoder(stdout), cmd: cmd,
	}
	child.decode(t, &child.process)
	require.NoError(t, child.process.Validate())
	require.EqualValues(t, cmd.Process.Pid, child.process.PID)
	return child
}

func (c *artifactSocketChild) decode(t *testing.T, value any) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- c.output.Decode(value) }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		_ = c.cmd.Process.Kill()
		t.Fatal("artifact child did not answer within 10 seconds")
	}
}

func (c *artifactSocketChild) request(t *testing.T, request artifactChildRequest) artifactChildResponse {
	t.Helper()
	require.NoError(t, c.input.Encode(request))
	var response artifactChildResponse
	c.decode(t, &response)
	return response
}

func (c *artifactSocketChild) artifact(t *testing.T, socket string, args ...string) artifactChildResponse {
	t.Helper()
	return c.request(t, artifactChildRequest{Action: "artifact", Args: append([]string{"--socket", socket}, args...)})
}

func artifactJSON[T any](t *testing.T, response artifactChildResponse) T {
	t.Helper()
	require.Empty(t, response.Error, "command output: %s", response.Output)
	var value T
	require.NoError(t, json.Unmarshal([]byte(response.Output), &value))
	return value
}

func requireArtifactNoKeys(t *testing.T, child *artifactSocketChild, f *artifactSocketFixture) {
	t.Helper()
	entries, err := os.ReadDir(child.dataDir)
	require.NoError(t, err)
	require.Empty(t, entries, "no local key or outgoing state without verified identity")
	var keys int
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(), "SELECT COUNT(*) FROM agent_keys").Scan(&keys))
	require.Zero(t, keys)
}

type artifactMCPClient struct {
	input  *io.PipeWriter
	output *bufio.Reader
	nextID int
}

func startArtifactMCP(t *testing.T, socket, dataDir string) *artifactMCPClient {
	t.Helper()
	link, err := endpoint.DialHub(t.Context(), socket)
	require.NoError(t, err)
	t.Cleanup(func() { _ = link.Close() })
	stdin, input := io.Pipe()
	output, stdout := io.Pipe()
	server, err := endpoint.New(t.Context(), dataDir, stdin, stdout, link)
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() {
		done <- server.Run(context.Background())
		_ = stdout.Close()
	}()
	t.Cleanup(func() {
		_ = input.Close()
		require.NoError(t, <-done)
		_ = output.Close()
	})
	return &artifactMCPClient{input: input, output: bufio.NewReader(output)}
}

func (c *artifactMCPClient) list() (string, error) {
	c.nextID++
	_, err := fmt.Fprintf(c.input,
		`{"jsonrpc":"2.0","id":%d,"method":"tools/call","params":{"name":"list_pending_messages","arguments":{}}}`+"\n", c.nextID)
	if err != nil {
		return "", err
	}
	line, err := c.output.ReadBytes('\n')
	return string(line), err
}

// TestArtifactSocketChild is only a subprocess entry point, not a passing
// contract test. Parents decide when its real process gets a session.
func TestArtifactSocketChild(t *testing.T) {
	if os.Getenv(artifactChildEnv) != "1" {
		t.Skip("subprocess entry point")
	}
	process, err := registry.CurrentProcess()
	require.NoError(t, err)
	encoder := json.NewEncoder(os.Stdout)
	require.NoError(t, encoder.Encode(process))
	decoder := json.NewDecoder(os.Stdin)
	var mcp *artifactMCPClient
	for {
		var request artifactChildRequest
		err := decoder.Decode(&request)
		if errors.Is(err, io.EOF) {
			return
		}
		require.NoError(t, err)
		require.NoError(t, encoder.Encode(runArtifactChildRequest(t, request, &mcp)))
	}
}

func runArtifactChildRequest(t *testing.T, request artifactChildRequest, mcp **artifactMCPClient) artifactChildResponse {
	t.Helper()
	var output string
	var err error
	switch request.Action {
	case "artifact":
		var stdout bytes.Buffer
		cmd := artifactcli.NewCmd(&deps.Deps{})
		cmd.SetOut(&stdout)
		cmd.SetErr(io.Discard)
		cmd.SilenceErrors, cmd.SilenceUsage = true, true
		cmd.SetArgs(request.Args)
		err = cmd.ExecuteContext(t.Context())
		output = stdout.String()
	case artifactWhoAmI:
		var link *endpoint.SocketHub
		link, err = endpoint.DialHub(t.Context(), request.Socket)
		if err == nil {
			defer func() { _ = link.Close() }()
			var agent protocol.AgentID
			agent, err = link.WhoAmI(t.Context())
			output = string(agent)
		}
	case artifactMCPStart:
		require.Nil(t, *mcp, "never replace the endpoint under test")
		dirs, resolveErr := xdg.Resolve()
		require.NoError(t, resolveErr)
		*mcp = startArtifactMCP(t, request.Socket, dirs.Data)
		output, err = (*mcp).list()
	case artifactMCPList:
		require.NotNil(t, *mcp)
		output, err = (*mcp).list()
	default:
		err = fmt.Errorf("unknown child operation %q", request.Action)
	}
	response := artifactChildResponse{Output: output}
	if err != nil {
		response.Error = err.Error()
	}
	return response
}
