package hub

import (
	"bufio"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
)

const (
	registerTestJSONRPCKey = "jsonrpc"
	registerTestMethodKey  = "method"
	registerTestParamsKey  = "params"
	registerTestV3KeyID    = "ed25519:agent-keyed:v3"
)

func TestHelperRegisteredParent(_ *testing.T) {
	if os.Getenv("GIFFORD_REGISTERED_PARENT") != "1" {
		return
	}
	if err := runRegisteredParent(); err != nil {
		os.Exit(2)
	}
}

func runRegisteredParent() error {
	conn, err := (&net.Dialer{}).DialContext(context.Background(), "unix", os.Getenv("GIFFORD_HUB_SOCK"))
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	process, err := registry.CurrentProcess()
	if err != nil {
		return err
	}
	request, err := json.Marshal(map[string]any{
		registerTestJSONRPCKey: jsonRPCVersion, registerTestMethodKey: MethodRegisterAgent, "id": 1,
		registerTestParamsKey: protocol.RegisterAgent{
			Manifest: registrationManifest(protocol.AgentID(os.Getenv("GIFFORD_REGISTER_AGENT"))),
			Process:  &process,
		},
	})
	if err != nil {
		return err
	}
	if _, err := conn.Write(append(request, '\n')); err != nil {
		return err
	}
	reader := bufio.NewReader(conn)
	registered, err := reader.ReadString('\n')
	if err != nil {
		return err
	}
	if _, err := io.WriteString(os.Stdout, registered); err != nil {
		return err
	}
	grandchild := exec.CommandContext(context.Background(), os.Args[0], "-test.run=TestHelperRegisteredGrandchild")
	grandchild.Env = append(os.Environ(),
		"GIFFORD_REGISTERED_GRANDCHILD=1", "GIFFORD_HUB_SOCK="+os.Getenv("GIFFORD_HUB_SOCK"))
	grandchild.Stdout = os.Stdout
	grandchild.Stderr = os.Stderr
	if err := grandchild.Run(); err != nil {
		return err
	}
	_, _ = io.Copy(io.Discard, os.Stdin)
	return nil
}

func TestHelperRegisteredGrandchild(_ *testing.T) {
	if os.Getenv("GIFFORD_REGISTERED_GRANDCHILD") != "1" {
		return
	}
	if err := runRegisteredGrandchild(); err != nil {
		os.Exit(2)
	}
}

func runRegisteredGrandchild() error {
	conn, err := (&net.Dialer{}).DialContext(context.Background(), "unix", os.Getenv("GIFFORD_HUB_SOCK"))
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	if _, err := io.WriteString(conn, `{"jsonrpc":"2.0","method":"whoami","id":1}`+"\n"); err != nil {
		return err
	}
	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return err
	}
	_, err = io.WriteString(os.Stdout, line)
	return err
}

type registrationFixture struct {
	store    *eventstore.Store
	service  *Service
	registry *registry.Registry
	server   *Server
	sock     string
	clock    *hubClock
}

type delayedActiveEndpointRegistrar struct {
	EndpointRegistrar
	committed chan struct{}
	release   chan struct{}
}

func (r *delayedActiveEndpointRegistrar) RegisterEndpoint(
	ctx context.Context,
	agent protocol.AgentID,
	pid int,
	raw json.RawMessage,
) (RegisterEndpointResult, error) {
	result, err := r.EndpointRegistrar.RegisterEndpoint(ctx, agent, pid, raw)
	if err == nil && result.Active {
		close(r.committed)
		select {
		case <-r.release:
		case <-ctx.Done():
			return RegisterEndpointResult{}, ctx.Err()
		}
	}
	return result, err
}

func newRegistrationFixture(t *testing.T, listen bool) *registrationFixture {
	t.Helper()
	store, err := eventstore.Open(t.Context(), filepath.Join(t.TempDir(), "gifford.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	clk := &hubClock{now: time.Date(2026, time.October, 29, 19, 30, 0, 0, time.UTC)}
	engine, err := channel.New(channel.Options{Clock: clk})
	require.NoError(t, err)
	registryDir := t.TempDir()
	require.NoError(t, os.Chmod(registryDir, 0o700))
	keyRegistry, err := registry.New(testHubNode, registryDir, clk)
	require.NoError(t, err)
	service, err := NewService(ServiceOptions{
		DB: store.DB(), NodeID: testHubNode, Engine: engine, Registry: keyRegistry, Clock: clk,
	})
	require.NoError(t, err)
	server, err := New(t.TempDir(), store.DB(), service.Handlers())
	require.NoError(t, err)
	fixture := &registrationFixture{
		store: store, service: service, registry: keyRegistry, server: server, clock: clk,
	}
	if listen {
		fixture.sock, err = server.Listen()
		require.NoError(t, err)
		go func() { _ = server.Serve(context.Background()) }()
		t.Cleanup(func() { require.NoError(t, server.Close()) })
	}
	return fixture
}

func registrationManifest(agent protocol.AgentID) protocol.CapabilityManifest {
	return protocol.CapabilityManifest{
		SchemaVersion: protocol.SchemaVersion, AgentID: agent, Harness: protocol.HarnessCodex,
		HarnessVersion: "test", AdapterType: protocol.AdapterTypeGenericPTY,
		InputGuardProvider: protocol.InputGuardProviderPTYProxy,
		CaptureMode:        protocol.CaptureModePTYProxy, Deliverable: true,
		Capabilities: protocol.Capabilities{PTYCapture: true, MCP: true},
	}
}

func processIdentityForPID(t *testing.T, pid int) protocol.ProcessIdentity {
	t.Helper()
	raw, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	require.NoError(t, err)
	closeParen := strings.LastIndexByte(string(raw), ')')
	require.GreaterOrEqual(t, closeParen, 0)
	fields := strings.Fields(string(raw[closeParen+1:]))
	require.GreaterOrEqual(t, len(fields), 20)
	start, err := strconv.ParseInt(fields[19], 10, 64)
	require.NoError(t, err)
	return protocol.ProcessIdentity{
		PID: int64(pid), ProcessStartTime: start, UID: int64(os.Getuid()), GID: int64(os.Getgid()),
	}
}

func registerAgentLine(t *testing.T, id int, manifest protocol.CapabilityManifest, process protocol.ProcessIdentity) string {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		registerTestJSONRPCKey: jsonRPCVersion, registerTestMethodKey: MethodRegisterAgent, "id": id,
		registerTestParamsKey: protocol.RegisterAgent{Manifest: manifest, Process: &process},
	})
	require.NoError(t, err)
	return string(raw)
}

func registerEndpointLine(
	t *testing.T,
	id int,
	process protocol.ProcessIdentity,
	public ed25519.PublicKey,
	keyID string,
) string {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		registerTestJSONRPCKey: jsonRPCVersion, registerTestMethodKey: MethodRegisterEndpoint, "id": id,
		registerTestParamsKey: RegisterEndpointParams{Process: process, PublicKey: public, KeyID: keyID},
	})
	require.NoError(t, err)
	return string(raw)
}

func TestRegisterAgentBindsConnectionAndEndsItsSession(t *testing.T) {
	fixture := newRegistrationFixture(t, true)
	const agent = protocol.AgentID("agent-registered")
	first := dialChild(t, fixture.sock)
	firstProcess := processIdentityForPID(t, first.cmd.Process.Pid)
	wrongUID := firstProcess
	wrongUID.UID++
	first.send(t, registerAgentLine(t, 1, registrationManifest(agent), wrongUID))
	require.Equal(t, codeInvalidRequest, errorCode(t, first.recv(t)))
	wrongGID := firstProcess
	wrongGID.GID++
	first.send(t, registerAgentLine(t, 2, registrationManifest(agent), wrongGID))
	require.Equal(t, codeInvalidRequest, errorCode(t, first.recv(t)))
	first.send(t, registerAgentLine(t, 3, registrationManifest(agent), firstProcess))
	registered := first.recv(t)
	require.NotContains(t, registered, "error")
	result := registered["result"].(map[string]any)
	require.Equal(t, string(agent), result["agent_id"])
	sessionID := result["session_id"].(string)

	first.send(t, `{"jsonrpc":"2.0","method":"whoami","id":2}`)
	require.Equal(t, string(agent), first.recv(t)["result"].(map[string]any)["agent_id"])
	var state string
	var deliverable bool
	var ended *string
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT state, deliverable, ended_at
		FROM sessions WHERE session_id = ?`, sessionID).Scan(&state, &deliverable, &ended))
	require.Equal(t, "CONNECTED", state)
	require.True(t, deliverable)
	require.Nil(t, ended)

	second := dialChild(t, fixture.sock)
	second.send(t, `{"jsonrpc":"2.0","method":"whoami","id":1}`)
	require.Equal(t, codeNotReady, errorCode(t, second.recv(t)))
	second.send(t, registerAgentLine(t, 2, registrationManifest(agent), firstProcess))
	require.Equal(t, codeInvalidRequest, errorCode(t, second.recv(t)))
	secondProcess := processIdentityForPID(t, second.cmd.Process.Pid)
	second.send(t, registerAgentLine(t, 3, registrationManifest(agent), secondProcess))
	require.Equal(t, codeInvalidRequest, errorCode(t, second.recv(t)))

	first.close(t)
	require.Eventually(t, func() bool {
		var endedAt *string
		var gotState string
		err := fixture.store.DB().QueryRowContext(t.Context(),
			"SELECT state, ended_at FROM sessions WHERE session_id = ?", sessionID).Scan(&gotState, &endedAt)
		return err == nil && gotState == "DISCONNECTED" && endedAt != nil
	}, 5*time.Second, 10*time.Millisecond)

	second.send(t, registerAgentLine(t, 4, registrationManifest(agent), secondProcess))
	retry := second.recv(t)
	require.NotContains(t, retry, "error")
	second.send(t, `{"jsonrpc":"2.0","method":"whoami","id":5}`)
	require.Equal(t, string(agent), second.recv(t)["result"].(map[string]any)["agent_id"])
	second.close(t)
}

func TestRegisteredProcessDescendantBindsToSameAgent(t *testing.T) {
	fixture := newRegistrationFixture(t, true)
	const agent = protocol.AgentID("agent-parent")
	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=TestHelperRegisteredParent")
	cmd.Env = append(os.Environ(),
		"GIFFORD_REGISTERED_PARENT=1", "GIFFORD_HUB_SOCK="+fixture.sock,
		"GIFFORD_REGISTER_AGENT="+string(agent))
	stdin, err := cmd.StdinPipe()
	require.NoError(t, err)
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())
	reader := bufio.NewReader(stdout)
	registeredLine, err := reader.ReadString('\n')
	require.NoError(t, err)
	var registered map[string]any
	require.NoError(t, json.Unmarshal([]byte(registeredLine), &registered))
	require.NotContains(t, registered, "error")
	grandchildLine, err := reader.ReadString('\n')
	require.NoError(t, err)
	var grandchild map[string]any
	require.NoError(t, json.Unmarshal([]byte(grandchildLine), &grandchild))
	require.Equal(t, string(agent), grandchild["result"].(map[string]any)["agent_id"])
	require.NoError(t, stdin.Close())
	require.NoError(t, cmd.Wait())
}

func TestListenClosesStalePTYProxySessions(t *testing.T) {
	fixture := newRegistrationFixture(t, false)
	self, err := registry.CurrentProcess()
	require.NoError(t, err)
	claim, err := json.Marshal(self)
	require.NoError(t, err)
	testHubSession(t.Context(), t, fixture.store.DB(), "agent-stale", claim)
	fixture.sock, err = fixture.server.Listen()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, fixture.server.Close()) })
	var state string
	var endedAt *string
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(),
		"SELECT state, ended_at FROM sessions WHERE session_id = 'session-hub'").Scan(&state, &endedAt))
	require.Equal(t, "DISCONNECTED", state)
	require.NotNil(t, endedAt)
}

func TestFailedSecondListenLeavesFirstHubSessionsConnected(t *testing.T) {
	fixture := newRegistrationFixture(t, false)
	_, err := fixture.server.Listen()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, fixture.server.Close()) })
	self, err := registry.CurrentProcess()
	require.NoError(t, err)
	claim, err := json.Marshal(self)
	require.NoError(t, err)
	testHubSession(t.Context(), t, fixture.store.DB(), "agent-live", claim)

	second, err := New(fixture.server.dataDir, fixture.store.DB(), fixture.service.Handlers())
	require.NoError(t, err)
	_, err = second.Listen()
	require.Error(t, err)
	var state string
	var endedAt *string
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(),
		"SELECT state, ended_at FROM sessions WHERE session_id = 'session-hub'").Scan(&state, &endedAt))
	require.Equal(t, "CONNECTED", state)
	require.Nil(t, endedAt)
}

func TestRegisterEndpointIsIdempotentAndSuspendsSubstitution(t *testing.T) {
	fixture := newRegistrationFixture(t, false)
	const agent = protocol.AgentID("agent-keyed")
	self, err := registry.CurrentProcess()
	require.NoError(t, err)
	claim, err := json.Marshal(self)
	require.NoError(t, err)
	testHubSession(t.Context(), t, fixture.store.DB(), string(agent), claim)
	firstPublic, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	register := func(keyID string, public ed25519.PublicKey) (RegisterEndpointResult, error) {
		raw, marshalErr := json.Marshal(RegisterEndpointParams{
			Process: self, PublicKey: public, KeyID: keyID,
		})
		require.NoError(t, marshalErr)
		return fixture.service.RegisterEndpoint(t.Context(), agent, os.Getpid(), raw)
	}
	first, err := register("ed25519:agent-keyed:v1", firstPublic)
	require.NoError(t, err)
	require.True(t, first.Active)
	require.Equal(t, registry.OutcomeRegistered, first.Outcome)
	second, err := register("ed25519:agent-keyed:v1", firstPublic)
	require.NoError(t, err)
	require.Equal(t, first, second)
	active, err := fixture.registry.ActiveSigningKey(t.Context(), fixture.store.DB(), agent)
	require.NoError(t, err)
	require.Equal(t, []byte(firstPublic), []byte(active.PublicKey))
	var rows int
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(),
		"SELECT count(*) FROM agent_keys WHERE agent_id = ?", agent).Scan(&rows))
	require.Equal(t, 1, rows)

	otherPublic, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	substitution, err := register("ed25519:agent-keyed:v2", otherPublic)
	require.NoError(t, err)
	require.False(t, substitution.Active)
	require.Equal(t, registry.OutcomeSuspended, substitution.Outcome)
	_, err = fixture.registry.ActiveSigningKey(t.Context(), fixture.store.DB(), agent)
	require.ErrorIs(t, err, registry.ErrNoUsableKey)
	blocked, err := fixture.registry.Blocked(t.Context(), fixture.store.DB(), agent)
	require.NoError(t, err)
	require.True(t, blocked)
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(),
		"SELECT count(*) FROM agent_keys WHERE agent_id = ?", agent).Scan(&rows))
	require.Equal(t, 2, rows)

	wrong := self
	wrong.PID++
	raw, err := json.Marshal(RegisterEndpointParams{
		Process: wrong, PublicKey: firstPublic, KeyID: registerTestV3KeyID,
	})
	require.NoError(t, err)
	_, err = fixture.service.RegisterEndpoint(t.Context(), agent, os.Getpid(), raw)
	var callErr *callError
	require.ErrorAs(t, err, &callErr)
	require.Equal(t, codeInvalidRequest, callErr.code)

	wrong = self
	wrong.ProcessStartTime++
	raw, err = json.Marshal(RegisterEndpointParams{
		Process: wrong, PublicKey: firstPublic, KeyID: registerTestV3KeyID,
	})
	require.NoError(t, err)
	_, err = fixture.service.RegisterEndpoint(t.Context(), agent, os.Getpid(), raw)
	require.ErrorAs(t, err, &callErr)
	require.Equal(t, codeDomainError, callErr.code)

	raw, err = json.Marshal(map[string]any{
		"process": self, "public_key": firstPublic,
		"key_id": registerTestV3KeyID, "session_id": "session-client-claim",
	})
	require.NoError(t, err)
	_, err = fixture.service.RegisterEndpoint(t.Context(), agent, os.Getpid(), raw)
	require.ErrorAs(t, err, &callErr)
	require.Equal(t, codeInvalidRequest, callErr.code)
}

func TestRegisteredEndpointLivesWithItsConnectionAndReplacement(t *testing.T) {
	fixture := newRegistrationFixture(t, true)
	const (
		agent   = protocol.AgentID("agent-live-endpoint")
		session = protocol.OpaqueID("session-hub")
		keyID   = "ed25519:agent-live-endpoint:v1"
	)
	self, err := registry.CurrentProcess()
	require.NoError(t, err)
	claim, err := json.Marshal(self)
	require.NoError(t, err)
	testHubSession(t.Context(), t, fixture.store.DB(), string(agent), claim)
	public, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	first := dialChild(t, fixture.sock)
	firstProcess := processIdentityForPID(t, first.cmd.Process.Pid)
	first.send(t, registerEndpointLine(t, 1, firstProcess, public, keyID))
	firstResponse := first.recv(t)
	require.NotContains(t, firstResponse, "error")
	require.Equal(t, true, firstResponse["result"].(map[string]any)["active"])
	identity, ok := fixture.server.RegisteredEndpoint(agent, session)
	require.True(t, ok)
	require.Equal(t, EndpointProcess{
		PID: firstProcess.PID, StartTime: firstProcess.ProcessStartTime,
	}, identity.Process)
	require.Equal(t, []EndpointProcess{{
		PID: self.PID, StartTime: self.ProcessStartTime,
	}}, identity.Ancestors)

	second := dialChild(t, fixture.sock)
	secondProcess := processIdentityForPID(t, second.cmd.Process.Pid)
	second.send(t, registerEndpointLine(t, 2, secondProcess, public, keyID))
	secondResponse := second.recv(t)
	require.NotContains(t, secondResponse, "error")
	identity, ok = fixture.server.RegisteredEndpoint(agent, session)
	require.True(t, ok)
	require.Equal(t, EndpointProcess{
		PID: secondProcess.PID, StartTime: secondProcess.ProcessStartTime,
	}, identity.Process)

	first.close(t)
	identity, ok = fixture.server.RegisteredEndpoint(agent, session)
	require.True(t, ok, "the replaced connection must not remove its replacement")
	require.Equal(t, int64(second.cmd.Process.Pid), identity.Process.PID)

	substitute := dialChild(t, fixture.sock)
	substituteProcess := processIdentityForPID(t, substitute.cmd.Process.Pid)
	otherPublic, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	substitute.send(t, registerEndpointLine(
		t, 3, substituteProcess, otherPublic, "ed25519:agent-live-endpoint:v2",
	))
	substituteResponse := substitute.recv(t)
	require.NotContains(t, substituteResponse, "error")
	require.Equal(t, false, substituteResponse["result"].(map[string]any)["active"])
	_, ok = fixture.server.RegisteredEndpoint(agent, session)
	require.False(t, ok, "a suspended substitution must revoke the prior exception")

	second.close(t)
	substitute.close(t)
	require.Eventually(t, func() bool {
		_, live := fixture.server.RegisteredEndpoint(agent, session)
		return !live
	}, 5*time.Second, 10*time.Millisecond)
}

func TestEndpointSuspensionCannotBeUndoneByDelayedActiveResponse(t *testing.T) {
	fixture := newRegistrationFixture(t, false)
	delayed := &delayedActiveEndpointRegistrar{
		EndpointRegistrar: fixture.service,
		committed:         make(chan struct{}),
		release:           make(chan struct{}),
	}
	fixture.server.handlers.EndpointRegistration = delayed
	var err error
	fixture.sock, err = fixture.server.Listen()
	require.NoError(t, err)
	go func() { _ = fixture.server.Serve(t.Context()) }()
	t.Cleanup(func() { require.NoError(t, fixture.server.Close()) })
	t.Cleanup(func() {
		select {
		case <-delayed.release:
		default:
			close(delayed.release)
		}
	})

	const (
		agent   = protocol.AgentID("agent-delayed-endpoint")
		session = protocol.OpaqueID("session-hub")
	)
	self, err := registry.CurrentProcess()
	require.NoError(t, err)
	claim, err := json.Marshal(self)
	require.NoError(t, err)
	testHubSession(t.Context(), t, fixture.store.DB(), string(agent), claim)
	public, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	first := dialChild(t, fixture.sock)
	first.send(t, registerEndpointLine(
		t, 1, processIdentityForPID(t, first.cmd.Process.Pid), public,
		"ed25519:agent-delayed-endpoint:v1",
	))
	select {
	case <-delayed.committed:
	case <-time.After(5 * time.Second):
		t.Fatal("active registration did not commit")
	}

	otherPublic, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	substitute := dialChild(t, fixture.sock)
	substitute.send(t, registerEndpointLine(
		t, 2, processIdentityForPID(t, substitute.cmd.Process.Pid), otherPublic,
		"ed25519:agent-delayed-endpoint:v2",
	))
	// Registration and publication are one ordered operation. Release the
	// first response before waiting for the substitution, so either request
	// may already be queued without deadlocking the serialized path.
	close(delayed.release)
	firstResponse := first.recv(t)
	require.NotContains(t, firstResponse, "error")
	require.Equal(t, true, firstResponse["result"].(map[string]any)["active"])
	substituteResponse := substitute.recv(t)
	require.NotContains(t, substituteResponse, "error")
	require.Equal(t, false, substituteResponse["result"].(map[string]any)["active"])
	_, present := fixture.server.RegisteredEndpoint(agent, session)
	require.False(t, present, "durable suspension must leave no process exception published")

	first.close(t)
	substitute.close(t)
}
