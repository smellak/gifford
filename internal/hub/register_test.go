package hub

import (
	"bufio"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/adapters"
	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/daemon/proxies"
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

type fakeSessionRuntime struct {
	mu       sync.RWMutex
	boot     string
	bootErr  error
	aliveErr error
	live     map[protocol.ProcessIdentity]bool
}

func (r *fakeSessionRuntime) bootID() (string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.boot, r.bootErr
}

func (r *fakeSessionRuntime) alive(process protocol.ProcessIdentity, bootID string) error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.bootErr != nil {
		return r.bootErr
	}
	if r.aliveErr != nil {
		return r.aliveErr
	}
	if bootID != r.boot || !r.live[process] {
		return errors.New("process identity is not live")
	}
	return nil
}

func (r *fakeSessionRuntime) setBoot(bootID string) {
	r.mu.Lock()
	r.boot = bootID
	r.mu.Unlock()
}

func (r *fakeSessionRuntime) setLive(process protocol.ProcessIdentity, live bool) {
	r.mu.Lock()
	r.live[process] = live
	r.mu.Unlock()
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
	controller, err := channel.NewController(channel.ControllerOptions{
		DB: store.DB(), Engine: engine, Clock: clk, NodeID: testHubNode,
	})
	require.NoError(t, err)
	require.NoError(t, controller.Reload(t.Context()))
	registryDir := t.TempDir()
	require.NoError(t, os.Chmod(registryDir, 0o700))
	keyRegistry, err := registry.New(testHubNode, registryDir, clk)
	require.NoError(t, err)
	service, err := NewService(ServiceOptions{
		DB: store.DB(), NodeID: testHubNode, Engine: engine, Approvals: controller,
		Registry: keyRegistry, Clock: clk,
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

func dialRegistrationConn(t *testing.T, sock string) net.Conn {
	t.Helper()
	conn, err := (&net.Dialer{}).DialContext(t.Context(), "unix", sock)
	require.NoError(t, err)
	return conn
}

func registrationExchange(t *testing.T, conn net.Conn, line string) map[string]any {
	t.Helper()
	require.NoError(t, conn.SetDeadline(time.Now().Add(10*time.Second)))
	_, err := io.WriteString(conn, line+"\n")
	require.NoError(t, err)
	raw, err := bufio.NewReader(conn).ReadString('\n')
	require.NoError(t, err)
	var response map[string]any
	require.NoError(t, json.Unmarshal([]byte(raw), &response))
	return response
}

func registeredSessionID(t *testing.T, response map[string]any) protocol.OpaqueID {
	t.Helper()
	require.NotContains(t, response, "error")
	result, ok := response["result"].(map[string]any)
	require.True(t, ok)
	raw, ok := result["session_id"].(string)
	require.True(t, ok)
	return protocol.OpaqueID(raw)
}

func waitServerConnections(t *testing.T, server *Server, want int) {
	t.Helper()
	require.Eventually(t, func() bool {
		server.mu.Lock()
		defer server.mu.Unlock()
		return len(server.conns) == want
	}, 5*time.Second, 10*time.Millisecond)
}

func registerDirect(
	t *testing.T,
	service *Service,
	manifest protocol.CapabilityManifest,
	process protocol.ProcessIdentity,
) (protocol.RegisterAgentResult, error) {
	t.Helper()
	raw, err := json.Marshal(protocol.RegisterAgent{Manifest: manifest, Process: &process})
	require.NoError(t, err)
	return service.RegisterAgent(t.Context(), peerIdentity{
		PID: int(process.PID), UID: process.UID, GID: process.GID,
	}, raw)
}

func insertLifecycleSession(
	t *testing.T,
	fixture *registrationFixture,
	session protocol.OpaqueID,
	agent protocol.AgentID,
	state protocol.SessionState,
	process protocol.ProcessIdentity,
	bootID any,
) {
	t.Helper()
	manifest, err := json.Marshal(registrationManifest(agent))
	require.NoError(t, err)
	processJSON, err := json.Marshal(process)
	require.NoError(t, err)
	stamp := eventstore.FormatTime(fixture.clock.Now())
	_, err = fixture.store.DB().ExecContext(t.Context(), `INSERT INTO agents
		(agent_id, node_id, harness, created_at, updated_at) VALUES (?, ?, ?, ?, ?)`,
		agent, testHubNode, protocol.HarnessCodex, stamp, stamp)
	require.NoError(t, err)
	_, err = fixture.store.DB().ExecContext(t.Context(), `INSERT INTO sessions
		(session_id, agent_id, state, capture_mode, deliverable, manifest_json,
		 process_json, started_at, ended_at, updated_at, boot_id)
		VALUES (?, ?, ?, ?, 1, ?, ?, ?, NULL, ?, ?)`,
		session, agent, state, protocol.CaptureModePTYProxy,
		string(manifest), string(processJSON), stamp, stamp, bootID)
	require.NoError(t, err)
}

func requireSessionLifecycle(
	t *testing.T,
	fixture *registrationFixture,
	session protocol.OpaqueID,
	wantState protocol.SessionState,
	wantEnded bool,
) {
	t.Helper()
	var state protocol.SessionState
	var ended *string
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(),
		"SELECT state, ended_at FROM sessions WHERE session_id = ?", session).
		Scan(&state, &ended))
	require.Equal(t, wantState, state)
	require.Equal(t, wantEnded, ended != nil)
}

func TestRegisterAgentResumesExactSessionAndReplacesTransportOwner(t *testing.T) {
	fixture := newRegistrationFixture(t, true)
	const agent = protocol.AgentID("agent-resume")
	self, err := registry.CurrentProcess()
	require.NoError(t, err)
	request := registerAgentLine(t, 1, registrationManifest(agent), self)

	first := dialRegistrationConn(t, fixture.sock)
	session := registeredSessionID(t, registrationExchange(t, first, request))
	var bootID string
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(),
		"SELECT boot_id FROM sessions WHERE session_id = ?", session).Scan(&bootID))
	require.NoError(t, validateBootID(bootID))
	waitServerConnections(t, fixture.server, 1)
	require.NoError(t, first.Close())
	waitServerConnections(t, fixture.server, 0)
	require.Eventually(t, func() bool {
		var state protocol.SessionState
		var ended *string
		err := fixture.store.DB().QueryRowContext(t.Context(),
			"SELECT state, ended_at FROM sessions WHERE session_id = ?", session).
			Scan(&state, &ended)
		return err == nil && state == protocol.SessionStateDetached && ended == nil
	}, 5*time.Second, 10*time.Millisecond)

	second := dialRegistrationConn(t, fixture.sock)
	require.Equal(t, session,
		registeredSessionID(t, registrationExchange(t, second, request)))
	requireSessionLifecycle(t, fixture, session, protocol.SessionStateConnected, false)

	third := dialRegistrationConn(t, fixture.sock)
	require.Equal(t, session,
		registeredSessionID(t, registrationExchange(t, third, request)))
	waitServerConnections(t, fixture.server, 2)
	require.NoError(t, second.Close())
	waitServerConnections(t, fixture.server, 1)
	requireSessionLifecycle(t, fixture, session, protocol.SessionStateConnected, false)

	require.NoError(t, third.Close())
	waitServerConnections(t, fixture.server, 0)
	require.Eventually(t, func() bool {
		var state protocol.SessionState
		var ended *string
		err := fixture.store.DB().QueryRowContext(t.Context(),
			"SELECT state, ended_at FROM sessions WHERE session_id = ?", session).
			Scan(&state, &ended)
		return err == nil && state == protocol.SessionStateDetached && ended == nil
	}, 5*time.Second, 10*time.Millisecond)
	var count int
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(),
		"SELECT count(*) FROM sessions WHERE agent_id = ?", agent).Scan(&count))
	require.Equal(t, 1, count)
}

func TestRegisterAgentResumesAuditedCapabilityPredecessorWithPoolOne(t *testing.T) {
	fixture := newRegistrationFixture(t, false)
	consumer, err := proxies.NewCapabilityStore(fixture.store, testHubNode)
	require.NoError(t, err)
	fixture.service.capabilityLineage = consumer
	native := registrationManifest("agent-capability-resume")
	native.AdapterType = protocol.AdapterTypeNativeAPIThinPTY
	native.Capabilities = protocol.Capabilities{
		StructuredEvents: true,
		NativeCancel:     true,
		PTYCapture:       true,
		MCP:              true,
	}
	process, err := registry.CurrentProcess()
	require.NoError(t, err)
	registered, err := registerDirect(t, fixture.service, native, process)
	require.NoError(t, err)
	fallback := adapters.GenericManifest(native.AgentID, native.Harness, native.HarnessVersion)
	payload, err := json.Marshal(adapters.CapabilitiesChangedEvent{
		EventType: protocol.EventTypeAdapterCapabilitiesChanged,
		Before:    native.Capabilities,
		After:     fallback.Capabilities,
	})
	require.NoError(t, err)
	event := protocol.EventEnvelope{
		EventID:        "01JB0000000000000000000047",
		SchemaVersion:  protocol.SchemaVersion,
		NodeID:         testHubNode,
		SessionID:      registered.SessionID,
		AgentID:        native.AgentID,
		Harness:        native.Harness,
		HarnessVersion: native.HarnessVersion,
		AdapterType:    native.AdapterType,
		SourceQuality:  protocol.SourceQualityProcessMetrics,
		EventType:      protocol.EventTypeAdapterCapabilitiesChanged,
		Origin:         protocol.OriginSystem,
		WallTime:       fixture.clock.Now().UTC(),
		Payload:        protocol.Payload(payload),
		Sensitivity:    protocol.SensitivityNormal,
	}
	require.NoError(t, consumer.ApplyCapabilitiesChanged(t.Context(), event))

	fixture.store.DB().SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	raw, err := json.Marshal(protocol.RegisterAgent{Manifest: native, Process: &process})
	require.NoError(t, err)
	resumed, err := fixture.service.RegisterAgent(ctx, peerIdentity{
		PID: int(process.PID), UID: process.UID, GID: process.GID,
	}, raw)
	require.NoError(t, err)
	require.Equal(t, registered, resumed)
	var sessions int
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM sessions WHERE agent_id = ?", native.AgentID).Scan(&sessions))
	require.Equal(t, 1, sessions)
}

func TestRegisterAgentTransportLossDisconnectsDeadProcess(t *testing.T) {
	fixture := newRegistrationFixture(t, true)
	const agent = protocol.AgentID("agent-dead-transport")
	self, err := registry.CurrentProcess()
	require.NoError(t, err)
	runtime := &fakeSessionRuntime{
		boot: "boot-dead-transport", live: map[protocol.ProcessIdentity]bool{self: true},
	}
	fixture.service.sessions = runtime
	conn := dialRegistrationConn(t, fixture.sock)
	session := registeredSessionID(t, registrationExchange(
		t, conn, registerAgentLine(t, 1, registrationManifest(agent), self),
	))
	runtime.setLive(self, false)
	require.NoError(t, conn.Close())
	require.Eventually(t, func() bool {
		var state protocol.SessionState
		var ended *string
		queryErr := fixture.store.DB().QueryRowContext(t.Context(),
			"SELECT state, ended_at FROM sessions WHERE session_id = ?", session).
			Scan(&state, &ended)
		return queryErr == nil && state == protocol.SessionStateDisconnected && ended != nil
	}, 5*time.Second, 10*time.Millisecond)
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
		var gotState string
		err := fixture.store.DB().QueryRowContext(t.Context(),
			"SELECT state FROM sessions WHERE session_id = ?", sessionID).Scan(&gotState)
		return err == nil && gotState != "CONNECTED"
	}, 5*time.Second, 10*time.Millisecond)

	second.send(t, registerAgentLine(t, 4, registrationManifest(agent), secondProcess))
	retry := second.recv(t)
	require.NotContains(t, retry, "error")
	var endedAt *string
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(),
		"SELECT state, ended_at FROM sessions WHERE session_id = ?", sessionID).Scan(&state, &endedAt))
	require.Equal(t, "DISCONNECTED", state)
	require.NotNil(t, endedAt)
	second.send(t, `{"jsonrpc":"2.0","method":"whoami","id":5}`)
	require.Equal(t, string(agent), second.recv(t)["result"].(map[string]any)["agent_id"])
	second.close(t)
}

func TestRegisterAgentRejectsNonResumableIdentityAndLiveDetachedOwner(t *testing.T) {
	t.Run("same process cannot mint a replacement", func(t *testing.T) {
		fixture := newRegistrationFixture(t, false)
		self, err := registry.CurrentProcess()
		require.NoError(t, err)
		runtime := &fakeSessionRuntime{
			boot: "boot-registration-a", live: map[protocol.ProcessIdentity]bool{self: true},
		}
		fixture.service.sessions = runtime
		manifest := registrationManifest("agent-same-process")
		wrongStart := self
		wrongStart.ProcessStartTime++
		_, err = registerDirect(t, fixture.service, manifest, wrongStart)
		require.Error(t, err)
		var count int
		require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(),
			"SELECT count(*) FROM sessions WHERE agent_id = ?", manifest.AgentID).Scan(&count))
		require.Zero(t, count)

		first, err := registerDirect(t, fixture.service, manifest, self)
		require.NoError(t, err)
		require.NoError(t, fixture.service.EndSession(t.Context(), first.SessionID))
		requireSessionLifecycle(t, fixture, first.SessionID, protocol.SessionStateDetached, false)

		resumed, err := registerDirect(t, fixture.service, manifest, self)
		require.NoError(t, err)
		require.Equal(t, first, resumed)
		require.NoError(t, fixture.service.EndSession(t.Context(), first.SessionID))

		changedManifest := manifest
		changedManifest.HarnessVersion = "other"
		_, err = registerDirect(t, fixture.service, changedManifest, self)
		require.Error(t, err)
		requireSessionLifecycle(t, fixture, first.SessionID, protocol.SessionStateDetached, false)

		runtime.setBoot("boot-registration-b")
		_, err = registerDirect(t, fixture.service, manifest, self)
		require.Error(t, err)
		requireSessionLifecycle(t, fixture, first.SessionID, protocol.SessionStateDetached, false)

		stamp := eventstore.FormatTime(fixture.clock.Now().Add(time.Second))
		_, err = fixture.store.DB().ExecContext(t.Context(), `UPDATE sessions
			SET state = ?, ended_at = ?, updated_at = ? WHERE session_id = ?`,
			protocol.SessionStateDisconnected, stamp, stamp, first.SessionID)
		require.NoError(t, err)
		runtime.setBoot("boot-registration-a")
		_, err = registerDirect(t, fixture.service, manifest, self)
		require.Error(t, err)
		require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(),
			"SELECT count(*) FROM sessions WHERE agent_id = ?", manifest.AgentID).Scan(&count))
		require.Equal(t, 1, count)
	})

	t.Run("legacy session without boot identity cannot resume", func(t *testing.T) {
		fixture := newRegistrationFixture(t, false)
		self, err := registry.CurrentProcess()
		require.NoError(t, err)
		const agent = protocol.AgentID("agent-legacy-resume")
		const session = protocol.OpaqueID("session-legacy-resume")
		fixture.service.sessions = &fakeSessionRuntime{
			boot: "boot-legacy-resume", live: map[protocol.ProcessIdentity]bool{self: true},
		}
		insertLifecycleSession(t, fixture, session, agent,
			protocol.SessionStateDetached, self, nil)
		_, err = registerDirect(t, fixture.service, registrationManifest(agent), self)
		require.Error(t, err)
		requireSessionLifecycle(t, fixture, session, protocol.SessionStateDetached, false)
		var count int
		require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(),
			"SELECT count(*) FROM sessions WHERE agent_id = ?", agent).Scan(&count))
		require.Equal(t, 1, count)
	})

	t.Run("different process waits for detached owner death", func(t *testing.T) {
		fixture := newRegistrationFixture(t, false)
		self, err := registry.CurrentProcess()
		require.NoError(t, err)
		old := self
		old.PID++
		old.ProcessStartTime++
		const agent = protocol.AgentID("agent-detached-owner")
		const oldSession = protocol.OpaqueID("session-detached-owner")
		runtime := &fakeSessionRuntime{
			boot: "boot-detached-owner",
			live: map[protocol.ProcessIdentity]bool{old: true, self: true},
		}
		fixture.service.sessions = runtime
		insertLifecycleSession(t, fixture, oldSession, agent,
			protocol.SessionStateDetached, old, "boot-detached-owner")

		_, err = registerDirect(t, fixture.service, registrationManifest(agent), self)
		require.Error(t, err)
		requireSessionLifecycle(t, fixture, oldSession, protocol.SessionStateDetached, false)

		runtime.setLive(old, false)
		registered, err := registerDirect(t, fixture.service, registrationManifest(agent), self)
		require.NoError(t, err)
		require.NotEqual(t, oldSession, registered.SessionID)
		requireSessionLifecycle(t, fixture, oldSession, protocol.SessionStateDisconnected, true)
		requireSessionLifecycle(t, fixture, registered.SessionID, protocol.SessionStateConnected, false)
	})
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

func TestCloseStaleSessionsOnlyKeepsExactLiveIdentityDetached(t *testing.T) {
	const bootID = "boot-startup"
	fixture := newRegistrationFixture(t, false)
	self, err := registry.CurrentProcess()
	require.NoError(t, err)
	dead := self
	dead.PID++
	dead.ProcessStartTime++
	runtime := &fakeSessionRuntime{
		boot: bootID, live: map[protocol.ProcessIdentity]bool{self: true},
	}
	fixture.service.sessions = runtime

	cases := []struct {
		session protocol.OpaqueID
		agent   protocol.AgentID
		state   protocol.SessionState
		process protocol.ProcessIdentity
		bootID  any
		want    protocol.SessionState
		ended   bool
	}{
		{
			"session-live-connected", "agent-live-connected", protocol.SessionStateConnected,
			self, bootID, protocol.SessionStateDetached, false,
		},
		{
			"session-live-detached", "agent-live-detached", protocol.SessionStateDetached,
			self, bootID, protocol.SessionStateDetached, false,
		},
		{
			"session-dead", "agent-dead", protocol.SessionStateConnected,
			dead, bootID, protocol.SessionStateDisconnected, true,
		},
		{
			"session-other-boot", "agent-other-boot", protocol.SessionStateConnected,
			self, "boot-other", protocol.SessionStateDisconnected, true,
		},
		{
			"session-legacy", "agent-legacy", protocol.SessionStateConnected,
			self, nil, protocol.SessionStateDisconnected, true,
		},
		{
			"session-malformed", "agent-malformed", protocol.SessionStateConnected,
			self, bootID, protocol.SessionStateDisconnected, true,
		},
	}
	for _, test := range cases {
		insertLifecycleSession(t, fixture, test.session, test.agent,
			test.state, test.process, test.bootID)
	}
	_, err = fixture.store.DB().ExecContext(t.Context(), `UPDATE sessions
		SET process_json = '{' WHERE session_id = 'session-malformed'`)
	require.NoError(t, err)
	_, err = fixture.server.Listen()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, fixture.server.Close()) })
	for _, test := range cases {
		requireSessionLifecycle(t, fixture, test.session, test.want, test.ended)
	}
	var legacyBoot *string
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(),
		"SELECT boot_id FROM sessions WHERE session_id = 'session-legacy'").Scan(&legacyBoot))
	require.Nil(t, legacyBoot, "startup must not invent a boot identity for legacy rows")
}

func TestCloseStaleSessionsDisconnectsUnverifiableProcess(t *testing.T) {
	fixture := newRegistrationFixture(t, false)
	self, err := registry.CurrentProcess()
	require.NoError(t, err)
	fixture.service.sessions = &fakeSessionRuntime{
		boot: "boot-unverifiable", aliveErr: os.ErrPermission,
		live: map[protocol.ProcessIdentity]bool{self: true},
	}
	const session = protocol.OpaqueID("session-unverifiable")
	insertLifecycleSession(t, fixture, session, "agent-unverifiable",
		protocol.SessionStateConnected, self, "boot-unverifiable")
	_, err = fixture.server.Listen()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, fixture.server.Close()) })
	requireSessionLifecycle(t, fixture, session, protocol.SessionStateDisconnected, true)
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

func TestDetachedSessionRevokesAnOpenEndpointAcrossResume(t *testing.T) {
	fixture := newRegistrationFixture(t, true)
	const agent = protocol.AgentID("agent-detached-endpoint")
	self, err := registry.CurrentProcess()
	require.NoError(t, err)
	request := registerAgentLine(t, 1, registrationManifest(agent), self)
	owner := dialRegistrationConn(t, fixture.sock)
	session := registeredSessionID(t, registrationExchange(t, owner, request))

	public, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	endpoint := dialChild(t, fixture.sock)
	endpointProcess := processIdentityForPID(t, endpoint.cmd.Process.Pid)
	endpoint.send(t, registerEndpointLine(
		t, 2, endpointProcess, public, "ed25519:agent-detached-endpoint:v1",
	))
	require.NotContains(t, endpoint.recv(t), "error")
	_, present := fixture.server.RegisteredEndpoint(agent, session)
	require.True(t, present)

	require.NoError(t, owner.Close())
	require.Eventually(t, func() bool {
		var state protocol.SessionState
		var ended *string
		queryErr := fixture.store.DB().QueryRowContext(t.Context(),
			"SELECT state, ended_at FROM sessions WHERE session_id = ?", session).
			Scan(&state, &ended)
		_, stillPublished := fixture.server.RegisteredEndpoint(agent, session)
		return queryErr == nil && state == protocol.SessionStateDetached && ended == nil && !stillPublished
	}, 5*time.Second, 10*time.Millisecond)

	replacement := dialRegistrationConn(t, fixture.sock)
	require.Equal(t, session,
		registeredSessionID(t, registrationExchange(t, replacement, request)))
	requireSessionLifecycle(t, fixture, session, protocol.SessionStateConnected, false)

	endpoint.send(t, `{"jsonrpc":"2.0","method":"whoami","id":3}`)
	require.NotContains(t, endpoint.readAll(t), "{",
		"the endpoint transport revoked at DETACHED must not survive a later resume")
	_ = endpoint.stdin.Close()
	select {
	case childErr := <-endpoint.done:
		require.NoError(t, childErr)
	case <-time.After(15 * time.Second):
		_ = endpoint.cmd.Process.Kill()
		t.Fatal("revoked endpoint child did not exit")
	}
	require.NoError(t, replacement.Close())
	require.Eventually(t, func() bool {
		var state protocol.SessionState
		err := fixture.store.DB().QueryRowContext(t.Context(),
			"SELECT state FROM sessions WHERE session_id = ?", session).Scan(&state)
		return err == nil && state == protocol.SessionStateDetached
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
