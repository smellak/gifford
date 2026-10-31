//go:build linux

package controlapi

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/agentsock"
	"github.com/smellak/gifford/internal/channel"
	giffordcrypto "github.com/smellak/gifford/internal/crypto"
	"github.com/smellak/gifford/internal/daemon"
	"github.com/smellak/gifford/internal/daemon/agentlink"
	"github.com/smellak/gifford/internal/hub"
	"github.com/smellak/gifford/internal/orchestrator"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
	"github.com/smellak/gifford/internal/tui"
)

const (
	readWriterHelperEnv  = "GIFFORD_CONTROL_READ_AGENT_HELPER"
	readWriterSocketEnv  = "GIFFORD_CONTROL_READ_HUB_SOCKET"
	readWriterRuntimeEnv = "GIFFORD_CONTROL_READ_RUNTIME"
	readWriterAgentEnv   = "GIFFORD_CONTROL_READ_AGENT"
)

// TestHelperControlReadAgent is a subprocess helper. It is the real process
// registered through the Hub socket and also owns the matching agent socket.
func TestHelperControlReadAgent(_ *testing.T) {
	if os.Getenv(readWriterHelperEnv) != "1" {
		return
	}
	if err := runControlReadAgentHelper(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
}

func runControlReadAgentHelper() error {
	agent := protocol.AgentID(os.Getenv(readWriterAgentEnv))
	process, err := registry.CurrentProcess()
	if err != nil {
		return err
	}
	registration := protocol.RegisterAgent{
		Manifest: protocol.CapabilityManifest{
			SchemaVersion:      protocol.SchemaVersion,
			AgentID:            agent,
			Harness:            protocol.HarnessCodex,
			HarnessVersion:     "writer-test",
			AdapterType:        protocol.AdapterTypeGenericPTY,
			InputGuardProvider: protocol.InputGuardProviderPTYProxy,
			CaptureMode:        protocol.CaptureModePTYProxy,
			Deliverable:        true,
			Capabilities:       protocol.Capabilities{PTYCapture: true},
		},
		Process: &process,
	}
	conn, err := (&net.Dialer{}).DialContext(context.Background(), "unix", os.Getenv(readWriterSocketEnv))
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	request := map[string]any{
		"jsonrpc": protocol.JSONRPCVersion,
		"method":  hub.MethodRegisterAgent,
		"params":  registration,
		"id":      1,
	}
	if err := json.NewEncoder(conn).Encode(request); err != nil {
		return err
	}
	line, err := bufio.NewReader(conn).ReadBytes('\n')
	if err != nil {
		return err
	}
	var response struct {
		Result protocol.RegisterAgentResult `json:"result"`
		Error  json.RawMessage              `json:"error"`
	}
	if err := json.Unmarshal(line, &response); err != nil {
		return err
	}
	if len(response.Error) != 0 && string(response.Error) != "null" {
		return fmt.Errorf("hub registration failed: %s", response.Error)
	}
	guard := &readWriterGuard{changed: make(chan struct{}, 1)}
	delivery := &readWriterDelivery{prepared: make(map[protocol.OpaqueID]agentsock.DeliveryReport)}
	server, err := agentsock.Listen(context.Background(), agentsock.Options{
		RuntimeDir: os.Getenv(readWriterRuntimeEnv), Register: registration,
		Guard: guard, Delivery: delivery, Clock: agentsock.SystemClock{},
	})
	if err != nil {
		return err
	}
	defer func() { _ = server.Close() }()
	if _, err := os.Stdout.Write(line); err != nil {
		return err
	}
	if _, err := io.WriteString(os.Stdout, "READY\n"); err != nil {
		return err
	}
	_, err = io.Copy(io.Discard, os.Stdin)
	return err
}

type readWriterGuard struct{ changed chan struct{} }

func (*readWriterGuard) InputGuard() protocol.InputGuardReport {
	return protocol.InputGuardReport{
		DraftState: protocol.DraftStateClosed, ActivityState: protocol.ActivityStateIdle,
		DraftGeneration: 2, InputGuardVersion: 7, Confidence: 1,
	}
}

func (g *readWriterGuard) GuardChanged() <-chan struct{} { return g.changed }

type readWriterDelivery struct {
	mu       sync.Mutex
	prepared map[protocol.OpaqueID]agentsock.DeliveryReport
}

func (*readWriterDelivery) CheckLine(string) error { return nil }

func (d *readWriterDelivery) Prepare(
	id protocol.OpaqueID,
	generation uint64,
	version int64,
) (agentsock.DeliveryReport, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	report := agentsock.DeliveryReport{
		Result: protocol.DeliveryResultKindPrepared, Generation: generation, Version: version,
	}
	d.prepared[id] = report
	return report, nil
}

func (d *readWriterDelivery) Commit(
	id protocol.OpaqueID,
	_ string,
	_ protocol.InjectMode,
) (agentsock.DeliveryReport, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	report, ok := d.prepared[id]
	if !ok {
		return agentsock.DeliveryReport{}, fmt.Errorf("delivery was not prepared")
	}
	delete(d.prepared, id)
	report.Result = protocol.DeliveryResultKindCommitted
	return report, nil
}

func (d *readWriterDelivery) Cancel(
	id protocol.OpaqueID,
	_ protocol.CancelReason,
) (agentsock.DeliveryReport, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	report := d.prepared[id]
	delete(d.prepared, id)
	report.Result = protocol.DeliveryResultKindCancelled
	return report, nil
}

func (*readWriterDelivery) DefaultMode() protocol.InjectMode { return protocol.InjectModeTyped }
func (*readWriterDelivery) InputHolds(string) bool           { return false }

type readWriterAgent struct {
	stdin   io.WriteCloser
	done    chan error
	process protocol.ProcessIdentity
	result  protocol.RegisterAgentResult
}

func startReadWriterAgent(
	t *testing.T,
	hubSocket, runtimeDir string,
	agent protocol.AgentID,
) readWriterAgent {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestHelperControlReadAgent$")
	cmd.Env = append(os.Environ(),
		readWriterHelperEnv+"=1",
		readWriterSocketEnv+"="+hubSocket,
		readWriterRuntimeEnv+"="+runtimeDir,
		readWriterAgentEnv+"="+string(agent),
	)
	stdin, err := cmd.StdinPipe()
	require.NoError(t, err)
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())
	process := processIdentityForControlRead(t, cmd.Process.Pid)
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	reader := bufio.NewReader(stdout)
	line, err := reader.ReadBytes('\n')
	require.NoError(t, err)
	var response struct {
		Result protocol.RegisterAgentResult `json:"result"`
		Error  json.RawMessage              `json:"error"`
	}
	require.NoError(t, json.Unmarshal(line, &response))
	require.Empty(t, response.Error)
	require.Equal(t, agent, response.Result.AgentID)
	ready, err := reader.ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, "READY\n", ready)
	child := readWriterAgent{stdin: stdin, done: done, process: process, result: response.Result}
	t.Cleanup(func() {
		_ = child.stdin.Close()
		if waitErr := <-child.done; waitErr != nil && t.Context().Err() == nil {
			t.Errorf("wait for control-read agent: %v", waitErr)
		}
	})
	return child
}

func processIdentityForControlRead(t *testing.T, pid int) protocol.ProcessIdentity {
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

type readWriterLinks struct{ client *agentlink.Client }

func (l readWriterLinks) Snapshot(agent protocol.AgentID) (agentlink.Snapshot, bool) {
	if l.client.Snapshot().AgentID != agent {
		return agentlink.Snapshot{}, false
	}
	return l.client.Snapshot(), true
}

func (l readWriterLinks) Client(agent protocol.AgentID) (*agentlink.Client, bool) {
	if l.client.Snapshot().AgentID != agent {
		return nil, false
	}
	return l.client, true
}

type readWriterProcesses struct{}

func (readWriterProcesses) Inspect(process protocol.ProcessIdentity) (orchestrator.ProcessTree, error) {
	child := orchestrator.ProcessToken{PID: process.PID + 1, StartTime: process.ProcessStartTime + 1}
	return orchestrator.ProcessTree{
		BootID:         "11111111-2222-3333-4444-555555555555",
		Root:           orchestrator.ProcessToken{PID: process.PID, StartTime: process.ProcessStartTime},
		DirectChildren: []orchestrator.ProcessToken{child},
		Descendants:    []orchestrator.ProcessToken{child},
	}, nil
}

func TestPanelReadsRealWriterRowsThroughControlSocket(t *testing.T) {
	fixture := newReadFixture(t, true)
	registryDir := t.TempDir()
	require.NoError(t, os.Chmod(registryDir, 0o700))
	keyRegistry, err := registry.New(readTestNode, registryDir, fixedReadClock{now: readTestTime})
	require.NoError(t, err)
	service, err := hub.NewService(hub.ServiceOptions{
		DB: fixture.store.DB(), NodeID: readTestNode, Engine: fixture.engine,
		Registry: keyRegistry, Clock: fixedReadClock{now: readTestTime},
	})
	require.NoError(t, err)
	hubServer, err := hub.New(t.TempDir(), fixture.store.DB(), service.Handlers())
	require.NoError(t, err)
	hubSocket, err := hubServer.Listen()
	require.NoError(t, err)
	go func() { _ = hubServer.Serve(context.Background()) }()
	t.Cleanup(func() { require.NoError(t, hubServer.Close()) })

	runtimeDir, err := os.MkdirTemp("", "gifford-control-read-") //nolint:usetesting // Unix socket paths have a small fixed limit.
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(runtimeDir)) })
	sender := startReadWriterAgent(t, hubSocket, runtimeDir, readTestSender)
	recipient := startReadWriterAgent(t, hubSocket, runtimeDir, readTestAgent)

	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	keyID := protocol.SigningKeyID("ed25519:" + string(readTestSender) + ":v1")
	registered, err := keyRegistry.Register(t.Context(), fixture.store.DB(), registry.RegisterRequest{
		Agent: readTestSender, Process: sender.process, PublicKey: publicKey, KeyID: string(keyID),
	})
	require.NoError(t, err)
	require.Equal(t, registry.OutcomeRegistered, registered.Outcome)

	admitParams := hub.AdmitMessageParams{
		ClientMsgID: "client-control-read", OriginNode: readTestNode,
		DestinationNode: readTestNode,
		Recipients: []hub.MessageRecipient{{
			Agent: readTestAgent, DeliveryMode: protocol.DeliveryModeWhenSafe, RequiresAck: true,
		}},
		ThreadID: "canary-readable-thread", Priority: protocol.PriorityHigh,
		CreatedAt: readTestTime, ExpiresAt: readTestTime.Add(time.Hour),
	}
	admitRaw, err := json.Marshal(admitParams)
	require.NoError(t, err)
	admittedRaw, err := service.AdmitMessage(t.Context(), readTestSender, admitRaw)
	require.NoError(t, err)
	admitted := admittedRaw.(hub.AdmitMessageResult)
	envelope := signedControlReadEnvelope(t, admitted, admitParams, keyID, privateKey)
	envelopeRaw, err := json.Marshal(envelope)
	require.NoError(t, err)
	_, err = service.SubmitMessageEnvelope(t.Context(), readTestSender, envelopeRaw)
	require.NoError(t, err)

	guardStore, err := agentlink.NewInputGuardStore(fixture.store.DB(), fixedReadClock{now: readTestTime})
	require.NoError(t, err)
	link, err := agentlink.Start(t.Context(), agentlink.Options{
		RuntimeDir: runtimeDir, DB: fixture.store.DB(), AgentID: readTestAgent,
		SessionID: recipient.result.SessionID, Writer: guardStore, Clock: agentlink.SystemClock{},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, link.Close()) })
	require.Eventually(t, func() bool {
		snapshot := link.Snapshot()
		return snapshot.State == agentlink.StateUp && snapshot.HasReport
	}, 5*time.Second, 5*time.Millisecond)
	startControlReadStateProducer(t, fixture, readTestAgent, recipient.result.SessionID)

	executor, err := orchestrator.NewExecutor(orchestrator.ExecutorOptions{
		DB: fixture.store.DB(), Channel: fixture.engine, Approvals: fixture.controller,
		Links: readWriterLinks{client: link}, Clock: fixedReadClock{now: readTestTime},
		Entropy: bytes.NewReader(make([]byte, 4096)), Processes: readWriterProcesses{},
	})
	require.NoError(t, err)
	candidate := daemon.QueueDelivery{
		MessageID: admitted.MessageID, AgentID: readTestAgent, DeliveryGeneration: 1,
	}
	require.NoError(t, executor.EvaluateAutomatic(t.Context(), candidate))
	approvals, err := fixture.controller.Approvals(t.Context(), protocol.ListApprovalsParams{
		State: protocol.ApprovalStatePending, AgentID: readTestAgent,
	})
	require.NoError(t, err)
	require.Len(t, approvals.Approvals, 1)
	_, err = executor.ResolveApproval(t.Context(), protocol.ResolveApprovalParams{
		ApprovalID: approvals.Approvals[0].ApprovalID, Option: "deliver_now",
	}, channel.Actor{Type: protocol.ActorTypeHuman, ID: "operator-read"})
	require.NoError(t, err)

	for table := range map[string]struct{}{
		"channel_control_decisions": {}, "orchestrator_decisions": {},
		"state_snapshots": {}, "delivery_attempts": {},
	} {
		var count int64
		require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(),
			"SELECT count(*) FROM "+table).Scan(&count))
		require.Positive(t, count, table)
	}
	var missingRecipient int64
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT count(*)
		FROM channel_control_decisions
		WHERE object_type = 'message' AND to_agent IS NULL`).Scan(&missingRecipient))
	require.Zero(t, missingRecipient, "the real Channel writer always records the message recipient")

	serveCtx, cancelServe := context.WithCancel(t.Context())
	serveDone := make(chan error, 1)
	go func() { serveDone <- fixture.server.Serve(serveCtx) }()
	require.Eventually(t, func() bool {
		_, statErr := os.Stat(fixture.server.Path())
		return statErr == nil
	}, 5*time.Second, 5*time.Millisecond)
	t.Cleanup(func() {
		cancelServe()
		require.NoError(t, <-serveDone)
	})
	client, err := tui.NewClient(tui.ClientOptions{SocketPath: fixture.server.Path(), Timeout: 2 * time.Second})
	require.NoError(t, err)

	var agents protocol.AgentListResult
	require.NoError(t, client.Call(t.Context(), protocol.ControlMethodAgentList,
		protocol.AgentListParams{}, &agents))
	require.Len(t, agents.Agents, 2)
	var messages protocol.MessageListResult
	require.NoError(t, client.Call(t.Context(), protocol.ControlMethodMessageList,
		protocol.MessageListParams{}, &messages))
	require.Len(t, messages.Messages, 1)
	var notifications protocol.NotificationListResult
	require.NoError(t, client.Call(t.Context(), protocol.ControlMethodNotificationList,
		protocol.NotificationListParams{}, &notifications))
	var decisions protocol.DecisionListResult
	require.NoError(t, client.Call(t.Context(), protocol.ControlMethodDecisionList,
		protocol.DecisionListParams{}, &decisions))
	require.NotEmpty(t, decisions.Decisions)
	var channelDecisionID protocol.DecisionID
	for _, decision := range decisions.Decisions {
		if decision.Type == protocol.DecisionTypeChannel {
			channelDecisionID = decision.DecisionID
			break
		}
	}
	require.NotEmpty(t, channelDecisionID)
	var detail protocol.DecisionGetResult
	require.NoError(t, client.Call(t.Context(), protocol.ControlMethodDecisionGet,
		protocol.DecisionGetParams{DecisionID: channelDecisionID}, &detail))
	require.NotNil(t, detail.ChannelEvidence)
	for key := range detail.ChannelEvidence.ControlVersions {
		require.False(t, strings.HasPrefix(key, "thread:"), "thread scope keys are private")
	}
	var status protocol.ModelStatusResult
	require.NoError(t, client.Call(t.Context(), protocol.ControlMethodModelStatus,
		protocol.EmptyParams{}, &status))
	require.Nil(t, status.ActiveProfile)

	for _, result := range []any{agents, messages, notifications, decisions, detail, status} {
		raw, marshalErr := json.Marshal(result)
		require.NoError(t, marshalErr)
		require.NotContains(t, string(raw), "canary-readable-thread")
		require.NotContains(t, string(raw), "canary-subject")
		require.NotContains(t, string(raw), "canary-body")
		require.NotContains(t, string(raw), "request_note")
	}
}

func startControlReadStateProducer(
	t *testing.T,
	fixture *readFixture,
	agent protocol.AgentID,
	session protocol.OpaqueID,
) {
	t.Helper()
	producer, err := orchestrator.NewStateProducer(orchestrator.StateProducerOptions{
		DB: fixture.store.DB(), Clock: fixedReadClock{now: readTestTime},
		Channel: fixture.controller,
	})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- producer.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		require.NoError(t, <-done)
	})
	require.Eventually(t, func() bool {
		var count int64
		err := fixture.store.DB().QueryRowContext(t.Context(), `SELECT count(*)
			FROM state_snapshots WHERE agent_id = ? AND session_id = ?
			AND producer_version = ?`, agent, session, orchestrator.StateProducerVersion).Scan(&count)
		return err == nil && count == 1
	}, 5*time.Second, 5*time.Millisecond)
}

func signedControlReadEnvelope(
	t *testing.T,
	admitted hub.AdmitMessageResult,
	params hub.AdmitMessageParams,
	keyID protocol.SigningKeyID,
	privateKey ed25519.PrivateKey,
) protocol.WireEnvelope {
	t.Helper()
	ciphertext := []byte(`{"subject":"canary-subject","body":"canary-body"}`)
	digest := sha256.Sum256(ciphertext)
	envelope := protocol.WireEnvelope{
		Protocol: protocol.WireProtocol, Version: protocol.WireVersion,
		MessageID: admitted.MessageID, ClientMsgID: params.ClientMsgID,
		OriginNode: params.OriginNode, OriginAgent: readTestSender,
		DestinationNode: params.DestinationNode, RecipientAgents: []protocol.AgentID{readTestAgent},
		CreatedAt: params.CreatedAt, ExpiresAt: params.ExpiresAt, Priority: params.Priority,
		ThreadRef: admitted.ThreadRef, HopCount: admitted.HopCount, RouteDigest: admitted.RouteDigest,
		ControlVersion: admitted.AdmissionResult.ControlVersion, ContentType: protocol.WireContentType,
		EncryptionProfile: protocol.EncryptionProfileNone, SenderSigningKeyID: keyID,
		RecipientKeyEnvelopes: []protocol.RecipientKeyEnvelope{}, Ciphertext: ciphertext,
		CiphertextSHA256: protocol.SHA256Hex(hex.EncodeToString(digest[:])),
	}
	require.NoError(t, giffordcrypto.SignWireEnvelope(privateKey, &envelope))
	require.NoError(t, envelope.Validate())
	return envelope
}
