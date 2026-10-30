package hub

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/channel"
	giffordcrypto "github.com/smellak/gifford/internal/crypto"
	"github.com/smellak/gifford/internal/daemon"
	"github.com/smellak/gifford/internal/daemon/agentlink"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/federation"
	"github.com/smellak/gifford/internal/orchestrator"
	"github.com/smellak/gifford/internal/policy"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
)

const (
	federationTestOriginNode       = protocol.NodeID("node-origin")
	federationTestOtherNode        = protocol.NodeID("node-other")
	federationTestSender           = protocol.AgentID("agent-origin")
	federationTestDisplayRecipient = protocol.AgentID("agent-display")
	federationTestMessage          = protocol.ULID("01ARZ3NDEKTSV4RRFFQ69G5FAY")
	federationTestOtherMessage     = protocol.ULID("01ARZ3NDEKTSV4RRFFQ69G5FAZ")
	federationTestThirdMessage     = protocol.ULID("01ARZ3NDEKTSV4RRFFQ69G5FB0")
	federationTestParentMessage    = protocol.ULID("01ARZ3NDEKTSV4RRFFQ69G5FAX")
	federationTestLoopID           = protocol.LoopID("loop_01ARZ3NDEKTSV4RRFFQ69G5FAX")
	federationTestKeyID            = "ed25519:agent-origin:v1"
	federationTestMaxBody          = 1 << 20
)

type federatedHubFixture struct {
	clock                  *hubClock
	store                  *eventstore.Store
	originStore            *eventstore.Store
	service                *Service
	controller             *channel.Controller
	destination            *registry.Registry
	origin                 *registry.Registry
	originDataDir          string
	destinationFingerprint string
	senderKey              ed25519.PrivateKey
	senderRecord           registry.DirectoryRecord
	request                federation.DeliveryRequest
	client                 *http.Client
	baseURL                string
}

type federatedOrchestratorLinks struct{}

func (federatedOrchestratorLinks) Snapshot(agent protocol.AgentID) (agentlink.Snapshot, bool) {
	return agentlink.Snapshot{
		AgentID: agent, SessionID: protocol.OpaqueID("session-" + string(agent)),
		State: agentlink.StateUp,
	}, true
}

func (federatedOrchestratorLinks) Client(protocol.AgentID) (*agentlink.Client, bool) {
	return nil, false
}

func newFederatedHubFixture(t *testing.T, allow bool) *federatedHubFixture {
	t.Helper()
	now := time.Date(2026, time.October, 29, 22, 30, 0, 0, time.UTC)
	clk := &hubClock{now: now}
	destinationDir := t.TempDir()
	originDir := t.TempDir()
	require.NoError(t, os.Chmod(destinationDir, 0o700))
	require.NoError(t, os.Chmod(originDir, 0o700))
	destinationIdentity, err := federation.EnsureNodeTLS(destinationDir, testHubNode)
	require.NoError(t, err)
	originIdentity, err := federation.EnsureNodeTLS(originDir, federationTestOriginNode)
	require.NoError(t, err)
	destinationStore, err := eventstore.Open(t.Context(), filepath.Join(destinationDir, "gifford.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, destinationStore.Close()) })
	originStore, err := eventstore.Open(t.Context(), filepath.Join(originDir, "gifford.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, originStore.Close()) })
	destinationRegistry, err := registry.New(testHubNode, destinationDir, clk)
	require.NoError(t, err)
	originRegistry, err := registry.New(federationTestOriginNode, originDir, clk)
	require.NoError(t, err)
	require.NoError(t, destinationRegistry.EnsureLocalNode(t.Context(), destinationStore.DB(), "org-local"))
	require.NoError(t, originRegistry.EnsureLocalNode(t.Context(), originStore.DB(), "org-origin"))
	destinationNodeKey, err := destinationRegistry.NodeSigningPublicKey()
	require.NoError(t, err)
	originNodeKey, err := originRegistry.NodeSigningPublicKey()
	require.NoError(t, err)
	_, err = destinationRegistry.AddPeer(
		t.Context(), destinationStore.DB(), federationTestOriginNode, "org-origin", originNodeKey,
	)
	require.NoError(t, err)
	require.NoError(t, destinationRegistry.SetPeerTransport(
		t.Context(), destinationStore.DB(), federationTestOriginNode,
		"https://origin.example.invalid:7443", originIdentity.Fingerprint,
	))
	_, err = originRegistry.AddPeer(
		t.Context(), originStore.DB(), testHubNode, "org-local", destinationNodeKey,
	)
	require.NoError(t, err)
	require.NoError(t, originRegistry.SetPeerTransport(
		t.Context(), originStore.DB(), testHubNode,
		"https://local.example.invalid:7443", destinationIdentity.Fingerprint,
	))

	engine, err := channel.New(channel.Options{Clock: clk})
	require.NoError(t, err)
	controller, err := channel.NewController(channel.ControllerOptions{
		DB: destinationStore.DB(), Engine: engine, Clock: clk, NodeID: testHubNode,
	})
	require.NoError(t, err)
	require.NoError(t, controller.Reload(t.Context()))
	compiled := federationTestPolicy(t, allow)
	service, err := NewService(ServiceOptions{
		DB: destinationStore.DB(), NodeID: testHubNode, Engine: engine,
		Registry: destinationRegistry, Clock: clk,
		FederationRegistry: destinationRegistry, Policy: compiled,
	})
	require.NoError(t, err)
	insertHubAgent(t, destinationStore, testHubRecipient)
	insertHubAgent(t, destinationStore, federationTestDisplayRecipient)

	senderKey, senderRecord := registerFederationSender(t, originStore, originRegistry, now)
	destinationBundle := federationTestBundle(t, originIdentity)
	server, err := federation.NewServer(federation.ServerOptions{
		Listen: testLoopback, Identity: destinationIdentity,
		TrustBundle: destinationBundle, DB: destinationStore.DB(), Clock: clk, Delivery: service,
	})
	require.NoError(t, err)
	address, err := server.Listen(t.Context())
	require.NoError(t, err)
	serveCtx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- server.Serve(serveCtx) }()
	t.Cleanup(func() {
		cancel()
		require.NoError(t, <-done)
	})
	originBundle := federationTestBundle(t, destinationIdentity)
	clientTLS, err := federation.NewClientTLSConfig(t.Context(), federation.ClientOptions{
		Identity: originIdentity, TrustBundle: originBundle,
		DB: originStore.DB(), TargetNode: testHubNode,
	})
	require.NoError(t, err)
	transport := &http.Transport{TLSClientConfig: clientTLS}
	t.Cleanup(transport.CloseIdleConnections)
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
	request := federationDeliveryRequest(t, now, senderKey, senderRecord, []MessageRecipient{{
		Agent: testHubRecipient, DeliveryMode: protocol.DeliveryModeWhenSafe, RequiresAck: true,
	}})
	return &federatedHubFixture{
		clock: clk, store: destinationStore, originStore: originStore,
		service: service, controller: controller,
		destination: destinationRegistry, origin: originRegistry,
		originDataDir: originDir, destinationFingerprint: destinationIdentity.Fingerprint,
		senderKey: senderKey, senderRecord: senderRecord, request: request,
		client: client, baseURL: "https://" + address,
	}
}

func federationTestPolicy(t *testing.T, allow bool) *policy.Policy {
	t.Helper()
	dir := t.TempDir()
	rules := "[]"
	if allow {
		rules = `
  - id: allow-origin-delivery
    effect: allow
    peer: node-origin
    action: deliver_message
    agent: "agent-*"`
	}
	require.NoError(t, os.WriteFile(filepath.Join(dir, "delivery.yaml"), []byte(
		"schema_version: 1\nrules: "+rules+"\n",
	), 0o600))
	compiled, err := policy.Load(dir)
	require.NoError(t, err)
	return compiled
}

func federationTestBundle(t *testing.T, identities ...*federation.NodeTLS) *federation.TrustBundle {
	t.Helper()
	dir := t.TempDir()
	for i, identity := range identities {
		raw := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: identity.Leaf.Raw})
		require.NotEmpty(t, raw)
		require.NoError(t, os.WriteFile(filepath.Join(dir, fmt.Sprintf("peer-%d.pem", i)), raw, 0o600))
	}
	bundle, err := federation.LoadTrustBundle(dir)
	require.NoError(t, err)
	return bundle
}

func registerFederationSender(
	t *testing.T,
	store *eventstore.Store,
	reg *registry.Registry,
	now time.Time,
) (ed25519.PrivateKey, registry.DirectoryRecord) {
	return registerFederationAgent(
		t, store, reg, now, federationTestOriginNode, federationTestSender,
		"session-origin", federationTestKeyID,
	)
}

func registerFederationAgent(
	t *testing.T,
	store *eventstore.Store,
	reg *registry.Registry,
	now time.Time,
	node protocol.NodeID,
	agent protocol.AgentID,
	sessionID string,
	keyID string,
) (ed25519.PrivateKey, registry.DirectoryRecord) {
	t.Helper()
	process, err := registry.CurrentProcess()
	require.NoError(t, err)
	processJSON, err := json.Marshal(process)
	require.NoError(t, err)
	stamp := eventstore.FormatTime(now)
	_, err = store.DB().ExecContext(t.Context(), `INSERT INTO agents
		(agent_id, node_id, harness, created_at, updated_at)
		VALUES (?, ?, 'test', ?, ?)`, agent, node, stamp, stamp)
	require.NoError(t, err)
	_, err = store.DB().ExecContext(t.Context(), `INSERT INTO sessions
		(session_id, agent_id, state, capture_mode, deliverable, manifest_json,
		 process_json, started_at, updated_at)
		VALUES (?, ?, 'CONNECTED', 'pty_proxy', 1, '{}', ?, ?, ?)`,
		sessionID, agent, string(processJSON), stamp, stamp)
	require.NoError(t, err)
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	result, err := reg.Register(t.Context(), store.DB(), registry.RegisterRequest{
		Agent: agent, Process: process, PublicKey: publicKey, KeyID: keyID,
	})
	require.NoError(t, err)
	require.Equal(t, registry.OutcomeRegistered, result.Outcome)
	directory, err := reg.ExportDirectory(t.Context(), store.DB())
	require.NoError(t, err)
	for _, record := range directory {
		if record.KeyID == keyID {
			return privateKey, record
		}
	}
	require.FailNow(t, "registered federation key missing from directory", keyID)
	return nil, registry.DirectoryRecord{}
}

func registerAdditionalFederationKey(
	t *testing.T,
	store *eventstore.Store,
	reg *registry.Registry,
	agent protocol.AgentID,
	keyID string,
) (ed25519.PrivateKey, registry.RegisterResult) {
	t.Helper()
	process, err := registry.CurrentProcess()
	require.NoError(t, err)
	processJSON, err := json.Marshal(process)
	require.NoError(t, err)
	result, err := store.DB().ExecContext(
		t.Context(), "UPDATE sessions SET process_json = ? WHERE agent_id = ? AND ended_at IS NULL",
		string(processJSON), agent,
	)
	require.NoError(t, err)
	affected, err := result.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), affected)
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	registered, err := reg.Register(t.Context(), store.DB(), registry.RegisterRequest{
		Agent: agent, Process: process, PublicKey: publicKey, KeyID: keyID,
	})
	require.NoError(t, err)
	return privateKey, registered
}

func federationAgentKeyStatus(t *testing.T, store *eventstore.Store, keyID string) string {
	t.Helper()
	var status string
	require.NoError(t, store.DB().QueryRowContext(
		t.Context(), "SELECT status FROM agent_keys WHERE key_id = ?", keyID,
	).Scan(&status))
	return status
}

func federationNodeKeyCount(
	t *testing.T,
	store *eventstore.Store,
	node protocol.NodeID,
) int64 {
	t.Helper()
	var count int64
	require.NoError(t, store.DB().QueryRowContext(
		t.Context(), "SELECT count(*) FROM agent_keys WHERE node_id = ?", node,
	).Scan(&count))
	return count
}

func federationDeliveryRequest(
	t *testing.T,
	now time.Time,
	senderKey ed25519.PrivateKey,
	senderRecord registry.DirectoryRecord,
	recipients []MessageRecipient,
) federation.DeliveryRequest {
	t.Helper()
	agents := make([]protocol.AgentID, len(recipients))
	metadata := make([]federation.DeliveryRecipient, len(recipients))
	for i, recipient := range recipients {
		agents[i] = recipient.Agent
		metadata[i] = federation.DeliveryRecipient{
			Agent: recipient.Agent, Mode: recipient.DeliveryMode, RequiresAck: recipient.RequiresAck,
		}
	}
	ciphertext := []byte("opaque federated payload")
	digest := sha256.Sum256(ciphertext)
	envelope := protocol.WireEnvelope{
		Protocol: protocol.WireProtocol, Version: protocol.WireVersion,
		MessageID: federationTestMessage, ClientMsgID: "client-federated-message",
		OriginNode: senderRecord.Node, OriginAgent: senderRecord.Agent,
		DestinationNode: testHubNode, RecipientAgents: agents,
		CreatedAt: now, ExpiresAt: now.Add(time.Hour), Priority: protocol.PriorityNormal,
		ThreadRef: "thr_01ARZ3NDEKTSV4RRFFQ69G5FAY", HopCount: 0,
		RouteDigest: localRouteDigest(
			federationTestMessage, senderRecord.Node, senderRecord.Agent, recipients,
		),
		ControlVersion: 1, ContentType: protocol.WireContentType,
		EncryptionProfile:     protocol.EncryptionProfileNone,
		SenderSigningKeyID:    protocol.SigningKeyID(senderRecord.KeyID),
		RecipientKeyEnvelopes: []protocol.RecipientKeyEnvelope{},
		Ciphertext:            ciphertext,
		CiphertextSHA256:      protocol.SHA256Hex(hex.EncodeToString(digest[:])),
	}
	require.NoError(t, giffordcrypto.SignWireEnvelope(senderKey, &envelope))
	rawEnvelope, err := json.Marshal(envelope)
	require.NoError(t, err)
	request := federation.DeliveryRequest{
		Envelope: rawEnvelope, ThreadID: "thread-federated", Recipients: metadata,
		DeliveryGeneration: 3, Nonce: bytes.Repeat([]byte{0x31}, 16), Sequence: 1,
		OriginControl: protocol.ObjectControlState{
			ObjectType:     protocol.DecisionObjectTypeMessage,
			ObjectID:       protocol.OpaqueID(federationTestMessage),
			ControlState:   protocol.ControlStateAllowed,
			ControlReason:  protocol.DecisionReasonAllowed,
			ControlVersion: 1,
		},
		Sender: senderRecord,
	}
	require.NoError(t, request.Validate())
	return request
}

func (f *federatedHubFixture) post(
	t *testing.T,
	request federation.DeliveryRequest,
) (int, []byte) {
	t.Helper()
	raw, err := json.Marshal(request)
	require.NoError(t, err)
	return f.postRaw(t, raw)
}

func (f *federatedHubFixture) postRaw(t *testing.T, raw []byte) (int, []byte) {
	t.Helper()
	request, err := http.NewRequestWithContext(
		t.Context(), http.MethodPost, f.baseURL+"/v1/deliver", bytes.NewReader(raw),
	)
	require.NoError(t, err)
	request.Header.Set("Content-Type", "application/json")
	response, err := f.client.Do(request)
	require.NoError(t, err)
	defer func() { require.NoError(t, response.Body.Close()) }()
	body, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	return response.StatusCode, body
}

func (f *federatedHubFixture) mutateEnvelope(
	t *testing.T,
	request federation.DeliveryRequest,
	mutate func(*protocol.WireEnvelope),
) federation.DeliveryRequest {
	t.Helper()
	envelope, _, err := request.DecodeEnvelope()
	require.NoError(t, err)
	mutate(&envelope)
	require.NoError(t, giffordcrypto.SignWireEnvelope(f.senderKey, &envelope))
	request.Envelope, err = json.Marshal(envelope)
	require.NoError(t, err)
	return request
}

func (f *federatedHubFixture) reidentify(
	t *testing.T,
	request federation.DeliveryRequest,
	messageID protocol.ULID,
	sequence int64,
	nonce []byte,
) federation.DeliveryRequest {
	t.Helper()
	envelope, _, err := request.DecodeEnvelope()
	require.NoError(t, err)
	envelope.MessageID = messageID
	envelope.ClientMsgID = protocol.OpaqueID("client-" + string(messageID))
	recipients := make([]MessageRecipient, len(request.Recipients))
	for i, recipient := range request.Recipients {
		recipients[i] = MessageRecipient{
			Agent: recipient.Agent, DeliveryMode: recipient.Mode, RequiresAck: recipient.RequiresAck,
		}
	}
	envelope.RouteDigest = localRouteDigest(
		messageID, envelope.OriginNode, envelope.OriginAgent, recipients,
	)
	require.NoError(t, giffordcrypto.SignWireEnvelope(f.senderKey, &envelope))
	request.Envelope, err = json.Marshal(envelope)
	require.NoError(t, err)
	request.Sequence = sequence
	request.Nonce = append([]byte(nil), nonce...)
	request.OriginControl.ObjectID = protocol.OpaqueID(messageID)
	require.NoError(t, request.Validate())
	return request
}

func (f *federatedHubFixture) receipt(
	t *testing.T,
	raw []byte,
	status federation.DeliveryReceiptStatus,
) federation.DeliveryReceipt {
	t.Helper()
	var receipt federation.DeliveryReceipt
	require.NoError(t, json.Unmarshal(raw, &receipt))
	require.Equal(t, status, receipt.Status)
	publicKey, err := f.destination.NodeSigningPublicKey()
	require.NoError(t, err)
	require.NoError(t, receipt.Verify(publicKey))
	return receipt
}

func requireFederationErrorCode(t *testing.T, raw []byte, code protocol.ErrorCode) {
	t.Helper()
	var body struct {
		ErrorCode protocol.ErrorCode `json:"error_code"`
	}
	require.NoError(t, json.Unmarshal(raw, &body))
	require.Equal(t, code, body.ErrorCode)
}

func federationTableCount(t *testing.T, store *eventstore.Store, table string) int64 {
	t.Helper()
	var count int64
	require.NoError(t, store.DB().QueryRowContext(t.Context(), "SELECT count(*) FROM "+table).Scan(&count))
	return count
}

func requireNoFederatedMessageRows(t *testing.T, f *federatedHubFixture) {
	t.Helper()
	for _, table := range []string{
		"messages", "message_envelopes", "message_recipients", "delivery_queue", "federation_inbox",
	} {
		require.Zero(t, federationTableCount(t, f.store, table), table)
	}
}

func seedFederationProbeDelivery(
	t *testing.T,
	f *federatedHubFixture,
	exactReceipt []byte,
) {
	t.Helper()
	exactRequest, err := f.request.CanonicalBytes()
	require.NoError(t, err)
	envelope, _, err := f.request.DecodeEnvelope()
	require.NoError(t, err)
	requestDigest, err := f.request.RequestSHA256()
	require.NoError(t, err)
	stamp := eventstore.FormatTime(f.clock.now)
	const outboxID = "message:hub-fedprobe:1"
	_, err = f.originStore.DB().ExecContext(t.Context(), `INSERT INTO federation_outbox
		(outbox_id, object_type, object_id, destination_node, state, attempts,
		 idempotency_key, created_at, updated_at)
		VALUES (?, 'message', ?, ?, 'STORED_REMOTE', 1, ?, ?, ?)`,
		outboxID, envelope.MessageID, envelope.DestinationNode, "hub-fedprobe:1", stamp, stamp)
	require.NoError(t, err)
	_, err = f.originStore.DB().ExecContext(t.Context(), `INSERT INTO federation_outbox_requests
		(outbox_id, destination_node, request, request_sha256, sequence, nonce)
		VALUES (?, ?, ?, ?, ?, ?)`, outboxID, envelope.DestinationNode, exactRequest,
		requestDigest, f.request.Sequence, f.request.Nonce)
	require.NoError(t, err)
	_, err = f.originStore.DB().ExecContext(t.Context(), `INSERT INTO federation_receipts
		(outbox_id, receipt, state, received_at) VALUES (?, ?, 'STORED_REMOTE', ?)`,
		outboxID, exactReceipt, stamp)
	require.NoError(t, err)
}

func TestFederationProbeExercisesRealHubBoundary(t *testing.T) {
	f := newFederatedHubFixture(t, true)
	status, exactReceipt := f.post(t, f.request)
	require.Equal(t, http.StatusOK, status)
	f.receipt(t, exactReceipt, federation.DeliveryReceiptStoredRemote)
	seedFederationProbeDelivery(t, f, exactReceipt)

	tables := []string{
		"messages", "message_envelopes", "message_recipients", "delivery_queue",
		"federation_inbox", "channel_control_decisions", "agent_keys",
	}
	before := make(map[string]int64, len(tables))
	for _, table := range tables {
		before[table] = federationTableCount(t, f.store, table)
	}
	f.clock.now = f.clock.now.Add(2 * time.Hour)
	var output bytes.Buffer
	err := federation.RunProbe(t.Context(), federation.ProbeOptions{
		Endpoint: f.baseURL, Fingerprint: f.destinationFingerprint,
		DataDir: f.originDataDir, Timeout: 5 * time.Second, Clock: f.clock, Output: &output,
	})
	require.NoError(t, err, output.String())
	require.Equal(t, strings.Join([]string{
		"PASS tls-1.3",
		"PASS tls-1.2-rejected",
		"PASS no-client-certificate",
		"PASS unenrolled-client-certificate",
		"PASS known-node-wrong-certificate",
		"PASS forged-origin",
		"PASS invalid-signature",
		"PASS exact-replay",
		"",
	}, "\n"), output.String())
	for _, table := range tables {
		require.Equal(t, before[table], federationTableCount(t, f.store, table), table)
	}
}

func TestFederatedDeliveryStoresClaimsAndReplaysExactReceipt(t *testing.T) {
	f := newFederatedHubFixture(t, true)
	recipients := []MessageRecipient{
		{Agent: testHubRecipient, DeliveryMode: protocol.DeliveryModeWhenSafe, RequiresAck: true},
		{Agent: federationTestDisplayRecipient, DeliveryMode: protocol.DeliveryModeDisplayOnly},
	}
	request := federationDeliveryRequest(t, f.clock.now, f.senderKey, f.senderRecord, recipients)
	status, firstReceipt := f.post(t, request)
	require.Equal(t, http.StatusOK, status)
	receipt := f.receipt(t, firstReceipt, federation.DeliveryReceiptStoredRemote)
	requestDigest, err := request.RequestSHA256()
	require.NoError(t, err)
	envelopeDigest, err := request.EnvelopeSHA256()
	require.NoError(t, err)
	require.Equal(t, federationTestMessage, receipt.MessageID)
	require.Equal(t, federationTestOriginNode, receipt.OriginNode)
	require.Equal(t, testHubNode, receipt.DestinationNode)
	require.Equal(t, []protocol.AgentID{testHubRecipient, federationTestDisplayRecipient}, receipt.RecipientAgents)
	require.Equal(t, requestDigest, receipt.RequestSHA256)
	require.Equal(t, envelopeDigest, receipt.EnvelopeSHA256)
	require.Equal(t, request.DeliveryGeneration, receipt.DeliveryGeneration)
	require.Equal(t, request.Sequence, receipt.Sequence)
	require.Equal(t, request.Nonce, receipt.Nonce)
	require.Equal(t, int64(1), federationTableCount(t, f.store, "messages"))
	require.Equal(t, int64(1), federationTableCount(t, f.store, "message_envelopes"))
	require.Equal(t, int64(2), federationTableCount(t, f.store, "message_recipients"))
	require.Equal(t, int64(1), federationTableCount(t, f.store, "delivery_queue"))
	require.Equal(t, int64(1), federationTableCount(t, f.store, "federation_inbox"))
	var generation int64
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
		"SELECT delivery_generation FROM delivery_queue").Scan(&generation))
	require.Equal(t, int64(1), generation)

	claimRaw, err := json.Marshal(ClaimMessageParams{
		MessageID: federationTestMessage, DeliveryGeneration: 1,
	})
	require.NoError(t, err)
	claimedRaw, err := f.service.ClaimMessage(t.Context(), testHubRecipient, claimRaw)
	require.NoError(t, err)
	claimed := claimedRaw.(ClaimMessageResult)
	require.JSONEq(t, string(request.Envelope), string(claimed.Envelope))
	require.Equal(t, federationTestKeyID, string(claimed.SenderSigningKeyID))
	ackRaw, err := json.Marshal(AckMessageParams{
		MessageID: federationTestMessage, DeliveryGeneration: 1, Status: AckMessageAcknowledged,
	})
	require.NoError(t, err)
	ackResult, err := f.service.AckMessage(t.Context(), testHubRecipient, ackRaw)
	require.NoError(t, err)
	require.Equal(t, AckMessageAcknowledged, ackResult.(AckMessageResult).Status)

	_, err = f.store.DB().ExecContext(t.Context(),
		"UPDATE sessions SET ended_at = ?", eventstore.FormatTime(f.clock.now))
	require.NoError(t, err)
	beforeReplayDecisions := federationTableCount(t, f.store, "channel_control_decisions")
	status, replayed := f.post(t, request)
	require.Equal(t, http.StatusOK, status)
	require.Equal(t, firstReceipt, replayed)
	require.Equal(t, beforeReplayDecisions, federationTableCount(t, f.store, "channel_control_decisions"))
	require.Equal(t, int64(1), federationTableCount(t, f.store, "federation_inbox"))

	beforeDecisions := federationTableCount(t, f.store, "channel_control_decisions")
	conflicts := []federation.DeliveryRequest{
		f.reidentify(t, request, federationTestMessage, 2, bytes.Repeat([]byte{0x32}, 16)),
		f.reidentify(t, request, federationTestOtherMessage, request.Sequence, bytes.Repeat([]byte{0x33}, 16)),
		f.reidentify(t, request, federationTestThirdMessage, 3, request.Nonce),
	}
	for _, conflict := range conflicts {
		status, body := f.post(t, conflict)
		require.Equal(t, http.StatusConflict, status)
		requireFederationErrorCode(t, body, protocol.ErrorCodeControlVersionConflict)
	}
	require.Equal(t, int64(1), federationTableCount(t, f.store, "messages"))
	require.Equal(t, int64(1), federationTableCount(t, f.store, "federation_inbox"))
	require.Equal(t, beforeDecisions, federationTableCount(t, f.store, "channel_control_decisions"))
}

func TestFederatedMultiRecipientDeliveryCreatesIndependentApprovals(t *testing.T) {
	f := newFederatedHubFixture(t, true)
	recipients := []MessageRecipient{
		{Agent: testHubRecipient, DeliveryMode: protocol.DeliveryModeWhenSafe, RequiresAck: true},
		{Agent: federationTestDisplayRecipient, DeliveryMode: protocol.DeliveryModeWhenSafe},
	}
	request := federationDeliveryRequest(t, f.clock.now, f.senderKey, f.senderRecord, recipients)
	status, rawReceipt := f.post(t, request)
	require.Equal(t, http.StatusOK, status)
	receipt := f.receipt(t, rawReceipt, federation.DeliveryReceiptStoredRemote)
	require.Equal(t,
		[]protocol.AgentID{testHubRecipient, federationTestDisplayRecipient},
		receipt.RecipientAgents,
	)

	engine, err := channel.New(channel.Options{Clock: f.clock})
	require.NoError(t, err)
	links := federatedOrchestratorLinks{}
	executor, err := orchestrator.NewExecutor(orchestrator.ExecutorOptions{
		DB: f.store.DB(), Channel: engine, Approvals: f.controller,
		Links: links, Clock: f.clock,
	})
	require.NoError(t, err)
	worker, err := daemon.NewWorker(daemon.WorkerOptions{
		DB: f.store.DB(), Processor: executor, Approvals: executor,
		Prepared: executor, Proxies: links,
	})
	require.NoError(t, err)
	processed, err := worker.RunOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, 2, processed)

	list, err := f.controller.Approvals(t.Context(), protocol.ListApprovalsParams{
		State: protocol.ApprovalStatePending,
	})
	require.NoError(t, err)
	require.Len(t, list.Approvals, 2)
	byAgent := make(map[protocol.AgentID]protocol.ApprovalID, len(list.Approvals))
	for _, approval := range list.Approvals {
		require.Equal(t, string(federationTestMessage), approval.ObjectID)
		byAgent[approval.AgentID] = approval.ApprovalID
	}
	require.True(t, byAgent[testHubRecipient].Valid())
	require.True(t, byAgent[federationTestDisplayRecipient].Valid())
	require.NotEqual(t, byAgent[testHubRecipient], byAgent[federationTestDisplayRecipient])
}

func TestFederatedDeliveryRejectsForgedOriginAtHubBoundary(t *testing.T) {
	f := newFederatedHubFixture(t, true)
	routeRecipients := make([]MessageRecipient, len(f.request.Recipients))
	for i, recipient := range f.request.Recipients {
		routeRecipients[i].Agent = recipient.Agent
	}
	forged := f.mutateEnvelope(t, f.request, func(envelope *protocol.WireEnvelope) {
		envelope.OriginNode = federationTestOtherNode
		envelope.RouteDigest = localRouteDigest(
			envelope.MessageID, envelope.OriginNode, envelope.OriginAgent, routeRecipients,
		)
	})
	forged.Sender.Node = federationTestOtherNode
	forged.Sender.Issuer = "node:" + string(federationTestOtherNode)
	require.NoError(t, forged.Validate())
	envelope, _, err := forged.DecodeEnvelope()
	require.NoError(t, err)
	require.NoError(t, verifyEnvelope(f.senderRecord.PublicKey, envelope))

	status, body := f.post(t, forged)
	require.Equal(t, http.StatusForbidden, status)
	requireFederationErrorCode(t, body, protocol.ErrorCodeNotFound)
	requireNoFederatedMessageRows(t, f)
	require.Zero(t, federationTableCount(t, f.store, "agent_keys"))
	require.Zero(t, federationTableCount(t, f.store, "channel_control_decisions"))
}

func TestFederatedDeliveryRejectsNonRootMetadataBeforeEffects(t *testing.T) {
	f := newFederatedHubFixture(t, true)
	destinationMismatch := f.mutateEnvelope(t, f.request, func(envelope *protocol.WireEnvelope) {
		envelope.DestinationNode = federationTestOtherNode
	})
	status, body := f.post(t, destinationMismatch)
	require.Equal(t, http.StatusForbidden, status)
	requireFederationErrorCode(t, body, protocol.ErrorCodeNotFound)

	unknownRecipient := federationDeliveryRequest(
		t, f.clock.now, f.senderKey, f.senderRecord,
		[]MessageRecipient{{
			Agent: "agent-unknown", DeliveryMode: protocol.DeliveryModeWhenSafe, RequiresAck: true,
		}},
	)
	status, body = f.post(t, unknownRecipient)
	require.Equal(t, http.StatusForbidden, status)
	requireFederationErrorCode(t, body, protocol.ErrorCodeNotFound)

	loop := f.mutateEnvelope(t, f.request, func(envelope *protocol.WireEnvelope) {
		generation := int64(1)
		turn := int64(1)
		envelope.ParentMessageID = federationTestParentMessage
		envelope.LoopID = federationTestLoopID
		envelope.LoopGeneration = &generation
		envelope.TurnIndex = &turn
	})
	status, body = f.post(t, loop)
	require.Equal(t, http.StatusConflict, status)
	requireFederationErrorCode(t, body, protocol.ErrorCodeControlVersionConflict)

	parentOnly := f.mutateEnvelope(t, f.request, func(envelope *protocol.WireEnvelope) {
		envelope.ParentMessageID = federationTestParentMessage
	})
	status, body = f.post(t, parentOnly)
	require.Equal(t, http.StatusConflict, status)
	requireFederationErrorCode(t, body, protocol.ErrorCodeControlVersionConflict)

	incomplete := f.mutateEnvelope(t, f.request, func(envelope *protocol.WireEnvelope) {
		envelope.ParentMessageID = federationTestParentMessage
		envelope.LoopID = federationTestLoopID
	})
	status, _ = f.post(t, incomplete)
	require.Equal(t, http.StatusBadRequest, status)

	loopWithoutParent := f.mutateEnvelope(t, f.request, func(envelope *protocol.WireEnvelope) {
		generation := int64(1)
		turn := int64(1)
		envelope.LoopID = federationTestLoopID
		envelope.LoopGeneration = &generation
		envelope.TurnIndex = &turn
	})
	status, _ = f.post(t, loopWithoutParent)
	require.Equal(t, http.StatusBadRequest, status)

	hopped := f.mutateEnvelope(t, f.request, func(envelope *protocol.WireEnvelope) {
		envelope.HopCount = 1
	})
	status, body = f.post(t, hopped)
	require.Equal(t, http.StatusForbidden, status)
	requireFederationErrorCode(t, body, protocol.ErrorCodeNotFound)

	wrongRoute := f.mutateEnvelope(t, f.request, func(envelope *protocol.WireEnvelope) {
		envelope.RouteDigest = protocol.Digest("sha256:" + strings.Repeat("f", 64))
	})
	status, body = f.post(t, wrongRoute)
	require.Equal(t, http.StatusForbidden, status)
	requireFederationErrorCode(t, body, protocol.ErrorCodeNotFound)

	raw, err := json.Marshal(f.request)
	require.NoError(t, err)
	raw = append(raw, bytes.Repeat([]byte(" "), federationTestMaxBody-len(raw)+1)...)
	status, _ = f.postRaw(t, raw)
	require.Equal(t, http.StatusRequestEntityTooLarge, status)
	requireNoFederatedMessageRows(t, f)
	require.Zero(t, federationTableCount(t, f.store, "agent_keys"), "sender key must not be imported")

	status, receipt := f.post(t, f.request)
	require.Equal(t, http.StatusOK, status)
	f.receipt(t, receipt, federation.DeliveryReceiptStoredRemote)
}

func TestFederatedDeliveryEnforcesRequestCoherence(t *testing.T) {
	f := newFederatedHubFixture(t, true)
	metadataMismatch := f.request
	metadataMismatch.Recipients = append(
		[]federation.DeliveryRecipient(nil), f.request.Recipients...,
	)
	metadataMismatch.Recipients[0].Agent = federationTestDisplayRecipient

	badGeneration := f.request
	badGeneration.DeliveryGeneration = 0

	lowerControlVersion := f.mutateEnvelope(t, f.request, func(envelope *protocol.WireEnvelope) {
		envelope.ControlVersion = 2
	})

	notAllowed := f.request
	notAllowed.OriginControl.ControlState = protocol.ControlStateHeld
	notAllowed.OriginControl.ControlReason = protocol.DecisionReasonLinkHold

	badAllowReason := f.request
	badAllowReason.OriginControl.ControlReason = protocol.DecisionReasonLinkDenied

	wrongControlObject := f.request
	wrongControlObject.OriginControl.ObjectID = protocol.OpaqueID(federationTestOtherMessage)

	for _, request := range []federation.DeliveryRequest{
		metadataMismatch,
		badGeneration,
		lowerControlVersion,
		notAllowed,
		badAllowReason,
		wrongControlObject,
	} {
		status, _ := f.post(t, request)
		require.Equal(t, http.StatusForbidden, status)
		requireNoFederatedMessageRows(t, f)
		require.Zero(t, federationTableCount(t, f.store, "agent_keys"))
	}

	released := f.request
	released.OriginControl.ControlVersion = 2
	released.OriginControl.ControlReason = protocol.DecisionReasonHoldReleased
	status, raw := f.post(t, released)
	require.Equal(t, http.StatusOK, status)
	f.receipt(t, raw, federation.DeliveryReceiptStoredRemote)
}

func TestFederatedDeliveryEnforcesTimeWindowWithoutMessageWrites(t *testing.T) {
	f := newFederatedHubFixture(t, true)
	future := f.mutateEnvelope(t, f.request, func(envelope *protocol.WireEnvelope) {
		envelope.CreatedAt = f.clock.now.Add(federationClockTolerance + time.Nanosecond)
		envelope.ExpiresAt = envelope.CreatedAt.Add(time.Hour)
	})
	status, _ := f.post(t, future)
	require.Equal(t, http.StatusBadRequest, status)

	expired := f.mutateEnvelope(t, f.request, func(envelope *protocol.WireEnvelope) {
		envelope.CreatedAt = f.clock.now.Add(-time.Hour)
		envelope.ExpiresAt = f.clock.now
	})
	status, body := f.post(t, expired)
	require.Equal(t, http.StatusConflict, status)
	requireFederationErrorCode(t, body, protocol.ErrorCodeExpired)
	requireNoFederatedMessageRows(t, f)
	require.Equal(t, int64(1), federationTableCount(t, f.store, "agent_keys"))

	atFutureBoundary := f.mutateEnvelope(t, f.request, func(envelope *protocol.WireEnvelope) {
		envelope.CreatedAt = f.clock.now.Add(federationClockTolerance)
		envelope.ExpiresAt = envelope.CreatedAt.Add(time.Hour)
	})
	status, raw := f.post(t, atFutureBoundary)
	require.Equal(t, http.StatusOK, status)
	f.receipt(t, raw, federation.DeliveryReceiptStoredRemote)
}

func TestFederatedDeliveryRejectsInvalidSenderAuthenticity(t *testing.T) {
	f := newFederatedHubFixture(t, true)
	unsigned := f.request
	unsigned.Sender.RecordSignature = nil
	status, _ := f.post(t, unsigned)
	require.Equal(t, http.StatusForbidden, status)
	require.Zero(t, federationTableCount(t, f.store, "agent_keys"))

	wrongKey := f.request
	wrongKey.Sender.KeyID = "ed25519:agent-origin:v2"
	status, _ = f.post(t, wrongKey)
	require.Equal(t, http.StatusForbidden, status)
	require.Zero(t, federationTableCount(t, f.store, "agent_keys"))

	badRecord := f.request
	badRecord.Sender.RecordSignature = append([]byte(nil), f.request.Sender.RecordSignature...)
	badRecord.Sender.RecordSignature[0] ^= 0xff
	status, body := f.post(t, badRecord)
	require.Equal(t, http.StatusForbidden, status)
	requireFederationErrorCode(t, body, protocol.ErrorCodeKeyUnavailable)
	require.Zero(t, federationTableCount(t, f.store, "agent_keys"))

	badDigest := f.mutateEnvelope(t, f.request, func(envelope *protocol.WireEnvelope) {
		envelope.Ciphertext = append(envelope.Ciphertext, 0x01)
	})
	status, _ = f.post(t, badDigest)
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, int64(1), federationTableCount(t, f.store, "agent_keys"))

	badSignature := f.request
	envelope, _, err := badSignature.DecodeEnvelope()
	require.NoError(t, err)
	envelope.Signature = append([]byte(nil), envelope.Signature...)
	envelope.Signature[0] ^= 0xff
	badSignature.Envelope, err = json.Marshal(envelope)
	require.NoError(t, err)
	status, _ = f.post(t, badSignature)
	require.Equal(t, http.StatusBadRequest, status)
	requireNoFederatedMessageRows(t, f)
	require.Zero(t, federationTableCount(t, f.store, "channel_control_decisions"))
}

func TestFederatedDeliveryRejectsRetiredSenderKeysAtHubBoundary(t *testing.T) {
	for _, test := range []struct {
		name              string
		action            registry.ReviewAction
		oldStatus         string
		replacementStatus string
		hasActiveKey      bool
	}{
		{
			name: "rotated", action: registry.ReviewRotate,
			oldStatus: "ROTATED", replacementStatus: "ACTIVE", hasActiveKey: true,
		},
		{
			name: "revoked", action: registry.ReviewRevoke,
			oldStatus: "REVOKED", replacementStatus: "REJECTED",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newFederatedHubFixture(t, true)
			replacementID := "ed25519:" + string(federationTestSender) + ":v2"
			_, registered := registerAdditionalFederationKey(
				t, f.originStore, f.origin, federationTestSender, replacementID,
			)
			require.Equal(t, registry.OutcomeSuspended, registered.Outcome)
			decision := registry.ReviewDecision{Action: test.action}
			if test.action == registry.ReviewRotate {
				decision.KeyID = replacementID
			}
			require.NoError(t, f.origin.Review(
				t.Context(), f.originStore.DB(), federationTestSender, decision,
			))
			directory, err := f.origin.ExportDirectory(t.Context(), f.originStore.DB())
			require.NoError(t, err)
			require.Len(t, directory, 2)
			imported, err := f.destination.ImportDirectory(
				t.Context(), f.store.DB(), federationTestOriginNode, directory,
			)
			require.NoError(t, err)
			require.Equal(t, 2, imported.Inserted)
			require.Equal(t, test.oldStatus, federationAgentKeyStatus(t, f.store, federationTestKeyID))
			require.Equal(t, test.replacementStatus, federationAgentKeyStatus(t, f.store, replacementID))

			status, body := f.post(t, f.request)
			require.Equal(t, http.StatusForbidden, status)
			requireFederationErrorCode(t, body, protocol.ErrorCodeKeyUnavailable)
			requireNoFederatedMessageRows(t, f)
			require.Zero(t, federationTableCount(t, f.store, "channel_control_decisions"))
			require.Equal(t, int64(2), federationNodeKeyCount(t, f.store, federationTestOriginNode))
			require.Equal(t, test.oldStatus, federationAgentKeyStatus(t, f.store, federationTestKeyID))
			require.Equal(t, test.replacementStatus, federationAgentKeyStatus(t, f.store, replacementID))
			active, err := f.destination.ActiveSigningKey(
				t.Context(), f.store.DB(), federationTestSender,
			)
			if test.hasActiveKey {
				require.NoError(t, err)
				require.Equal(t, replacementID, active.KeyID)
			} else {
				require.ErrorIs(t, err, registry.ErrNoUsableKey)
				blocked, blockedErr := f.destination.Blocked(
					t.Context(), f.store.DB(), federationTestSender,
				)
				require.NoError(t, blockedErr)
				require.True(t, blocked)
			}
			peer, err := f.destination.Peer(t.Context(), f.store.DB(), federationTestOriginNode)
			require.NoError(t, err)
			require.Equal(t, registry.PeerStatusTrusted, peer.Status)
		})
	}
}

func TestFederatedDeliveryCannotReactivateLocalAgentAtHubBoundary(t *testing.T) {
	for _, test := range []struct {
		name    string
		revoked bool
	}{
		{name: "active local agent"},
		{name: "revoked local agent", revoked: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newFederatedHubFixture(t, true)
			localKeyID := "ed25519:" + string(testHubRecipient) + ":v9"
			_, registered := registerAdditionalFederationKey(
				t, f.store, f.destination, testHubRecipient, localKeyID,
			)
			require.Equal(t, registry.OutcomeRegistered, registered.Outcome)
			localKey, err := f.destination.ActiveSigningKey(
				t.Context(), f.store.DB(), testHubRecipient,
			)
			require.NoError(t, err)
			require.Equal(t, localKeyID, localKey.KeyID)

			replacementID := "ed25519:" + string(testHubRecipient) + ":v10"
			if test.revoked {
				_, replacement := registerAdditionalFederationKey(
					t, f.store, f.destination, testHubRecipient, replacementID,
				)
				require.Equal(t, registry.OutcomeSuspended, replacement.Outcome)
				require.NoError(t, f.destination.Review(
					t.Context(), f.store.DB(), testHubRecipient,
					registry.ReviewDecision{Action: registry.ReviewRevoke},
				))
				require.Equal(t, "REVOKED", federationAgentKeyStatus(t, f.store, localKeyID))
				require.Equal(t, "REJECTED", federationAgentKeyStatus(t, f.store, replacementID))
			}

			remoteKeyID := "ed25519:" + string(testHubRecipient) + ":v1"
			remoteKey, remoteRecord := registerFederationAgent(
				t, f.originStore, f.origin, f.clock.now, federationTestOriginNode,
				testHubRecipient, "session-origin-local-claim", remoteKeyID,
			)
			request := federationDeliveryRequest(
				t, f.clock.now, remoteKey, remoteRecord,
				[]MessageRecipient{{
					Agent:        federationTestDisplayRecipient,
					DeliveryMode: protocol.DeliveryModeWhenSafe,
					RequiresAck:  true,
				}},
			)
			require.NoError(t, request.Validate())
			require.Zero(t, federationNodeKeyCount(t, f.store, federationTestOriginNode))

			status, body := f.post(t, request)
			require.Equal(t, http.StatusForbidden, status)
			requireFederationErrorCode(t, body, protocol.ErrorCodeKeyUnavailable)
			requireNoFederatedMessageRows(t, f)
			require.Zero(t, federationTableCount(t, f.store, "channel_control_decisions"))
			require.Zero(t, federationNodeKeyCount(t, f.store, federationTestOriginNode))
			route, err := f.destination.Route(t.Context(), f.store.DB(), testHubRecipient)
			require.NoError(t, err)
			require.Equal(t, registry.RouteResult{
				Kind: registry.RouteLocal, Node: testHubNode,
			}, route)

			if test.revoked {
				_, err = f.destination.ActiveSigningKey(t.Context(), f.store.DB(), testHubRecipient)
				require.ErrorIs(t, err, registry.ErrNoUsableKey)
				blocked, blockedErr := f.destination.Blocked(
					t.Context(), f.store.DB(), testHubRecipient,
				)
				require.NoError(t, blockedErr)
				require.True(t, blocked)
				require.Equal(t, "REVOKED", federationAgentKeyStatus(t, f.store, localKeyID))
				require.Equal(t, "REJECTED", federationAgentKeyStatus(t, f.store, replacementID))
			} else {
				after, activeErr := f.destination.ActiveSigningKey(
					t.Context(), f.store.DB(), testHubRecipient,
				)
				require.NoError(t, activeErr)
				require.Equal(t, localKey.KeyID, after.KeyID)
				require.Equal(t, localKey.PublicKey, after.PublicKey)
			}
		})
	}
}

func TestFederatedDeliveryAppliesPolicyAndChannelOutcomes(t *testing.T) {
	t.Run("policy deny", func(t *testing.T) {
		f := newFederatedHubFixture(t, false)
		status, raw := f.post(t, f.request)
		require.Equal(t, http.StatusForbidden, status)
		f.receipt(t, raw, federation.DeliveryReceiptControlDenied)
		requireNoFederatedMessageRows(t, f)
		require.Zero(t, federationTableCount(t, f.store, "channel_control_decisions"))
	})

	t.Run("channel deny", func(t *testing.T) {
		f := newFederatedHubFixture(t, true)
		setFederationTestLink(t, f, protocol.LinkModeDeny)
		status, raw := f.post(t, f.request)
		require.Equal(t, http.StatusForbidden, status)
		f.receipt(t, raw, federation.DeliveryReceiptControlDenied)
		requireNoFederatedMessageRows(t, f)
		require.Equal(t, int64(1), federationTableCount(t, f.store, "channel_control_decisions"))
	})

	for _, test := range []struct {
		name  string
		mode  protocol.LinkMode
		state protocol.ControlState
	}{
		{"hold", protocol.LinkModeHold, protocol.ControlStateHeld},
		{"approval", protocol.LinkModeRequireApproval, protocol.ControlStateApprovalRequired},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newFederatedHubFixture(t, true)
			setFederationTestLink(t, f, test.mode)
			status, raw := f.post(t, f.request)
			require.Equal(t, http.StatusOK, status)
			f.receipt(t, raw, federation.DeliveryReceiptStoredRemote)
			require.Equal(t, int64(1), federationTableCount(t, f.store, "messages"))
			require.Equal(t, int64(1), federationTableCount(t, f.store, "federation_inbox"))
			require.Zero(t, federationTableCount(t, f.store, "delivery_queue"))
			var state protocol.ControlState
			require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
				"SELECT control_state FROM messages WHERE message_id = ?", federationTestMessage,
			).Scan(&state))
			require.Equal(t, test.state, state)
		})
	}
}

func TestFederatedDeliveryRollsBackAnInterruptedTransaction(t *testing.T) {
	f := newFederatedHubFixture(t, true)
	_, err := f.store.DB().ExecContext(t.Context(), `CREATE TRIGGER fail_federation_inbox
		BEFORE INSERT ON federation_inbox
		BEGIN
			SELECT RAISE(ABORT, 'injected federation inbox failure');
		END`)
	require.NoError(t, err)

	status, _ := f.post(t, f.request)
	require.Equal(t, http.StatusInternalServerError, status)
	requireNoFederatedMessageRows(t, f)
	require.Zero(t, federationTableCount(t, f.store, "channel_control_decisions"))
	require.Equal(t, int64(1), federationTableCount(t, f.store, "agent_keys"))
}

func setFederationTestLink(t *testing.T, f *federatedHubFixture, mode protocol.LinkMode) {
	t.Helper()
	versionKey := "link:" + string(federationTestSender) + "→" + string(testHubRecipient) + ":message"
	_, err := f.controller.SetLink(t.Context(), protocol.SetLinkParams{
		FromAgent: federationTestSender, ToAgent: testHubRecipient,
		Mode: mode, Plane: protocol.PlaneMessage,
		ExpectedVersions: protocol.ControlVersions{versionKey: 0},
	}, channel.Actor{Type: protocol.ActorTypeHuman, ID: testOperatorID})
	require.NoError(t, err)
}

func TestFederatedDeliveryEnforcesSequenceWindowBoundary(t *testing.T) {
	f := newFederatedHubFixture(t, true)
	stamp := eventstore.FormatTime(f.clock.now)
	_, err := f.store.DB().ExecContext(t.Context(), `INSERT INTO federation_inbox
		(origin_node, sequence, nonce, message_id, delivery_generation,
		 request_sha256, envelope_sha256, receipt, received_at)
		VALUES (?, 2000, ?, ?, 1, ?, ?, 'prior-receipt', ?)`,
		federationTestOriginNode, bytes.Repeat([]byte{0x41}, 16), federationTestOtherMessage,
		strings.Repeat("a", 64), strings.Repeat("b", 64), stamp)
	require.NoError(t, err)

	tooOld := f.request
	tooOld.Sequence = 975
	status, body := f.post(t, tooOld)
	require.Equal(t, http.StatusConflict, status)
	requireFederationErrorCode(t, body, protocol.ErrorCodeControlVersionConflict)
	require.Zero(t, federationTableCount(t, f.store, "agent_keys"))

	boundary := f.request
	boundary.Sequence = 976
	status, raw := f.post(t, boundary)
	require.Equal(t, http.StatusOK, status)
	f.receipt(t, raw, federation.DeliveryReceiptStoredRemote)
	require.Equal(t, int64(2), federationTableCount(t, f.store, "federation_inbox"))
}

func TestFederatedDeliveryPersistsSenderKeyCollisionQuarantine(t *testing.T) {
	f := newFederatedHubFixture(t, true)
	imported, err := f.destination.ImportDirectory(
		t.Context(), f.store.DB(), federationTestOriginNode,
		[]registry.DirectoryRecord{f.senderRecord},
	)
	require.NoError(t, err)
	require.Equal(t, 1, imported.Inserted)

	alternativeStore, err := eventstore.Open(t.Context(), filepath.Join(t.TempDir(), "alternative.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, alternativeStore.Close()) })
	alternativeRegistry, err := registry.New(federationTestOriginNode, f.originDataDir, f.clock)
	require.NoError(t, err)
	require.NoError(t, alternativeRegistry.EnsureLocalNode(
		t.Context(), alternativeStore.DB(), "org-origin",
	))
	alternativeKey, alternativeRecord := registerFederationSender(
		t, alternativeStore, alternativeRegistry, f.clock.now,
	)
	require.False(t, bytes.Equal(f.senderRecord.PublicKey, alternativeRecord.PublicKey))
	request := federationDeliveryRequest(t, f.clock.now, alternativeKey, alternativeRecord, []MessageRecipient{{
		Agent: testHubRecipient, DeliveryMode: protocol.DeliveryModeWhenSafe, RequiresAck: true,
	}})
	status, body := f.post(t, request)
	require.Equal(t, http.StatusForbidden, status)
	requireFederationErrorCode(t, body, protocol.ErrorCodeKeyUnavailable)
	requireNoFederatedMessageRows(t, f)
	peer, err := f.destination.Peer(t.Context(), f.store.DB(), federationTestOriginNode)
	require.NoError(t, err)
	require.Equal(t, registry.PeerStatusSuspended, peer.Status)
	require.Equal(t, int64(1), federationTableCount(t, f.store, "agent_keys"))
	var storedPublic []byte
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
		"SELECT public_key FROM agent_keys WHERE key_id = ?", federationTestKeyID,
	).Scan(&storedPublic))
	require.Equal(t, f.senderRecord.PublicKey, storedPublic)
}
