package federation

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	giffordcrypto "github.com/smellak/gifford/internal/crypto"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
)

const (
	probeTestLocalNode  = protocol.NodeID("node-probe-a")
	probeTestRemoteNode = protocol.NodeID("node-probe-b")
	probeTestAgent      = protocol.AgentID("agent-probe-a")
	probeTestRecipient  = protocol.AgentID("agent-probe-b")
)

type probeTestClock struct{ now time.Time }

func (c probeTestClock) Now() time.Time { return c.now }

type probeTestReceiver struct {
	mu                 sync.Mutex
	allowForgedOrigin  bool
	exactStoredRequest []byte
	exactStoredReceipt []byte
	effects            int
}

func (r *probeTestReceiver) ReceiveDelivery(
	_ context.Context,
	authenticatedPeer protocol.NodeID,
	request DeliveryRequest,
) ([]byte, error) {
	envelope, _, err := request.DecodeEnvelope()
	if err != nil {
		return nil, NewDeliveryError(DeliveryBadRequest, "", nil, err)
	}
	if envelope.OriginNode != authenticatedPeer {
		if r.allowForgedOrigin {
			return []byte(`{"accepted":true}`), nil
		}
		return nil, NewDeliveryError(
			DeliveryForbidden, protocol.ErrorCodeNotFound, nil,
			context.Canceled,
		)
	}
	if err := request.Validate(); err != nil {
		return nil, NewDeliveryError(DeliveryForbidden, "", nil, err)
	}
	if err := giffordcrypto.VerifyWireEnvelope(request.Sender.PublicKey, envelope); err != nil {
		return nil, NewDeliveryError(DeliveryBadRequest, "", nil, err)
	}
	exact, err := request.CanonicalBytes()
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if bytes.Equal(exact, r.exactStoredRequest) {
		return bytes.Clone(r.exactStoredReceipt), nil
	}
	r.effects++
	return bytes.Clone(r.exactStoredReceipt), nil
}

func (r *probeTestReceiver) effectCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.effects
}

type probeTestFixture struct {
	dataDir     string
	endpoint    string
	fingerprint string
	clock       probeTestClock
	receiver    *probeTestReceiver
}

func newProbeTestFixture(t *testing.T, allowForgedOrigin bool) probeTestFixture {
	t.Helper()
	clk := probeTestClock{now: time.Date(2026, time.October, 30, 16, 0, 0, 0, time.UTC)}
	localDir := privateDataDir(t)
	remoteDir := privateDataDir(t)
	localIdentity, err := EnsureNodeTLS(localDir, probeTestLocalNode)
	require.NoError(t, err)
	remoteIdentity, err := EnsureNodeTLS(remoteDir, probeTestRemoteNode)
	require.NoError(t, err)
	localStore, err := eventstore.Open(t.Context(), filepath.Join(localDir, "gifford.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, localStore.Close()) })
	remoteStore, err := eventstore.Open(t.Context(), filepath.Join(remoteDir, "gifford.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, remoteStore.Close()) })
	localRegistry, err := registry.New(probeTestLocalNode, localDir, clk)
	require.NoError(t, err)
	remoteRegistry, err := registry.New(probeTestRemoteNode, remoteDir, clk)
	require.NoError(t, err)
	require.NoError(t, localRegistry.EnsureLocalNode(t.Context(), localStore.DB(), "org-probe-a"))
	require.NoError(t, remoteRegistry.EnsureLocalNode(t.Context(), remoteStore.DB(), "org-probe-b"))
	localNodeKey, err := localRegistry.NodeSigningPublicKey()
	require.NoError(t, err)
	remoteNodeKey, err := remoteRegistry.NodeSigningPublicKey()
	require.NoError(t, err)
	_, err = localRegistry.AddPeer(
		t.Context(), localStore.DB(), probeTestRemoteNode, "org-probe-b", remoteNodeKey,
	)
	require.NoError(t, err)
	_, err = remoteRegistry.AddPeer(
		t.Context(), remoteStore.DB(), probeTestLocalNode, "org-probe-a", localNodeKey,
	)
	require.NoError(t, err)
	require.NoError(t, localRegistry.SetPeerTransport(
		t.Context(), localStore.DB(), probeTestRemoteNode,
		"https://node-probe-b.example.invalid:7443", remoteIdentity.Fingerprint,
	))
	require.NoError(t, remoteRegistry.SetPeerTransport(
		t.Context(), remoteStore.DB(), probeTestLocalNode,
		"https://node-probe-a.example.invalid:7443", localIdentity.Fingerprint,
	))

	request := probeSignedRequest(t, clk.now)
	exactRequest, err := request.CanonicalBytes()
	require.NoError(t, err)
	exactReceipt := probeSignedReceipt(t, clk.now, request, remoteRegistry)
	seedProbeDelivery(t, localStore, clk.now, request, exactRequest, exactReceipt)
	receiver := &probeTestReceiver{
		allowForgedOrigin: allowForgedOrigin, exactStoredRequest: exactRequest,
		exactStoredReceipt: exactReceipt, effects: 1,
	}
	server, err := NewServer(ServerOptions{
		Listen: loopbackEphemeral, Identity: remoteIdentity,
		TrustBundle: mustProbeBundle(t, localIdentity), DB: remoteStore.DB(),
		Clock: clk, Delivery: receiver,
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
	return probeTestFixture{
		dataDir: localDir, endpoint: "https://" + address,
		fingerprint: remoteIdentity.Fingerprint, clock: clk, receiver: receiver,
	}
}

func mustProbeBundle(t *testing.T, identities ...*NodeTLS) *TrustBundle {
	t.Helper()
	bundle, err := LoadTrustBundle(writeTrustBundle(t, identities...))
	require.NoError(t, err)
	return bundle
}

func probeSignedRequest(t *testing.T, now time.Time) DeliveryRequest {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	ciphertext := []byte("opaque fedprobe test payload")
	ciphertextDigest := sha256.Sum256(ciphertext)
	envelope := protocol.WireEnvelope{
		Protocol: protocol.WireProtocol, Version: protocol.WireVersion,
		MessageID: "01ARZ3NDEKTSV4RRFFQ69G5FPR", ClientMsgID: "client-fedprobe",
		OriginNode: probeTestLocalNode, OriginAgent: probeTestAgent,
		DestinationNode: probeTestRemoteNode, RecipientAgents: []protocol.AgentID{probeTestRecipient},
		CreatedAt: now.Add(-time.Minute), ExpiresAt: now.Add(time.Hour), Priority: protocol.PriorityNormal,
		ThreadRef: "thr_01ARZ3NDEKTSV4RRFFQ69G5FPR", HopCount: 0,
		ControlVersion: 1, ContentType: protocol.WireContentType,
		EncryptionProfile:     protocol.EncryptionProfileNone,
		SenderSigningKeyID:    "ed25519:agent-probe-a:v1",
		RecipientKeyEnvelopes: []protocol.RecipientKeyEnvelope{},
		Ciphertext:            ciphertext, CiphertextSHA256: protocol.SHA256Hex(hex.EncodeToString(ciphertextDigest[:])),
	}
	envelope.RouteDigest = probeRouteDigest(
		envelope.MessageID, envelope.OriginNode, envelope.OriginAgent, envelope.RecipientAgents,
	)
	require.NoError(t, giffordcrypto.SignWireEnvelope(privateKey, &envelope))
	exactEnvelope, err := json.Marshal(envelope)
	require.NoError(t, err)
	return DeliveryRequest{
		Envelope: exactEnvelope, ThreadID: "thread-fedprobe",
		Recipients: []DeliveryRecipient{{
			Agent: probeTestRecipient, Mode: protocol.DeliveryModeWhenSafe, RequiresAck: true,
		}},
		DeliveryGeneration: 1, Nonce: bytes.Repeat([]byte{0x45}, deliveryNonceBytes), Sequence: 1,
		OriginControl: protocol.ObjectControlState{
			ObjectType: protocol.DecisionObjectTypeMessage,
			ObjectID:   protocol.OpaqueID(envelope.MessageID), ControlState: protocol.ControlStateAllowed,
			ControlReason: protocol.DecisionReasonAllowed, ControlVersion: 1,
		},
		Sender: registry.DirectoryRecord{
			KeyID: "ed25519:agent-probe-a:v1", Agent: probeTestAgent, Node: probeTestLocalNode,
			Purpose: "signing", Algorithm: "ed25519", PublicKey: publicKey, Status: "ACTIVE",
			ValidFrom: eventstore.FormatTime(now.Add(-time.Hour)), Issuer: "node:" + string(probeTestLocalNode),
			RecordSignature: bytes.Repeat([]byte{0x46}, ed25519.SignatureSize),
		},
	}
}

func probeSignedReceipt(
	t *testing.T,
	now time.Time,
	request DeliveryRequest,
	signer *registry.Registry,
) []byte {
	t.Helper()
	envelope, _, err := request.DecodeEnvelope()
	require.NoError(t, err)
	requestDigest, err := request.RequestSHA256()
	require.NoError(t, err)
	envelopeDigest, err := request.EnvelopeSHA256()
	require.NoError(t, err)
	receipt := DeliveryReceipt{
		MessageID: envelope.MessageID, OriginNode: envelope.OriginNode,
		DestinationNode: envelope.DestinationNode, RecipientAgents: envelope.RecipientAgents,
		DeliveryGeneration: request.DeliveryGeneration, Nonce: bytes.Clone(request.Nonce),
		Sequence: request.Sequence, EnvelopeSHA256: envelopeDigest, RequestSHA256: requestDigest,
		Status: DeliveryReceiptStoredRemote, ReceivedAt: now,
	}
	require.NoError(t, receipt.Sign(signer))
	exact, err := json.Marshal(receipt)
	require.NoError(t, err)
	return exact
}

func seedProbeDelivery(
	t *testing.T,
	store *eventstore.Store,
	now time.Time,
	request DeliveryRequest,
	exactRequest, exactReceipt []byte,
) {
	t.Helper()
	envelope, _, err := request.DecodeEnvelope()
	require.NoError(t, err)
	requestDigest, err := request.RequestSHA256()
	require.NoError(t, err)
	stamp := eventstore.FormatTime(now)
	const outboxID = "message:fedprobe:1"
	_, err = store.DB().ExecContext(t.Context(), `INSERT INTO federation_outbox
		(outbox_id, object_type, object_id, destination_node, state, attempts,
		 idempotency_key, created_at, updated_at)
		VALUES (?, 'message', ?, ?, 'STORED_REMOTE', 1, ?, ?, ?)`,
		outboxID, envelope.MessageID, envelope.DestinationNode, "fedprobe:1", stamp, stamp)
	require.NoError(t, err)
	_, err = store.DB().ExecContext(t.Context(), `INSERT INTO federation_outbox_requests
		(outbox_id, destination_node, request, request_sha256, sequence, nonce)
		VALUES (?, ?, ?, ?, ?, ?)`, outboxID, envelope.DestinationNode, exactRequest,
		requestDigest, request.Sequence, request.Nonce)
	require.NoError(t, err)
	_, err = store.DB().ExecContext(t.Context(), `INSERT INTO federation_receipts
		(outbox_id, receipt, state, received_at) VALUES (?, ?, 'STORED_REMOTE', ?)`,
		outboxID, exactReceipt, stamp)
	require.NoError(t, err)
}

func TestFederationProbeExercisesLiveNegativeMatrix(t *testing.T) {
	t.Parallel()
	fixture := newProbeTestFixture(t, false)
	var output bytes.Buffer
	err := RunProbe(t.Context(), ProbeOptions{
		Endpoint: fixture.endpoint, Fingerprint: fixture.fingerprint,
		DataDir: fixture.dataDir, Timeout: 5 * time.Second, Clock: fixture.clock, Output: &output,
	})
	require.NoError(t, err)
	require.Equal(t, strings.Join([]string{
		"PASS tls-1.3",
		"PASS tls-1.2-rejected",
		"PASS no-client-certificate",
		"PASS unenrolled-client-certificate",
		"PASS forged-origin",
		"PASS invalid-signature",
		"PASS exact-replay",
		"",
	}, "\n"), output.String())
	require.Equal(t, 1, fixture.receiver.effectCount())
}

func TestFederationProbeFailsWhenPeerAcceptsForgedOrigin(t *testing.T) {
	t.Parallel()
	fixture := newProbeTestFixture(t, true)
	var output, diagnostics bytes.Buffer
	status := ProbeMain([]string{
		"--endpoint", fixture.endpoint,
		"--fingerprint", fixture.fingerprint,
		"--data-dir", fixture.dataDir,
		"--timeout", "5s",
	}, &output, &diagnostics)
	require.Equal(t, 1, status)
	require.Empty(t, diagnostics.String())
	require.Contains(t, output.String(), "FAIL forged-origin: forged origin returned HTTP 200\n")
	require.NotContains(t, output.String(), "FAIL invalid-signature")
	require.NotContains(t, output.String(), "FAIL exact-replay")
}

var _ DeliveryReceiver = (*probeTestReceiver)(nil)
