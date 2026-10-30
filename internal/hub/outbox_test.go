package hub

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/channel"
	giffordcrypto "github.com/smellak/gifford/internal/crypto"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/federation"
	"github.com/smellak/gifford/internal/policy"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
)

const (
	outboxOriginNode      = protocol.NodeID("node-outbox-a")
	outboxDestinationNode = protocol.NodeID("node-outbox-b")
	outboxSender          = protocol.AgentID("agent-outbox-sender")
	outboxRecipient       = protocol.AgentID("agent-outbox-recipient")
	outboxLocalRecipient  = protocol.AgentID("agent-outbox-local")
)

type outboundFederationFixture struct {
	clock          *hubClock
	store          *eventstore.Store
	remote         *eventstore.Store
	service        *Service
	controller     *channel.Controller
	senderKey      ed25519.PrivateKey
	localRegistry  *registry.Registry
	remoteRegistry *registry.Registry
	originIdentity *federation.NodeTLS
	remoteIdentity *federation.NodeTLS
}

type outboundZeroReader struct{}

func (outboundZeroReader) Read(buffer []byte) (int, error) {
	for i := range buffer {
		buffer[i] = 0
	}
	return len(buffer), nil
}

type outboundLostReceiptReceiver struct {
	delegate federation.DeliveryReceiver
	requests [][]byte
	receipts [][]byte
}

func (r *outboundLostReceiptReceiver) ReceiveDelivery(
	ctx context.Context,
	peer protocol.NodeID,
	request federation.DeliveryRequest,
) ([]byte, error) {
	exact, err := request.CanonicalBytes()
	if err != nil {
		return nil, err
	}
	r.requests = append(r.requests, append([]byte(nil), exact...))
	receipt, err := r.delegate.ReceiveDelivery(ctx, peer, request)
	if err != nil {
		return nil, err
	}
	r.receipts = append(r.receipts, append([]byte(nil), receipt...))
	if len(r.receipts) == 1 {
		panic(http.ErrAbortHandler)
	}
	return receipt, nil
}

func newOutboundFederationFixture(t *testing.T) *outboundFederationFixture {
	t.Helper()
	now := time.Date(2026, time.October, 29, 23, 20, 0, 0, time.UTC)
	clk := &hubClock{now: now}
	originDir := privateHubDataDir(t)
	remoteDir := privateHubDataDir(t)
	originIdentity, err := federation.EnsureNodeTLS(originDir, outboxOriginNode)
	require.NoError(t, err)
	remoteIdentity, err := federation.EnsureNodeTLS(remoteDir, outboxDestinationNode)
	require.NoError(t, err)
	store, err := eventstore.Open(t.Context(), filepath.Join(originDir, "gifford.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	remoteStore, err := eventstore.Open(t.Context(), filepath.Join(remoteDir, "gifford.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, remoteStore.Close()) })
	localRegistry, err := registry.New(outboxOriginNode, originDir, clk)
	require.NoError(t, err)
	remoteRegistry, err := registry.New(outboxDestinationNode, remoteDir, clk)
	require.NoError(t, err)
	require.NoError(t, localRegistry.EnsureLocalNode(t.Context(), store.DB(), "org-outbox-a"))
	require.NoError(t, remoteRegistry.EnsureLocalNode(t.Context(), remoteStore.DB(), "org-outbox-b"))
	remoteNodeKey, err := remoteRegistry.NodeSigningPublicKey()
	require.NoError(t, err)
	localNodeKey, err := localRegistry.NodeSigningPublicKey()
	require.NoError(t, err)
	_, err = localRegistry.AddPeer(
		t.Context(), store.DB(), outboxDestinationNode, "org-outbox-b", remoteNodeKey,
	)
	require.NoError(t, err)
	_, err = remoteRegistry.AddPeer(
		t.Context(), remoteStore.DB(), outboxOriginNode, "org-outbox-a", localNodeKey,
	)
	require.NoError(t, err)

	senderPrivate := registerOutboundAgent(
		t, store, localRegistry, outboxOriginNode, outboxSender,
	)
	registerOutboundAgent(t, remoteStore, remoteRegistry, outboxDestinationNode, outboxRecipient)
	remoteDirectory, err := remoteRegistry.ExportDirectory(t.Context(), remoteStore.DB())
	require.NoError(t, err)
	require.Len(t, remoteDirectory, 1)
	_, err = localRegistry.ImportDirectory(
		t.Context(), store.DB(), outboxDestinationNode, remoteDirectory,
	)
	require.NoError(t, err)

	engine, err := channel.New(channel.Options{Clock: clk})
	require.NoError(t, err)
	controller, err := channel.NewController(channel.ControllerOptions{
		DB: store.DB(), Engine: engine, Clock: clk, NodeID: outboxOriginNode,
	})
	require.NoError(t, err)
	require.NoError(t, controller.Reload(t.Context()))
	service, err := NewService(ServiceOptions{
		DB: store.DB(), NodeID: outboxOriginNode, Engine: engine,
		Registry: localRegistry, Clock: clk, FederationRegistry: localRegistry,
	})
	require.NoError(t, err)
	return &outboundFederationFixture{
		clock: clk, store: store, remote: remoteStore, service: service,
		controller: controller, senderKey: senderPrivate,
		localRegistry: localRegistry, remoteRegistry: remoteRegistry,
		originIdentity: originIdentity, remoteIdentity: remoteIdentity,
	}
}

func privateHubDataDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0o700))
	return dir
}

func registerOutboundAgent(
	t *testing.T,
	store *eventstore.Store,
	reg *registry.Registry,
	node protocol.NodeID,
	agent protocol.AgentID,
) ed25519.PrivateKey {
	t.Helper()
	process, err := registry.CurrentProcess()
	require.NoError(t, err)
	processJSON, err := json.Marshal(process)
	require.NoError(t, err)
	stamp := eventstore.FormatTime(time.Date(2026, time.October, 29, 23, 0, 0, 0, time.UTC))
	_, err = store.DB().ExecContext(t.Context(), `INSERT INTO agents
		(agent_id, node_id, harness, created_at, updated_at)
		VALUES (?, ?, 'test', ?, ?)`, agent, node, stamp, stamp)
	require.NoError(t, err)
	_, err = store.DB().ExecContext(t.Context(), `INSERT INTO sessions
		(session_id, agent_id, state, capture_mode, deliverable, manifest_json,
		 process_json, started_at, updated_at)
		VALUES (?, ?, 'CONNECTED', 'pty_proxy', 1, '{}', ?, ?, ?)`,
		"session-"+string(agent), agent, string(processJSON), stamp, stamp)
	require.NoError(t, err)
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	keyID := "ed25519:" + string(agent) + ":v1"
	_, err = reg.Register(t.Context(), store.DB(), registry.RegisterRequest{
		Agent: agent, Process: process, PublicKey: publicKey, KeyID: keyID,
	})
	require.NoError(t, err)
	return privateKey
}

func (f *outboundFederationFixture) params(clientID protocol.OpaqueID) AdmitMessageParams {
	return AdmitMessageParams{
		ClientMsgID: clientID,
		Recipients: []MessageRecipient{{
			Agent: outboxRecipient, DeliveryMode: protocol.DeliveryModeWhenSafe, RequiresAck: true,
		}},
		ThreadID: "thread-" + protocol.ThreadID(clientID), Priority: protocol.PriorityNormal,
		CreatedAt: f.clock.now, ExpiresAt: f.clock.now.Add(time.Hour),
	}
}

func (f *outboundFederationFixture) admit(
	t *testing.T,
	params AdmitMessageParams,
) (AdmitMessageResult, error) {
	t.Helper()
	raw, err := json.Marshal(params)
	require.NoError(t, err)
	answer, callErr := f.service.AdmitMessage(t.Context(), outboxSender, raw)
	if callErr != nil {
		return AdmitMessageResult{}, callErr
	}
	return answer.(AdmitMessageResult), nil
}

func (f *outboundFederationFixture) envelope(
	t *testing.T,
	params AdmitMessageParams,
	admitted AdmitMessageResult,
) protocol.WireEnvelope {
	t.Helper()
	ciphertext := []byte("opaque outbound federation payload")
	digest := sha256.Sum256(ciphertext)
	recipients := make([]protocol.AgentID, len(params.Recipients))
	for i, recipient := range params.Recipients {
		recipients[i] = recipient.Agent
	}
	envelope := protocol.WireEnvelope{
		Protocol: protocol.WireProtocol, Version: protocol.WireVersion,
		MessageID: admitted.MessageID, ClientMsgID: params.ClientMsgID,
		OriginNode: admitted.OriginNode, OriginAgent: outboxSender,
		DestinationNode: admitted.DestinationNode, RecipientAgents: recipients,
		CreatedAt: params.CreatedAt, ExpiresAt: params.ExpiresAt, Priority: params.Priority,
		ThreadRef: admitted.ThreadRef, ParentMessageID: params.ParentMessageID,
		HopCount: admitted.HopCount, RouteDigest: admitted.RouteDigest,
		ControlVersion: admitted.AdmissionResult.ControlVersion,
		ContentType:    protocol.WireContentType, EncryptionProfile: protocol.EncryptionProfileNone,
		SenderSigningKeyID:    protocol.SigningKeyID("ed25519:" + string(outboxSender) + ":v1"),
		RecipientKeyEnvelopes: []protocol.RecipientKeyEnvelope{},
		Ciphertext:            ciphertext, CiphertextSHA256: protocol.SHA256Hex(hex.EncodeToString(digest[:])),
	}
	require.NoError(t, giffordcrypto.SignWireEnvelope(f.senderKey, &envelope))
	require.NoError(t, envelope.Validate())
	return envelope
}

func (f *outboundFederationFixture) remoteService(t *testing.T) *Service {
	t.Helper()
	remoteEngine, err := channel.New(channel.Options{Clock: f.clock})
	require.NoError(t, err)
	remoteController, err := channel.NewController(channel.ControllerOptions{
		DB: f.remote.DB(), Engine: remoteEngine, Clock: f.clock, NodeID: outboxDestinationNode,
	})
	require.NoError(t, err)
	require.NoError(t, remoteController.Reload(t.Context()))
	service, err := NewService(ServiceOptions{
		DB: f.remote.DB(), NodeID: outboxDestinationNode, Engine: remoteEngine,
		Registry: f.remoteRegistry, Clock: f.clock,
		FederationRegistry: f.remoteRegistry, Policy: outboundFederationPolicy(t),
	})
	require.NoError(t, err)
	require.NoError(t, f.remoteRegistry.SetPeerTransport(
		t.Context(), f.remote.DB(), outboxOriginNode,
		"https://origin.example.invalid:7443", f.originIdentity.Fingerprint,
	))
	return service
}

func (f *outboundFederationFixture) server(
	t *testing.T,
	listen string,
	receiver federation.DeliveryReceiver,
) (*federation.Server, string) {
	t.Helper()
	server, err := federation.NewServer(federation.ServerOptions{
		Listen: listen, Identity: f.remoteIdentity,
		TrustBundle: federationTestBundle(t, f.originIdentity),
		DB:          f.remote.DB(), Clock: f.clock, Delivery: receiver, Directory: f.remoteRegistry,
	})
	require.NoError(t, err)
	address, err := server.Listen(t.Context())
	require.NoError(t, err)
	return server, address
}

func startOutboundFederationServer(t *testing.T, server *federation.Server) func() {
	t.Helper()
	serveCtx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- server.Serve(serveCtx) }()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			require.NoError(t, <-done)
		})
	}
	t.Cleanup(stop)
	return stop
}

func (f *outboundFederationFixture) replicator(
	t *testing.T,
	address string,
) *federation.Replicator {
	t.Helper()
	require.NoError(t, f.localRegistry.SetPeerTransport(
		t.Context(), f.store.DB(), outboxDestinationNode,
		"https://"+address, f.remoteIdentity.Fingerprint,
	))
	replicator, err := federation.NewReplicator(federation.ReplicatorOptions{
		DB: f.store.DB(), Registry: f.localRegistry, Identity: f.originIdentity,
		TrustBundle: federationTestBundle(t, f.remoteIdentity), Clock: f.clock,
		Random: outboundZeroReader{}, LeaseOwner: "e2e-worker",
	})
	require.NoError(t, err)
	return replicator
}

func (f *outboundFederationFixture) queue(
	t *testing.T,
	clientID protocol.OpaqueID,
	offset time.Duration,
) (protocol.ULID, []byte) {
	t.Helper()
	params := f.params(clientID)
	params.CreatedAt = params.CreatedAt.Add(offset)
	params.ExpiresAt = params.ExpiresAt.Add(offset)
	admitted, err := f.admit(t, params)
	require.NoError(t, err)
	envelope := f.envelope(t, params, admitted)
	exactEnvelope, err := json.Marshal(envelope)
	require.NoError(t, err)
	_, err = f.service.SubmitMessageEnvelope(t.Context(), outboxSender, exactEnvelope)
	require.NoError(t, err)
	var request []byte
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(), `SELECT request.request
		FROM federation_outbox_requests AS request
		JOIN federation_outbox AS outbox ON outbox.outbox_id = request.outbox_id
		WHERE outbox.object_id = ?`, admitted.MessageID).Scan(&request))
	return admitted.MessageID, request
}

func TestOutboundFederationAdmissionQueuesCanonicalRequest(t *testing.T) {
	f := newOutboundFederationFixture(t)
	params := f.params("client-outbox-1")
	admitted, err := f.admit(t, params)
	require.NoError(t, err)
	require.Equal(t, outboxOriginNode, admitted.OriginNode)
	require.Equal(t, outboxDestinationNode, admitted.DestinationNode)
	envelope := f.envelope(t, params, admitted)
	exactEnvelope, err := json.Marshal(envelope)
	require.NoError(t, err)
	answer, err := f.service.SubmitMessageEnvelope(t.Context(), outboxSender, exactEnvelope)
	require.NoError(t, err)
	submitted := answer.(SubmitMessageEnvelopeResult)
	require.Equal(t, messageStateQueuedRemote, submitted.State)
	require.Equal(t, protocol.ControlStateAllowed, submitted.ControlState)

	var exactRequest []byte
	var requestSHA protocol.SHA256Hex
	var sequence int64
	var nonce []byte
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(), `SELECT request,
		request_sha256, sequence, nonce FROM federation_outbox_requests`).Scan(
		&exactRequest, &requestSHA, &sequence, &nonce))
	require.Equal(t, int64(1), sequence)
	require.Len(t, nonce, 16)
	request, err := federation.DecodeDeliveryRequest(exactRequest)
	require.NoError(t, err)
	require.Equal(t, exactEnvelope, []byte(request.Envelope))
	require.Equal(t, params.ThreadID, request.ThreadID)
	require.Equal(t, []federation.DeliveryRecipient{{
		Agent: outboxRecipient, Mode: protocol.DeliveryModeWhenSafe, RequiresAck: true,
	}}, request.Recipients)
	require.Equal(t, int64(1), request.DeliveryGeneration)
	require.Equal(t, sequence, request.Sequence)
	require.Equal(t, nonce, request.Nonce)
	require.Equal(t, protocol.ObjectControlState{
		ObjectType: protocol.DecisionObjectTypeMessage,
		ObjectID:   protocol.OpaqueID(admitted.MessageID), ControlState: submitted.ControlState,
		ControlReason: submitted.ControlReason, ControlVersion: submitted.ControlVersion,
	}, request.OriginControl)
	require.Equal(t, "ed25519:"+string(outboxSender)+":v1", request.Sender.KeyID)
	computedSHA, err := request.RequestSHA256()
	require.NoError(t, err)
	require.Equal(t, requestSHA, computedSHA)
	canonical, err := request.CanonicalBytes()
	require.NoError(t, err)
	require.Equal(t, canonical, exactRequest)

	var messageState, outboxState, objectType, objectID, idempotency string
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(), `SELECT message.state,
		outbox.state, outbox.object_type, outbox.object_id, outbox.idempotency_key
		FROM messages AS message JOIN federation_outbox AS outbox ON outbox.object_id = message.message_id`).
		Scan(&messageState, &outboxState, &objectType, &objectID, &idempotency))
	require.Equal(t, messageStateQueuedRemote, messageState)
	require.Equal(t, "QUEUED", outboxState)
	require.Equal(t, "message", objectType)
	require.Equal(t, string(admitted.MessageID), objectID)
	require.Equal(t, string(admitted.MessageID)+":1", idempotency)
	require.Zero(t, federationTableCount(t, f.store, "delivery_queue"))

	answer, err = f.service.SubmitMessageEnvelope(t.Context(), outboxSender, exactEnvelope)
	require.NoError(t, err)
	require.Equal(t, messageStateQueuedRemote, answer.(SubmitMessageEnvelopeResult).State)
	require.Equal(t, int64(1), federationTableCount(t, f.store, "federation_outbox"))
}

func TestOutboundFederationHoldCreatesNoOutbox(t *testing.T) {
	f := newOutboundFederationFixture(t)
	_, err := f.controller.SetLink(t.Context(), protocol.SetLinkParams{
		FromAgent: outboxSender, ToAgent: outboxRecipient,
		Mode: protocol.LinkModeHold, Plane: protocol.PlaneMessage,
		ExpectedVersions: protocol.ControlVersions{
			"link:" + string(outboxSender) + "→" + string(outboxRecipient) + ":message": 0,
		},
	}, channel.Actor{Type: protocol.ActorTypeHuman, ID: "operator-outbox"})
	require.NoError(t, err)
	params := f.params("client-outbox-hold")
	admitted, err := f.admit(t, params)
	require.NoError(t, err)
	require.Equal(t, protocol.AdmissionStatusAcceptedHeld, admitted.AdmissionResult.Result)
	envelope := f.envelope(t, params, admitted)
	raw, err := json.Marshal(envelope)
	require.NoError(t, err)
	answer, err := f.service.SubmitMessageEnvelope(t.Context(), outboxSender, raw)
	require.NoError(t, err)
	submitted := answer.(SubmitMessageEnvelopeResult)
	require.Equal(t, protocol.ControlStateHeld, submitted.ControlState)
	require.Equal(t, messageStatePending, submitted.State)
	require.Equal(t, int64(1), federationTableCount(t, f.store, "message_envelopes"))
	require.Zero(t, federationTableCount(t, f.store, "federation_outbox"))
	require.Zero(t, federationTableCount(t, f.store, "federation_outbox_requests"))
	require.Zero(t, federationTableCount(t, f.store, "delivery_queue"))
}

func TestOutboundFederationRejectsMixedAndParentWithoutState(t *testing.T) {
	t.Run("mixed local and remote", func(t *testing.T) {
		f := newOutboundFederationFixture(t)
		insertOutboundSession(t, f.store, outboxOriginNode, outboxLocalRecipient)
		params := f.params("client-outbox-mixed")
		params.Recipients = append(params.Recipients, MessageRecipient{
			Agent: outboxLocalRecipient, DeliveryMode: protocol.DeliveryModeWhenSafe,
		})
		_, err := f.admit(t, params)
		got := requireCallCode(t, err, codeDomainError)
		require.Equal(t, protocol.ErrorCodeRecipientNotDeliverable, got.domain)
		require.Zero(t, federationTableCount(t, f.store, "messages"))
		require.Zero(t, federationTableCount(t, f.store, "channel_control_decisions"))
	})

	t.Run("remote parent", func(t *testing.T) {
		f := newOutboundFederationFixture(t)
		params := f.params("client-outbox-parent")
		params.ParentMessageID = "01ARZ3NDEKTSV4RRFFQ69G5FAX"
		_, err := f.admit(t, params)
		got := requireCallCode(t, err, codeDomainError)
		require.Equal(t, protocol.ErrorCodeControlVersionConflict, got.domain)
		require.Zero(t, federationTableCount(t, f.store, "messages"))
		require.Zero(t, federationTableCount(t, f.store, "channel_control_decisions"))
	})
}

func insertOutboundSession(
	t *testing.T,
	store *eventstore.Store,
	node protocol.NodeID,
	agent protocol.AgentID,
) {
	t.Helper()
	stamp := eventstore.FormatTime(time.Date(2026, time.October, 29, 23, 0, 0, 0, time.UTC))
	_, err := store.DB().ExecContext(t.Context(), `INSERT INTO agents
		(agent_id, node_id, harness, created_at, updated_at)
		VALUES (?, ?, 'test', ?, ?)`, agent, node, stamp, stamp)
	require.NoError(t, err)
	_, err = store.DB().ExecContext(t.Context(), `INSERT INTO sessions
		(session_id, agent_id, state, capture_mode, deliverable, manifest_json,
		 started_at, updated_at)
		VALUES (?, ?, 'CONNECTED', 'pty_proxy', 1, '{}', ?, ?)`,
		"session-"+string(agent), agent, stamp, stamp)
	require.NoError(t, err)
}

func TestOutboundFederationSequenceIsStrictlyIncreasing(t *testing.T) {
	f := newOutboundFederationFixture(t)
	var requests [][]byte
	for i, clientID := range []protocol.OpaqueID{"client-outbox-sequence-1", "client-outbox-sequence-2"} {
		params := f.params(clientID)
		params.CreatedAt = params.CreatedAt.Add(time.Duration(i) * time.Millisecond)
		params.ExpiresAt = params.ExpiresAt.Add(time.Duration(i) * time.Millisecond)
		admitted, err := f.admit(t, params)
		require.NoError(t, err)
		envelope := f.envelope(t, params, admitted)
		raw, err := json.Marshal(envelope)
		require.NoError(t, err)
		_, err = f.service.SubmitMessageEnvelope(t.Context(), outboxSender, raw)
		require.NoError(t, err)
	}
	rows, err := f.store.DB().QueryContext(t.Context(), `SELECT request
		FROM federation_outbox_requests ORDER BY sequence`)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()
	for rows.Next() {
		var raw []byte
		require.NoError(t, rows.Scan(&raw))
		requests = append(requests, raw)
	}
	require.NoError(t, rows.Err())
	require.Len(t, requests, 2)
	first, err := federation.DecodeDeliveryRequest(requests[0])
	require.NoError(t, err)
	second, err := federation.DecodeDeliveryRequest(requests[1])
	require.NoError(t, err)
	require.Equal(t, int64(1), first.Sequence)
	require.Equal(t, int64(2), second.Sequence)
	require.False(t, bytes.Equal(first.Nonce, second.Nonce))
}

func TestRecipientGateUsesBlockedBeforeRouteAndMapsRouteFailures(t *testing.T) {
	t.Run("blocked precedes route", func(t *testing.T) {
		f := newHubFixture(t)
		f.keys.blocked[testHubRecipient] = true
		f.keys.routes[testHubRecipient] = registry.RouteResult{
			Kind: registry.RouteFederated, Node: outboxDestinationNode,
		}
		_, err := callAdmit(t, f, testAdmitParams(f))
		got := requireCallCode(t, err, codeDomainError)
		require.Equal(t, protocol.ErrorCodeKeyUnavailable, got.domain)
		require.Empty(t, f.keys.routeCalls)
	})

	t.Run("route not found", func(t *testing.T) {
		f := newHubFixture(t)
		f.keys.routes[testHubRecipient] = registry.RouteResult{Kind: registry.RouteNotFound}
		_, err := callAdmit(t, f, testAdmitParams(f))
		got := requireCallCode(t, err, codeDomainError)
		require.Equal(t, protocol.ErrorCodeRecipientNotDeliverable, got.domain)
	})

	t.Run("peer blocked fallback", func(t *testing.T) {
		f := newHubFixture(t)
		f.keys.routes[testHubRecipient] = registry.RouteResult{
			Kind: registry.RoutePeerBlocked, Node: outboxDestinationNode,
		}
		_, err := callAdmit(t, f, testAdmitParams(f))
		got := requireCallCode(t, err, codeDomainError)
		require.Equal(t, protocol.ErrorCodeKeyUnavailable, got.domain)
	})

	t.Run("two remote nodes", func(t *testing.T) {
		f := newHubFixture(t)
		other := protocol.AgentID("agent-other-remote")
		f.keys.routes[testHubRecipient] = registry.RouteResult{
			Kind: registry.RouteFederated, Node: outboxDestinationNode,
		}
		f.keys.routes[other] = registry.RouteResult{
			Kind: registry.RouteFederated, Node: "node-outbox-c",
		}
		params := testAdmitParams(f)
		params.DestinationNode = ""
		params.Recipients = append(params.Recipients, MessageRecipient{
			Agent: other, DeliveryMode: protocol.DeliveryModeWhenSafe,
		})
		_, err := callAdmit(t, f, params)
		got := requireCallCode(t, err, codeDomainError)
		require.Equal(t, protocol.ErrorCodeRecipientNotDeliverable, got.domain)
		requireNoHubDecision(t, f)
	})
}

func TestOutboundFederationEndToEndOverMTLS(t *testing.T) {
	f := newOutboundFederationFixture(t)
	remoteEngine, err := channel.New(channel.Options{Clock: f.clock})
	require.NoError(t, err)
	remoteController, err := channel.NewController(channel.ControllerOptions{
		DB: f.remote.DB(), Engine: remoteEngine, Clock: f.clock, NodeID: outboxDestinationNode,
	})
	require.NoError(t, err)
	require.NoError(t, remoteController.Reload(t.Context()))
	remotePolicy := outboundFederationPolicy(t)
	remoteService, err := NewService(ServiceOptions{
		DB: f.remote.DB(), NodeID: outboxDestinationNode, Engine: remoteEngine,
		Registry: f.remoteRegistry, Clock: f.clock,
		FederationRegistry: f.remoteRegistry, Policy: remotePolicy,
	})
	require.NoError(t, err)
	require.NoError(t, f.remoteRegistry.SetPeerTransport(
		t.Context(), f.remote.DB(), outboxOriginNode,
		"https://origin.example.invalid:7443", f.originIdentity.Fingerprint,
	))
	remoteBundle := federationTestBundle(t, f.originIdentity)
	server, err := federation.NewServer(federation.ServerOptions{
		Listen: testLoopback, Identity: f.remoteIdentity, TrustBundle: remoteBundle,
		DB: f.remote.DB(), Clock: f.clock, Delivery: remoteService, Directory: f.remoteRegistry,
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
	require.NoError(t, f.localRegistry.SetPeerTransport(
		t.Context(), f.store.DB(), outboxDestinationNode,
		"https://"+address, f.remoteIdentity.Fingerprint,
	))
	localBundle := federationTestBundle(t, f.remoteIdentity)
	replicator, err := federation.NewReplicator(federation.ReplicatorOptions{
		DB: f.store.DB(), Registry: f.localRegistry, Identity: f.originIdentity,
		TrustBundle: localBundle, Clock: f.clock, LeaseOwner: "e2e-worker",
	})
	require.NoError(t, err)

	params := f.params("client-outbox-e2e")
	admitted, err := f.admit(t, params)
	require.NoError(t, err)
	envelope := f.envelope(t, params, admitted)
	exact, err := json.Marshal(envelope)
	require.NoError(t, err)
	_, err = f.service.SubmitMessageEnvelope(t.Context(), outboxSender, exact)
	require.NoError(t, err)
	claimed, err := replicator.ReplicateOnce(t.Context())
	require.NoError(t, err)
	require.True(t, claimed)

	var localState, outboxState, receiptState string
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(), `SELECT message.state,
		outbox.state, receipt.state FROM messages AS message
		JOIN federation_outbox AS outbox ON outbox.object_id = message.message_id
		JOIN federation_receipts AS receipt ON receipt.outbox_id = outbox.outbox_id
		WHERE message.message_id = ?`, admitted.MessageID).
		Scan(&localState, &outboxState, &receiptState))
	require.Equal(t, messageStateStoredRemote, localState)
	require.Equal(t, "STORED_REMOTE", outboxState)
	require.Equal(t, string(federation.DeliveryReceiptStoredRemote), receiptState)

	var remoteState, queueState string
	require.NoError(t, f.remote.DB().QueryRowContext(t.Context(), `SELECT message.state,
		queue.state FROM messages AS message JOIN delivery_queue AS queue
		ON queue.message_id = message.message_id WHERE message.message_id = ?`, admitted.MessageID).
		Scan(&remoteState, &queueState))
	require.Equal(t, messageStatePending, remoteState)
	require.Equal(t, "QUEUED", queueState)
	claimRaw, err := json.Marshal(ClaimMessageParams{
		MessageID: admitted.MessageID, DeliveryGeneration: 1,
	})
	require.NoError(t, err)
	answer, err := remoteService.ClaimMessage(t.Context(), outboxRecipient, claimRaw)
	require.NoError(t, err)
	claimedMessage := answer.(ClaimMessageResult)
	require.Equal(t, admitted.MessageID, claimedMessage.MessageID)
	require.Equal(t, protocol.DeliveryModeWhenSafe, claimedMessage.DeliveryMode)
	require.True(t, claimedMessage.RequiresAck)
	require.Equal(t, exact, []byte(claimedMessage.Envelope))
}

func TestOutboundFederationControlDeniedOverMTLSIsFinalAndOpaque(t *testing.T) {
	f := newOutboundFederationFixture(t)
	remoteEngine, err := channel.New(channel.Options{Clock: f.clock})
	require.NoError(t, err)
	remoteController, err := channel.NewController(channel.ControllerOptions{
		DB: f.remote.DB(), Engine: remoteEngine, Clock: f.clock, NodeID: outboxDestinationNode,
	})
	require.NoError(t, err)
	require.NoError(t, remoteController.Reload(t.Context()))
	remoteService, err := NewService(ServiceOptions{
		DB: f.remote.DB(), NodeID: outboxDestinationNode, Engine: remoteEngine,
		Registry: f.remoteRegistry, Clock: f.clock,
		FederationRegistry: f.remoteRegistry, Policy: outboundFederationDenyPolicy(t),
	})
	require.NoError(t, err)
	require.NoError(t, f.remoteRegistry.SetPeerTransport(
		t.Context(), f.remote.DB(), outboxOriginNode,
		"https://origin.example.invalid:7443", f.originIdentity.Fingerprint,
	))
	server, address := f.server(t, testLoopback, remoteService)
	stop := startOutboundFederationServer(t, server)
	replicator := f.replicator(t, address)
	messageID, _ := f.queue(t, "client-outbox-control-denied", 0)

	claimed, err := replicator.ReplicateOnce(t.Context())
	require.NoError(t, err)
	require.True(t, claimed)
	stop()

	var outboxState, messageState string
	var attempts int64
	var nextAttempt, lastError sql.NullString
	var rawReceipt []byte
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(), `SELECT outbox.state,
		outbox.attempts, outbox.next_attempt_at, outbox.last_error, message.state, receipt.receipt
		FROM federation_outbox AS outbox
		JOIN messages AS message ON message.message_id = outbox.object_id
		JOIN federation_receipts AS receipt ON receipt.outbox_id = outbox.outbox_id
		WHERE outbox.object_id = ?`, messageID).Scan(
		&outboxState, &attempts, &nextAttempt, &lastError, &messageState, &rawReceipt,
	))
	require.Equal(t, "CONTROL_DENIED", outboxState)
	require.Equal(t, messageStateFailedPermanent, messageState)
	require.Equal(t, int64(1), attempts)
	require.False(t, nextAttempt.Valid)
	require.False(t, lastError.Valid)
	receipt, err := federation.DecodeDeliveryReceipt(rawReceipt)
	require.NoError(t, err)
	require.Equal(t, federation.DeliveryReceiptControlDenied, receipt.Status)
	remotePublicKey, err := f.remoteRegistry.NodeSigningPublicKey()
	require.NoError(t, err)
	require.NoError(t, receipt.Verify(remotePublicKey))
	require.NotContains(t, string(rawReceipt), outboundFederationDenyRule)
	require.NotContains(t, string(rawReceipt), "policy")
	require.Zero(t, federationTableCount(t, f.remote, "federation_inbox"))
	var remoteMessages int64
	require.NoError(t, f.remote.DB().QueryRowContext(t.Context(),
		"SELECT count(*) FROM messages WHERE message_id = ?", messageID).Scan(&remoteMessages))
	require.Zero(t, remoteMessages)
	claimed, err = replicator.ReplicateOnce(t.Context())
	require.NoError(t, err)
	require.False(t, claimed)
}

func TestOutboundFederationBacklogDrainsInSequenceAfterMTLSPeerReturns(t *testing.T) {
	f := newOutboundFederationFixture(t)
	remoteService := f.remoteService(t)
	reservation, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", testLoopback)
	require.NoError(t, err)
	address := reservation.Addr().String()
	require.NoError(t, reservation.Close())
	replicator := f.replicator(t, address)

	messageIDs := make([]protocol.ULID, 0, 3)
	for i, clientID := range []protocol.OpaqueID{
		"client-outbox-backlog-1",
		"client-outbox-backlog-2",
		"client-outbox-backlog-3",
	} {
		messageID, _ := f.queue(t, clientID, time.Duration(i)*time.Millisecond)
		messageIDs = append(messageIDs, messageID)
	}
	require.Equal(t, int64(3), federationTableCount(t, f.store, "federation_outbox"))
	require.Zero(t, federationTableCount(t, f.remote, "federation_inbox"))

	startedAt := f.clock.now
	claimed, err := replicator.ReplicateOnce(t.Context())
	require.NoError(t, err)
	require.True(t, claimed)
	var outboxState, messageState, nextAttemptText string
	var attempts int64
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(), `SELECT outbox.state,
		outbox.attempts, outbox.next_attempt_at, message.state
		FROM federation_outbox AS outbox
		JOIN federation_outbox_requests AS request ON request.outbox_id = outbox.outbox_id
		JOIN messages AS message ON message.message_id = outbox.object_id
		WHERE request.sequence = 1`).Scan(
		&outboxState, &attempts, &nextAttemptText, &messageState,
	))
	require.Equal(t, "FAILED_RETRYABLE", outboxState)
	require.Equal(t, messageStateQueuedRemote, messageState)
	require.Equal(t, int64(1), attempts)
	firstRetryAt, err := time.Parse(eventstore.TimeLayout, nextAttemptText)
	require.NoError(t, err)
	require.Equal(t, startedAt.Add(500*time.Millisecond), firstRetryAt)

	f.clock.now = firstRetryAt.Add(-time.Nanosecond)
	claimed, err = replicator.ReplicateOnce(t.Context())
	require.NoError(t, err)
	require.False(t, claimed)
	f.clock.now = firstRetryAt
	claimed, err = replicator.ReplicateOnce(t.Context())
	require.NoError(t, err)
	require.True(t, claimed)
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(), `SELECT state, attempts,
		next_attempt_at FROM federation_outbox WHERE object_id = ?`, messageIDs[0]).Scan(
		&outboxState, &attempts, &nextAttemptText,
	))
	require.Equal(t, "FAILED_RETRYABLE", outboxState)
	require.Equal(t, int64(2), attempts)
	secondRetryAt, err := time.Parse(eventstore.TimeLayout, nextAttemptText)
	require.NoError(t, err)
	require.Equal(t, firstRetryAt.Add(time.Second), secondRetryAt)

	f.clock.now = secondRetryAt.Add(-time.Nanosecond)
	claimed, err = replicator.ReplicateOnce(t.Context())
	require.NoError(t, err)
	require.False(t, claimed)
	f.clock.now = secondRetryAt
	server, boundAddress := f.server(t, address, remoteService)
	require.Equal(t, address, boundAddress)
	stop := startOutboundFederationServer(t, server)
	for range messageIDs {
		claimed, err = replicator.ReplicateOnce(t.Context())
		require.NoError(t, err)
		require.True(t, claimed)
	}
	claimed, err = replicator.ReplicateOnce(t.Context())
	require.NoError(t, err)
	require.False(t, claimed)
	stop()

	rows, err := f.store.DB().QueryContext(t.Context(), `SELECT request.sequence,
		outbox.object_id, outbox.state, outbox.attempts
		FROM federation_outbox AS outbox
		JOIN federation_outbox_requests AS request ON request.outbox_id = outbox.outbox_id
		ORDER BY request.sequence`)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()
	index := 0
	for rows.Next() {
		var sequence, gotAttempts int64
		var messageID protocol.ULID
		var state string
		require.NoError(t, rows.Scan(&sequence, &messageID, &state, &gotAttempts))
		require.Equal(t, int64(index+1), sequence)
		require.Equal(t, messageIDs[index], messageID)
		require.Equal(t, "STORED_REMOTE", state)
		expectedAttempts := int64(1)
		if index == 0 {
			expectedAttempts = 3
		}
		require.Equal(t, expectedAttempts, gotAttempts)
		index++
	}
	require.NoError(t, rows.Err())
	require.Equal(t, len(messageIDs), index)
	require.Equal(t, int64(3), federationTableCount(t, f.store, "federation_receipts"))

	remoteRows, err := f.remote.DB().QueryContext(t.Context(), `SELECT sequence, message_id
		FROM federation_inbox WHERE origin_node = ? ORDER BY sequence`, outboxOriginNode)
	require.NoError(t, err)
	defer func() { require.NoError(t, remoteRows.Close()) }()
	index = 0
	for remoteRows.Next() {
		var sequence int64
		var messageID protocol.ULID
		require.NoError(t, remoteRows.Scan(&sequence, &messageID))
		require.Equal(t, int64(index+1), sequence)
		require.Equal(t, messageIDs[index], messageID)
		index++
	}
	require.NoError(t, remoteRows.Err())
	require.Equal(t, len(messageIDs), index)
	for _, messageID := range messageIDs {
		var stored, queued int64
		require.NoError(t, f.remote.DB().QueryRowContext(t.Context(),
			"SELECT count(*) FROM messages WHERE message_id = ?", messageID).Scan(&stored))
		require.NoError(t, f.remote.DB().QueryRowContext(t.Context(),
			"SELECT count(*) FROM delivery_queue WHERE message_id = ?", messageID).Scan(&queued))
		require.Equal(t, int64(1), stored)
		require.Equal(t, int64(1), queued)
	}
}

func TestOutboundFederationRetriesLostReceiptOverMTLS(t *testing.T) {
	f := newOutboundFederationFixture(t)
	receiver := &outboundLostReceiptReceiver{delegate: f.remoteService(t)}
	server, address := f.server(t, testLoopback, receiver)
	stop := startOutboundFederationServer(t, server)
	replicator := f.replicator(t, address)
	messageID, storedRequest := f.queue(t, "client-outbox-lost-receipt", 0)

	startedAt := f.clock.now
	claimed, err := replicator.ReplicateOnce(t.Context())
	require.NoError(t, err)
	require.True(t, claimed)
	stop()
	require.Len(t, receiver.requests, 1)
	require.Len(t, receiver.receipts, 1)
	require.Equal(t, storedRequest, receiver.requests[0])
	require.Equal(t, int64(1), federationTableCount(t, f.remote, "federation_inbox"))
	require.Zero(t, federationTableCount(t, f.store, "federation_receipts"))

	var outboxState, messageState, nextAttemptText string
	var attempts int64
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(), `SELECT outbox.state,
		outbox.attempts, outbox.next_attempt_at, message.state
		FROM federation_outbox AS outbox JOIN messages AS message
		ON message.message_id = outbox.object_id WHERE outbox.object_id = ?`, messageID).Scan(
		&outboxState, &attempts, &nextAttemptText, &messageState,
	))
	require.Equal(t, "FAILED_RETRYABLE", outboxState)
	require.Equal(t, messageStateQueuedRemote, messageState)
	require.Equal(t, int64(1), attempts)
	nextAttempt, err := time.Parse(eventstore.TimeLayout, nextAttemptText)
	require.NoError(t, err)
	require.Equal(t, startedAt.Add(500*time.Millisecond), nextAttempt)

	f.clock.now = nextAttempt
	restarted, reboundAddress := f.server(t, address, receiver)
	require.Equal(t, address, reboundAddress)
	stopRestarted := startOutboundFederationServer(t, restarted)
	claimed, err = replicator.ReplicateOnce(t.Context())
	require.NoError(t, err)
	require.True(t, claimed)
	stopRestarted()

	require.Len(t, receiver.requests, 2)
	require.Len(t, receiver.receipts, 2)
	require.Equal(t, storedRequest, receiver.requests[0])
	require.Equal(t, receiver.requests[0], receiver.requests[1])
	require.Equal(t, receiver.receipts[0], receiver.receipts[1])
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(), `SELECT outbox.state,
		outbox.attempts, message.state FROM federation_outbox AS outbox
		JOIN messages AS message ON message.message_id = outbox.object_id
		WHERE outbox.object_id = ?`, messageID).Scan(
		&outboxState, &attempts, &messageState,
	))
	require.Equal(t, "STORED_REMOTE", outboxState)
	require.Equal(t, messageStateStoredRemote, messageState)
	require.Equal(t, int64(2), attempts)
	require.Equal(t, int64(1), federationTableCount(t, f.store, "federation_receipts"))
	require.Equal(t, int64(1), federationTableCount(t, f.remote, "federation_inbox"))

	var storedReceipt, replayReceipt []byte
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(), `SELECT receipt
		FROM federation_receipts`).Scan(&storedReceipt))
	require.NoError(t, f.remote.DB().QueryRowContext(t.Context(), `SELECT receipt
		FROM federation_inbox WHERE message_id = ?`, messageID).Scan(&replayReceipt))
	require.Equal(t, receiver.receipts[0], storedReceipt)
	require.Equal(t, receiver.receipts[0], replayReceipt)
	var storedMessages, queuedDeliveries int64
	require.NoError(t, f.remote.DB().QueryRowContext(t.Context(),
		"SELECT count(*) FROM messages WHERE message_id = ?", messageID).Scan(&storedMessages))
	require.NoError(t, f.remote.DB().QueryRowContext(t.Context(),
		"SELECT count(*) FROM delivery_queue WHERE message_id = ?", messageID).Scan(&queuedDeliveries))
	require.Equal(t, int64(1), storedMessages)
	require.Equal(t, int64(1), queuedDeliveries)
}

func outboundFederationPolicy(t *testing.T) *policy.Policy {
	t.Helper()
	dir := t.TempDir()
	raw := []byte(`schema_version: 1
rules:
  - id: allow-outbox-origin
    effect: allow
    peer: node-outbox-a
    action: deliver_message
    agent: "agent-outbox-*"
`)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "delivery.yaml"), raw, 0o600))
	compiled, err := policy.Load(dir)
	require.NoError(t, err)
	return compiled
}

const outboundFederationDenyRule = "deny-federated-recipient"

func outboundFederationDenyPolicy(t *testing.T) *policy.Policy {
	t.Helper()
	dir := t.TempDir()
	raw := []byte(`schema_version: 1
rules:
  - id: ` + outboundFederationDenyRule + `
    effect: deny
    peer: node-outbox-a
    action: deliver_message
    agent: "agent-outbox-*"
`)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "delivery.yaml"), raw, 0o600))
	compiled, err := policy.Load(dir)
	require.NoError(t, err)
	return compiled
}
