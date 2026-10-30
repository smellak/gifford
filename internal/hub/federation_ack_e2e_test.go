package hub

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/federation"
	"github.com/smellak/gifford/internal/protocol"
)

type lostFederatedAckReceipt struct {
	delegate federation.AckReceiver
	requests [][]byte
	receipts [][]byte
}

func (r *lostFederatedAckReceipt) ReceiveFederatedAck(
	ctx context.Context,
	peer protocol.NodeID,
	request federation.AckRequest,
) ([]byte, error) {
	exact, err := request.CanonicalBytes()
	if err != nil {
		return nil, err
	}
	r.requests = append(r.requests, append([]byte(nil), exact...))
	receipt, err := r.delegate.ReceiveFederatedAck(ctx, peer, request)
	if err != nil {
		return nil, err
	}
	r.receipts = append(r.receipts, append([]byte(nil), receipt...))
	if len(r.receipts) == 1 {
		panic(http.ErrAbortHandler)
	}
	return receipt, nil
}

type ackRoundTripFixture struct {
	base               *outboundFederationFixture
	remoteService      *Service
	originReceiver     federation.AckReceiver
	originAddress      string
	stopOrigin         func()
	deliveryReplicator *federation.Replicator
	ackReplicator      *federation.Replicator
}

func newAckRoundTripFixture(
	t *testing.T,
	wrapOrigin func(federation.AckReceiver) federation.AckReceiver,
) *ackRoundTripFixture {
	t.Helper()
	f := newOutboundFederationFixture(t)
	originReceiver := federation.AckReceiver(f.service)
	if wrapOrigin != nil {
		originReceiver = wrapOrigin(originReceiver)
	}
	originServer, err := federation.NewServer(federation.ServerOptions{
		Listen: testLoopback, Identity: f.originIdentity,
		TrustBundle: federationTestBundle(t, f.remoteIdentity),
		DB:          f.store.DB(), Clock: f.clock, Ack: originReceiver, Directory: f.localRegistry,
	})
	require.NoError(t, err)
	originAddress, err := originServer.Listen(t.Context())
	require.NoError(t, err)
	stopOrigin := startOutboundFederationServer(t, originServer)
	require.NoError(t, f.remoteRegistry.SetPeerTransport(
		t.Context(), f.remote.DB(), outboxOriginNode,
		"https://"+originAddress, f.originIdentity.Fingerprint,
	))

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
		FederationRegistry: f.remoteRegistry, Policy: outboundFederationPolicy(t),
	})
	require.NoError(t, err)
	destinationServer, err := federation.NewServer(federation.ServerOptions{
		Listen: testLoopback, Identity: f.remoteIdentity,
		TrustBundle: federationTestBundle(t, f.originIdentity),
		DB:          f.remote.DB(), Clock: f.clock, Delivery: remoteService,
		Ack: remoteService, Directory: f.remoteRegistry,
	})
	require.NoError(t, err)
	destinationAddress, err := destinationServer.Listen(t.Context())
	require.NoError(t, err)
	startOutboundFederationServer(t, destinationServer)
	require.NoError(t, f.localRegistry.SetPeerTransport(
		t.Context(), f.store.DB(), outboxDestinationNode,
		"https://"+destinationAddress, f.remoteIdentity.Fingerprint,
	))

	deliveryReplicator, err := federation.NewReplicator(federation.ReplicatorOptions{
		DB: f.store.DB(), Registry: f.localRegistry, Identity: f.originIdentity,
		TrustBundle: federationTestBundle(t, f.remoteIdentity), Clock: f.clock,
		Random: outboundZeroReader{}, LeaseOwner: "ack-e2e-delivery",
	})
	require.NoError(t, err)
	ackReplicator, err := federation.NewReplicator(federation.ReplicatorOptions{
		DB: f.remote.DB(), Registry: f.remoteRegistry, Identity: f.remoteIdentity,
		TrustBundle: federationTestBundle(t, f.originIdentity), Clock: f.clock,
		Random: outboundZeroReader{}, LeaseOwner: "ack-e2e-ack",
	})
	require.NoError(t, err)
	return &ackRoundTripFixture{
		base: f, remoteService: remoteService, originReceiver: originReceiver,
		originAddress: originAddress, stopOrigin: stopOrigin,
		deliveryReplicator: deliveryReplicator, ackReplicator: ackReplicator,
	}
}

func (f *ackRoundTripFixture) restartOrigin(t *testing.T) {
	t.Helper()
	server, err := federation.NewServer(federation.ServerOptions{
		Listen: f.originAddress, Identity: f.base.originIdentity,
		TrustBundle: federationTestBundle(t, f.base.remoteIdentity),
		DB:          f.base.store.DB(), Clock: f.base.clock,
		Ack: f.originReceiver, Directory: f.base.localRegistry,
	})
	require.NoError(t, err)
	address, err := server.Listen(t.Context())
	require.NoError(t, err)
	require.Equal(t, f.originAddress, address)
	f.stopOrigin = startOutboundFederationServer(t, server)
}

func (f *ackRoundTripFixture) deliver(t *testing.T, clientID protocol.OpaqueID) protocol.ULID {
	t.Helper()
	messageID, _ := f.base.queue(t, clientID, 0)
	claimed, err := f.deliveryReplicator.ReplicateOnce(t.Context())
	require.NoError(t, err)
	require.True(t, claimed)
	return messageID
}

func (f *ackRoundTripFixture) claim(t *testing.T, messageID protocol.ULID) {
	t.Helper()
	raw, err := json.Marshal(ClaimMessageParams{
		MessageID: messageID, DeliveryGeneration: 1,
	})
	require.NoError(t, err)
	_, err = f.remoteService.ClaimMessage(t.Context(), outboxRecipient, raw)
	require.NoError(t, err)
}

func (f *ackRoundTripFixture) ack(
	t *testing.T,
	messageID protocol.ULID,
	status AckMessageStatus,
) {
	t.Helper()
	raw, err := json.Marshal(AckMessageParams{
		MessageID: messageID, DeliveryGeneration: 1, Status: status,
	})
	require.NoError(t, err)
	_, err = f.remoteService.AckMessage(t.Context(), outboxRecipient, raw)
	require.NoError(t, err)
}

func (f *ackRoundTripFixture) replicateAck(t *testing.T) {
	t.Helper()
	claimed, err := f.ackReplicator.ReplicateOnce(t.Context())
	require.NoError(t, err)
	require.True(t, claimed)
}

func TestFederatedAckRoundTripOverMTLS(t *testing.T) {
	f := newAckRoundTripFixture(t, nil)
	messageID := f.deliver(t, "client-ack-e2e")
	f.claim(t, messageID)
	f.ack(t, messageID, AckMessageProcessing)
	f.replicateAck(t)

	var state string
	require.NoError(t, f.base.store.DB().QueryRowContext(t.Context(),
		"SELECT state FROM messages WHERE message_id = ?", messageID).Scan(&state))
	require.Equal(t, messageStateStoredRemote, state)
	f.ack(t, messageID, AckMessageAcknowledged)
	f.replicateAck(t)
	require.NoError(t, f.base.store.DB().QueryRowContext(t.Context(),
		"SELECT state FROM messages WHERE message_id = ?", messageID).Scan(&state))
	require.Equal(t, messageStateAcknowledged, state)
	require.Equal(t, int64(2), federationTableCount(t, f.base.store, "acks"))
	require.Equal(t, int64(2), federationTableCount(t, f.base.store, "federation_ack_inbox"))
	require.Equal(t, int64(2), federationTableCount(t, f.base.remote, "federation_ack_receipts"))

	rows, err := f.base.remote.DB().QueryContext(t.Context(), `SELECT request.sequence,
		outbox.state FROM federation_outbox AS outbox
		JOIN federation_outbox_acks AS request ON request.outbox_id = outbox.outbox_id
		ORDER BY request.sequence`)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()
	sequence := int64(1)
	for rows.Next() {
		var got int64
		var outboxState string
		require.NoError(t, rows.Scan(&got, &outboxState))
		require.Equal(t, sequence, got)
		require.Equal(t, "STORED_REMOTE", outboxState)
		sequence++
	}
	require.NoError(t, rows.Err())
	require.Equal(t, int64(3), sequence)
}

func TestFederatedAckRetriesLostSignedReceipt(t *testing.T) {
	var lost *lostFederatedAckReceipt
	f := newAckRoundTripFixture(t, func(delegate federation.AckReceiver) federation.AckReceiver {
		lost = &lostFederatedAckReceipt{delegate: delegate}
		return lost
	})
	messageID := f.deliver(t, "client-ack-lost-receipt")
	f.claim(t, messageID)
	f.ack(t, messageID, AckMessageAcknowledged)
	startedAt := f.base.clock.now
	f.replicateAck(t)
	require.Len(t, lost.requests, 1)
	require.Len(t, lost.receipts, 1)
	require.Equal(t, int64(1), federationTableCount(t, f.base.store, "federation_ack_inbox"))
	require.Zero(t, federationTableCount(t, f.base.remote, "federation_ack_receipts"))

	var nextAttempt string
	require.NoError(t, f.base.remote.DB().QueryRowContext(t.Context(), `SELECT next_attempt_at
		FROM federation_outbox WHERE object_type = 'ack'`).Scan(&nextAttempt))
	retryAt, err := time.Parse(eventstore.TimeLayout, nextAttempt)
	require.NoError(t, err)
	require.Equal(t, startedAt.Add(500*time.Millisecond), retryAt)
	f.base.clock.now = retryAt
	f.replicateAck(t)
	require.Len(t, lost.requests, 2)
	require.Len(t, lost.receipts, 2)
	require.Equal(t, lost.requests[0], lost.requests[1])
	require.Equal(t, lost.receipts[0], lost.receipts[1])
	require.Equal(t, int64(1), federationTableCount(t, f.base.store, "federation_ack_inbox"))
	require.Equal(t, int64(1), federationTableCount(t, f.base.remote, "federation_ack_receipts"))
}

func TestFederatedAckBacklogDrainsInOrderAfterOriginReturns(t *testing.T) {
	f := newAckRoundTripFixture(t, nil)
	messageID := f.deliver(t, "client-ack-backlog")
	f.claim(t, messageID)
	f.ack(t, messageID, AckMessageProcessing)
	f.ack(t, messageID, AckMessageAcknowledged)
	f.stopOrigin()

	f.replicateAck(t)
	var nextAttempt string
	require.NoError(t, f.base.remote.DB().QueryRowContext(t.Context(), `SELECT next_attempt_at
		FROM federation_outbox AS outbox JOIN federation_outbox_acks AS request
		ON request.outbox_id = outbox.outbox_id WHERE request.sequence = 1`).Scan(&nextAttempt))
	claimed, err := f.ackReplicator.ReplicateOnce(t.Context())
	require.NoError(t, err)
	require.False(t, claimed)

	f.restartOrigin(t)
	retryAt, err := time.Parse(eventstore.TimeLayout, nextAttempt)
	require.NoError(t, err)
	f.base.clock.now = retryAt
	f.replicateAck(t)
	f.replicateAck(t)

	rows, err := f.base.store.DB().QueryContext(t.Context(), `SELECT sequence, status
		FROM federation_ack_inbox WHERE origin_node = ? ORDER BY sequence`, outboxDestinationNode)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()
	want := []protocol.AckStatus{protocol.AckStatusProcessing, protocol.AckStatusAcknowledged}
	index := 0
	for rows.Next() {
		var sequence int64
		var status protocol.AckStatus
		require.NoError(t, rows.Scan(&sequence, &status))
		require.Equal(t, int64(index+1), sequence)
		require.Equal(t, want[index], status)
		index++
	}
	require.NoError(t, rows.Err())
	require.Equal(t, len(want), index)
	var state string
	require.NoError(t, f.base.store.DB().QueryRowContext(t.Context(),
		"SELECT state FROM messages WHERE message_id = ?", messageID).Scan(&state))
	require.Equal(t, messageStateAcknowledged, state)
}

func TestFederatedAckRetriesQueuedRemoteAfterDeliveryReceipt(t *testing.T) {
	f := newAckRoundTripFixture(t, nil)
	messageID, exactDelivery := f.base.queue(t, "client-ack-queued-remote", 0)
	delivery, err := federation.DecodeDeliveryRequest(exactDelivery)
	require.NoError(t, err)
	_, err = f.remoteService.ReceiveDelivery(t.Context(), outboxOriginNode, delivery)
	require.NoError(t, err)
	f.claim(t, messageID)
	f.ack(t, messageID, AckMessageAcknowledged)

	f.replicateAck(t)
	var ackState, lastError, nextAttempt string
	require.NoError(t, f.base.remote.DB().QueryRowContext(t.Context(), `SELECT state,
		last_error, next_attempt_at FROM federation_outbox WHERE object_type = 'ack'`).Scan(
		&ackState, &lastError, &nextAttempt,
	))
	require.Equal(t, "FAILED_RETRYABLE", ackState)
	require.Equal(t, "HTTP_403", lastError)
	require.Zero(t, federationTableCount(t, f.base.store, "federation_ack_inbox"))

	claimed, err := f.deliveryReplicator.ReplicateOnce(t.Context())
	require.NoError(t, err)
	require.True(t, claimed)
	var originState string
	require.NoError(t, f.base.store.DB().QueryRowContext(t.Context(),
		"SELECT state FROM messages WHERE message_id = ?", messageID).Scan(&originState))
	require.Equal(t, messageStateStoredRemote, originState)
	retryAt, err := time.Parse(eventstore.TimeLayout, nextAttempt)
	require.NoError(t, err)
	f.base.clock.now = retryAt
	f.replicateAck(t)
	require.NoError(t, f.base.store.DB().QueryRowContext(t.Context(),
		"SELECT state FROM messages WHERE message_id = ?", messageID).Scan(&originState))
	require.Equal(t, messageStateAcknowledged, originState)
	require.Equal(t, int64(1), federationTableCount(t, f.base.store, "federation_ack_inbox"))
}
