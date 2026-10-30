package federation

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
)

const replicationOtherNode = protocol.NodeID("node-c")

type replicationClock struct{ now time.Time }

func (c *replicationClock) Now() time.Time { return c.now }

type zeroReader struct{}

func (zeroReader) Read(value []byte) (int, error) {
	clear(value)
	return len(value), nil
}

type deliverySenderFunc func(context.Context, registry.Peer, []byte) (DeliveryResponse, error)

func (f deliverySenderFunc) Send(
	ctx context.Context,
	peer registry.Peer,
	exact []byte,
) (DeliveryResponse, error) {
	return f(ctx, peer, exact)
}

type replicationFixture struct {
	clock  *replicationClock
	store  *eventstore.Store
	local  *registry.Registry
	peers  map[protocol.NodeID]*registry.Registry
	sender DeliverySender
}

func newReplicationFixture(t *testing.T) *replicationFixture {
	t.Helper()
	clk := &replicationClock{now: time.Date(2026, time.October, 29, 22, 10, 0, 0, time.UTC)}
	dataDir := privateDataDir(t)
	store, err := eventstore.Open(t.Context(), filepath.Join(dataDir, "gifford.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	local, err := registry.New(deliveryTestOriginNode, dataDir, clk)
	require.NoError(t, err)
	require.NoError(t, local.EnsureLocalNode(t.Context(), store.DB(), "org-a"))
	fixture := &replicationFixture{
		clock: clk, store: store, local: local,
		peers: make(map[protocol.NodeID]*registry.Registry),
	}
	fixture.addPeer(t, deliveryTestDestinationNode)
	return fixture
}

func (f *replicationFixture) addPeer(t *testing.T, node protocol.NodeID) {
	t.Helper()
	peer, err := registry.New(node, privateDataDir(t), f.clock)
	require.NoError(t, err)
	publicKey, err := peer.NodeSigningPublicKey()
	require.NoError(t, err)
	_, err = f.local.AddPeer(t.Context(), f.store.DB(), node, "org-"+string(node), publicKey)
	require.NoError(t, err)
	require.NoError(t, f.local.SetPeerTransport(
		t.Context(), f.store.DB(), node, "https://"+string(node)+".example.invalid:7443",
		strings.Repeat("a", 64),
	))
	f.peers[node] = peer
}

func (f *replicationFixture) newReplicator(t *testing.T, owner string) *Replicator {
	t.Helper()
	replicator, err := NewReplicator(ReplicatorOptions{
		DB: f.store.DB(), Registry: f.local, Clock: f.clock, Random: zeroReader{},
		LeaseOwner: owner, Sender: f.sender,
	})
	require.NoError(t, err)
	return replicator
}

func replicationRequest(
	t *testing.T,
	messageID protocol.ULID,
	destination protocol.NodeID,
	sequence int64,
	nonce byte,
) DeliveryRequest {
	t.Helper()
	request := testDeliveryRequest(t)
	envelope, _, err := request.DecodeEnvelope()
	require.NoError(t, err)
	envelope.MessageID = messageID
	envelope.ClientMsgID = protocol.OpaqueID("client-" + string(messageID))
	envelope.DestinationNode = destination
	request.Envelope, err = json.Marshal(envelope)
	require.NoError(t, err)
	request.Sequence = sequence
	request.Nonce = bytes.Repeat([]byte{nonce}, deliveryNonceBytes)
	request.OriginControl.ObjectID = protocol.OpaqueID(messageID)
	require.NoError(t, request.Validate())
	return request
}

func (f *replicationFixture) seed(
	t *testing.T,
	request DeliveryRequest,
	createdAt time.Time,
) []byte {
	t.Helper()
	exact, err := request.CanonicalBytes()
	require.NoError(t, err)
	requestSHA, err := request.RequestSHA256()
	require.NoError(t, err)
	envelope, _, err := request.DecodeEnvelope()
	require.NoError(t, err)
	nowText := eventstore.FormatTime(f.clock.now)
	_, err = f.store.DB().ExecContext(t.Context(), `INSERT INTO messages
		(message_id, client_msg_id, origin_node, from_agent, destination_node,
		 thread_ref, thread_id, priority, state, created_at, expires_at, updated_at,
		 hop_count, route_digest)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'QUEUED_REMOTE', ?, ?, ?, ?, ?)`,
		envelope.MessageID, envelope.ClientMsgID, envelope.OriginNode, envelope.OriginAgent,
		envelope.DestinationNode, envelope.ThreadRef, request.ThreadID, envelope.Priority,
		eventstore.FormatTime(envelope.CreatedAt), eventstore.FormatTime(envelope.ExpiresAt),
		nowText, envelope.HopCount, envelope.RouteDigest)
	require.NoError(t, err)
	outboxID := "test:" + string(envelope.MessageID)
	_, err = f.store.DB().ExecContext(t.Context(), `INSERT INTO federation_outbox
		(outbox_id, object_type, object_id, destination_node, state, attempts,
		 next_attempt_at, idempotency_key, created_at, updated_at)
		VALUES (?, 'message', ?, ?, 'QUEUED', 0, ?, ?, ?, ?)`,
		outboxID, envelope.MessageID, envelope.DestinationNode, nowText,
		string(envelope.MessageID)+":1", eventstore.FormatTime(createdAt), nowText)
	require.NoError(t, err)
	_, err = f.store.DB().ExecContext(t.Context(), `INSERT INTO federation_outbox_requests
		(outbox_id, destination_node, request, request_sha256, sequence, nonce)
		VALUES (?, ?, ?, ?, ?, ?)`, outboxID, envelope.DestinationNode,
		exact, requestSHA, request.Sequence, request.Nonce)
	require.NoError(t, err)
	return exact
}

func (f *replicationFixture) signedResponse(
	t *testing.T,
	peer protocol.NodeID,
	exact []byte,
	status DeliveryReceiptStatus,
	httpStatus int,
) DeliveryResponse {
	t.Helper()
	request, err := DecodeDeliveryRequest(exact)
	require.NoError(t, err)
	envelope, _, err := request.DecodeEnvelope()
	require.NoError(t, err)
	requestSHA, err := request.RequestSHA256()
	require.NoError(t, err)
	envelopeSHA, err := request.EnvelopeSHA256()
	require.NoError(t, err)
	recipients := make([]protocol.AgentID, len(request.Recipients))
	for i, recipient := range request.Recipients {
		recipients[i] = recipient.Agent
	}
	receipt := DeliveryReceipt{
		MessageID: envelope.MessageID, OriginNode: envelope.OriginNode,
		DestinationNode: peer, RecipientAgents: recipients,
		DeliveryGeneration: request.DeliveryGeneration,
		Nonce:              append([]byte(nil), request.Nonce...), Sequence: request.Sequence,
		EnvelopeSHA256: envelopeSHA, RequestSHA256: requestSHA,
		Status: status, ReceivedAt: f.clock.now,
	}
	require.NoError(t, receipt.Sign(f.peers[peer]))
	raw, err := json.Marshal(receipt)
	require.NoError(t, err)
	return DeliveryResponse{StatusCode: httpStatus, Body: raw}
}

func replicationAckRequest(nonce byte, status protocol.AckStatus) AckRequest {
	return AckRequest{
		MessageID: deliveryTestMessage, Agent: deliveryTestRecipientB,
		DeliveryGeneration: 1, Status: status,
		AckedAt: time.Date(2026, time.October, 29, 22, 9, 0, 0, time.UTC),
		Nonce:   bytes.Repeat([]byte{nonce}, deliveryNonceBytes), Sequence: 1,
	}
}

func (f *replicationFixture) seedAck(
	t *testing.T,
	request AckRequest,
	createdAt time.Time,
) []byte {
	t.Helper()
	exact, err := request.CanonicalBytes()
	require.NoError(t, err)
	requestSHA, err := request.RequestSHA256()
	require.NoError(t, err)
	stable := fmt.Sprintf("%s:%s:%d:%s", request.MessageID, request.Agent,
		request.DeliveryGeneration, request.Status)
	outboxID := "test-ack:" + stable
	nowText := eventstore.FormatTime(f.clock.now)
	_, err = f.store.DB().ExecContext(t.Context(), `INSERT INTO federation_outbox
		(outbox_id, object_type, object_id, destination_node, state, attempts,
		 next_attempt_at, idempotency_key, created_at, updated_at)
		VALUES (?, 'ack', ?, ?, 'QUEUED', 0, ?, ?, ?, ?)`,
		outboxID, stable, deliveryTestDestinationNode, nowText, stable,
		eventstore.FormatTime(createdAt), nowText)
	require.NoError(t, err)
	_, err = f.store.DB().ExecContext(t.Context(), `INSERT INTO federation_outbox_acks
		(outbox_id, destination_node, request, request_sha256, sequence, nonce)
		VALUES (?, ?, ?, ?, ?, ?)`, outboxID, deliveryTestDestinationNode, exact, requestSHA,
		request.Sequence, request.Nonce)
	require.NoError(t, err)
	return exact
}

func (f *replicationFixture) signedAckResponse(
	t *testing.T,
	peer protocol.NodeID,
	exact []byte,
) DeliveryResponse {
	t.Helper()
	request, err := DecodeAckRequest(exact)
	require.NoError(t, err)
	requestSHA, err := request.RequestSHA256()
	require.NoError(t, err)
	receipt := AckReceipt{
		MessageID: request.MessageID, Agent: request.Agent,
		DeliveryGeneration: request.DeliveryGeneration, Status: request.Status,
		OriginNode: peer, DestinationNode: f.local.NodeID(), RequestSHA256: requestSHA,
		Nonce: append([]byte(nil), request.Nonce...), Sequence: request.Sequence,
		ReceivedAt: f.clock.now,
	}
	require.NoError(t, receipt.Sign(f.peers[peer]))
	raw, err := json.Marshal(receipt)
	require.NoError(t, err)
	return DeliveryResponse{StatusCode: http.StatusOK, Body: raw}
}

func replicationState(t *testing.T, f *replicationFixture) (string, string, int64, string) {
	t.Helper()
	var outboxState, messageState string
	var attempts int64
	var lastError *string
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(), `SELECT outbox.state,
		outbox.attempts, outbox.last_error, message.state
		FROM federation_outbox AS outbox JOIN messages AS message ON message.message_id = outbox.object_id
		WHERE outbox.object_id = ?`, deliveryTestMessage).Scan(&outboxState, &attempts, &lastError, &messageState))
	last := ""
	if lastError != nil {
		last = *lastError
	}
	return outboxState, messageState, attempts, last
}

func TestReplicatorStoresValidReceiptAndRemoteState(t *testing.T) {
	f := newReplicationFixture(t)
	request := replicationRequest(t, deliveryTestMessage, deliveryTestDestinationNode, 1, 0x11)
	exact := f.seed(t, request, f.clock.now)
	var sent []byte
	f.sender = deliverySenderFunc(func(ctx context.Context, peer registry.Peer, raw []byte) (DeliveryResponse, error) {
		sent = append([]byte(nil), raw...)
		var state string
		require.NoError(t, f.store.DB().QueryRowContext(ctx,
			"SELECT state FROM federation_outbox WHERE object_id = ?", deliveryTestMessage).Scan(&state))
		require.Equal(t, outboxReplicating, state)
		return f.signedResponse(t, peer.Node, raw, DeliveryReceiptStoredRemote, 200), nil
	})
	replicator := f.newReplicator(t, "worker-a")
	claimed, err := replicator.ReplicateOnce(t.Context())
	require.NoError(t, err)
	require.True(t, claimed)
	require.Equal(t, exact, sent)
	outboxState, messageState, attempts, last := replicationState(t, f)
	require.Equal(t, outboxStoredRemote, outboxState)
	require.Equal(t, messageStoredRemote, messageState)
	require.Equal(t, int64(1), attempts)
	require.Empty(t, last)
	var stored []byte
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
		"SELECT receipt FROM federation_receipts WHERE outbox_id = ?",
		"test:"+string(deliveryTestMessage)).Scan(&stored))
	require.Equal(t, f.signedResponse(t, deliveryTestDestinationNode, exact,
		DeliveryReceiptStoredRemote, 200).Body, stored)
}

func TestReplicatorRetriesInvalidReceiptWithIdenticalBytes(t *testing.T) {
	f := newReplicationFixture(t)
	request := replicationRequest(t, deliveryTestMessage, deliveryTestDestinationNode, 1, 0x21)
	exact := f.seed(t, request, f.clock.now)
	var sent [][]byte
	valid := false
	f.sender = deliverySenderFunc(func(_ context.Context, peer registry.Peer, raw []byte) (DeliveryResponse, error) {
		sent = append(sent, append([]byte(nil), raw...))
		response := f.signedResponse(t, peer.Node, raw, DeliveryReceiptStoredRemote, 200)
		if !valid {
			response.Body[len(response.Body)-2] ^= 1
		}
		return response, nil
	})
	var alerts []ReplicatorAlert
	replicator, err := NewReplicator(ReplicatorOptions{
		DB: f.store.DB(), Registry: f.local, Clock: f.clock, Random: zeroReader{},
		LeaseOwner: "worker-a", Sender: f.sender,
		Alert: func(alert ReplicatorAlert) { alerts = append(alerts, alert) },
	})
	require.NoError(t, err)
	claimed, err := replicator.ReplicateOnce(t.Context())
	require.NoError(t, err)
	require.True(t, claimed)
	outboxState, messageState, attempts, last := replicationState(t, f)
	require.Equal(t, outboxFailedRetryable, outboxState)
	require.Equal(t, messageQueuedRemote, messageState)
	require.Equal(t, int64(1), attempts)
	require.Equal(t, "INVALID_RECEIPT", last)
	require.Equal(t, []ReplicatorAlert{{Code: "INVALID_RECEIPT"}}, alerts)
	var next string
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
		"SELECT next_attempt_at FROM federation_outbox WHERE object_id = ?", deliveryTestMessage).Scan(&next))
	nextAt, err := time.Parse(eventstore.TimeLayout, next)
	require.NoError(t, err)
	require.Equal(t, f.clock.now.Add(500*time.Millisecond), nextAt)
	var receipts int64
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
		"SELECT count(*) FROM federation_receipts").Scan(&receipts))
	require.Zero(t, receipts)

	f.clock.now = nextAt
	valid = true
	claimed, err = replicator.ReplicateOnce(t.Context())
	require.NoError(t, err)
	require.True(t, claimed)
	require.Len(t, sent, 2)
	require.Equal(t, exact, sent[0])
	require.Equal(t, sent[0], sent[1])
	outboxState, messageState, attempts, _ = replicationState(t, f)
	require.Equal(t, outboxStoredRemote, outboxState)
	require.Equal(t, messageStoredRemote, messageState)
	require.Equal(t, int64(2), attempts)
}

func TestReplicatorBackoffUsesHalfToFullExponentialWindow(t *testing.T) {
	replicator := &Replicator{random: zeroReader{}}
	require.Equal(t, 500*time.Millisecond, replicator.backoff(1))
	require.Equal(t, time.Second, replicator.backoff(2))
	require.Equal(t, 2*time.Second, replicator.backoff(3))
	require.Equal(t, 150*time.Second, replicator.backoff(64))
}

func TestReplicatorRejectsSignedReceiptForDifferentRequest(t *testing.T) {
	f := newReplicationFixture(t)
	request := replicationRequest(t, deliveryTestMessage, deliveryTestDestinationNode, 1, 0x25)
	f.seed(t, request, f.clock.now)
	f.sender = deliverySenderFunc(func(_ context.Context, peer registry.Peer, raw []byte) (DeliveryResponse, error) {
		response := f.signedResponse(t, peer.Node, raw, DeliveryReceiptStoredRemote, 200)
		var receipt DeliveryReceipt
		require.NoError(t, json.Unmarshal(response.Body, &receipt))
		receipt.Sequence++
		require.NoError(t, receipt.Sign(f.peers[peer.Node]))
		encoded, err := json.Marshal(receipt)
		require.NoError(t, err)
		response.Body = encoded
		return response, nil
	})
	var alerts []ReplicatorAlert
	replicator, err := NewReplicator(ReplicatorOptions{
		DB: f.store.DB(), Registry: f.local, Clock: f.clock, Random: zeroReader{},
		LeaseOwner: "worker-a", Sender: f.sender,
		Alert: func(alert ReplicatorAlert) { alerts = append(alerts, alert) },
	})
	require.NoError(t, err)
	claimed, err := replicator.ReplicateOnce(t.Context())
	require.NoError(t, err)
	require.True(t, claimed)
	outboxState, messageState, _, last := replicationState(t, f)
	require.Equal(t, outboxFailedRetryable, outboxState)
	require.Equal(t, messageQueuedRemote, messageState)
	require.Equal(t, "INVALID_RECEIPT", last)
	require.Equal(t, []ReplicatorAlert{{Code: "INVALID_RECEIPT"}}, alerts)
	var receipts int64
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
		"SELECT count(*) FROM federation_receipts").Scan(&receipts))
	require.Zero(t, receipts)
}

func TestReplicatorSequencesPerPeerWhileOtherPeerProgresses(t *testing.T) {
	f := newReplicationFixture(t)
	f.addPeer(t, replicationOtherNode)
	messageB1 := protocol.ULID("01ARZ3NDEKTSV4RRFFQ69G5FAV")
	messageB2 := protocol.ULID("01ARZ3NDEKTSV4RRFFQ69G5FAW")
	messageC1 := protocol.ULID("01ARZ3NDEKTSV4RRFFQ69G5FAX")
	f.seed(t, replicationRequest(t, messageB1, deliveryTestDestinationNode, 1, 0x31), f.clock.now)
	f.seed(t, replicationRequest(t, messageB2, deliveryTestDestinationNode, 2, 0x32), f.clock.now)
	f.seed(t, replicationRequest(t, messageC1, replicationOtherNode, 1, 0x33), f.clock.now)
	var calls []protocol.ULID
	f.sender = deliverySenderFunc(func(_ context.Context, peer registry.Peer, raw []byte) (DeliveryResponse, error) {
		request, err := DecodeDeliveryRequest(raw)
		require.NoError(t, err)
		envelope, _, err := request.DecodeEnvelope()
		require.NoError(t, err)
		calls = append(calls, envelope.MessageID)
		if envelope.MessageID == messageB1 && len(calls) == 1 {
			return DeliveryResponse{}, errors.New("peer is down")
		}
		return f.signedResponse(t, peer.Node, raw, DeliveryReceiptStoredRemote, 200), nil
	})
	replicator := f.newReplicator(t, "worker-a")
	claimed, err := replicator.ReplicateOnce(t.Context())
	require.NoError(t, err)
	require.True(t, claimed)
	claimed, err = replicator.ReplicateOnce(t.Context())
	require.NoError(t, err)
	require.True(t, claimed)
	require.Equal(t, []protocol.ULID{messageB1, messageC1}, calls)
	claimed, err = replicator.ReplicateOnce(t.Context())
	require.NoError(t, err)
	require.False(t, claimed, "sequence 2 stays blocked behind sequence 1 backoff")

	f.clock.now = f.clock.now.Add(500 * time.Millisecond)
	claimed, err = replicator.ReplicateOnce(t.Context())
	require.NoError(t, err)
	require.True(t, claimed)
	claimed, err = replicator.ReplicateOnce(t.Context())
	require.NoError(t, err)
	require.True(t, claimed)
	require.Equal(t, []protocol.ULID{messageB1, messageC1, messageB1, messageB2}, calls)
}

func TestReplicatorReclaimsExpiredLeaseWithoutDoubleSend(t *testing.T) {
	f := newReplicationFixture(t)
	request := replicationRequest(t, deliveryTestMessage, deliveryTestDestinationNode, 1, 0x41)
	f.seed(t, request, f.clock.now)
	f.sender = deliverySenderFunc(func(_ context.Context, peer registry.Peer, raw []byte) (DeliveryResponse, error) {
		return f.signedResponse(t, peer.Node, raw, DeliveryReceiptStoredRemote, 200), nil
	})
	first := f.newReplicator(t, "worker-a")
	claimed, found, err := first.claim(t.Context())
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, int64(1), claimed.attempts)

	second := f.newReplicator(t, "worker-b")
	didWork, err := second.ReplicateOnce(t.Context())
	require.NoError(t, err)
	require.False(t, didWork)
	f.clock.now = f.clock.now.Add(defaultDeliveryLease)
	didWork, err = second.ReplicateOnce(t.Context())
	require.NoError(t, err)
	require.True(t, didWork)
	outboxState, _, attempts, _ := replicationState(t, f)
	require.Equal(t, outboxStoredRemote, outboxState)
	require.Equal(t, int64(2), attempts)
}

func TestConcurrentReplicatorsDoNotDoubleSendLiveLease(t *testing.T) {
	f := newReplicationFixture(t)
	request := replicationRequest(t, deliveryTestMessage, deliveryTestDestinationNode, 1, 0x42)
	f.seed(t, request, f.clock.now)
	entered := make(chan struct{})
	release := make(chan struct{})
	sends := make(chan struct{}, 2)
	f.sender = deliverySenderFunc(func(_ context.Context, peer registry.Peer, raw []byte) (DeliveryResponse, error) {
		sends <- struct{}{}
		close(entered)
		<-release
		return f.signedResponse(t, peer.Node, raw, DeliveryReceiptStoredRemote, 200), nil
	})
	first := f.newReplicator(t, "worker-a")
	second := f.newReplicator(t, "worker-b")
	done := make(chan error, 1)
	go func() {
		_, err := first.ReplicateOnce(t.Context())
		done <- err
	}()
	<-entered
	claimed, err := second.ReplicateOnce(t.Context())
	require.NoError(t, err)
	require.False(t, claimed)
	close(release)
	require.NoError(t, <-done)
	require.Len(t, sends, 1)
}

func TestReplicatorWaitsWhilePeerIsNotTrusted(t *testing.T) {
	for _, status := range []registry.PeerStatus{registry.PeerStatusSuspended, registry.PeerStatusRevoked} {
		t.Run(string(status), func(t *testing.T) {
			f := newReplicationFixture(t)
			request := replicationRequest(t, deliveryTestMessage, deliveryTestDestinationNode, 1, 0x45)
			f.seed(t, request, f.clock.now)
			require.NoError(t, f.local.SetPeerStatus(
				t.Context(), f.store.DB(), deliveryTestDestinationNode, status,
			))
			called := false
			f.sender = deliverySenderFunc(func(context.Context, registry.Peer, []byte) (DeliveryResponse, error) {
				called = true
				return DeliveryResponse{}, nil
			})
			claimed, err := f.newReplicator(t, "worker-a").ReplicateOnce(t.Context())
			require.NoError(t, err)
			require.False(t, claimed)
			require.False(t, called)
			outboxState, messageState, attempts, last := replicationState(t, f)
			require.Equal(t, outboxQueued, outboxState)
			require.Equal(t, messageQueuedRemote, messageState)
			require.Zero(t, attempts)
			require.Empty(t, last)
		})
	}
}

func TestReplicatorRecoversAfterReceiptTransactionFailure(t *testing.T) {
	f := newReplicationFixture(t)
	request := replicationRequest(t, deliveryTestMessage, deliveryTestDestinationNode, 1, 0x46)
	exact := f.seed(t, request, f.clock.now)
	var sent [][]byte
	f.sender = deliverySenderFunc(func(_ context.Context, peer registry.Peer, raw []byte) (DeliveryResponse, error) {
		sent = append(sent, append([]byte(nil), raw...))
		return f.signedResponse(t, peer.Node, raw, DeliveryReceiptStoredRemote, 200), nil
	})
	_, err := f.store.DB().ExecContext(t.Context(), `CREATE TRIGGER fail_federation_receipt
		BEFORE INSERT ON federation_receipts
		BEGIN SELECT RAISE(ABORT, 'injected receipt failure'); END`)
	require.NoError(t, err)
	replicator := f.newReplicator(t, "worker-a")
	claimed, err := replicator.ReplicateOnce(t.Context())
	require.Error(t, err)
	require.True(t, claimed)
	var receiptCount int64
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
		"SELECT count(*) FROM federation_receipts").Scan(&receiptCount))
	require.Zero(t, receiptCount)
	_, err = f.store.DB().ExecContext(t.Context(), "DROP TRIGGER fail_federation_receipt")
	require.NoError(t, err)
	f.clock.now = f.clock.now.Add(defaultDeliveryLease)
	claimed, err = replicator.ReplicateOnce(t.Context())
	require.NoError(t, err)
	require.True(t, claimed)
	require.Len(t, sent, 2)
	require.Equal(t, exact, sent[0])
	require.Equal(t, sent[0], sent[1])
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
		"SELECT count(*) FROM federation_receipts").Scan(&receiptCount))
	require.Equal(t, int64(1), receiptCount)
}

func TestReplicatorTerminalBoundariesAndControlDenied(t *testing.T) {
	t.Run("24h exact dead letters", func(t *testing.T) {
		f := newReplicationFixture(t)
		request := replicationRequest(t, deliveryTestMessage, deliveryTestDestinationNode, 1, 0x51)
		f.seed(t, request, f.clock.now.Add(-defaultDeliveryMaxAge))
		called := false
		f.sender = deliverySenderFunc(func(context.Context, registry.Peer, []byte) (DeliveryResponse, error) {
			called = true
			return DeliveryResponse{}, nil
		})
		claimed, err := f.newReplicator(t, "worker-a").ReplicateOnce(t.Context())
		require.NoError(t, err)
		require.True(t, claimed)
		require.False(t, called)
		outboxState, messageState, _, last := replicationState(t, f)
		require.Equal(t, outboxDeadLetter, outboxState)
		require.Equal(t, messageFailedPermanent, messageState)
		require.Equal(t, "MAX_AGE", last)
	})

	t.Run("24h minus 1ms still sends", func(t *testing.T) {
		f := newReplicationFixture(t)
		request := replicationRequest(t, deliveryTestMessage, deliveryTestDestinationNode, 1, 0x52)
		f.seed(t, request, f.clock.now.Add(-defaultDeliveryMaxAge+time.Millisecond))
		called := false
		f.sender = deliverySenderFunc(func(_ context.Context, peer registry.Peer, raw []byte) (DeliveryResponse, error) {
			called = true
			return f.signedResponse(t, peer.Node, raw, DeliveryReceiptStoredRemote, 200), nil
		})
		claimed, err := f.newReplicator(t, "worker-a").ReplicateOnce(t.Context())
		require.NoError(t, err)
		require.True(t, claimed)
		require.True(t, called)
	})

	t.Run("expires without sending", func(t *testing.T) {
		f := newReplicationFixture(t)
		request := replicationRequest(t, deliveryTestMessage, deliveryTestDestinationNode, 1, 0x53)
		envelope, _, err := request.DecodeEnvelope()
		require.NoError(t, err)
		envelope.ExpiresAt = f.clock.now
		request.Envelope, err = json.Marshal(envelope)
		require.NoError(t, err)
		require.NoError(t, request.Validate())
		f.seed(t, request, f.clock.now)
		called := false
		f.sender = deliverySenderFunc(func(context.Context, registry.Peer, []byte) (DeliveryResponse, error) {
			called = true
			return DeliveryResponse{}, nil
		})
		claimed, err := f.newReplicator(t, "worker-a").ReplicateOnce(t.Context())
		require.NoError(t, err)
		require.True(t, claimed)
		require.False(t, called)
		outboxState, messageState, _, last := replicationState(t, f)
		require.Equal(t, outboxExpired, outboxState)
		require.Equal(t, messageExpired, messageState)
		require.Equal(t, "EXPIRED", last)
	})

	t.Run("valid control denied is stored and final", func(t *testing.T) {
		f := newReplicationFixture(t)
		request := replicationRequest(t, deliveryTestMessage, deliveryTestDestinationNode, 1, 0x54)
		f.seed(t, request, f.clock.now)
		f.sender = deliverySenderFunc(func(_ context.Context, peer registry.Peer, raw []byte) (DeliveryResponse, error) {
			return f.signedResponse(t, peer.Node, raw, DeliveryReceiptControlDenied, 403), nil
		})
		claimed, err := f.newReplicator(t, "worker-a").ReplicateOnce(t.Context())
		require.NoError(t, err)
		require.True(t, claimed)
		outboxState, messageState, _, last := replicationState(t, f)
		require.Equal(t, outboxControlDenied, outboxState)
		require.Equal(t, messageFailedPermanent, messageState)
		require.Empty(t, last)
		var receiptState string
		require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
			"SELECT state FROM federation_receipts").Scan(&receiptState))
		require.Equal(t, string(DeliveryReceiptControlDenied), receiptState)
	})
}

func TestReplicatorDeadLettersDeterministicHTTPRefusals(t *testing.T) {
	for _, status := range []int{400, 409, 413} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			f := newReplicationFixture(t)
			request := replicationRequest(t, deliveryTestMessage, deliveryTestDestinationNode, 1, byte(status))
			f.seed(t, request, f.clock.now)
			f.sender = deliverySenderFunc(func(context.Context, registry.Peer, []byte) (DeliveryResponse, error) {
				return DeliveryResponse{StatusCode: status}, nil
			})
			claimed, err := f.newReplicator(t, "worker-a").ReplicateOnce(t.Context())
			require.NoError(t, err)
			require.True(t, claimed)
			outboxState, messageState, _, last := replicationState(t, f)
			require.Equal(t, outboxDeadLetter, outboxState)
			require.Equal(t, messageFailedPermanent, messageState)
			require.Equal(t, "HTTP_"+strconv.Itoa(status), last)
		})
	}
}

func TestReplicatorStoresValidAckReceipt(t *testing.T) {
	f := newReplicationFixture(t)
	request := replicationAckRequest(0x61, protocol.AckStatusProcessing)
	exact := f.seedAck(t, request, f.clock.now)
	var sent []byte
	f.sender = deliverySenderFunc(func(
		_ context.Context,
		peer registry.Peer,
		raw []byte,
	) (DeliveryResponse, error) {
		sent = append([]byte(nil), raw...)
		return f.signedAckResponse(t, peer.Node, raw), nil
	})
	claimed, err := f.newReplicator(t, "ack-worker").ReplicateOnce(t.Context())
	require.NoError(t, err)
	require.True(t, claimed)
	require.Equal(t, exact, sent)

	var state string
	var attempts int64
	var stored []byte
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(), `SELECT outbox.state,
		outbox.attempts, receipt.receipt FROM federation_outbox AS outbox
		JOIN federation_ack_receipts AS receipt ON receipt.outbox_id = outbox.outbox_id
		WHERE outbox.object_type = 'ack'`).Scan(&state, &attempts, &stored))
	require.Equal(t, outboxStoredRemote, state)
	require.Equal(t, int64(1), attempts)
	want := f.signedAckResponse(t, deliveryTestDestinationNode, exact)
	require.Equal(t, want.Body, stored)
}

func TestReplicatorRetriesMismatchedAckReceiptWithIdenticalRequest(t *testing.T) {
	f := newReplicationFixture(t)
	request := replicationAckRequest(0x62, protocol.AckStatusAcknowledged)
	exact := f.seedAck(t, request, f.clock.now)
	var sent [][]byte
	f.sender = deliverySenderFunc(func(
		_ context.Context,
		peer registry.Peer,
		raw []byte,
	) (DeliveryResponse, error) {
		sent = append(sent, append([]byte(nil), raw...))
		response := f.signedAckResponse(t, peer.Node, raw)
		if len(sent) == 1 {
			var receipt AckReceipt
			require.NoError(t, json.Unmarshal(response.Body, &receipt))
			receipt.Sequence++
			require.NoError(t, receipt.Sign(f.peers[peer.Node]))
			encoded, err := json.Marshal(receipt)
			require.NoError(t, err)
			response.Body = encoded
		}
		return response, nil
	})
	replicator := f.newReplicator(t, "ack-worker")
	claimed, err := replicator.ReplicateOnce(t.Context())
	require.NoError(t, err)
	require.True(t, claimed)
	var state, nextAttempt string
	var attempts int64
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(), `SELECT state, attempts,
		next_attempt_at FROM federation_outbox WHERE object_type = 'ack'`).Scan(
		&state, &attempts, &nextAttempt,
	))
	require.Equal(t, outboxFailedRetryable, state)
	require.Equal(t, int64(1), attempts)
	require.Zero(t, func() int64 {
		var count int64
		require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
			"SELECT count(*) FROM federation_ack_receipts").Scan(&count))
		return count
	}())

	retryAt, err := time.Parse(eventstore.TimeLayout, nextAttempt)
	require.NoError(t, err)
	f.clock.now = retryAt
	claimed, err = replicator.ReplicateOnce(t.Context())
	require.NoError(t, err)
	require.True(t, claimed)
	require.Len(t, sent, 2)
	require.Equal(t, exact, sent[0])
	require.Equal(t, sent[0], sent[1])
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(), `SELECT state, attempts
		FROM federation_outbox WHERE object_type = 'ack'`).Scan(&state, &attempts))
	require.Equal(t, outboxStoredRemote, state)
	require.Equal(t, int64(2), attempts)
}

func TestReplicatorRejectsBadAckReceiptAuthenticity(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, *replicationFixture, *AckReceipt)
	}{
		{"bad signature", func(_ *testing.T, _ *replicationFixture, receipt *AckReceipt) {
			receipt.Signature[0] ^= 0xff
		}},
		{"other key", func(t *testing.T, f *replicationFixture, receipt *AckReceipt) {
			wrong, err := registry.New("node-wrong-ack-signer", privateDataDir(t), f.clock)
			require.NoError(t, err)
			require.NoError(t, receipt.Sign(wrong))
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newReplicationFixture(t)
			request := replicationAckRequest(0x63, protocol.AckStatusAcknowledged)
			f.seedAck(t, request, f.clock.now)
			f.sender = deliverySenderFunc(func(
				_ context.Context,
				peer registry.Peer,
				raw []byte,
			) (DeliveryResponse, error) {
				response := f.signedAckResponse(t, peer.Node, raw)
				var receipt AckReceipt
				require.NoError(t, json.Unmarshal(response.Body, &receipt))
				test.mutate(t, f, &receipt)
				encoded, err := json.Marshal(receipt)
				require.NoError(t, err)
				return DeliveryResponse{StatusCode: http.StatusOK, Body: encoded}, nil
			})
			claimed, err := f.newReplicator(t, "ack-auth-worker").ReplicateOnce(t.Context())
			require.NoError(t, err)
			require.True(t, claimed)
			var state, lastError string
			require.NoError(t, f.store.DB().QueryRowContext(t.Context(), `SELECT state,
				last_error FROM federation_outbox WHERE object_type = 'ack'`).Scan(
				&state, &lastError,
			))
			require.Equal(t, outboxFailedRetryable, state)
			require.Equal(t, "INVALID_ACK_RECEIPT", lastError)
			var receipts int64
			require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
				"SELECT count(*) FROM federation_ack_receipts").Scan(&receipts))
			require.Zero(t, receipts)
		})
	}
}

func TestReplicatorAckHTTPClassificationAndMaxAge(t *testing.T) {
	for _, test := range []struct {
		name      string
		status    int
		wantState string
		wantError string
	}{
		{"bad request", http.StatusBadRequest, outboxDeadLetter, "HTTP_400"},
		{"conflict", http.StatusConflict, outboxDeadLetter, "HTTP_409"},
		{"too large", http.StatusRequestEntityTooLarge, outboxDeadLetter, "HTTP_413"},
		{"forbidden", http.StatusForbidden, outboxFailedRetryable, "HTTP_403"},
		{"server", http.StatusServiceUnavailable, outboxFailedRetryable, "HTTP_5XX"},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newReplicationFixture(t)
			request := replicationAckRequest(byte(test.status), protocol.AckStatusProcessing)
			f.seedAck(t, request, f.clock.now)
			f.sender = deliverySenderFunc(func(
				context.Context, registry.Peer, []byte,
			) (DeliveryResponse, error) {
				return DeliveryResponse{StatusCode: test.status}, nil
			})
			claimed, err := f.newReplicator(t, "ack-worker").ReplicateOnce(t.Context())
			require.NoError(t, err)
			require.True(t, claimed)
			var state string
			var lastError *string
			require.NoError(t, f.store.DB().QueryRowContext(t.Context(), `SELECT state,
				last_error FROM federation_outbox WHERE object_type = 'ack'`).Scan(
				&state, &lastError,
			))
			require.Equal(t, test.wantState, state)
			require.NotNil(t, lastError)
			require.Equal(t, test.wantError, *lastError)
		})
	}

	t.Run("max age", func(t *testing.T) {
		f := newReplicationFixture(t)
		request := replicationAckRequest(0x6f, protocol.AckStatusProcessing)
		f.seedAck(t, request,
			f.clock.now.Add(-defaultDeliveryMaxAge))
		called := false
		f.sender = deliverySenderFunc(func(
			context.Context, registry.Peer, []byte,
		) (DeliveryResponse, error) {
			called = true
			return DeliveryResponse{}, nil
		})
		claimed, err := f.newReplicator(t, "ack-worker").ReplicateOnce(t.Context())
		require.NoError(t, err)
		require.True(t, claimed)
		require.False(t, called)
		var state, lastError string
		require.NoError(t, f.store.DB().QueryRowContext(t.Context(), `SELECT state,
			last_error FROM federation_outbox WHERE object_type = 'ack'`).Scan(
			&state, &lastError,
		))
		require.Equal(t, outboxDeadLetter, state)
		require.Equal(t, "MAX_AGE", lastError)
	})
}

func TestReplicatorMessageAndAckStreamsDoNotBlockEachOther(t *testing.T) {
	for _, firstFailure := range []string{federationObjectMessage, federationObjectAck} {
		t.Run(firstFailure+" retry does not block other stream", func(t *testing.T) {
			f := newReplicationFixture(t)
			deliverySequence, ackSequence := int64(1), int64(2)
			if firstFailure == federationObjectAck {
				deliverySequence, ackSequence = ackSequence, deliverySequence
			}
			delivery := replicationRequest(
				t, deliveryTestMessage, deliveryTestDestinationNode, deliverySequence, 0x71,
			)
			ack := replicationAckRequest(0x72, protocol.AckStatusProcessing)
			ack.Sequence = ackSequence
			deliveryCreated := f.clock.now
			ackCreated := f.clock.now.Add(time.Millisecond)
			if firstFailure == federationObjectAck {
				deliveryCreated, ackCreated = ackCreated, deliveryCreated
			}
			f.seed(t, delivery, deliveryCreated)
			f.seedAck(t, ack, ackCreated)
			calls := make([]string, 0, 2)
			f.sender = deliverySenderFunc(func(
				_ context.Context,
				peer registry.Peer,
				raw []byte,
			) (DeliveryResponse, error) {
				if _, err := DecodeAckRequest(raw); err == nil {
					calls = append(calls, federationObjectAck)
					if firstFailure == federationObjectAck {
						return DeliveryResponse{}, errors.New("ack transport unavailable")
					}
					return f.signedAckResponse(t, peer.Node, raw), nil
				}
				calls = append(calls, federationObjectMessage)
				if firstFailure == federationObjectMessage {
					return DeliveryResponse{}, errors.New("delivery transport unavailable")
				}
				return f.signedResponse(
					t, peer.Node, raw, DeliveryReceiptStoredRemote, http.StatusOK,
				), nil
			})
			replicator := f.newReplicator(t, "streams-worker")
			claimed, err := replicator.ReplicateOnce(t.Context())
			require.NoError(t, err)
			require.True(t, claimed)
			claimed, err = replicator.ReplicateOnce(t.Context())
			require.NoError(t, err)
			require.True(t, claimed)
			require.Equal(t, []string{firstFailure, map[string]string{
				federationObjectMessage: federationObjectAck,
				federationObjectAck:     federationObjectMessage,
			}[firstFailure]}, calls)

			rows, err := f.store.DB().QueryContext(t.Context(), `SELECT object_type, state
				FROM federation_outbox ORDER BY object_type`)
			require.NoError(t, err)
			defer func() { require.NoError(t, rows.Close()) }()
			states := make(map[string]string)
			for rows.Next() {
				var objectType, state string
				require.NoError(t, rows.Scan(&objectType, &state))
				states[objectType] = state
			}
			require.NoError(t, rows.Err())
			require.Equal(t, outboxFailedRetryable, states[firstFailure])
			require.Equal(t, outboxStoredRemote, states[map[string]string{
				federationObjectMessage: federationObjectAck,
				federationObjectAck:     federationObjectMessage,
			}[firstFailure]])
		})
	}
}
