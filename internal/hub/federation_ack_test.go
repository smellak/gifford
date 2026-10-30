package hub

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/federation"
	"github.com/smellak/gifford/internal/protocol"
)

const outboxSecondRecipient = protocol.AgentID("agent-outbox-recipient-two")

func queueOutboundWithParams(
	t *testing.T,
	f *outboundFederationFixture,
	params AdmitMessageParams,
) protocol.ULID {
	t.Helper()
	admitted, err := f.admit(t, params)
	require.NoError(t, err)
	envelope := f.envelope(t, params, admitted)
	exact, err := json.Marshal(envelope)
	require.NoError(t, err)
	_, err = f.service.SubmitMessageEnvelope(t.Context(), outboxSender, exact)
	require.NoError(t, err)
	return admitted.MessageID
}

func storedRemoteAckRequest(
	t *testing.T,
	f *outboundFederationFixture,
	params AdmitMessageParams,
	status protocol.AckStatus,
	nonce byte,
) federation.AckRequest {
	t.Helper()
	messageID := queueOutboundWithParams(t, f, params)
	_, err := f.store.DB().ExecContext(t.Context(),
		"UPDATE messages SET state = 'STORED_REMOTE' WHERE message_id = ?", messageID)
	require.NoError(t, err)
	return federation.AckRequest{
		MessageID: messageID, Agent: params.Recipients[0].Agent,
		DeliveryGeneration: 1, Status: status, AckedAt: f.clock.now,
		Nonce: bytes.Repeat([]byte{nonce}, 16), Sequence: 1,
	}
}

func decodeFederatedAckReceipt(t *testing.T, raw []byte) federation.AckReceipt {
	t.Helper()
	receipt, err := federation.DecodeAckReceipt(raw)
	require.NoError(t, err)
	return receipt
}

func TestAckMessageQueuesOnlyNewFederatedProgress(t *testing.T) {
	f := newFederatedHubFixture(t, true)
	status, _ := f.post(t, f.request)
	require.Equal(t, 200, status)
	claimRaw, err := json.Marshal(ClaimMessageParams{
		MessageID: federationTestMessage, DeliveryGeneration: 1,
	})
	require.NoError(t, err)
	_, err = f.service.ClaimMessage(t.Context(), testHubRecipient, claimRaw)
	require.NoError(t, err)

	ack := func(status AckMessageStatus) {
		raw, marshalErr := json.Marshal(AckMessageParams{
			MessageID: federationTestMessage, DeliveryGeneration: 1, Status: status,
		})
		require.NoError(t, marshalErr)
		_, callErr := f.service.AckMessage(t.Context(), testHubRecipient, raw)
		require.NoError(t, callErr)
	}
	ack(AckMessageProcessing)
	require.Equal(t, int64(1), federationTableCount(t, f.store, "federation_outbox_acks"))
	ack(AckMessageProcessing)
	require.Equal(t, int64(1), federationTableCount(t, f.store, "federation_outbox_acks"))
	ack(AckMessageAcknowledged)
	require.Equal(t, int64(2), federationTableCount(t, f.store, "federation_outbox_acks"))
	ack(AckMessageMismatch)
	require.Equal(t, int64(2), federationTableCount(t, f.store, "federation_outbox_acks"))

	rows, err := f.store.DB().QueryContext(t.Context(), `SELECT request, sequence, destination_node
		FROM federation_outbox_acks ORDER BY sequence`)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()
	wantStatuses := []protocol.AckStatus{
		protocol.AckStatusProcessing, protocol.AckStatusAcknowledged,
	}
	index := 0
	for rows.Next() {
		var raw []byte
		var sequence int64
		var destination protocol.NodeID
		require.NoError(t, rows.Scan(&raw, &sequence, &destination))
		request, decodeErr := federation.DecodeAckRequest(raw)
		require.NoError(t, decodeErr)
		require.Equal(t, int64(index+1), sequence)
		require.Equal(t, wantStatuses[index], request.Status)
		require.Equal(t, federationTestOriginNode, destination)
		index++
	}
	require.NoError(t, rows.Err())
	require.Equal(t, len(wantStatuses), index)
}

func TestLocalAckMessageDoesNotCreateFederationWork(t *testing.T) {
	f := newHubFixture(t)
	params := testAdmitParams(f)
	answer, err := callAdmit(t, f, params)
	require.NoError(t, err)
	admitted := answer.(AdmitMessageResult)
	params = resolvedAdmissionParams(params, admitted)
	envelope := signedEnvelopeFor(t, f, admitted, params)
	exact, err := json.Marshal(envelope)
	require.NoError(t, err)
	_, err = f.service.SubmitMessageEnvelope(t.Context(), testHubSender, exact)
	require.NoError(t, err)
	claimRaw, err := json.Marshal(ClaimMessageParams{
		MessageID: admitted.MessageID, DeliveryGeneration: 1,
	})
	require.NoError(t, err)
	_, err = f.service.ClaimMessage(t.Context(), testHubRecipient, claimRaw)
	require.NoError(t, err)
	ackRaw, err := json.Marshal(AckMessageParams{
		MessageID: admitted.MessageID, DeliveryGeneration: 1, Status: AckMessageAcknowledged,
	})
	require.NoError(t, err)
	_, err = f.service.AckMessage(t.Context(), testHubRecipient, ackRaw)
	require.NoError(t, err)
	require.Zero(t, federationTableCount(t, f.store, "federation_outbox_acks"))
}

func TestReceiveFederatedAckProgressReplayAndClosure(t *testing.T) {
	f := newOutboundFederationFixture(t)
	processing := storedRemoteAckRequest(
		t, f, f.params("client-ack-progress"), protocol.AckStatusProcessing, 0x51,
	)
	first, err := f.service.ReceiveFederatedAck(t.Context(), outboxDestinationNode, processing)
	require.NoError(t, err)
	replayed, err := f.service.ReceiveFederatedAck(t.Context(), outboxDestinationNode, processing)
	require.NoError(t, err)
	require.Equal(t, first, replayed)
	receipt := decodeFederatedAckReceipt(t, first)
	publicKey, err := f.localRegistry.NodeSigningPublicKey()
	require.NoError(t, err)
	require.NoError(t, receipt.Verify(publicKey))
	digest, err := processing.RequestSHA256()
	require.NoError(t, err)
	require.Equal(t, digest, receipt.RequestSHA256)
	require.Equal(t, outboxOriginNode, receipt.OriginNode)
	require.Equal(t, outboxDestinationNode, receipt.DestinationNode)
	require.Equal(t, int64(1), federationTableCount(t, f.store, "federation_ack_inbox"))

	var state string
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
		"SELECT state FROM messages WHERE message_id = ?", processing.MessageID).Scan(&state))
	require.Equal(t, messageStateStoredRemote, state)

	acknowledged := processing
	acknowledged.Status = protocol.AckStatusAcknowledged
	acknowledged.Sequence = 2
	acknowledged.Nonce = bytes.Repeat([]byte{0x52}, 16)
	acknowledged.AckedAt = acknowledged.AckedAt.Add(time.Second)
	_, err = f.service.ReceiveFederatedAck(t.Context(), outboxDestinationNode, acknowledged)
	require.NoError(t, err)
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
		"SELECT state FROM messages WHERE message_id = ?", processing.MessageID).Scan(&state))
	require.Equal(t, messageStateAcknowledged, state)
	require.Equal(t, int64(2), federationTableCount(t, f.store, "acks"))
	require.Equal(t, int64(2), federationTableCount(t, f.store, "federation_ack_inbox"))
}

func TestReceiveFederatedAckRejectsBindingBeforeEffects(t *testing.T) {
	for _, test := range []struct {
		name  string
		patch func(*outboundFederationFixture, *federation.AckRequest) protocol.NodeID
	}{
		{"queued remote", func(_ *outboundFederationFixture, _ *federation.AckRequest) protocol.NodeID {
			return outboxDestinationNode
		}},
		{"wrong peer", func(f *outboundFederationFixture, request *federation.AckRequest) protocol.NodeID {
			_, err := f.store.DB().ExecContext(t.Context(),
				"UPDATE messages SET state = 'STORED_REMOTE' WHERE message_id = ?", request.MessageID)
			require.NoError(t, err)
			return "node-outbox-other"
		}},
		{"wrong recipient", func(f *outboundFederationFixture, request *federation.AckRequest) protocol.NodeID {
			_, err := f.store.DB().ExecContext(t.Context(),
				"UPDATE messages SET state = 'STORED_REMOTE' WHERE message_id = ?", request.MessageID)
			require.NoError(t, err)
			request.Agent = "agent-not-sent"
			return outboxDestinationNode
		}},
		{"wrong generation", func(f *outboundFederationFixture, request *federation.AckRequest) protocol.NodeID {
			_, err := f.store.DB().ExecContext(t.Context(),
				"UPDATE messages SET state = 'STORED_REMOTE' WHERE message_id = ?", request.MessageID)
			require.NoError(t, err)
			request.DeliveryGeneration++
			return outboxDestinationNode
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newOutboundFederationFixture(t)
			params := f.params(protocol.OpaqueID(
				"client-ack-binding-" + strings.ReplaceAll(test.name, " ", "-"),
			))
			messageID := queueOutboundWithParams(t, f, params)
			request := federation.AckRequest{
				MessageID: messageID, Agent: outboxRecipient, DeliveryGeneration: 1,
				Status: protocol.AckStatusAcknowledged, AckedAt: f.clock.now,
				Nonce: bytes.Repeat([]byte{0x61}, 16), Sequence: 1,
			}
			peer := test.patch(f, &request)
			_, err := f.service.ReceiveFederatedAck(t.Context(), peer, request)
			require.Error(t, err)
			require.Zero(t, federationTableCount(t, f.store, "acks"))
			require.Zero(t, federationTableCount(t, f.store, "federation_ack_inbox"))
		})
	}
}

func TestReceiveFederatedAckConflictsAndWindow(t *testing.T) {
	f := newOutboundFederationFixture(t)
	request := storedRemoteAckRequest(
		t, f, f.params("client-ack-conflicts"), protocol.AckStatusProcessing, 0x71,
	)
	_, err := f.service.ReceiveFederatedAck(t.Context(), outboxDestinationNode, request)
	require.NoError(t, err)

	sequenceCollision := request
	sequenceCollision.Status = protocol.AckStatusAcknowledged
	_, err = f.service.ReceiveFederatedAck(t.Context(), outboxDestinationNode, sequenceCollision)
	require.Error(t, err)
	nonceCollision := sequenceCollision
	nonceCollision.Sequence = 2
	_, err = f.service.ReceiveFederatedAck(t.Context(), outboxDestinationNode, nonceCollision)
	require.Error(t, err)
	identityCollision := request
	identityCollision.Sequence = 3
	identityCollision.Nonce = bytes.Repeat([]byte{0x73}, 16)
	_, err = f.service.ReceiveFederatedAck(t.Context(), outboxDestinationNode, identityCollision)
	require.Error(t, err)
	require.Equal(t, int64(1), federationTableCount(t, f.store, "federation_ack_inbox"))

	stamp := eventstore.FormatTime(f.clock.now)
	_, err = f.store.DB().ExecContext(t.Context(), `INSERT INTO federation_ack_inbox
		(origin_node, sequence, nonce, message_id, agent_id, delivery_generation,
		 status, request_sha256, receipt, received_at)
		VALUES (?, 2000, ?, ?, 'agent-window', 1, 'PROCESSING', ?, 'prior', ?)`,
		outboxDestinationNode, bytes.Repeat([]byte{0x74}, 16), federationTestOtherMessage,
		strings.Repeat("a", 64), stamp)
	require.NoError(t, err)
	tooOld := sequenceCollision
	tooOld.Sequence = 975
	tooOld.Nonce = bytes.Repeat([]byte{0x75}, 16)
	_, err = f.service.ReceiveFederatedAck(t.Context(), outboxDestinationNode, tooOld)
	require.Error(t, err)
	boundary := tooOld
	boundary.Sequence = 976
	boundary.Nonce = bytes.Repeat([]byte{0x76}, 16)
	_, err = f.service.ReceiveFederatedAck(t.Context(), outboxDestinationNode, boundary)
	require.NoError(t, err)
}

func TestReceiveFederatedAckMultiRecipientAndOptionalRecipientClosure(t *testing.T) {
	t.Run("multi recipient", func(t *testing.T) {
		f := newOutboundFederationFixture(t)
		registerOutboundAgent(t, f.remote, f.remoteRegistry, outboxDestinationNode, outboxSecondRecipient)
		directory, err := f.remoteRegistry.ExportDirectory(t.Context(), f.remote.DB())
		require.NoError(t, err)
		_, err = f.localRegistry.ImportDirectory(
			t.Context(), f.store.DB(), outboxDestinationNode, directory,
		)
		require.NoError(t, err)
		params := f.params("client-ack-multi")
		params.Recipients = append(params.Recipients, MessageRecipient{
			Agent: outboxSecondRecipient, DeliveryMode: protocol.DeliveryModeDisplayOnly,
			RequiresAck: true,
		})
		first := storedRemoteAckRequest(
			t, f, params, protocol.AckStatusAcknowledged, 0x81,
		)
		_, err = f.service.ReceiveFederatedAck(t.Context(), outboxDestinationNode, first)
		require.NoError(t, err)
		var state string
		require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
			"SELECT state FROM messages WHERE message_id = ?", first.MessageID).Scan(&state))
		require.Equal(t, messageStateStoredRemote, state)

		second := first
		second.Agent = outboxSecondRecipient
		second.Sequence = 2
		second.Nonce = bytes.Repeat([]byte{0x82}, 16)
		_, err = f.service.ReceiveFederatedAck(t.Context(), outboxDestinationNode, second)
		require.NoError(t, err)
		require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
			"SELECT state FROM messages WHERE message_id = ?", first.MessageID).Scan(&state))
		require.Equal(t, messageStateAcknowledged, state)
	})

	t.Run("optional recipient", func(t *testing.T) {
		f := newOutboundFederationFixture(t)
		params := f.params("client-ack-optional")
		params.Recipients[0].RequiresAck = false
		request := storedRemoteAckRequest(
			t, f, params, protocol.AckStatusAcknowledged, 0x83,
		)
		_, err := f.service.ReceiveFederatedAck(t.Context(), outboxDestinationNode, request)
		require.NoError(t, err)
		var agent protocol.AgentID
		var status protocol.AckStatus
		require.NoError(t, f.store.DB().QueryRowContext(t.Context(), `SELECT agent_id, status
			FROM acks WHERE message_id = ?`, request.MessageID).Scan(&agent, &status))
		require.Equal(t, request.Agent, agent)
		require.Equal(t, protocol.AckStatusAcknowledged, status)
		var state string
		require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
			"SELECT state FROM messages WHERE message_id = ?", request.MessageID).Scan(&state))
		require.Equal(t, messageStateStoredRemote, state)
	})

	t.Run("required recipient closes without optional recipient acknowledgement", func(t *testing.T) {
		f := newOutboundFederationFixture(t)
		registerOutboundAgent(t, f.remote, f.remoteRegistry, outboxDestinationNode, outboxSecondRecipient)
		directory, err := f.remoteRegistry.ExportDirectory(t.Context(), f.remote.DB())
		require.NoError(t, err)
		_, err = f.localRegistry.ImportDirectory(
			t.Context(), f.store.DB(), outboxDestinationNode, directory,
		)
		require.NoError(t, err)
		params := f.params("client-ack-required-and-optional")
		params.Recipients = append(params.Recipients, MessageRecipient{
			Agent: outboxSecondRecipient, DeliveryMode: protocol.DeliveryModeDisplayOnly,
			RequiresAck: false,
		})
		request := storedRemoteAckRequest(
			t, f, params, protocol.AckStatusAcknowledged, 0x84,
		)
		_, err = f.service.ReceiveFederatedAck(t.Context(), outboxDestinationNode, request)
		require.NoError(t, err)
		var state string
		require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
			"SELECT state FROM messages WHERE message_id = ?", request.MessageID).Scan(&state))
		require.Equal(t, messageStateAcknowledged, state)
		var optionalAcks int64
		require.NoError(t, f.store.DB().QueryRowContext(t.Context(), `SELECT count(*) FROM acks
			WHERE message_id = ? AND agent_id = ?`, request.MessageID, outboxSecondRecipient).
			Scan(&optionalAcks))
		require.Zero(t, optionalAcks)
	})
}

func TestReceiveFederatedAckRollsBackAtomicFailure(t *testing.T) {
	f := newOutboundFederationFixture(t)
	request := storedRemoteAckRequest(
		t, f, f.params("client-ack-atomic"), protocol.AckStatusAcknowledged, 0x91,
	)
	_, err := f.store.DB().ExecContext(t.Context(), `CREATE TRIGGER fail_ack_inbox
		BEFORE INSERT ON federation_ack_inbox
		BEGIN SELECT RAISE(ABORT, 'injected ack inbox failure'); END`)
	require.NoError(t, err)
	_, err = f.service.ReceiveFederatedAck(t.Context(), outboxDestinationNode, request)
	require.Error(t, err)
	require.Zero(t, federationTableCount(t, f.store, "acks"))
	require.Zero(t, federationTableCount(t, f.store, "federation_ack_inbox"))
	var state string
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
		"SELECT state FROM messages WHERE message_id = ?", request.MessageID).Scan(&state))
	require.Equal(t, messageStateStoredRemote, state)

	_, err = f.store.DB().ExecContext(t.Context(), "DROP TRIGGER fail_ack_inbox")
	require.NoError(t, err)
	_, err = f.service.ReceiveFederatedAck(t.Context(), outboxDestinationNode, request)
	require.NoError(t, err)
	require.Equal(t, int64(1), federationTableCount(t, f.store, "acks"))
	require.Equal(t, int64(1), federationTableCount(t, f.store, "federation_ack_inbox"))
}
