package federation

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
)

const (
	deliveryTestMessage         = protocol.ULID("01ARZ3NDEKTSV4RRFFQ69G5FAV")
	deliveryTestOriginNode      = protocol.NodeID("node-a")
	deliveryTestDestinationNode = protocol.NodeID("node-b")
	deliveryTestOriginAgent     = protocol.AgentID("agent-a")
	deliveryTestRecipientB      = protocol.AgentID("agent-b")
	deliveryTestRecipientC      = protocol.AgentID("agent-c")
	deliveryTestSenderKeyID     = "ed25519:agent-a:v1"
	deliveryTestGenerationCase  = "generation"
	deliveryTestNonceCase       = "nonce"
	deliveryTestSequenceCase    = "sequence"
)

func testDeliveryRequest(t *testing.T) DeliveryRequest {
	t.Helper()
	created := time.Date(2026, time.October, 29, 22, 0, 0, 0, time.UTC)
	ciphertext := []byte("opaque federation ciphertext")
	ciphertextDigest := sha256.Sum256(ciphertext)
	envelope := protocol.WireEnvelope{
		Protocol: protocol.WireProtocol, Version: protocol.WireVersion,
		MessageID: deliveryTestMessage, ClientMsgID: "client-delivery-test",
		OriginNode: deliveryTestOriginNode, OriginAgent: deliveryTestOriginAgent,
		DestinationNode: deliveryTestDestinationNode,
		RecipientAgents: []protocol.AgentID{deliveryTestRecipientB, deliveryTestRecipientC},
		CreatedAt:       created, ExpiresAt: created.Add(time.Hour), Priority: protocol.PriorityNormal,
		ThreadRef: "thr_01ARZ3NDEKTSV4RRFFQ69G5FAV", HopCount: 0,
		RouteDigest: protocol.Digest("sha256:" + strings.Repeat("a", 64)), ControlVersion: 7,
		ContentType: protocol.WireContentType, EncryptionProfile: protocol.EncryptionProfileNone,
		SenderSigningKeyID: deliveryTestSenderKeyID, RecipientKeyEnvelopes: []protocol.RecipientKeyEnvelope{},
		Ciphertext: ciphertext, CiphertextSHA256: protocol.SHA256Hex(hex.EncodeToString(ciphertextDigest[:])),
		Signature: bytes.Repeat([]byte{0x42}, ed25519.SignatureSize),
	}
	require.NoError(t, envelope.Validate())
	raw, err := json.Marshal(envelope)
	require.NoError(t, err)
	validFrom := eventstore.FormatTime(created.Add(-time.Hour))
	return DeliveryRequest{
		Envelope: append(append([]byte(" \n"), raw...), '\t'),
		ThreadID: "thread-federation", Recipients: []DeliveryRecipient{
			{Agent: deliveryTestRecipientB, Mode: protocol.DeliveryModeWhenSafe, RequiresAck: true},
			{Agent: deliveryTestRecipientC, Mode: protocol.DeliveryModeDisplayOnly, RequiresAck: false},
		},
		DeliveryGeneration: 1, Nonce: bytes.Repeat([]byte{0x11}, deliveryNonceBytes), Sequence: 1,
		OriginControl: protocol.ObjectControlState{
			ObjectType: protocol.DecisionObjectTypeMessage, ObjectID: protocol.OpaqueID(deliveryTestMessage),
			ControlState: protocol.ControlStateAllowed, ControlReason: protocol.DecisionReasonAllowed,
			ControlVersion: 7,
		},
		Sender: registry.DirectoryRecord{
			KeyID: deliveryTestSenderKeyID, Agent: deliveryTestOriginAgent, Node: deliveryTestOriginNode,
			Purpose: "signing", Algorithm: "ed25519", PublicKey: bytes.Repeat([]byte{0x22}, ed25519.PublicKeySize),
			Status: activeDirectoryStatus, ValidFrom: validFrom, Issuer: "node:" + string(deliveryTestOriginNode),
			RecordSignature: bytes.Repeat([]byte{0x33}, ed25519.SignatureSize),
		},
	}
}

func TestDeliveryRequestValidatesFrameRelationships(t *testing.T) {
	t.Parallel()
	valid := testDeliveryRequest(t)
	require.NoError(t, valid.Validate())

	newer := testDeliveryRequest(t)
	newer.OriginControl.ControlVersion++
	newer.OriginControl.ControlReason = protocol.DecisionReasonHoldReleased
	newer.OriginControl.ApprovalID = "apr_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	require.NoError(t, newer.Validate())

	tests := []struct {
		name  string
		patch func(*DeliveryRequest)
	}{
		{"recipient order", func(r *DeliveryRequest) { r.Recipients[0], r.Recipients[1] = r.Recipients[1], r.Recipients[0] }},
		{"recipient mode", func(r *DeliveryRequest) { r.Recipients[0].Mode = "UNKNOWN" }},
		{deliveryTestGenerationCase, func(r *DeliveryRequest) { r.DeliveryGeneration = 0 }},
		{deliveryTestNonceCase, func(r *DeliveryRequest) { r.Nonce = r.Nonce[:15] }},
		{deliveryTestSequenceCase, func(r *DeliveryRequest) { r.Sequence = 0 }},
		{"origin state", func(r *DeliveryRequest) { r.OriginControl.ControlState = protocol.ControlStateHeld }},
		{"origin object", func(r *DeliveryRequest) { r.OriginControl.ObjectID = "other-message" }},
		{"origin version", func(r *DeliveryRequest) { r.OriginControl.ControlVersion-- }},
		{"origin reason", func(r *DeliveryRequest) { r.OriginControl.ControlReason = protocol.DecisionReasonLinkHold }},
		{"origin cancelled", func(r *DeliveryRequest) { r.OriginControl.Cancelled = true }},
		{"sender key", func(r *DeliveryRequest) { r.Sender.KeyID = "ed25519:agent-a:v2" }},
		{"sender agent", func(r *DeliveryRequest) { r.Sender.Agent = "agent-other" }},
		{"sender node", func(r *DeliveryRequest) { r.Sender.Node = "node-other" }},
		{"sender signature", func(r *DeliveryRequest) { r.Sender.RecordSignature = nil }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			request := testDeliveryRequest(t)
			test.patch(&request)
			require.Error(t, request.Validate())
		})
	}
}

func TestDeliveryRequestHashesCanonicalRequestAndExactEnvelope(t *testing.T) {
	t.Parallel()
	request := testDeliveryRequest(t)
	canonical, err := request.CanonicalBytes()
	require.NoError(t, err)
	wantRequest := sha256.Sum256(canonical)
	requestDigest, err := request.RequestSHA256()
	require.NoError(t, err)
	require.Equal(t, protocol.SHA256Hex(hex.EncodeToString(wantRequest[:])), requestDigest)

	_, exact, err := request.DecodeEnvelope()
	require.NoError(t, err)
	wantEnvelope := sha256.Sum256(exact)
	envelopeDigest, err := request.EnvelopeSHA256()
	require.NoError(t, err)
	require.Equal(t, protocol.SHA256Hex(hex.EncodeToString(wantEnvelope[:])), envelopeDigest)

	changed := request
	changed.Sequence++
	changedDigest, err := changed.RequestSHA256()
	require.NoError(t, err)
	require.NotEqual(t, requestDigest, changedDigest)

	wire, err := json.Marshal(request)
	require.NoError(t, err)
	decoded, err := DecodeDeliveryRequest(wire)
	require.NoError(t, err)
	decodedDigest, err := decoded.RequestSHA256()
	require.NoError(t, err)
	require.Equal(t, requestDigest, decodedDigest)
}

func TestDeliveryReceiptSignsUnderNodeDomain(t *testing.T) {
	t.Parallel()
	reg, err := registry.New("node-b", privateDataDir(t), clock.System{})
	require.NoError(t, err)
	receipt := DeliveryReceipt{
		MessageID: deliveryTestMessage, OriginNode: deliveryTestOriginNode,
		DestinationNode:    deliveryTestDestinationNode,
		RecipientAgents:    []protocol.AgentID{deliveryTestRecipientB, deliveryTestRecipientC},
		DeliveryGeneration: 3,
		Nonce:              bytes.Repeat([]byte{0x55}, deliveryNonceBytes),
		Sequence:           17,
		RequestSHA256:      protocol.SHA256Hex(strings.Repeat("a", 64)),
		EnvelopeSHA256:     protocol.SHA256Hex(strings.Repeat("b", 64)),
		Status:             DeliveryReceiptStoredRemote,
		ReceivedAt:         time.Date(2026, time.October, 29, 22, 1, 0, 0, time.UTC),
	}
	require.NoError(t, receipt.Sign(reg))
	require.Len(t, receipt.Signature, ed25519.SignatureSize)
	publicKey, err := reg.NodeSigningPublicKey()
	require.NoError(t, err)
	require.NoError(t, receipt.Verify(publicKey))

	payload, err := receipt.signingPayload()
	require.NoError(t, err)
	domainInput := append([]byte("gifford-federation-receipt-v1\x00"), payload...)
	require.True(t, ed25519.Verify(publicKey, domainInput, receipt.Signature))

	tampered := receipt
	tampered.RequestSHA256 = protocol.SHA256Hex(strings.Repeat("c", 64))
	require.Error(t, tampered.Verify(publicKey))
	for _, test := range []struct {
		name  string
		patch func(*DeliveryReceipt)
	}{
		{deliveryTestGenerationCase, func(r *DeliveryReceipt) { r.DeliveryGeneration++ }},
		{deliveryTestNonceCase, func(r *DeliveryReceipt) { r.Nonce = bytes.Repeat([]byte{0x56}, deliveryNonceBytes) }},
		{deliveryTestSequenceCase, func(r *DeliveryReceipt) { r.Sequence++ }},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			tampered := receipt
			test.patch(&tampered)
			require.Error(t, tampered.Verify(publicKey))
		})
	}
	require.False(t, registry.VerifyFederationReceipt(publicKey, payload, bytes.Repeat([]byte{0}, 64)))
}

func TestDeliveryReceiptRejectsInvalidDeliveryIdentity(t *testing.T) {
	t.Parallel()
	receipt := DeliveryReceipt{
		MessageID: deliveryTestMessage, OriginNode: deliveryTestOriginNode,
		DestinationNode:    deliveryTestDestinationNode,
		RecipientAgents:    []protocol.AgentID{deliveryTestRecipientB},
		DeliveryGeneration: 1,
		Nonce:              bytes.Repeat([]byte{0x55}, deliveryNonceBytes),
		Sequence:           1,
		EnvelopeSHA256:     protocol.SHA256Hex(strings.Repeat("a", 64)),
		RequestSHA256:      protocol.SHA256Hex(strings.Repeat("b", 64)),
		Status:             DeliveryReceiptStoredRemote,
		ReceivedAt:         time.Date(2026, time.October, 29, 22, 1, 0, 0, time.UTC),
		Signature:          bytes.Repeat([]byte{0x44}, ed25519.SignatureSize),
	}
	for _, test := range []struct {
		name  string
		patch func(*DeliveryReceipt)
	}{
		{deliveryTestGenerationCase, func(r *DeliveryReceipt) { r.DeliveryGeneration = 0 }},
		{deliveryTestNonceCase, func(r *DeliveryReceipt) { r.Nonce = r.Nonce[:deliveryNonceBytes-1] }},
		{deliveryTestSequenceCase, func(r *DeliveryReceipt) { r.Sequence = 0 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			invalid := receipt
			test.patch(&invalid)
			require.Error(t, invalid.Validate())
		})
	}
}

func TestFederationInboxUniqueReplayKeys(t *testing.T) {
	t.Parallel()
	store, err := eventstore.Open(t.Context(), filepath.Join(t.TempDir(), "gifford.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	stamp := eventstore.FormatTime(time.Date(2026, time.October, 29, 22, 2, 0, 0, time.UTC))
	insert := func(origin string, sequence int64, nonce []byte, message string) error {
		_, err := store.DB().ExecContext(t.Context(), `INSERT INTO federation_inbox
			(origin_node, sequence, nonce, message_id, delivery_generation,
			 request_sha256, envelope_sha256, receipt, received_at)
			VALUES (?, ?, ?, ?, 1, ?, ?, ?, ?)`, origin, sequence, nonce, message,
			strings.Repeat("a", 64), strings.Repeat("b", 64), []byte("signed-receipt"), stamp)
		return err
	}
	nonceA := bytes.Repeat([]byte{0x01}, deliveryNonceBytes)
	nonceB := bytes.Repeat([]byte{0x02}, deliveryNonceBytes)
	require.NoError(t, insert(string(deliveryTestOriginNode), 1, nonceA, "01ARZ3NDEKTSV4RRFFQ69G5FAV"))
	require.Error(t, insert(string(deliveryTestOriginNode), 1, nonceB, "01ARZ3NDEKTSV4RRFFQ69G5FAW"))
	require.Error(t, insert(string(deliveryTestOriginNode), 2, nonceA, "01ARZ3NDEKTSV4RRFFQ69G5FAX"))
	require.Error(t, insert(string(deliveryTestOriginNode), 2, nonceB, "01ARZ3NDEKTSV4RRFFQ69G5FAV"))
	require.NoError(t, insert("node-other", 1, nonceA, "01ARZ3NDEKTSV4RRFFQ69G5FAY"))
}
