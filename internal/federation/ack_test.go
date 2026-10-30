package federation

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
)

func testAckRequest() AckRequest {
	return AckRequest{
		MessageID: deliveryTestMessage, Agent: deliveryTestRecipientB,
		DeliveryGeneration: 3, Status: protocol.AckStatusProcessing,
		AckedAt: time.Date(2026, time.October, 29, 22, 2, 0, 0, time.UTC),
		Nonce:   bytes.Repeat([]byte{0x61}, deliveryNonceBytes), Sequence: 19,
	}
}

func TestAckRequestCanonicalRoundTripAndStrictDecode(t *testing.T) {
	t.Parallel()
	request := testAckRequest()
	exact, err := request.CanonicalBytes()
	require.NoError(t, err)
	decoded, err := DecodeAckRequest(exact)
	require.NoError(t, err)
	require.Equal(t, request, decoded)

	want := sha256.Sum256(exact)
	digest, err := request.RequestSHA256()
	require.NoError(t, err)
	require.Equal(t, protocol.SHA256Hex(hex.EncodeToString(want[:])), digest)

	var object map[string]any
	require.NoError(t, json.Unmarshal(exact, &object))
	object["unknown"] = true
	withUnknown, err := json.Marshal(object)
	require.NoError(t, err)
	_, err = DecodeAckRequest(withUnknown)
	require.Error(t, err)
	_, err = DecodeAckRequest(append(exact, []byte(" {}")...))
	require.Error(t, err)

	for _, test := range []struct {
		name  string
		patch func(*AckRequest)
	}{
		{"message id", func(r *AckRequest) { r.MessageID = "bad" }},
		{"agent", func(r *AckRequest) { r.Agent = "" }},
		{deliveryTestGenerationCase, func(r *AckRequest) { r.DeliveryGeneration = 0 }},
		{"status", func(r *AckRequest) { r.Status = "MISMATCH" }},
		{"acked at", func(r *AckRequest) { r.AckedAt = time.Time{} }},
		{deliveryTestNonceCase, func(r *AckRequest) { r.Nonce = r.Nonce[:15] }},
		{deliveryTestSequenceCase, func(r *AckRequest) { r.Sequence = 0 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			invalid := testAckRequest()
			test.patch(&invalid)
			require.Error(t, invalid.Validate())
		})
	}
}

func TestAckReceiptUsesSeparateNodeSignatureDomain(t *testing.T) {
	t.Parallel()
	reg, err := registry.New(deliveryTestOriginNode, privateDataDir(t), clock.System{})
	require.NoError(t, err)
	request := testAckRequest()
	digest, err := request.RequestSHA256()
	require.NoError(t, err)
	receipt := AckReceipt{
		MessageID: request.MessageID, Agent: request.Agent,
		DeliveryGeneration: request.DeliveryGeneration, Status: request.Status,
		OriginNode: deliveryTestOriginNode, DestinationNode: deliveryTestDestinationNode,
		RequestSHA256: digest, Nonce: append([]byte(nil), request.Nonce...),
		Sequence:   request.Sequence,
		ReceivedAt: time.Date(2026, time.October, 29, 22, 3, 0, 0, time.UTC),
	}
	require.NoError(t, receipt.Sign(reg))
	publicKey, err := reg.NodeSigningPublicKey()
	require.NoError(t, err)
	require.NoError(t, receipt.Verify(publicKey))

	payload, err := receipt.signingPayload()
	require.NoError(t, err)
	require.True(t, ed25519.Verify(publicKey,
		append([]byte("gifford-federation-ack-receipt-v1\x00"), payload...), receipt.Signature))
	require.False(t, registry.VerifyFederationReceipt(publicKey, payload, receipt.Signature))

	exact, err := json.Marshal(receipt)
	require.NoError(t, err)
	decoded, err := DecodeAckReceipt(exact)
	require.NoError(t, err)
	require.Equal(t, receipt, decoded)

	for _, test := range []struct {
		name  string
		patch func(*AckReceipt)
	}{
		{"request", func(r *AckReceipt) { r.RequestSHA256 = protocol.SHA256Hex(strings.Repeat("f", 64)) }},
		{deliveryTestGenerationCase, func(r *AckReceipt) { r.DeliveryGeneration++ }},
		{deliveryTestNonceCase, func(r *AckReceipt) { r.Nonce[0]++ }},
		{deliveryTestSequenceCase, func(r *AckReceipt) { r.Sequence++ }},
		{"nodes", func(r *AckReceipt) { r.DestinationNode = r.OriginNode }},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			tampered := receipt
			tampered.Nonce = append([]byte(nil), receipt.Nonce...)
			test.patch(&tampered)
			require.Error(t, tampered.Verify(publicKey))
		})
	}
}
