package federation

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
)

func testArtifactAckRequest() ArtifactAckRequest {
	return ArtifactAckRequest{
		ArtifactID: artifactTestID, Recipient: deliveryTestRecipientB,
		AckedAt: time.Date(2026, time.October, 31, 23, 30, 0, 0, time.UTC),
		Nonce:   bytes.Repeat([]byte{0x71}, deliveryNonceBytes), Sequence: 7,
	}
}

func TestArtifactAckRequestCanonicalRoundTripAndStrictDecode(t *testing.T) {
	t.Parallel()
	request := testArtifactAckRequest()
	exact, err := request.CanonicalBytes()
	require.NoError(t, err)
	decoded, err := DecodeArtifactAckRequest(exact)
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
	_, err = DecodeArtifactAckRequest(withUnknown)
	require.Error(t, err)
	_, err = DecodeArtifactAckRequest(append(exact, []byte(" {}")...))
	require.Error(t, err)

	for _, test := range []struct {
		name  string
		patch func(*ArtifactAckRequest)
	}{
		{"artifact", func(r *ArtifactAckRequest) { r.ArtifactID = "" }},
		{"recipient", func(r *ArtifactAckRequest) { r.Recipient = "" }},
		{"time", func(r *ArtifactAckRequest) { r.AckedAt = time.Time{} }},
		{"nonce", func(r *ArtifactAckRequest) { r.Nonce = r.Nonce[:15] }},
		{"sequence", func(r *ArtifactAckRequest) { r.Sequence = 0 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			invalid := testArtifactAckRequest()
			test.patch(&invalid)
			require.Error(t, invalid.Validate())
		})
	}
}

func TestArtifactAckReceiptUsesSeparateNodeSignatureDomain(t *testing.T) {
	t.Parallel()
	reg, err := registry.New(deliveryTestOriginNode, privateDataDir(t), clock.System{})
	require.NoError(t, err)
	request := testArtifactAckRequest()
	digest, err := request.RequestSHA256()
	require.NoError(t, err)
	receipt := ArtifactAckReceipt{
		ArtifactID: request.ArtifactID, Recipient: request.Recipient,
		OriginNode: deliveryTestOriginNode, DestinationNode: deliveryTestDestinationNode,
		RequestSHA256: digest, Nonce: append([]byte(nil), request.Nonce...),
		Sequence:   request.Sequence,
		ReceivedAt: time.Date(2026, time.October, 31, 23, 31, 0, 0, time.UTC),
	}
	require.NoError(t, receipt.Sign(reg))
	publicKey, err := reg.NodeSigningPublicKey()
	require.NoError(t, err)
	require.NoError(t, receipt.Verify(publicKey))
	payload, err := receipt.signingPayload()
	require.NoError(t, err)
	require.False(t, registry.VerifyFederationReceipt(publicKey, payload, receipt.Signature))
	require.False(t, registry.VerifyFederationAckReceipt(publicKey, payload, receipt.Signature))
	require.False(t, registry.VerifyFederationArtifactReceipt(publicKey, payload, receipt.Signature))

	exact, err := json.Marshal(receipt)
	require.NoError(t, err)
	decoded, err := DecodeArtifactAckReceipt(exact)
	require.NoError(t, err)
	require.Equal(t, receipt, decoded)
	tampered := receipt
	tampered.Sequence++
	require.Error(t, tampered.Verify(publicKey))
}
