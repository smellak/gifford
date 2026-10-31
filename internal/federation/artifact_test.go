package federation

import (
	"bytes"
	"crypto/ed25519"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
)

const artifactTestID = protocol.OpaqueID("art_1")

func testArtifactManifestRequest() ArtifactManifestRequest {
	reserved := time.Date(2026, time.October, 31, 20, 0, 0, 0, time.UTC)
	return ArtifactManifestRequest{
		ArtifactID: artifactTestID, OriginNode: deliveryTestOriginNode,
		DestinationNode: deliveryTestDestinationNode, Sender: deliveryTestOriginAgent,
		Reservation: protocol.ReserveArtifactParams{
			ClientMsgID: "client-artifact", Recipient: deliveryTestRecipientB,
			EncryptionProfile: protocol.ArtifactEncryptionProfile,
			CiphertextSize:    32, ChunkCount: 1,
			CiphertextSHA256:  protocol.SHA256Hex(strings.Repeat("a", 64)),
			EncryptedManifest: bytes.Repeat([]byte{0x11}, 40),
			RecipientKeyID:    "x25519:agent-b:v1", KeyEnvelope: []byte{0x22},
			SenderKeyID: deliveryTestSenderKeyID,
			Signature:   bytes.Repeat([]byte{0x33}, ed25519.SignatureSize),
			ReservedAt:  reserved, ExpiresAt: reserved.Add(24 * time.Hour),
		},
		SenderRecord: registry.DirectoryRecord{
			KeyID: deliveryTestSenderKeyID, Agent: deliveryTestOriginAgent,
			Node: deliveryTestOriginNode, Purpose: registry.KeyPurposeSigning,
			Algorithm: registry.KeyAlgorithmEd25519,
			PublicKey: bytes.Repeat([]byte{0x44}, ed25519.PublicKeySize),
			Status:    activeDirectoryStatus, ValidFrom: eventstore.FormatTime(reserved.Add(-time.Hour)),
			Issuer:          "node:" + string(deliveryTestOriginNode),
			RecordSignature: bytes.Repeat([]byte{0x55}, ed25519.SignatureSize),
		},
		OriginControl: protocol.ObjectControlState{
			ObjectType: protocol.DecisionObjectTypeArtifact, ObjectID: artifactTestID,
			ControlState:  protocol.ControlStateAllowed,
			ControlReason: protocol.DecisionReasonAllowed, ControlVersion: 3,
		},
	}
}

func TestArtifactManifestRequestCanonicalValidation(t *testing.T) {
	t.Parallel()
	request := testArtifactManifestRequest()
	require.NoError(t, request.Validate())
	exact, err := request.CanonicalBytes()
	require.NoError(t, err)
	decoded, err := DecodeArtifactManifestRequest(exact)
	require.NoError(t, err)
	require.Equal(t, request, decoded)
	wantDigest, err := request.RequestSHA256()
	require.NoError(t, err)
	gotDigest, err := decoded.RequestSHA256()
	require.NoError(t, err)
	require.Equal(t, wantDigest, gotDigest)

	withUnknown := append([]byte(nil), exact[:len(exact)-1]...)
	withUnknown = append(withUnknown, []byte(`,"unknown":true}`)...)
	_, err = DecodeArtifactManifestRequest(withUnknown)
	require.Error(t, err)

	tests := []struct {
		name  string
		patch func(*ArtifactManifestRequest)
	}{
		{"same node", func(r *ArtifactManifestRequest) { r.DestinationNode = r.OriginNode }},
		{"origin object", func(r *ArtifactManifestRequest) { r.OriginControl.ObjectID = "art_other" }},
		{"origin state", func(r *ArtifactManifestRequest) { r.OriginControl.ControlState = protocol.ControlStateHeld }},
		{"origin reason", func(r *ArtifactManifestRequest) { r.OriginControl.ControlReason = protocol.DecisionReasonLinkHold }},
		{"sender key", func(r *ArtifactManifestRequest) { r.SenderRecord.KeyID = "ed25519:agent-a:v2" }},
		{"sender issuer", func(r *ArtifactManifestRequest) { r.SenderRecord.Issuer = "node:other" }},
		{"sender expiry", func(r *ArtifactManifestRequest) { empty := ""; r.SenderRecord.ValidUntil = &empty }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			changed := testArtifactManifestRequest()
			test.patch(&changed)
			require.Error(t, changed.Validate())
		})
	}
}

func TestArtifactRequestFramesAreStrictAndBound(t *testing.T) {
	t.Parallel()
	chunk := ArtifactChunkRequest{
		ArtifactID: artifactTestID, OriginNode: deliveryTestOriginNode,
		DestinationNode: deliveryTestDestinationNode, ChunkIndex: 0,
		Ciphertext: bytes.Repeat([]byte{0x61}, 32),
		SHA256:     protocol.SHA256Hex(strings.Repeat("b", 64)),
	}
	exactChunk, err := chunk.CanonicalBytes()
	require.NoError(t, err)
	decodedChunk, err := DecodeArtifactChunkRequest(exactChunk)
	require.NoError(t, err)
	require.Equal(t, chunk, decodedChunk)
	chunkDigest, err := chunk.RequestSHA256()
	require.NoError(t, err)
	changed := chunk
	changed.ChunkIndex++
	changedDigest, err := changed.RequestSHA256()
	require.NoError(t, err)
	require.NotEqual(t, chunkDigest, changedDigest)

	complete := ArtifactCompleteRequest{
		ArtifactID: chunk.ArtifactID, OriginNode: chunk.OriginNode,
		DestinationNode: chunk.DestinationNode,
	}
	exactComplete, err := complete.CanonicalBytes()
	require.NoError(t, err)
	decodedComplete, err := DecodeArtifactCompleteRequest(exactComplete)
	require.NoError(t, err)
	require.Equal(t, complete, decodedComplete)
	trailing := bytes.Clone(exactComplete)
	trailing = append(trailing, []byte(" true")...)
	_, err = DecodeArtifactCompleteRequest(trailing)
	require.Error(t, err)
}

func TestArtifactReceiptUsesSeparateSigningDomain(t *testing.T) {
	t.Parallel()
	reg, err := registry.New(deliveryTestDestinationNode, privateDataDir(t), clock.System{})
	require.NoError(t, err)
	receipt := ArtifactReceipt{
		ArtifactID: artifactTestID, OriginNode: deliveryTestOriginNode,
		DestinationNode: deliveryTestDestinationNode, Phase: ArtifactReceiptComplete,
		RequestSHA256: protocol.SHA256Hex(strings.Repeat("c", 64)),
		NextChunk:     2, ChunkCount: 2, Status: ArtifactReceiptStoredRemote,
		ReceivedAt: time.Date(2026, time.October, 31, 20, 1, 0, 0, time.UTC),
	}
	require.NoError(t, receipt.Sign(reg))
	publicKey, err := reg.NodeSigningPublicKey()
	require.NoError(t, err)
	require.NoError(t, receipt.Verify(publicKey))
	payload, err := receipt.signingPayload()
	require.NoError(t, err)
	require.False(t, registry.VerifyFederationReceipt(publicKey, payload, receipt.Signature))

	raw, err := json.Marshal(receipt)
	require.NoError(t, err)
	decoded, err := DecodeArtifactReceipt(raw)
	require.NoError(t, err)
	require.Equal(t, receipt, decoded)
	tampered := receipt
	tampered.NextChunk--
	require.Error(t, tampered.Verify(publicKey))
}
