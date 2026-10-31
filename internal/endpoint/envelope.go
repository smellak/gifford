package endpoint

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	giffordcrypto "github.com/smellak/gifford/internal/crypto"
	"github.com/smellak/gifford/internal/hub"
	"github.com/smellak/gifford/internal/protocol"
)

// decodeClaim verifies the signed opaque envelope before decoding its local
// profile-none LogicalMessage. Every duplicated routing field is then bound
// back to the signed header before plaintext can leave the Endpoint.
func decodeClaim(claim hub.ClaimMessageResult, recipient protocol.AgentID) (protocol.LogicalMessage, error) {
	var envelope protocol.WireEnvelope
	if err := json.Unmarshal(claim.Envelope, &envelope); err != nil {
		return protocol.LogicalMessage{}, fmt.Errorf("decode claimed envelope: %w", ErrIntegrity)
	}
	if err := envelope.Validate(); err != nil || envelope.MessageID != claim.MessageID ||
		envelope.SenderSigningKeyID != claim.SenderSigningKeyID ||
		len(claim.SenderSigningPublicKey) != ed25519.PublicKeySize {
		return protocol.LogicalMessage{}, fmt.Errorf("validate claimed envelope: %w", ErrIntegrity)
	}
	if err := giffordcrypto.VerifyWireEnvelope(
		ed25519.PublicKey(claim.SenderSigningPublicKey), envelope,
	); err != nil {
		return protocol.LogicalMessage{}, fmt.Errorf("verify claimed envelope: %w", ErrIntegrity)
	}
	if !recipient.Valid() || !containsAgent(envelope.RecipientAgents, recipient) {
		return protocol.LogicalMessage{}, fmt.Errorf("bind claimed recipient: %w", ErrIntegrity)
	}
	digest := sha256.Sum256(envelope.Ciphertext)
	if string(envelope.CiphertextSHA256) != hex.EncodeToString(digest[:]) {
		return protocol.LogicalMessage{}, fmt.Errorf("verify claimed ciphertext: %w", ErrIntegrity)
	}
	if envelope.EncryptionProfile != protocol.EncryptionProfileNone {
		return protocol.LogicalMessage{}, fmt.Errorf("unsupported encryption profile: %w", ErrIntegrity)
	}
	var message protocol.LogicalMessage
	if err := json.Unmarshal(envelope.Ciphertext, &message); err != nil {
		return protocol.LogicalMessage{}, fmt.Errorf("decode logical message: %w", ErrIntegrity)
	}
	if err := message.Validate(); err != nil || !logicalMessageBound(message, envelope) ||
		!containsAgent(message.ToAgents, recipient) {
		return protocol.LogicalMessage{}, fmt.Errorf("bind logical message: %w", ErrIntegrity)
	}
	return message, nil
}

func containsAgent(agents []protocol.AgentID, wanted protocol.AgentID) bool {
	for _, agent := range agents {
		if agent == wanted {
			return true
		}
	}
	return false
}

func logicalMessageBound(message protocol.LogicalMessage, envelope protocol.WireEnvelope) bool {
	if message.MessageID != envelope.MessageID || message.ClientMsgID != envelope.ClientMsgID ||
		message.FromAgent != envelope.OriginAgent || message.OriginNode != envelope.OriginNode ||
		message.DestinationNode != envelope.DestinationNode || message.Priority != envelope.Priority ||
		!message.CreatedAt.Equal(envelope.CreatedAt) || !message.ExpiresAt.Equal(envelope.ExpiresAt) ||
		message.ParentMessageID != envelope.ParentMessageID || message.HopCount != envelope.HopCount ||
		message.LoopID != envelope.LoopID ||
		!equalInt64Pointer(message.LoopGeneration, envelope.LoopGeneration) ||
		!equalInt64Pointer(message.TurnIndex, envelope.TurnIndex) ||
		message.RouteDigest != envelope.RouteDigest || message.ControlVersion != envelope.ControlVersion ||
		len(message.ToAgents) != len(envelope.RecipientAgents) {
		return false
	}
	for i := range message.ToAgents {
		if message.ToAgents[i] != envelope.RecipientAgents[i] {
			return false
		}
	}
	return true
}

func equalInt64Pointer(a, b *int64) bool {
	return a == nil && b == nil || a != nil && b != nil && *a == *b
}
