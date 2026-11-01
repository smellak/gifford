package federation

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"time"

	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
)

// ArtifactAckReceiver owns acknowledgement binding, replay protection and
// origin-side retention after mTLS authenticates the destination node.
type ArtifactAckReceiver interface {
	ReceiveFederatedArtifactAck(context.Context, protocol.NodeID, ArtifactAckRequest) ([]byte, error)
}

// ArtifactAckRequest is the canonical frame a destination persists before it
// removes a remotely received artifact's ciphertext.
type ArtifactAckRequest struct {
	ArtifactID protocol.OpaqueID `json:"artifact_id"`
	Recipient  protocol.AgentID  `json:"recipient"`
	AckedAt    time.Time         `json:"acked_at"`
	Nonce      []byte            `json:"nonce"`
	Sequence   int64             `json:"sequence"`
}

// Validate checks fields that do not require origin-side durable state.
func (r ArtifactAckRequest) Validate() error {
	if !r.ArtifactID.Valid() || !r.Recipient.Valid() || r.AckedAt.IsZero() ||
		len(r.Nonce) != deliveryNonceBytes || r.Sequence < 1 {
		return fmt.Errorf("federation artifact acknowledgement has invalid metadata")
	}
	return nil
}

// CanonicalBytes returns the deterministic bytes retried by the outbox.
func (r ArtifactAckRequest) CanonicalBytes() ([]byte, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(r)
	if err != nil {
		return nil, fmt.Errorf("encode artifact acknowledgement: %w", err)
	}
	return raw, nil
}

// RequestSHA256 binds the origin's receipt to the exact persisted request.
func (r ArtifactAckRequest) RequestSHA256() (protocol.SHA256Hex, error) {
	raw, err := r.CanonicalBytes()
	if err != nil {
		return "", err
	}
	return hashBytes(raw), nil
}

// DecodeArtifactAckRequest strictly decodes one acknowledgement frame.
func DecodeArtifactAckRequest(raw []byte) (ArtifactAckRequest, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var request ArtifactAckRequest
	if err := decoder.Decode(&request); err != nil {
		return ArtifactAckRequest{}, fmt.Errorf("decode artifact acknowledgement: %w", err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return ArtifactAckRequest{}, fmt.Errorf("decode artifact acknowledgement: %w", err)
	}
	if err := request.Validate(); err != nil {
		return ArtifactAckRequest{}, err
	}
	return request, nil
}

// ArtifactAckReceipt is the origin-signed acceptance of one exact request.
type ArtifactAckReceipt struct {
	ArtifactID      protocol.OpaqueID  `json:"artifact_id"`
	Recipient       protocol.AgentID   `json:"recipient"`
	OriginNode      protocol.NodeID    `json:"origin_node"`
	DestinationNode protocol.NodeID    `json:"destination_node"`
	RequestSHA256   protocol.SHA256Hex `json:"request_sha256"`
	Nonce           []byte             `json:"nonce"`
	Sequence        int64              `json:"sequence"`
	ReceivedAt      time.Time          `json:"received_at"`
	Signature       []byte             `json:"signature,omitempty"`
}

// DecodeArtifactAckReceipt strictly decodes one signed receipt.
func DecodeArtifactAckReceipt(raw []byte) (ArtifactAckReceipt, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var receipt ArtifactAckReceipt
	if err := decoder.Decode(&receipt); err != nil {
		return ArtifactAckReceipt{}, fmt.Errorf("decode artifact acknowledgement receipt: %w", err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return ArtifactAckReceipt{}, fmt.Errorf("decode artifact acknowledgement receipt: %w", err)
	}
	if err := receipt.Validate(); err != nil {
		return ArtifactAckReceipt{}, err
	}
	return receipt, nil
}

// Sign authenticates the receipt without exposing the node private key.
func (r *ArtifactAckReceipt) Sign(reg *registry.Registry) error {
	if r == nil || reg == nil {
		return fmt.Errorf("sign artifact acknowledgement receipt: invalid input")
	}
	payload, err := r.signingPayload()
	if err != nil {
		return err
	}
	signature, err := reg.SignFederationArtifactAckReceipt(payload)
	if err != nil {
		return fmt.Errorf("sign artifact acknowledgement receipt: %w", err)
	}
	r.Signature = signature
	return nil
}

// Verify checks the receipt structure and artifact-ACK-domain signature.
func (r ArtifactAckReceipt) Verify(publicKey ed25519.PublicKey) error {
	if err := r.Validate(); err != nil {
		return err
	}
	payload, err := r.signingPayload()
	if err != nil {
		return err
	}
	if !registry.VerifyFederationArtifactAckReceipt(publicKey, payload, r.Signature) {
		return fmt.Errorf("verify artifact acknowledgement receipt: invalid signature")
	}
	return nil
}

// Validate checks a fully signed acknowledgement receipt.
func (r ArtifactAckReceipt) Validate() error {
	if err := r.validatePayload(); err != nil {
		return err
	}
	if len(r.Signature) != ed25519.SignatureSize {
		return fmt.Errorf("artifact acknowledgement receipt signature must be %d bytes", ed25519.SignatureSize)
	}
	return nil
}

func (r ArtifactAckReceipt) signingPayload() ([]byte, error) {
	if err := r.validatePayload(); err != nil {
		return nil, err
	}
	r.Signature = nil
	payload, err := json.Marshal(r)
	if err != nil {
		return nil, fmt.Errorf("encode artifact acknowledgement receipt signing payload: %w", err)
	}
	return payload, nil
}

func (r ArtifactAckReceipt) validatePayload() error {
	if !r.ArtifactID.Valid() || !r.Recipient.Valid() || !r.OriginNode.Valid() ||
		!r.DestinationNode.Valid() || r.OriginNode == r.DestinationNode ||
		!r.RequestSHA256.Valid() || len(r.Nonce) != deliveryNonceBytes ||
		r.Sequence < 1 || r.ReceivedAt.IsZero() {
		return fmt.Errorf("artifact acknowledgement receipt has invalid metadata")
	}
	return nil
}
