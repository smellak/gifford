package federation

import (
	"bytes"
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"time"

	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
)

// AckRequest is the canonical federation frame sent back to the message's
// origin after a remote recipient records delivery progress.
type AckRequest struct {
	MessageID          protocol.ULID      `json:"message_id"`
	Agent              protocol.AgentID   `json:"agent_id"`
	DeliveryGeneration int64              `json:"delivery_generation"`
	Status             protocol.AckStatus `json:"status"`
	AckedAt            time.Time          `json:"acked_at"`
	Nonce              []byte             `json:"nonce"`
	Sequence           int64              `json:"sequence"`
}

// DecodeAckRequest strictly decodes and validates one acknowledgement frame.
func DecodeAckRequest(raw []byte) (AckRequest, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var request AckRequest
	if err := decoder.Decode(&request); err != nil {
		return AckRequest{}, fmt.Errorf("decode federation ack request: %w", err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return AckRequest{}, fmt.Errorf("decode federation ack request: %w", err)
	}
	if err := request.Validate(); err != nil {
		return AckRequest{}, err
	}
	return request, nil
}

// Validate checks every acknowledgement field that does not require local
// message or peer state.
func (r AckRequest) Validate() error {
	if !r.MessageID.Valid() || !r.Agent.Valid() || r.DeliveryGeneration < 1 ||
		!r.Status.Valid() || r.AckedAt.IsZero() || len(r.Nonce) != deliveryNonceBytes ||
		r.Sequence < 1 {
		return fmt.Errorf("federation ack request has invalid metadata")
	}
	return nil
}

// CanonicalBytes returns the deterministic request bytes persisted and retried
// byte for byte by the acknowledgement outbox.
func (r AckRequest) CanonicalBytes() ([]byte, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(r)
	if err != nil {
		return nil, fmt.Errorf("encode canonical federation ack request: %w", err)
	}
	return encoded, nil
}

// RequestSHA256 identifies the canonical acknowledgement request.
func (r AckRequest) RequestSHA256() (protocol.SHA256Hex, error) {
	canonical, err := r.CanonicalBytes()
	if err != nil {
		return "", err
	}
	return hashBytes(canonical), nil
}

// AckReceipt binds A's signed acceptance to the exact request B persisted.
type AckReceipt struct {
	MessageID          protocol.ULID      `json:"message_id"`
	Agent              protocol.AgentID   `json:"agent_id"`
	DeliveryGeneration int64              `json:"delivery_generation"`
	Status             protocol.AckStatus `json:"status"`
	OriginNode         protocol.NodeID    `json:"origin_node"`
	DestinationNode    protocol.NodeID    `json:"destination_node"`
	RequestSHA256      protocol.SHA256Hex `json:"request_sha256"`
	Nonce              []byte             `json:"nonce"`
	Sequence           int64              `json:"sequence"`
	ReceivedAt         time.Time          `json:"received_at"`
	Signature          []byte             `json:"signature,omitempty"`
}

// DecodeAckReceipt strictly decodes one signed acknowledgement receipt.
func DecodeAckReceipt(raw []byte) (AckReceipt, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var receipt AckReceipt
	if err := decoder.Decode(&receipt); err != nil {
		return AckReceipt{}, fmt.Errorf("decode federation ack receipt: %w", err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return AckReceipt{}, fmt.Errorf("decode federation ack receipt: %w", err)
	}
	if err := receipt.Validate(); err != nil {
		return AckReceipt{}, err
	}
	return receipt, nil
}

// Sign authenticates the canonical receipt under the acknowledgement-specific
// domain while keeping the node private key inside Registry.
func (r *AckReceipt) Sign(reg *registry.Registry) error {
	if r == nil || reg == nil {
		return fmt.Errorf("sign federation ack receipt: invalid input")
	}
	payload, err := r.signingPayload()
	if err != nil {
		return err
	}
	signature, err := reg.SignFederationAckReceipt(payload)
	if err != nil {
		return fmt.Errorf("sign federation ack receipt: %w", err)
	}
	r.Signature = signature
	return nil
}

// Verify checks the receipt structure and its acknowledgement-domain signature.
func (r AckReceipt) Verify(publicKey ed25519.PublicKey) error {
	if err := r.Validate(); err != nil {
		return err
	}
	payload, err := r.signingPayload()
	if err != nil {
		return err
	}
	if !registry.VerifyFederationAckReceipt(publicKey, payload, r.Signature) {
		return fmt.Errorf("verify federation ack receipt: invalid signature")
	}
	return nil
}

// Validate checks a fully signed acknowledgement receipt.
func (r AckReceipt) Validate() error {
	if err := r.validatePayload(); err != nil {
		return err
	}
	if len(r.Signature) != ed25519.SignatureSize {
		return fmt.Errorf("federation ack receipt signature must be %d bytes", ed25519.SignatureSize)
	}
	return nil
}

func (r AckReceipt) signingPayload() ([]byte, error) {
	if err := r.validatePayload(); err != nil {
		return nil, err
	}
	r.Signature = nil
	payload, err := json.Marshal(r)
	if err != nil {
		return nil, fmt.Errorf("encode federation ack receipt signing payload: %w", err)
	}
	return payload, nil
}

func (r AckReceipt) validatePayload() error {
	if !r.MessageID.Valid() || !r.Agent.Valid() || r.DeliveryGeneration < 1 ||
		!r.Status.Valid() || !r.OriginNode.Valid() || !r.DestinationNode.Valid() ||
		r.OriginNode == r.DestinationNode || !r.RequestSHA256.Valid() ||
		len(r.Nonce) != deliveryNonceBytes || r.Sequence < 1 || r.ReceivedAt.IsZero() {
		return fmt.Errorf("federation ack receipt has invalid metadata")
	}
	return nil
}
