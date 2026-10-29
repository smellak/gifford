package federation

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
)

const deliveryNonceBytes = 16

// DeliveryRecipient is the receiver-side metadata absent from WireEnvelope.
// Its order must remain identical to WireEnvelope.RecipientAgents.
type DeliveryRecipient struct {
	Agent       protocol.AgentID      `json:"agent"`
	Mode        protocol.DeliveryMode `json:"mode"`
	RequiresAck bool                  `json:"requires_ack"`
}

// DeliveryRequest is the additive federation frame sent to POST /v1/deliver.
// Envelope remains raw so its exact signed bytes can be hashed and persisted.
type DeliveryRequest struct {
	Envelope           json.RawMessage             `json:"envelope"`
	ThreadID           protocol.ThreadID           `json:"thread_id"`
	Recipients         []DeliveryRecipient         `json:"recipients"`
	DeliveryGeneration int64                       `json:"delivery_generation"`
	Nonce              []byte                      `json:"nonce"`
	Sequence           int64                       `json:"sequence"`
	OriginControl      protocol.ObjectControlState `json:"origin_control"`
	Sender             registry.DirectoryRecord    `json:"sender"`
}

// DecodeDeliveryRequest strictly decodes and validates one delivery frame.
func DecodeDeliveryRequest(raw []byte) (DeliveryRequest, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var request DeliveryRequest
	if err := decoder.Decode(&request); err != nil {
		return DeliveryRequest{}, fmt.Errorf("decode delivery request: %w", err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return DeliveryRequest{}, fmt.Errorf("decode delivery request: %w", err)
	}
	if err := request.Validate(); err != nil {
		return DeliveryRequest{}, err
	}
	return request, nil
}

// Validate checks the frame and all relationships that do not require local
// registry or policy state.
func (r DeliveryRequest) Validate() error {
	envelope, _, err := r.DecodeEnvelope()
	if err != nil {
		return err
	}
	if !r.ThreadID.Valid() {
		return fmt.Errorf("delivery request thread_id is invalid")
	}
	if len(r.Recipients) != len(envelope.RecipientAgents) {
		return fmt.Errorf("delivery request recipient metadata does not match envelope")
	}
	for i, recipient := range r.Recipients {
		if recipient.Agent != envelope.RecipientAgents[i] || !recipient.Mode.Valid() {
			return fmt.Errorf("delivery request recipient metadata %d is invalid", i)
		}
	}
	if r.DeliveryGeneration < 1 {
		return fmt.Errorf("delivery request generation must be at least 1")
	}
	if len(r.Nonce) != deliveryNonceBytes {
		return fmt.Errorf("delivery request nonce must be %d bytes", deliveryNonceBytes)
	}
	if r.Sequence < 1 {
		return fmt.Errorf("delivery request sequence must be positive")
	}
	if err := validateOriginControl(r.OriginControl, envelope); err != nil {
		return err
	}
	if err := validateSenderRecord(r.Sender, envelope); err != nil {
		return err
	}
	return nil
}

// DecodeEnvelope returns the validated envelope and its exact trimmed bytes.
func (r DeliveryRequest) DecodeEnvelope() (protocol.WireEnvelope, []byte, error) {
	exact := bytes.Clone(bytes.TrimSpace(r.Envelope))
	if len(exact) == 0 {
		return protocol.WireEnvelope{}, nil, fmt.Errorf("delivery request envelope is required")
	}
	decoder := json.NewDecoder(bytes.NewReader(exact))
	decoder.DisallowUnknownFields()
	var envelope protocol.WireEnvelope
	if err := decoder.Decode(&envelope); err != nil {
		return protocol.WireEnvelope{}, nil, fmt.Errorf("decode delivery envelope: %w", err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return protocol.WireEnvelope{}, nil, fmt.Errorf("decode delivery envelope: %w", err)
	}
	if err := envelope.Validate(); err != nil {
		return protocol.WireEnvelope{}, nil, fmt.Errorf("validate delivery envelope: %w", err)
	}
	return envelope, exact, nil
}

// CanonicalBytes returns the deterministic JSON used for request replay
// identity. The raw envelope participates as a JSON value, not as a string.
func (r DeliveryRequest) CanonicalBytes() ([]byte, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(r)
	if err != nil {
		return nil, fmt.Errorf("encode canonical delivery request: %w", err)
	}
	return encoded, nil
}

// RequestSHA256 hashes the canonical bytes of the complete delivery request.
func (r DeliveryRequest) RequestSHA256() (protocol.SHA256Hex, error) {
	canonical, err := r.CanonicalBytes()
	if err != nil {
		return "", err
	}
	return hashBytes(canonical), nil
}

// EnvelopeSHA256 hashes the exact trimmed RawMessage bytes, matching local
// submit persistence rather than re-encoding WireEnvelope.
func (r DeliveryRequest) EnvelopeSHA256() (protocol.SHA256Hex, error) {
	_, exact, err := r.DecodeEnvelope()
	if err != nil {
		return "", err
	}
	return hashBytes(exact), nil
}

func validateOriginControl(control protocol.ObjectControlState, envelope protocol.WireEnvelope) error {
	if err := control.Validate(); err != nil {
		return fmt.Errorf("delivery request origin_control: %w", err)
	}
	if control.ObjectType != protocol.DecisionObjectTypeMessage ||
		control.ObjectID != protocol.OpaqueID(envelope.MessageID) ||
		control.ControlState != protocol.ControlStateAllowed ||
		control.ControlVersion != envelope.ControlVersion ||
		!allowedControlReason(control.ControlReason) || control.ApprovalID != "" || control.Cancelled {
		return fmt.Errorf("delivery request origin_control does not match envelope")
	}
	return nil
}

func allowedControlReason(reason protocol.DecisionReason) bool {
	switch reason {
	case protocol.DecisionReasonAllowed, protocol.DecisionReasonApprovedOnce,
		protocol.DecisionReasonApprovedThread, protocol.DecisionReasonHoldReleased,
		protocol.DecisionReasonStepGranted, protocol.DecisionReasonBudgetExtended:
		return true
	default:
		return false
	}
}

func validateSenderRecord(record registry.DirectoryRecord, envelope protocol.WireEnvelope) error {
	if record.KeyID != string(envelope.SenderSigningKeyID) || record.Agent != envelope.OriginAgent ||
		record.Node != envelope.OriginNode || record.Purpose != "signing" || record.Algorithm != "ed25519" ||
		len(record.PublicKey) != ed25519.PublicKeySize || record.Status != "ACTIVE" ||
		record.ValidFrom == "" || record.Issuer != "node:"+string(envelope.OriginNode) ||
		len(record.RecordSignature) != ed25519.SignatureSize {
		return fmt.Errorf("delivery request sender directory record does not match envelope")
	}
	if record.ValidUntil != nil && *record.ValidUntil == "" {
		return fmt.Errorf("delivery request sender directory record has an empty expiry")
	}
	return nil
}

func hashBytes(value []byte) protocol.SHA256Hex {
	digest := sha256.Sum256(value)
	return protocol.SHA256Hex(hex.EncodeToString(digest[:]))
}

// DeliveryReceiptStatus is the durable receiver outcome carried back to the
// sending node.
type DeliveryReceiptStatus string

const (
	// DeliveryReceiptStoredRemote means the request was stored atomically.
	DeliveryReceiptStoredRemote DeliveryReceiptStatus = "STORED_REMOTE"
	// DeliveryReceiptControlDenied means policy or channel denied the request.
	DeliveryReceiptControlDenied DeliveryReceiptStatus = "CONTROL_DENIED"
)

// Valid reports whether the receipt status is recognized.
func (s DeliveryReceiptStatus) Valid() bool {
	return s == DeliveryReceiptStoredRemote || s == DeliveryReceiptControlDenied
}

// DeliveryReceipt binds a signed receiver outcome to both the whole request
// and the exact envelope bytes.
type DeliveryReceipt struct {
	MessageID       protocol.ULID         `json:"message_id"`
	OriginNode      protocol.NodeID       `json:"origin_node"`
	DestinationNode protocol.NodeID       `json:"destination_node"`
	RecipientAgents []protocol.AgentID    `json:"recipient_agents"`
	RequestSHA256   protocol.SHA256Hex    `json:"request_sha256"`
	EnvelopeSHA256  protocol.SHA256Hex    `json:"envelope_sha256"`
	Status          DeliveryReceiptStatus `json:"status"`
	ReceivedAt      time.Time             `json:"received_at"`
	Signature       []byte                `json:"signature,omitempty"`
}

// Sign authenticates the canonical receipt with the node key held by Registry.
func (r *DeliveryReceipt) Sign(reg *registry.Registry) error {
	if r == nil || reg == nil {
		return fmt.Errorf("sign delivery receipt: invalid input")
	}
	payload, err := r.signingPayload()
	if err != nil {
		return err
	}
	signature, err := reg.SignFederationReceipt(payload)
	if err != nil {
		return fmt.Errorf("sign delivery receipt: %w", err)
	}
	r.Signature = signature
	return nil
}

// Verify checks the receipt structure and node signature under the receipt
// domain without exposing any node private key.
func (r DeliveryReceipt) Verify(publicKey ed25519.PublicKey) error {
	if err := r.Validate(); err != nil {
		return err
	}
	payload, err := r.signingPayload()
	if err != nil {
		return err
	}
	if !registry.VerifyFederationReceipt(publicKey, payload, r.Signature) {
		return fmt.Errorf("verify delivery receipt: invalid signature")
	}
	return nil
}

// Validate checks a fully signed receipt.
func (r DeliveryReceipt) Validate() error {
	if err := r.validatePayload(); err != nil {
		return err
	}
	if len(r.Signature) != ed25519.SignatureSize {
		return fmt.Errorf("delivery receipt signature must be %d bytes", ed25519.SignatureSize)
	}
	return nil
}

func (r DeliveryReceipt) signingPayload() ([]byte, error) {
	if err := r.validatePayload(); err != nil {
		return nil, err
	}
	r.Signature = nil
	payload, err := json.Marshal(r)
	if err != nil {
		return nil, fmt.Errorf("encode delivery receipt signing payload: %w", err)
	}
	return payload, nil
}

func (r DeliveryReceipt) validatePayload() error {
	if !r.MessageID.Valid() || !r.OriginNode.Valid() || !r.DestinationNode.Valid() ||
		r.OriginNode == r.DestinationNode || !r.RequestSHA256.Valid() || !r.EnvelopeSHA256.Valid() ||
		!r.Status.Valid() || r.ReceivedAt.IsZero() {
		return fmt.Errorf("delivery receipt has invalid metadata")
	}
	if len(r.RecipientAgents) == 0 || len(r.RecipientAgents) > 32 {
		return fmt.Errorf("delivery receipt has invalid recipients")
	}
	seen := make(map[protocol.AgentID]struct{}, len(r.RecipientAgents))
	for _, agent := range r.RecipientAgents {
		if !agent.Valid() {
			return fmt.Errorf("delivery receipt has invalid recipient")
		}
		if _, exists := seen[agent]; exists {
			return fmt.Errorf("delivery receipt repeats recipient")
		}
		seen[agent] = struct{}{}
	}
	return nil
}
