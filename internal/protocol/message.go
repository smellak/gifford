package protocol

import (
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

// This file holds the logical message and wire envelope of §5.5: what
// agents say and what Hubs transport.

// RequestedBudget is a narrowed budget request for a loop a reply may open.
type RequestedBudget struct {
	MaxTurns               *int64 `json:"max_turns,omitempty" jsonschema:"minimum=1"`
	MaxMessages            *int64 `json:"max_messages,omitempty" jsonschema:"minimum=1"`
	MaxDurationMS          *int64 `json:"max_duration_ms,omitempty" jsonschema:"minimum=1000"`
	MaxMessagesPerMinute   *int64 `json:"max_messages_per_minute,omitempty" jsonschema:"minimum=1"`
	MaxHops                *int64 `json:"max_hops,omitempty" jsonschema:"minimum=1"`
	MaxConsecutiveSamePair *int64 `json:"max_consecutive_same_pair,omitempty" jsonschema:"minimum=1"`
	MaxRetries             *int64 `json:"max_retries,omitempty" jsonschema:"minimum=0"`
	MaxArtifactCount       *int64 `json:"max_artifact_count,omitempty" jsonschema:"minimum=0"`
	MaxArtifactBytes       *int64 `json:"max_artifact_bytes,omitempty" jsonschema:"minimum=0"`
}

// Validate checks that at least one field is present.
func (b RequestedBudget) Validate() error {
	if err := checkRoot(&b); err != nil {
		return err
	}
	if b.MaxTurns == nil && b.MaxMessages == nil && b.MaxDurationMS == nil &&
		b.MaxMessagesPerMinute == nil && b.MaxHops == nil && b.MaxConsecutiveSamePair == nil &&
		b.MaxRetries == nil && b.MaxArtifactCount == nil && b.MaxArtifactBytes == nil {
		return &ValidationError{Path: "max_turns", Reason: "at least one budget field is required"}
	}
	return nil
}

// LogicalMessage is the plaintext message inside the sending and receiving
// Agent Endpoints.
type LogicalMessage struct {
	MessageID         ULID               `json:"message_id"`
	ClientMsgID       OpaqueID           `json:"client_msg_id"`
	ThreadID          ThreadID           `json:"thread_id"`
	CorrelationID     OpaqueID           `json:"correlation_id,omitempty"`
	FromAgent         AgentID            `json:"from_agent"`
	ToAgents          []AgentID          `json:"to_agents" jsonschema:"minItems=1,maxItems=32,uniqueItems=true"`
	OriginNode        NodeID             `json:"origin_node"`
	DestinationNode   NodeID             `json:"destination_node"`
	Subject           string             `json:"subject" jsonschema:"minLength=1,maxLength=200"`
	Body              string             `json:"body" jsonschema:"minLength=1,maxLength=65536"`
	Priority          Priority           `json:"priority"`
	DeliveryMode      DeliveryMode       `json:"delivery_mode"`
	CreatedAt         time.Time          `json:"created_at"`
	ExpiresAt         time.Time          `json:"expires_at"`
	RequiresAck       bool               `json:"requires_ack"`
	ContextRefs       []string           `json:"context_refs,omitempty" jsonschema:"maxItems=32"`
	Sensitivity       MessageSensitivity `json:"sensitivity"`
	ParentMessageID   ULID               `json:"parent_message_id,omitempty"`
	ThreadSequence    int64              `json:"thread_sequence" jsonschema:"minimum=1"`
	LoopID            LoopID             `json:"loop_id,omitempty"`
	LoopGeneration    *int64             `json:"loop_generation,omitempty" jsonschema:"minimum=1"`
	TurnIndex         *int64             `json:"turn_index,omitempty" jsonschema:"minimum=1"`
	HopCount          int64              `json:"hop_count" jsonschema:"minimum=0"`
	RouteDigest       Digest             `json:"route_digest"`
	RequestedAutonomy Autonomy           `json:"requested_autonomy,omitempty"`
	RequestedBudget   *RequestedBudget   `json:"requested_budget,omitempty"`
	ControlVersion    int64              `json:"control_version" jsonschema:"minimum=0"`
}

// Validate checks the logical message.
func (m LogicalMessage) Validate() error {
	if err := checkRoot(&m); err != nil {
		return err
	}
	for _, ref := range m.ContextRefs {
		if n := utf8.RuneCountInString(ref); n < 1 || n > 256 {
			return &ValidationError{Path: "context_refs", Reason: "each entry is 1–256 characters"}
		}
	}
	if err := checkLoopTriple(m.ParentMessageID != "", m.LoopID != "", m.LoopGeneration != nil, m.TurnIndex != nil); err != nil {
		return err
	}
	if !m.ExpiresAt.After(m.CreatedAt) {
		return &ValidationError{Path: pathExpiresAt, Reason: "must be after created_at"}
	}
	for _, to := range m.ToAgents {
		if to == m.FromAgent {
			return &ValidationError{Path: "to_agents", Reason: "must not contain from_agent"}
		}
	}
	return nil
}

// checkLoopTriple enforces the loop-field rules shared by the logical
// message and the wire envelope: a root message never opens a loop, and
// the loop fields are all present or all absent.
func checkLoopTriple(hasParent, hasLoop, hasGeneration, hasTurn bool) error {
	if !hasParent && (hasLoop || hasGeneration || hasTurn) {
		return &ValidationError{Path: pathLoopID, Reason: "a root message never opens a loop"}
	}
	if hasLoop != hasGeneration || hasLoop != hasTurn {
		return &ValidationError{Path: pathLoopID, Reason: "loop fields are all present or all absent"}
	}
	return nil
}

// RecipientKeyEnvelope wraps the content key for one recipient.
type RecipientKeyEnvelope struct {
	RecipientAgent  AgentID         `json:"recipient_agent"`
	EncryptionKeyID EncryptionKeyID `json:"encryption_key_id"`
	EncapsulatedKey []byte          `json:"encapsulated_key"`
	WrappedKey      []byte          `json:"wrapped_key"`
}

// Validate checks the key envelope.
func (e RecipientKeyEnvelope) Validate() error {
	if err := checkRoot(&e); err != nil {
		return err
	}
	agent, ok := keyAgentPart(string(e.EncryptionKeyID))
	if !ok || AgentID(agent) != e.RecipientAgent {
		return &ValidationError{Path: "encryption_key_id", Reason: "agent part must equal recipient_agent"}
	}
	if len(e.EncapsulatedKey) == 0 {
		return &ValidationError{Path: "encapsulated_key", Reason: reasonRequired}
	}
	if len(e.WrappedKey) == 0 {
		return &ValidationError{Path: "wrapped_key", Reason: reasonRequired}
	}
	return nil
}

// keyAgentPart extracts the agent name from a key id of the form
// "<algo>:<agent>:v<digits>".
func keyAgentPart(id string) (string, bool) {
	_, rest, found := strings.Cut(id, ":")
	if !found {
		return "", false
	}
	name, ver, found := strings.Cut(rest, ":")
	if !found || len(ver) < 2 || ver[0] != 'v' {
		return "", false
	}
	return name, true
}

// WireEnvelope is the only thing Hubs transport and persist.
type WireEnvelope struct {
	Protocol              string                 `json:"protocol"`
	Version               int64                  `json:"version" jsonschema:"enum=1"`
	MessageID             ULID                   `json:"message_id"`
	ClientMsgID           OpaqueID               `json:"client_msg_id"`
	OriginNode            NodeID                 `json:"origin_node"`
	OriginAgent           AgentID                `json:"origin_agent"`
	DestinationNode       NodeID                 `json:"destination_node"`
	RecipientAgents       []AgentID              `json:"recipient_agents" jsonschema:"minItems=1,maxItems=32,uniqueItems=true"`
	CreatedAt             time.Time              `json:"created_at"`
	ExpiresAt             time.Time              `json:"expires_at"`
	Priority              Priority               `json:"priority"`
	ThreadRef             ThreadRef              `json:"thread_ref"`
	ParentMessageID       ULID                   `json:"parent_message_id,omitempty"`
	LoopID                LoopID                 `json:"loop_id,omitempty"`
	LoopGeneration        *int64                 `json:"loop_generation,omitempty" jsonschema:"minimum=1"`
	TurnIndex             *int64                 `json:"turn_index,omitempty" jsonschema:"minimum=1"`
	HopCount              int64                  `json:"hop_count" jsonschema:"minimum=0"`
	RouteDigest           Digest                 `json:"route_digest"`
	ControlVersion        int64                  `json:"control_version" jsonschema:"minimum=0"`
	ContentType           string                 `json:"content_type"`
	EncryptionProfile     EncryptionProfile      `json:"encryption_profile"`
	SenderSigningKeyID    SigningKeyID           `json:"sender_signing_key_id"`
	RecipientKeyEnvelopes []RecipientKeyEnvelope `json:"recipient_key_envelopes"`
	Ciphertext            []byte                 `json:"ciphertext"`
	CiphertextSHA256      SHA256Hex              `json:"ciphertext_sha256"`
	Signature             []byte                 `json:"signature"`
}

// Validate checks the wire envelope.
func (w WireEnvelope) Validate() error {
	if w.Version != WireVersion {
		return fmt.Errorf("version %d: %w", w.Version, ErrUnsupportedVersion)
	}
	if err := checkRoot(&w); err != nil {
		return err
	}
	if w.Protocol != WireProtocol {
		return &ValidationError{Path: "protocol", Reason: fmt.Sprintf("must be %q", WireProtocol)}
	}
	if w.ContentType != WireContentType {
		return &ValidationError{Path: "content_type", Reason: fmt.Sprintf("must be %q", WireContentType)}
	}
	if err := checkLoopTriple(w.ParentMessageID != "", w.LoopID != "", w.LoopGeneration != nil, w.TurnIndex != nil); err != nil {
		return err
	}
	if !w.ExpiresAt.After(w.CreatedAt) {
		return &ValidationError{Path: pathExpiresAt, Reason: "must be after created_at"}
	}
	if w.EncryptionProfile == EncryptionProfileNone && len(w.RecipientKeyEnvelopes) != 0 {
		return &ValidationError{Path: "recipient_key_envelopes", Reason: "must be empty with profile none"}
	}
	if w.EncryptionProfile == EncryptionProfileGiffordE2EV1 {
		if err := checkEnvelopesMatch(w.RecipientKeyEnvelopes, w.RecipientAgents); err != nil {
			return err
		}
	}
	agent, ok := keyAgentPart(string(w.SenderSigningKeyID))
	if !ok || AgentID(agent) != w.OriginAgent {
		return &ValidationError{Path: "sender_signing_key_id", Reason: "agent part must equal origin_agent"}
	}
	if len(w.Signature) != 64 {
		return &ValidationError{Path: "signature", Reason: "must be 64 bytes"}
	}
	if len(w.Ciphertext) == 0 {
		return &ValidationError{Path: "ciphertext", Reason: "required"}
	}
	return nil
}

// checkEnvelopesMatch requires exactly one envelope per recipient agent:
// each recipient is consumed by its first envelope, in any order.
func checkEnvelopesMatch(envelopes []RecipientKeyEnvelope, recipients []AgentID) error {
	if len(envelopes) != len(recipients) {
		return &ValidationError{Path: "recipient_key_envelopes", Reason: "exactly one envelope per recipient agent"}
	}
	remaining := make(map[AgentID]bool, len(recipients))
	for _, r := range recipients {
		remaining[r] = true
	}
	for i, e := range envelopes {
		if !remaining[e.RecipientAgent] {
			return &ValidationError{Path: fmt.Sprintf("recipient_key_envelopes[%d]", i), Reason: "duplicate or unknown recipient envelope"}
		}
		delete(remaining, e.RecipientAgent)
	}
	return nil
}
