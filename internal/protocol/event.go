package protocol

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"
)

// EventEnvelope is the common event envelope: every event the Core stores
// carries one. Ordering across restarts is by sequence, never by wall_time.
type EventEnvelope struct {
	EventID          ULID          `json:"event_id"`
	SchemaVersion    int64         `json:"schema_version" jsonschema:"enum=1"`
	NodeID           NodeID        `json:"node_id"`
	SessionID        OpaqueID      `json:"session_id,omitempty"`
	AgentID          AgentID       `json:"agent_id,omitempty"`
	Harness          Harness       `json:"harness,omitempty"`
	HarnessVersion   string        `json:"harness_version,omitempty" jsonschema:"minLength=1,maxLength=64"`
	AdapterType      AdapterType   `json:"adapter_type,omitempty"`
	SourceQuality    SourceQuality `json:"source_quality"`
	EventType        EventType     `json:"event_type"`
	Direction        Direction     `json:"direction,omitempty"`
	Stream           Stream        `json:"stream,omitempty"`
	Origin           Origin        `json:"origin,omitempty"`
	WallTime         time.Time     `json:"wall_time"`
	MonotonicNS      int64         `json:"monotonic_ns" jsonschema:"minimum=0"`
	Sequence         int64         `json:"sequence" jsonschema:"minimum=0"`
	TurnID           OpaqueID      `json:"turn_id,omitempty"`
	ToolCallID       OpaqueID      `json:"tool_call_id,omitempty"`
	MessageID        ULID          `json:"message_id,omitempty"`
	DeliveryID       OpaqueID      `json:"delivery_id,omitempty"`
	DecisionID       OpaqueID      `json:"decision_id,omitempty"`
	ParentEventID    ULID          `json:"parent_event_id,omitempty"`
	CorrelationID    OpaqueID      `json:"correlation_id,omitempty"`
	Payload          Payload       `json:"payload"`
	Sensitivity      Sensitivity   `json:"sensitivity"`
	RedactionApplied bool          `json:"redaction_applied"`
}

// Validate checks the envelope and its registered payload.
func (e EventEnvelope) Validate() error {
	if e.SchemaVersion != SchemaVersion {
		return fmt.Errorf("schema_version %d: %w", e.SchemaVersion, ErrUnsupportedVersion)
	}
	if err := checkRoot(&e); err != nil {
		return err
	}
	if !isAgentScoped(e.EventType, e.Stream) {
		if err := checkGiffordScoped(e); err != nil {
			return err
		}
	} else {
		if err := checkAgentScoped(e); err != nil {
			return err
		}
		if e.SourceQuality == SourceQualityInternal {
			return &ValidationError{Path: pathSourceQuality, Reason: "internal is never ML evidence"}
		}
	}
	if err := checkStreamOrigin(e); err != nil {
		return err
	}
	if err := checkCaptureEnvelope(e); err != nil {
		return err
	}
	if isMessageControl(e.EventType) && e.MessageID == "" {
		return &ValidationError{Path: "message_id", Reason: "required for message.control_*"}
	}
	if isChannelPayloadEvent(e.EventType, e.Stream) {
		var channelPayload ChannelEventPayload
		if err := decodePayload(e.Payload, &channelPayload); err != nil {
			return err
		}
		if err := channelPayload.ValidateFor(e.EventType); err != nil {
			return prefixPath(err, "payload")
		}
	} else if newPayload := lookupPayload(e.EventType); newPayload != nil {
		if err := decodePayload(e.Payload, newPayload()); err != nil {
			return err
		}
	}
	return nil
}

// JSON paths and reasons repeated across the envelope checks.
const (
	pathSourceQuality = "source_quality"
	pathAdapterType   = "adapter_type"
	pathOrigin        = "origin"
	pathCapabilities  = "capabilities"
	pathContract      = "contract"
	pathProbabilities = "probabilities"
	pathAdapterName   = "adapter_name"
	reasonAgentScoped = "required for agent-scoped events"
	reasonRange01     = "must be in 0–1"
)

// agentScopedFamilies are the families that describe one observed agent.
func agentScopedFamily(family string) bool {
	switch family {
	case "input", "output", "io", familyProcess, familyPTY, "session", "adapter",
		"prompt", "turn", "tool", "question", "notification",
		"background_task", "subagent", "context", familyCapture:
		return true
	default:
		return false
	}
}

// familyOf splits "family.name" into the family.
func familyOf(t EventType) string {
	s := string(t)
	for i := range len(s) {
		if s[i] == '.' {
			return s[:i]
		}
	}
	return s
}

// isAgentScoped reports whether the event describes one observed agent:
// an agent-scoped family, or an approval event from the harness itself.
func isAgentScoped(t EventType, stream Stream) bool {
	if agentScopedFamily(familyOf(t)) {
		return true
	}
	return familyOf(t) == "approval" && stream == StreamNativeEvent
}

func checkAgentScoped(e EventEnvelope) error {
	if e.SessionID == "" {
		return &ValidationError{Path: "session_id", Reason: reasonAgentScoped}
	}
	if e.AgentID == "" {
		return &ValidationError{Path: "agent_id", Reason: reasonAgentScoped}
	}
	if e.AdapterType == "" {
		return &ValidationError{Path: pathAdapterType, Reason: reasonAgentScoped}
	}
	return nil
}

func checkGiffordScoped(e EventEnvelope) error {
	if e.Origin != OriginSystem {
		return &ValidationError{Path: pathOrigin, Reason: "must be system for Gifford-scoped events"}
	}
	if e.SourceQuality != SourceQualityInternal {
		return &ValidationError{Path: pathSourceQuality, Reason: "must be internal for Gifford-scoped events"}
	}
	if e.Stream != "" {
		return &ValidationError{Path: "stream", Reason: "must be absent for Gifford-scoped events"}
	}
	return nil
}

func checkStreamOrigin(e EventEnvelope) error {
	if e.Stream != StreamNativeEvent {
		return nil
	}
	if e.Origin != OriginHarness {
		return &ValidationError{Path: pathOrigin, Reason: "must be harness with stream native_event"}
	}
	switch e.SourceQuality {
	case SourceQualityNativeStructured, SourceQualityNativeHook:
		return nil
	default:
		return &ValidationError{Path: pathSourceQuality, Reason: "must be native_structured or native_hook with stream native_event"}
	}
}

func checkCaptureEnvelope(e EventEnvelope) error {
	if familyOf(e.EventType) != familyCapture {
		return nil
	}
	if e.Stream != StreamPTY {
		return &ValidationError{Path: "stream", Reason: "capture.* requires stream pty"}
	}
	switch e.SourceQuality {
	case SourceQualityBootstrapPipePane, SourceQualityBootstrapImport:
	default:
		return &ValidationError{Path: pathSourceQuality, Reason: "capture.* requires a bootstrap source quality"}
	}
	if e.EventType == EventTypeCaptureChunk {
		if e.Direction != DirectionOut {
			return &ValidationError{Path: "direction", Reason: "capture.chunk requires direction out"}
		}
		if e.Origin != OriginHarness {
			return &ValidationError{Path: pathOrigin, Reason: "capture.chunk requires origin harness"}
		}
	}
	return nil
}

func isMessageControl(t EventType) bool {
	return familyOf(t) == familyMessage
}

// payloadRegistration binds a payload constructor to its event types.
type payloadRegistration struct {
	New   func() any
	Types []EventType
}

// lookupPayload returns the payload constructor for event types with a
// registered payload, or nil for opaque payloads.
func lookupPayload(t EventType) func() any {
	for _, reg := range payloadRegistrations() {
		for _, registered := range reg.Types {
			if t == registered {
				return reg.New
			}
		}
	}
	return nil
}

// payloadRegistrations lists the payload constructors and their event
// types. Slice 3 registers the channel events here.
func payloadRegistrations() []payloadRegistration {
	var regs []payloadRegistration
	registerCapturePayloads(&regs)
	registerChannelPayloads(&regs)
	return regs
}

// decodeStrict decodes raw JSON with unknown fields rejected.
func decodeStrict(raw Payload, into any) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(into); err != nil {
		return fmt.Errorf("strict decode: %w", err)
	}
	return nil
}

// decodePayload strictly decodes raw into the registered payload, validates
// it and prefixes violations with "payload".
func decodePayload(raw Payload, into any) error {
	if err := decodeStrict(raw, into); err != nil {
		return &ValidationError{Path: "payload", Reason: fmt.Sprintf("bad payload: %v", err)}
	}
	if v, ok := into.(nestedValidator); ok {
		if err := v.Validate(); err != nil {
			return prefixPath(err, "payload")
		}
	}
	return nil
}
