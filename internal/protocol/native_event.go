package protocol

import (
	"encoding/json"
	"fmt"
)

// NativeEventResult is the optional content-free outcome of a native event.
type NativeEventResult string

// Native event result values.
const (
	NativeEventResultOK        NativeEventResult = "ok"
	NativeEventResultFailed    NativeEventResult = "failed"
	NativeEventResultCancelled NativeEventResult = "cancelled"
	NativeEventResultDenied    NativeEventResult = "denied"
)

// EnumValues returns the native result values in wire order.
func (NativeEventResult) EnumValues() []string {
	return []string{
		string(NativeEventResultOK), string(NativeEventResultFailed),
		string(NativeEventResultCancelled), string(NativeEventResultDenied),
	}
}

// Valid reports whether r is a known native result.
func (r NativeEventResult) Valid() bool {
	switch r {
	case NativeEventResultOK, NativeEventResultFailed,
		NativeEventResultCancelled, NativeEventResultDenied:
		return true
	default:
		return false
	}
}

// NativeToolKind is the optional coarse, content-free class of a tool call.
type NativeToolKind string

// Native tool kind values.
const (
	NativeToolKindShell  NativeToolKind = "shell"
	NativeToolKindFile   NativeToolKind = "file"
	NativeToolKindSearch NativeToolKind = "search"
	NativeToolKindWeb    NativeToolKind = "web"
	NativeToolKindMCP    NativeToolKind = "mcp"
	NativeToolKindOther  NativeToolKind = "other"
)

// EnumValues returns the native tool kinds in wire order.
func (NativeToolKind) EnumValues() []string {
	return []string{
		string(NativeToolKindShell), string(NativeToolKindFile),
		string(NativeToolKindSearch), string(NativeToolKindWeb),
		string(NativeToolKindMCP), string(NativeToolKindOther),
	}
}

// Valid reports whether k is a known native tool kind.
func (k NativeToolKind) Valid() bool {
	switch k {
	case NativeToolKindShell, NativeToolKindFile, NativeToolKindSearch,
		NativeToolKindWeb, NativeToolKindMCP, NativeToolKindOther:
		return true
	default:
		return false
	}
}

// NativeDurableEventPayload is the complete native-event payload stored by
// the Core. References with first-class envelope fields stay in the envelope;
// no prompt, draft, arguments, output, answer or other free text can fit here.
type NativeDurableEventPayload struct {
	EmitterEventID OpaqueID          `json:"emitter_event_id" jsonschema:"maxLength=64"`
	SessionRef     OpaqueID          `json:"session_ref,omitempty" jsonschema:"maxLength=64"`
	Result         NativeEventResult `json:"result,omitempty"`
	ToolKind       NativeToolKind    `json:"tool_kind,omitempty"`
}

// Validate checks the closed native payload.
func (p NativeDurableEventPayload) Validate() error { return checkRoot(&p) }

// DecodeNativeDurableEventPayload strictly decodes a native durable payload.
func DecodeNativeDurableEventPayload(raw Payload) (*NativeDurableEventPayload, error) {
	var payload NativeDurableEventPayload
	if err := decodeStrict(raw, &payload); err != nil {
		return nil, &ValidationError{Path: durablePathPayload, Reason: fmt.Sprintf("bad payload: %v", err)}
	}
	if err := payload.Validate(); err != nil {
		return nil, prefixPath(err, durablePathPayload)
	}
	return &payload, nil
}

// CanonicalNativeDurableEventPayload validates raw and returns its stable
// closed JSON representation.
func CanonicalNativeDurableEventPayload(raw Payload) (Payload, error) {
	payload, err := DecodeNativeDurableEventPayload(raw)
	if err != nil {
		return nil, err
	}
	canonical, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("canonical native durable payload: %w", err)
	}
	return Payload(canonical), nil
}

// ValidateNativeDurableEvent applies the distinct content-free durable
// profile for native lifecycle observations. It deliberately does not widen
// ValidateDurableEventEnvelope, which remains the PTY-screen profile.
func ValidateNativeDurableEvent(event EventEnvelope) error {
	if err := event.Validate(); err != nil {
		return err
	}
	if !IsNativeEventType(event.EventType) {
		return &ValidationError{Path: "event_type", Reason: "not allowed for native durable events"}
	}
	if event.SourceQuality != SourceQualityNativeStructured &&
		event.SourceQuality != SourceQualityNativeHook {
		return &ValidationError{Path: "source_quality", Reason: "must be native_structured or native_hook"}
	}
	if event.Stream != StreamNativeEvent {
		return &ValidationError{Path: "stream", Reason: "must be native_event"}
	}
	if event.Origin != OriginHarness {
		return &ValidationError{Path: "origin", Reason: "must be harness"}
	}
	if event.Direction != "" {
		return &ValidationError{Path: durablePathDirection, Reason: "must be absent for native durable events"}
	}
	if !event.Harness.Valid() || event.HarnessVersion == "" {
		return &ValidationError{Path: "harness", Reason: "required for native durable events"}
	}
	if event.WallTime.IsZero() || event.WallTime != event.WallTime.UTC() {
		return &ValidationError{Path: "wall_time", Reason: "must be a non-zero UTC instant"}
	}
	if event.MonotonicNS != 0 {
		return &ValidationError{Path: "monotonic_ns", Reason: "must be zero for emitter-clock events"}
	}
	if event.Sensitivity != SensitivityNormal {
		return &ValidationError{Path: "sensitivity", Reason: "must be normal for native durable events"}
	}
	if event.RedactionApplied {
		return &ValidationError{Path: "redaction_applied", Reason: "must be false for native durable events"}
	}
	for _, field := range []struct {
		path  string
		value string
	}{
		{path: "message_id", value: string(event.MessageID)},
		{path: durablePathDeliveryID, value: string(event.DeliveryID)},
		{path: "decision_id", value: string(event.DecisionID)},
		{path: "parent_event_id", value: string(event.ParentEventID)},
	} {
		if field.value != "" {
			return &ValidationError{Path: field.path, Reason: "must be absent for native durable events"}
		}
	}
	for _, field := range []struct {
		path  string
		value OpaqueID
	}{
		{path: "turn_id", value: event.TurnID},
		{path: durablePathToolCallID, value: event.ToolCallID},
		{path: durablePathCorrelationID, value: event.CorrelationID},
	} {
		if field.value != "" && len(field.value) > 64 {
			return &ValidationError{Path: field.path, Reason: "must be at most 64 bytes"}
		}
	}
	if NativeEventNeedsToolRef(event.EventType) && event.ToolCallID == "" {
		return &ValidationError{Path: durablePathToolCallID, Reason: "required for native tool events"}
	}
	if NativeEventNeedsRequestRef(event.EventType) && event.CorrelationID == "" {
		return &ValidationError{Path: durablePathCorrelationID, Reason: "required for native request events"}
	}
	payload, err := DecodeNativeDurableEventPayload(event.Payload)
	if err != nil {
		return err
	}
	if event.EventType == EventTypeQuestionResolved && payload.Result == "" {
		return &ValidationError{Path: "payload.result", Reason: "required for question.resolved"}
	}
	return nil
}
