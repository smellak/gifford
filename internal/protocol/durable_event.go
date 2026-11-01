package protocol

import (
	"encoding/json"
	"fmt"
	"reflect"
)

const (
	durablePathCorrelationID = "correlation_id"
	durablePathDeliveryID    = "delivery_id"
	durablePathDirection     = "direction"
	durablePathEventType     = "event_type"
	durablePathPayload       = "payload"
	durablePathState         = "state"
	durablePathToolCallID    = "tool_call_id"
)

// DurableEventSpec binds one explicitly admitted Proxy event type to its
// closed payload. There are no family or prefix matches in this contract.
type DurableEventSpec struct {
	Type               EventType
	Direction          Direction
	Origin             Origin
	EnvelopeDeliveryID bool
	NewPayload         func() any
}

var durableEventSpecs = []DurableEventSpec{
	{Type: EventTypePTYOpened, Origin: OriginSystem, NewPayload: func() any { return &DurablePTYOpened{} }},
	{Type: EventTypePTYClosed, Origin: OriginSystem, NewPayload: func() any { return &DurablePTYClosed{} }},
	{Type: EventTypeIOChunk, Direction: DirectionOut, Origin: OriginHarness, NewPayload: func() any { return &DurableIOChunk{} }},
	{Type: EventTypeInputKeyChunk, Direction: DirectionIn, Origin: OriginHuman, NewPayload: func() any { return &DurableInputKeyChunk{} }},
	{Type: EventTypeInputControlKey, Direction: DirectionIn, Origin: OriginHuman, NewPayload: func() any { return &DurableInputControlKey{} }},
	{Type: EventTypeInputPasteStarted, Direction: DirectionIn, Origin: OriginHuman, NewPayload: func() any { return &DurableInputPaste{} }},
	{Type: EventTypeInputPasteCompleted, Direction: DirectionIn, Origin: OriginHuman, NewPayload: func() any { return &DurableInputPaste{} }},
	{Type: EventTypeInputSubmitDetected, Direction: DirectionIn, Origin: OriginHuman, NewPayload: func() any { return &DurableInputSubmit{} }},
	{Type: EventTypeInputResize, Direction: DirectionIn, Origin: OriginHuman, NewPayload: func() any { return &DurableInputResize{} }},
	{Type: EventTypeInputDraftOpened, Direction: DirectionIn, Origin: OriginHuman, NewPayload: func() any { return &DurableInputDraft{} }},
	{Type: EventTypeInputDraftUpdated, Direction: DirectionIn, Origin: OriginHuman, NewPayload: func() any { return &DurableInputDraft{} }},
	{Type: EventTypeInputDraftSubmitted, Direction: DirectionIn, Origin: OriginHuman, NewPayload: func() any { return &DurableInputDraft{} }},
	{Type: EventTypeInputDraftCancelled, Direction: DirectionIn, Origin: OriginHuman, NewPayload: func() any { return &DurableInputDraft{} }},
	{Type: EventTypeInputDraftCleared, Direction: DirectionIn, Origin: OriginHuman, NewPayload: func() any { return &DurableInputDraft{} }},
	{Type: EventTypeInputDraftUnknown, Direction: DirectionIn, Origin: OriginHuman, NewPayload: func() any { return &DurableInputDraft{} }},
	{Type: EventTypeInputDeliveryPrepared, Direction: DirectionIn, Origin: OriginOrchestrator, EnvelopeDeliveryID: true, NewPayload: func() any { return &DurableDelivery{} }},
	{Type: EventTypeInputDeliveryRevalidated, Direction: DirectionIn, Origin: OriginOrchestrator, EnvelopeDeliveryID: true, NewPayload: func() any { return &DurableDelivery{} }},
	{Type: EventTypeInputDeliveryStarted, Direction: DirectionIn, Origin: OriginOrchestrator, EnvelopeDeliveryID: true, NewPayload: func() any { return &DurableDelivery{} }},
	{Type: EventTypeInputDeliveryChunk, Direction: DirectionIn, Origin: OriginOrchestrator, EnvelopeDeliveryID: true, NewPayload: func() any { return &DurableDeliveryChunk{} }},
	{Type: EventTypeInputDeliveryCommitted, Direction: DirectionIn, Origin: OriginOrchestrator, EnvelopeDeliveryID: true, NewPayload: func() any { return &DurableDelivery{} }},
	{Type: EventTypeInputDeliveryAbortedHumanPriority, Direction: DirectionIn, Origin: OriginOrchestrator, EnvelopeDeliveryID: true, NewPayload: func() any { return &DurableDeliveryAborted{} }},
	{Type: EventTypeInputDeliveryCancelled, Direction: DirectionIn, Origin: OriginOrchestrator, EnvelopeDeliveryID: true, NewPayload: func() any { return &DurableDeliveryCancelled{} }},
	{Type: EventTypeInputDeliveryFailed, Direction: DirectionIn, Origin: OriginOrchestrator, EnvelopeDeliveryID: true, NewPayload: func() any { return &DurableDeliveryFailed{} }},
	{Type: EventTypeCaptureQuotaReached, Origin: OriginSystem, NewPayload: func() any { return &CaptureQuotaReached{} }},
	{Type: EventTypeCaptureGapDetected, Origin: OriginSystem, NewPayload: func() any { return &CaptureGapDetected{} }},
}

// DurableEventSpecs returns the exact general-event allowlist. The returned
// slice is independent so callers cannot alter the process-wide policy.
func DurableEventSpecs() []DurableEventSpec {
	return append([]DurableEventSpec(nil), durableEventSpecs...)
}

// IsDurableEventType reports whether t is one of the exact admitted names.
func IsDurableEventType(t EventType) bool { return lookupDurableEventSpec(t) != nil }

func lookupDurableEventSpec(t EventType) *DurableEventSpec {
	for i := range durableEventSpecs {
		if durableEventSpecs[i].Type == t {
			return &durableEventSpecs[i]
		}
	}
	return nil
}

// DecodeDurableEventPayload strictly decodes and validates the closed payload
// for t. Missing, unknown and null fields are rejected before scalar values
// are checked. JSON numbers must therefore decode directly into their Go
// integer type; fractions and exponent notation are not accepted.
func DecodeDurableEventPayload(t EventType, raw Payload) (any, error) {
	spec := lookupDurableEventSpec(t)
	if spec == nil {
		return nil, &ValidationError{Path: durablePathEventType, Reason: "not allowed for durable events"}
	}
	payload := spec.NewPayload()
	if err := checkDurablePayloadPresence(raw, payload); err != nil {
		return nil, err
	}
	if err := decodeStrict(raw, payload); err != nil {
		return nil, &ValidationError{Path: durablePathPayload, Reason: fmt.Sprintf("bad payload: %v", err)}
	}
	if validator, ok := payload.(nestedValidator); ok {
		if err := validator.Validate(); err != nil {
			return nil, prefixPath(err, durablePathPayload)
		}
	}
	if err := validateDurablePayloadFor(t, payload); err != nil {
		return nil, prefixPath(err, durablePathPayload)
	}
	return payload, nil
}

// ValidateDurableEventPayload validates without retaining the decoded value.
func ValidateDurableEventPayload(t EventType, raw Payload) error {
	_, err := DecodeDurableEventPayload(t, raw)
	return err
}

// CanonicalDurableEventPayload validates raw and returns the deterministic
// JSON form of its registered closed struct. Storage and replay comparison use
// this form so harmless member order or whitespace never creates a conflict.
func CanonicalDurableEventPayload(t EventType, raw Payload) (Payload, error) {
	payload, err := DecodeDurableEventPayload(t, raw)
	if err != nil {
		return nil, err
	}
	canonical, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("canonical durable payload: %w", err)
	}
	return Payload(canonical), nil
}

// ValidateDurableEventEnvelope applies the exact, content-free envelope
// profile for a general Proxy event. EventEnvelope.Validate remains the base
// contract; this function only narrows values and optional lineage fields.
func ValidateDurableEventEnvelope(event EventEnvelope) error {
	if err := event.Validate(); err != nil {
		return err
	}
	spec := lookupDurableEventSpec(event.EventType)
	if spec == nil {
		return &ValidationError{Path: durablePathEventType, Reason: "not allowed for durable events"}
	}
	if event.SourceQuality != SourceQualityPTYScreen {
		return &ValidationError{Path: "source_quality", Reason: "must be pty_screen for durable events"}
	}
	if event.Stream != StreamPTY {
		return &ValidationError{Path: pathStream, Reason: "must be pty for durable events"}
	}
	if event.Direction != spec.Direction {
		return &ValidationError{Path: durablePathDirection, Reason: "does not match durable event type"}
	}
	if event.Origin != spec.Origin {
		return &ValidationError{Path: "origin", Reason: "does not match durable event type"}
	}
	if event.Sensitivity != SensitivityNormal {
		return &ValidationError{Path: "sensitivity", Reason: "must be normal for durable events"}
	}
	if event.RedactionApplied {
		return &ValidationError{Path: "redaction_applied", Reason: "must be false for durable events"}
	}
	for _, field := range []struct {
		path  string
		value string
	}{
		{path: "turn_id", value: string(event.TurnID)},
		{path: durablePathToolCallID, value: string(event.ToolCallID)},
		{path: "message_id", value: string(event.MessageID)},
		{path: "decision_id", value: string(event.DecisionID)},
		{path: "parent_event_id", value: string(event.ParentEventID)},
		{path: durablePathCorrelationID, value: string(event.CorrelationID)},
	} {
		if field.value != "" {
			return &ValidationError{Path: field.path, Reason: "must be absent for durable events"}
		}
	}
	payload, err := DecodeDurableEventPayload(event.EventType, event.Payload)
	if err != nil {
		return err
	}
	if opened, ok := payload.(*DurablePTYOpened); ok &&
		(opened.Harness != event.Harness || opened.HarnessVersion != event.HarnessVersion) {
		return &ValidationError{Path: "payload.harness", Reason: "must match the envelope harness and version"}
	}
	deliveryID, carriesDeliveryID := durablePayloadDeliveryID(payload)
	if spec.EnvelopeDeliveryID {
		if !carriesDeliveryID || event.DeliveryID != deliveryID {
			return &ValidationError{Path: durablePathDeliveryID, Reason: "must match payload.delivery_id"}
		}
	} else if event.DeliveryID != "" {
		return &ValidationError{Path: durablePathDeliveryID, Reason: "must be absent for this durable event type"}
	}
	return nil
}

func durablePayloadDeliveryID(payload any) (OpaqueID, bool) {
	switch value := payload.(type) {
	case *DurableDelivery:
		return value.DeliveryID, true
	case *DurableDeliveryChunk:
		return value.DeliveryID, true
	case *DurableDeliveryAborted:
		return value.DeliveryID, true
	case *DurableDeliveryCancelled:
		return value.DeliveryID, true
	case *DurableDeliveryFailed:
		return value.DeliveryID, true
	default:
		return "", false
	}
}

func checkDurablePayloadPresence(raw Payload, payload any) error {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil || object == nil {
		return &ValidationError{Path: durablePathPayload, Reason: "must be a JSON object"}
	}
	t := reflect.TypeOf(payload)
	if t == nil || t.Kind() != reflect.Pointer || t.Elem().Kind() != reflect.Struct {
		return &ValidationError{Path: durablePathPayload, Reason: "invalid payload registration"}
	}
	return checkPresenceObject(object, t.Elem(), durablePathPayload)
}

func validateDurablePayloadFor(t EventType, payload any) error {
	switch value := payload.(type) {
	case *DurableInputPaste:
		if t == EventTypeInputPasteStarted && value.SizeBytes != 0 {
			return &ValidationError{Path: "size_bytes", Reason: "must be zero for input.paste_started"}
		}
	case *DurableInputDraft:
		return value.ValidateFor(t)
	case *CaptureQuotaReached:
		if value.QuotaScope != QuotaScopeAgent {
			return &ValidationError{Path: "quota_scope", Reason: "must be agent"}
		}
		if value.Bytes <= value.LimitBytes {
			return &ValidationError{Path: "bytes", Reason: "must exceed limit_bytes"}
		}
	case *CaptureGapDetected:
		if value.Cause != CaptureGapCauseQuota && value.Cause != CaptureGapCauseIngestDied {
			return &ValidationError{Path: "cause", Reason: "must be quota or ingest_died"}
		}
	}
	return nil
}

// DurablePTYOpened is the content-free pty.opened projection sent to Core.
type DurablePTYOpened struct {
	Cols           int64   `json:"cols" jsonschema:"minimum=1,maximum=10000"`
	Rows           int64   `json:"rows" jsonschema:"minimum=1,maximum=10000"`
	Harness        Harness `json:"harness"`
	HarnessVersion string  `json:"harness_version" jsonschema:"minLength=1,maxLength=64"`
}

// Validate checks the payload and the bounded screen area.
func (p DurablePTYOpened) Validate() error {
	if err := checkRoot(&p); err != nil {
		return err
	}
	return validateDurableDimensions(p.Cols, p.Rows)
}

// DurablePTYClosed is the necessarily empty pty.closed payload.
type DurablePTYClosed struct{}

// Validate checks the empty payload.
func (p DurablePTYClosed) Validate() error { return checkRoot(&p) }

// DurableIOChunk is the safe io.chunk projection. Exact bytes, screen hashes
// and content digests remain local and never enter this payload.
type DurableIOChunk struct {
	ByteCount         int64 `json:"byte_count" jsonschema:"minimum=1,maximum=16384"`
	ANSIPresent       bool  `json:"ansi_present"`
	RecordingSequence int64 `json:"recording_sequence" jsonschema:"minimum=0"`
}

// Validate checks the payload.
func (p DurableIOChunk) Validate() error { return checkRoot(&p) }

// DurableInputKeyChunk counts printable runes without carrying them.
type DurableInputKeyChunk struct {
	PrintableCount    int64  `json:"printable_count" jsonschema:"minimum=1"`
	DraftGeneration   uint64 `json:"draft_generation"`
	InputGuardVersion int64  `json:"input_guard_version" jsonschema:"minimum=0"`
}

// Validate checks the payload.
func (p DurableInputKeyChunk) Validate() error { return checkRoot(&p) }

// DurableInputKeyCode is a non-text key class. Rune and extended deliberately
// identify only the class and never include the original rune or sequence.
type DurableInputKeyCode string

// DurableInputKeyCode values.
const (
	DurableInputKeyArrowUp    DurableInputKeyCode = "arrow_up"
	DurableInputKeyArrowDown  DurableInputKeyCode = "arrow_down"
	DurableInputKeyArrowLeft  DurableInputKeyCode = "arrow_left"
	DurableInputKeyArrowRight DurableInputKeyCode = "arrow_right"
	DurableInputKeyHome       DurableInputKeyCode = "home"
	DurableInputKeyEnd        DurableInputKeyCode = "end"
	DurableInputKeyInsert     DurableInputKeyCode = "insert"
	DurableInputKeyDelete     DurableInputKeyCode = "delete"
	DurableInputKeyPageUp     DurableInputKeyCode = "page_up"
	DurableInputKeyPageDown   DurableInputKeyCode = "page_down"
	DurableInputKeyBackspace  DurableInputKeyCode = "backspace"
	DurableInputKeyTab        DurableInputKeyCode = "tab"
	DurableInputKeyF1         DurableInputKeyCode = "f1"
	DurableInputKeyF2         DurableInputKeyCode = "f2"
	DurableInputKeyF3         DurableInputKeyCode = "f3"
	DurableInputKeyF4         DurableInputKeyCode = "f4"
	DurableInputKeyF5         DurableInputKeyCode = "f5"
	DurableInputKeyF6         DurableInputKeyCode = "f6"
	DurableInputKeyF7         DurableInputKeyCode = "f7"
	DurableInputKeyF8         DurableInputKeyCode = "f8"
	DurableInputKeyF9         DurableInputKeyCode = "f9"
	DurableInputKeyF10        DurableInputKeyCode = "f10"
	DurableInputKeyF11        DurableInputKeyCode = "f11"
	DurableInputKeyF12        DurableInputKeyCode = "f12"
	DurableInputKeyRune       DurableInputKeyCode = "rune"
	DurableInputKeyExtended   DurableInputKeyCode = "extended"
	DurableInputKeyUnknown    DurableInputKeyCode = "unknown"
)

// EnumValues returns every admitted key class.
func (DurableInputKeyCode) EnumValues() []string {
	return []string{
		string(DurableInputKeyArrowUp), string(DurableInputKeyArrowDown),
		string(DurableInputKeyArrowLeft), string(DurableInputKeyArrowRight),
		string(DurableInputKeyHome), string(DurableInputKeyEnd), string(DurableInputKeyInsert),
		string(DurableInputKeyDelete), string(DurableInputKeyPageUp), string(DurableInputKeyPageDown),
		string(DurableInputKeyBackspace), string(DurableInputKeyTab), string(DurableInputKeyF1),
		string(DurableInputKeyF2), string(DurableInputKeyF3), string(DurableInputKeyF4),
		string(DurableInputKeyF5), string(DurableInputKeyF6), string(DurableInputKeyF7),
		string(DurableInputKeyF8), string(DurableInputKeyF9), string(DurableInputKeyF10),
		string(DurableInputKeyF11), string(DurableInputKeyF12), string(DurableInputKeyRune),
		string(DurableInputKeyExtended), string(DurableInputKeyUnknown),
	}
}

// Valid reports whether the key class is admitted.
func (v DurableInputKeyCode) Valid() bool {
	for _, allowed := range DurableInputKeyCode("").EnumValues() {
		if string(v) == allowed {
			return true
		}
	}
	return false
}

// DurableInputControlKey carries key class and modifiers, never key content.
type DurableInputControlKey struct {
	Key               DurableInputKeyCode `json:"key"`
	Shift             bool                `json:"shift"`
	Alt               bool                `json:"alt"`
	Ctrl              bool                `json:"ctrl"`
	DraftGeneration   uint64              `json:"draft_generation"`
	InputGuardVersion int64               `json:"input_guard_version" jsonschema:"minimum=0"`
}

// Validate checks the payload.
func (p DurableInputControlKey) Validate() error { return checkRoot(&p) }

// DurableInputPaste counts paste bytes without carrying them.
type DurableInputPaste struct {
	SizeBytes         int64  `json:"size_bytes" jsonschema:"minimum=0"`
	DraftGeneration   uint64 `json:"draft_generation"`
	InputGuardVersion int64  `json:"input_guard_version" jsonschema:"minimum=0"`
}

// Validate checks the payload.
func (p DurableInputPaste) Validate() error { return checkRoot(&p) }

// DurableInputSubmit records only the guard lineage of a submit.
type DurableInputSubmit struct {
	DraftGeneration   uint64 `json:"draft_generation"`
	InputGuardVersion int64  `json:"input_guard_version" jsonschema:"minimum=0"`
}

// Validate checks the payload.
func (p DurableInputSubmit) Validate() error { return checkRoot(&p) }

// DurableInputResize records a bounded terminal size and guard lineage.
type DurableInputResize struct {
	Cols              int64  `json:"cols" jsonschema:"minimum=1,maximum=10000"`
	Rows              int64  `json:"rows" jsonschema:"minimum=1,maximum=10000"`
	DraftGeneration   uint64 `json:"draft_generation"`
	InputGuardVersion int64  `json:"input_guard_version" jsonschema:"minimum=0"`
}

// Validate checks the payload and the bounded screen area.
func (p DurableInputResize) Validate() error {
	if err := checkRoot(&p); err != nil {
		return err
	}
	return validateDurableDimensions(p.Cols, p.Rows)
}

func validateDurableDimensions(cols, rows int64) error {
	if cols*rows > 250_000 {
		return &ValidationError{Path: "rows", Reason: "cols times rows exceeds 250000"}
	}
	return nil
}

// DurableDraftState is the lowercase state emitted by the session guard.
type DurableDraftState string

// DurableDraftState values.
const (
	DurableDraftStateClosed     DurableDraftState = "closed"
	DurableDraftStateOpen       DurableDraftState = "open"
	DurableDraftStatePasting    DurableDraftState = "pasting"
	DurableDraftStateSubmitting DurableDraftState = "submitting"
	DurableDraftStateUnknown    DurableDraftState = "unknown"
)

// EnumValues returns every session guard state.
func (DurableDraftState) EnumValues() []string {
	return []string{
		string(DurableDraftStateClosed), string(DurableDraftStateOpen),
		string(DurableDraftStatePasting), string(DurableDraftStateSubmitting),
		string(DurableDraftStateUnknown),
	}
}

// Valid reports whether the state is known.
func (v DurableDraftState) Valid() bool {
	switch v {
	case DurableDraftStateClosed, DurableDraftStateOpen, DurableDraftStatePasting,
		DurableDraftStateSubmitting, DurableDraftStateUnknown:
		return true
	default:
		return false
	}
}

// DurableInputDraft records a state transition and guard lineage.
type DurableInputDraft struct {
	State             DurableDraftState `json:"state"`
	DraftGeneration   uint64            `json:"draft_generation"`
	InputGuardVersion int64             `json:"input_guard_version" jsonschema:"minimum=0"`
}

// Validate checks the payload independently of its event name.
func (p DurableInputDraft) Validate() error { return checkRoot(&p) }

// ValidateFor enforces the fixed event/state pairs emitted by Session.
func (p DurableInputDraft) ValidateFor(t EventType) error {
	switch t {
	case EventTypeInputDraftOpened:
		if p.State != DurableDraftStateOpen && p.State != DurableDraftStatePasting {
			return &ValidationError{Path: durablePathState, Reason: "must be open or pasting for input.draft_opened"}
		}
	case EventTypeInputDraftUnknown:
		if p.State != DurableDraftStateUnknown {
			return &ValidationError{Path: durablePathState, Reason: "must be unknown for input.draft_unknown"}
		}
	case EventTypeInputDraftUpdated:
		if p.State == DurableDraftStateUnknown {
			return &ValidationError{Path: durablePathState, Reason: "must not be unknown for input.draft_updated"}
		}
	case EventTypeInputDraftSubmitted, EventTypeInputDraftCancelled, EventTypeInputDraftCleared:
		if p.State != DurableDraftStateClosed {
			return &ValidationError{Path: durablePathState, Reason: "must be closed for terminal input draft events"}
		}
	default:
	}
	return nil
}

// DurableDelivery is the common payload of delivery lifecycle steps that do
// not add a count or reason.
type DurableDelivery struct {
	DeliveryID OpaqueID `json:"delivery_id"`
	Generation uint64   `json:"generation"`
	Version    int64    `json:"version" jsonschema:"minimum=0"`
}

// Validate checks the payload.
func (p DurableDelivery) Validate() error { return checkRoot(&p) }

// DurableDeliveryChunk counts written bytes without carrying them.
type DurableDeliveryChunk struct {
	DeliveryID OpaqueID `json:"delivery_id"`
	Generation uint64   `json:"generation"`
	Version    int64    `json:"version" jsonschema:"minimum=0"`
	Bytes      int64    `json:"bytes" jsonschema:"minimum=1"`
}

// Validate checks the payload.
func (p DurableDeliveryChunk) Validate() error { return checkRoot(&p) }

// DurableDeliveryAbortReason is a fixed, content-free safety reason.
type DurableDeliveryAbortReason string

// DurableDeliveryAbortReason values.
const (
	DurableDeliveryAbortProfileNotMeasured   DurableDeliveryAbortReason = "profile not measured"
	DurableDeliveryAbortKeyIncomplete        DurableDeliveryAbortReason = "human key sequence incomplete"
	DurableDeliveryAbortSnapshotMismatch     DurableDeliveryAbortReason = "snapshot mismatch"
	DurableDeliveryAbortOutputNotQuiet       DurableDeliveryAbortReason = "agent output not quiet"
	DurableDeliveryAbortNoInputEvidence      DurableDeliveryAbortReason = "no input evidence"
	DurableDeliveryAbortInputLineNotEmpty    DurableDeliveryAbortReason = "input line not empty"
	DurableDeliveryAbortHumanInputArrived    DurableDeliveryAbortReason = "human input arrived"
	DurableDeliveryAbortCommitNotReserved    DurableDeliveryAbortReason = "commit not reserved"
	DurableDeliveryAbortLoopBehindWriter     DurableDeliveryAbortReason = "loop behind writer"
	DurableDeliveryAbortHumanInputInFlight   DurableDeliveryAbortReason = "human input in flight"
	DurableDeliveryAbortHumanInputSincePrep  DurableDeliveryAbortReason = "human input since prepare"
	DurableDeliveryAbortDraftNotClosed       DurableDeliveryAbortReason = "draft not closed"
	DurableDeliveryAbortGenerationMoved      DurableDeliveryAbortReason = "generation or version moved"
	DurableDeliveryAbortHumanSubmittedRecent DurableDeliveryAbortReason = "human submitted recently"
	DurableDeliveryAbortAnotherLease         DurableDeliveryAbortReason = "another lease"
)

// EnumValues returns every fixed abort reason.
func (DurableDeliveryAbortReason) EnumValues() []string {
	return []string{
		string(DurableDeliveryAbortProfileNotMeasured), string(DurableDeliveryAbortKeyIncomplete),
		string(DurableDeliveryAbortSnapshotMismatch), string(DurableDeliveryAbortOutputNotQuiet),
		string(DurableDeliveryAbortNoInputEvidence), string(DurableDeliveryAbortInputLineNotEmpty),
		string(DurableDeliveryAbortHumanInputArrived), string(DurableDeliveryAbortCommitNotReserved),
		string(DurableDeliveryAbortLoopBehindWriter), string(DurableDeliveryAbortHumanInputInFlight),
		string(DurableDeliveryAbortHumanInputSincePrep), string(DurableDeliveryAbortDraftNotClosed),
		string(DurableDeliveryAbortGenerationMoved), string(DurableDeliveryAbortHumanSubmittedRecent),
		string(DurableDeliveryAbortAnotherLease),
	}
}

// Valid reports whether the abort reason is admitted.
func (v DurableDeliveryAbortReason) Valid() bool {
	for _, allowed := range DurableDeliveryAbortReason("").EnumValues() {
		if string(v) == allowed {
			return true
		}
	}
	return false
}

// DurableDeliveryAborted records a terminal safety refusal.
type DurableDeliveryAborted struct {
	DeliveryID OpaqueID                   `json:"delivery_id"`
	Generation uint64                     `json:"generation"`
	Version    int64                      `json:"version" jsonschema:"minimum=0"`
	Reason     DurableDeliveryAbortReason `json:"reason"`
}

// Validate checks the payload.
func (p DurableDeliveryAborted) Validate() error { return checkRoot(&p) }

// DurableDeliveryCancelled records a typed cancellation.
type DurableDeliveryCancelled struct {
	DeliveryID OpaqueID     `json:"delivery_id"`
	Reason     CancelReason `json:"reason"`
}

// Validate checks the payload.
func (p DurableDeliveryCancelled) Validate() error { return checkRoot(&p) }

// DurableDeliveryFailureReason is a fixed code derived from local errors.
type DurableDeliveryFailureReason string

// DurableDeliveryFailureReason values.
const (
	DurableDeliveryFailureLineInvalid       DurableDeliveryFailureReason = "LINE_INVALID"
	DurableDeliveryFailureModeInvalid       DurableDeliveryFailureReason = "MODE_INVALID"
	DurableDeliveryFailurePTYWriteFailed    DurableDeliveryFailureReason = "PTY_WRITE_FAILED"
	DurableDeliveryFailureSessionNotRunning DurableDeliveryFailureReason = "SESSION_NOT_RUNNING"
)

// EnumValues returns every fixed failure code.
func (DurableDeliveryFailureReason) EnumValues() []string {
	return []string{
		string(DurableDeliveryFailureLineInvalid), string(DurableDeliveryFailureModeInvalid),
		string(DurableDeliveryFailurePTYWriteFailed), string(DurableDeliveryFailureSessionNotRunning),
	}
}

// Valid reports whether the failure code is admitted.
func (v DurableDeliveryFailureReason) Valid() bool {
	switch v {
	case DurableDeliveryFailureLineInvalid, DurableDeliveryFailureModeInvalid,
		DurableDeliveryFailurePTYWriteFailed, DurableDeliveryFailureSessionNotRunning:
		return true
	default:
		return false
	}
}

// DurableDeliveryFailed records a fixed failure code, never err.Error text.
type DurableDeliveryFailed struct {
	DeliveryID OpaqueID                     `json:"delivery_id"`
	Reason     DurableDeliveryFailureReason `json:"reason"`
}

// Validate checks the payload.
func (p DurableDeliveryFailed) Validate() error { return checkRoot(&p) }
