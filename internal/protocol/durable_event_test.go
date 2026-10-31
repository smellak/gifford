package protocol

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	durableTestHarnessVersion = "9.8.7"
	durableClosedDraftPayload = `{"state":"closed","draft_generation":1,"input_guard_version":2}`
	durableDeliveryPayload    = `{"delivery_id":"delivery-1","generation":1,"version":2}`
)

func TestDurableEventSpecsAreExactAndPayloadsValidate(t *testing.T) {
	t.Parallel()
	want := []EventType{
		EventTypePTYOpened,
		EventTypePTYClosed,
		EventTypeIOChunk,
		EventTypeInputKeyChunk,
		EventTypeInputControlKey,
		EventTypeInputPasteStarted,
		EventTypeInputPasteCompleted,
		EventTypeInputSubmitDetected,
		EventTypeInputResize,
		EventTypeInputDraftOpened,
		EventTypeInputDraftUpdated,
		EventTypeInputDraftSubmitted,
		EventTypeInputDraftCancelled,
		EventTypeInputDraftCleared,
		EventTypeInputDraftUnknown,
		EventTypeInputDeliveryPrepared,
		EventTypeInputDeliveryRevalidated,
		EventTypeInputDeliveryStarted,
		EventTypeInputDeliveryChunk,
		EventTypeInputDeliveryCommitted,
		EventTypeInputDeliveryAbortedHumanPriority,
		EventTypeInputDeliveryCancelled,
		EventTypeInputDeliveryFailed,
		EventTypeCaptureQuotaReached,
		EventTypeCaptureGapDetected,
	}
	valid := map[EventType]string{
		EventTypePTYOpened:                         `{"cols":80,"rows":24,"harness":"codex","harness_version":"9.8.7"}`,
		EventTypePTYClosed:                         `{}`,
		EventTypeIOChunk:                           `{"byte_count":16,"ansi_present":true,"recording_sequence":0}`,
		EventTypeInputKeyChunk:                     `{"printable_count":1,"draft_generation":0,"input_guard_version":0}`,
		EventTypeInputControlKey:                   `{"key":"rune","shift":false,"alt":false,"ctrl":false,"draft_generation":0,"input_guard_version":0}`,
		EventTypeInputPasteStarted:                 `{"size_bytes":0,"draft_generation":0,"input_guard_version":0}`,
		EventTypeInputPasteCompleted:               `{"size_bytes":12,"draft_generation":1,"input_guard_version":2}`,
		EventTypeInputSubmitDetected:               `{"draft_generation":1,"input_guard_version":2}`,
		EventTypeInputResize:                       `{"cols":100,"rows":30,"draft_generation":1,"input_guard_version":2}`,
		EventTypeInputDraftOpened:                  `{"state":"open","draft_generation":1,"input_guard_version":2}`,
		EventTypeInputDraftUpdated:                 `{"state":"submitting","draft_generation":1,"input_guard_version":2}`,
		EventTypeInputDraftSubmitted:               durableClosedDraftPayload,
		EventTypeInputDraftCancelled:               durableClosedDraftPayload,
		EventTypeInputDraftCleared:                 durableClosedDraftPayload,
		EventTypeInputDraftUnknown:                 `{"state":"unknown","draft_generation":1,"input_guard_version":2}`,
		EventTypeInputDeliveryPrepared:             durableDeliveryPayload,
		EventTypeInputDeliveryRevalidated:          durableDeliveryPayload,
		EventTypeInputDeliveryStarted:              durableDeliveryPayload,
		EventTypeInputDeliveryChunk:                `{"delivery_id":"delivery-1","generation":1,"version":2,"bytes":1}`,
		EventTypeInputDeliveryCommitted:            durableDeliveryPayload,
		EventTypeInputDeliveryAbortedHumanPriority: `{"delivery_id":"delivery-1","generation":1,"version":2,"reason":"human input arrived"}`,
		EventTypeInputDeliveryCancelled:            `{"delivery_id":"delivery-1","reason":"HUMAN_PRIORITY"}`,
		EventTypeInputDeliveryFailed:               `{"delivery_id":"delivery-1","reason":"PTY_WRITE_FAILED"}`,
		EventTypeCaptureQuotaReached:               `{"capture_session_id":"capture-1","quota_scope":"agent","bytes":101,"limit_bytes":100}`,
		EventTypeCaptureGapDetected:                `{"capture_session_id":"capture-1","cause":"quota"}`,
	}

	specs := DurableEventSpecs()
	require.Len(t, specs, 25)
	require.Len(t, valid, 25)
	seen := make(map[EventType]bool, len(specs))
	for i, spec := range specs {
		require.Equal(t, want[i], spec.Type)
		require.NotNil(t, spec.NewPayload)
		require.False(t, seen[spec.Type], "duplicate spec %s", spec.Type)
		seen[spec.Type] = true
		require.True(t, IsDurableEventType(spec.Type))
		decoded, err := DecodeDurableEventPayload(spec.Type, Payload(valid[spec.Type]))
		require.NoError(t, err, spec.Type)
		require.IsType(t, spec.NewPayload(), decoded, spec.Type)
		envelope := durableTestEnvelope(spec, Payload(valid[spec.Type]))
		require.NoError(t, ValidateDurableEventEnvelope(envelope), spec.Type)
	}

	specs[0].Type = EventTypeControlPause
	require.Equal(t, EventTypePTYOpened, DurableEventSpecs()[0].Type)
	for _, denied := range []EventType{
		EventTypeControlPause,
		EventTypeInputPasteChunk,
		EventTypeOutputScreenSnapshot,
		EventTypeCaptureChunk,
		"input.future_event",
	} {
		require.False(t, IsDurableEventType(denied))
		err := ValidateDurableEventPayload(denied, Payload(`{}`))
		require.ErrorIs(t, err, ErrInvalid)
	}
}

func TestDurableEventEnvelopeProfileIsClosed(t *testing.T) {
	t.Parallel()
	base := durableTestEnvelope(DurableEventSpec{
		Type: EventTypeIOChunk, Direction: DirectionOut, Origin: OriginHarness,
	}, Payload(`{"byte_count":1,"ansi_present":false,"recording_sequence":0}`))
	tests := map[string]func(*EventEnvelope){
		"source quality": func(event *EventEnvelope) { event.SourceQuality = SourceQualityNativeStructured },
		"stream":         func(event *EventEnvelope) { event.Stream = StreamStdout },
		"direction":      func(event *EventEnvelope) { event.Direction = DirectionIn },
		"origin":         func(event *EventEnvelope) { event.Origin = OriginSystem },
		"sensitivity":    func(event *EventEnvelope) { event.Sensitivity = SensitivitySensitive },
		"redaction":      func(event *EventEnvelope) { event.RedactionApplied = true },
		"turn":           func(event *EventEnvelope) { event.TurnID = "turn-1" },
		"tool call":      func(event *EventEnvelope) { event.ToolCallID = "tool-1" },
		"message":        func(event *EventEnvelope) { event.MessageID = "01JB0000000000000000000071" },
		"delivery":       func(event *EventEnvelope) { event.DeliveryID = "delivery-1" },
		"decision":       func(event *EventEnvelope) { event.DecisionID = "decision-1" },
		"parent":         func(event *EventEnvelope) { event.ParentEventID = "01JB0000000000000000000072" },
		"correlation":    func(event *EventEnvelope) { event.CorrelationID = "correlation-1" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			event := base
			mutate(&event)
			require.ErrorIs(t, ValidateDurableEventEnvelope(event), ErrInvalid)
		})
	}

	deliverySpec := DurableEventSpec{
		Type: EventTypeInputDeliveryCommitted, Direction: DirectionIn,
		Origin: OriginOrchestrator, EnvelopeDeliveryID: true,
	}
	delivery := durableTestEnvelope(deliverySpec,
		Payload(durableDeliveryPayload))
	require.NoError(t, ValidateDurableEventEnvelope(delivery))
	delivery.DeliveryID = "delivery-2"
	require.ErrorIs(t, ValidateDurableEventEnvelope(delivery), ErrInvalid)

	opened := durableTestEnvelope(DurableEventSpec{
		Type: EventTypePTYOpened, Origin: OriginSystem,
	}, Payload(`{"cols":80,"rows":24,"harness":"codex","harness_version":"other"}`))
	require.ErrorIs(t, ValidateDurableEventEnvelope(opened), ErrInvalid)
}

func durableTestEnvelope(spec DurableEventSpec, payload Payload) EventEnvelope {
	event := EventEnvelope{
		EventID: "01JB0000000000000000000070", SchemaVersion: SchemaVersion,
		NodeID: "node-durable", SessionID: "session-durable", AgentID: "agent-durable",
		Harness: HarnessCodex, HarnessVersion: durableTestHarnessVersion, AdapterType: AdapterTypeGenericPTY,
		SourceQuality: SourceQualityPTYScreen, EventType: spec.Type,
		Direction: spec.Direction, Stream: StreamPTY, Origin: spec.Origin,
		WallTime: time.Date(2026, 10, 31, 4, 0, 0, 0, time.UTC), MonotonicNS: 1, Sequence: 1,
		Payload: payload, Sensitivity: SensitivityNormal,
	}
	if spec.EnvelopeDeliveryID {
		event.DeliveryID = "delivery-1"
	}
	return event
}

func TestDurableEventPayloadsRejectOpenOrMissingShapes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		typeID  EventType
		payload string
	}{
		{name: "unknown field", typeID: EventTypePTYClosed, payload: `{"text":"private"}`},
		{name: "missing zero bool", typeID: EventTypeIOChunk, payload: `{"byte_count":1,"recording_sequence":0}`},
		{name: "missing zero integer", typeID: EventTypeInputPasteStarted, payload: `{"draft_generation":0,"input_guard_version":0}`},
		{name: "null", typeID: EventTypeInputSubmitDetected, payload: `{"draft_generation":null,"input_guard_version":0}`},
		{name: "null object", typeID: EventTypePTYClosed, payload: `null`},
		{name: "array", typeID: EventTypePTYClosed, payload: `[]`},
		{name: "content digest", typeID: EventTypeIOChunk, payload: `{"byte_count":1,"ansi_present":false,"recording_sequence":0,"chunk_digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`},
		{name: "screen hash", typeID: EventTypeIOChunk, payload: `{"byte_count":1,"ansi_present":false,"recording_sequence":0,"screen_hash_after":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := ValidateDurableEventPayload(tc.typeID, Payload(tc.payload))
			require.ErrorIs(t, err, ErrInvalid)
		})
	}
}

func TestCanonicalDurableEventPayloadNormalizesMemberOrder(t *testing.T) {
	t.Parallel()
	canonical, err := CanonicalDurableEventPayload(EventTypeIOChunk,
		Payload(`{ "recording_sequence": 7, "ansi_present": true, "byte_count": 8 }`))
	require.NoError(t, err)
	require.Equal(t,
		Payload(`{"byte_count":8,"ansi_present":true,"recording_sequence":7}`), canonical)
}

func TestDurableEventIntegersAreExactAndBounded(t *testing.T) {
	t.Parallel()
	for _, payload := range []string{
		`{"byte_count":1.0,"ansi_present":false,"recording_sequence":0}`,
		`{"byte_count":1e0,"ansi_present":false,"recording_sequence":0}`,
		`{"byte_count":1,"ansi_present":false,"recording_sequence":-1}`,
		`{"byte_count":16385,"ansi_present":false,"recording_sequence":0}`,
		`{"byte_count":1,"ansi_present":"false","recording_sequence":0}`,
	} {
		require.ErrorIs(t, ValidateDurableEventPayload(EventTypeIOChunk, Payload(payload)), ErrInvalid)
	}

	maxGeneration := Payload(`{"draft_generation":18446744073709551615,"input_guard_version":0}`)
	decoded, err := DecodeDurableEventPayload(EventTypeInputSubmitDetected, maxGeneration)
	require.NoError(t, err)
	require.Equal(t, uint64(math.MaxUint64), decoded.(*DurableInputSubmit).DraftGeneration)
	for _, payload := range []string{
		`{"draft_generation":18446744073709551616,"input_guard_version":0}`,
		`{"draft_generation":-1,"input_guard_version":0}`,
		`{"draft_generation":1e0,"input_guard_version":0}`,
		`{"draft_generation":0,"input_guard_version":9223372036854775808}`,
	} {
		require.ErrorIs(t, ValidateDurableEventPayload(EventTypeInputSubmitDetected, Payload(payload)), ErrInvalid)
	}
}

func TestDurableEventPayloadCrossFieldRules(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		typeID  EventType
		payload string
	}{
		{name: "opened area", typeID: EventTypePTYOpened, payload: `{"cols":10000,"rows":26,"harness":"codex","harness_version":"1"}`},
		{name: "resize area", typeID: EventTypeInputResize, payload: `{"cols":10000,"rows":26,"draft_generation":0,"input_guard_version":0}`},
		{name: "unknown harness", typeID: EventTypePTYOpened, payload: `{"cols":80,"rows":24,"harness":"other","harness_version":"1"}`},
		{name: "paste start nonzero", typeID: EventTypeInputPasteStarted, payload: `{"size_bytes":1,"draft_generation":0,"input_guard_version":0}`},
		{name: "opened closed", typeID: EventTypeInputDraftOpened, payload: `{"state":"closed","draft_generation":0,"input_guard_version":0}`},
		{name: "unknown open", typeID: EventTypeInputDraftUnknown, payload: `{"state":"open","draft_generation":0,"input_guard_version":0}`},
		{name: "updated unknown", typeID: EventTypeInputDraftUpdated, payload: `{"state":"unknown","draft_generation":0,"input_guard_version":0}`},
		{name: "submitted open", typeID: EventTypeInputDraftSubmitted, payload: `{"state":"open","draft_generation":0,"input_guard_version":0}`},
		{name: "cancelled pasting", typeID: EventTypeInputDraftCancelled, payload: `{"state":"pasting","draft_generation":0,"input_guard_version":0}`},
		{name: "cleared submitting", typeID: EventTypeInputDraftCleared, payload: `{"state":"submitting","draft_generation":0,"input_guard_version":0}`},
		{name: "text key", typeID: EventTypeInputControlKey, payload: `{"key":"x","shift":false,"alt":false,"ctrl":false,"draft_generation":0,"input_guard_version":0}`},
		{name: "free abort reason", typeID: EventTypeInputDeliveryAbortedHumanPriority, payload: `{"delivery_id":"delivery-1","generation":0,"version":0,"reason":"private failure"}`},
		{name: "free failure reason", typeID: EventTypeInputDeliveryFailed, payload: `{"delivery_id":"delivery-1","reason":"write /private/path: secret"}`},
		{name: "bad cancellation", typeID: EventTypeInputDeliveryCancelled, payload: `{"delivery_id":"delivery-1","reason":"OTHER"}`},
		{name: "node quota", typeID: EventTypeCaptureQuotaReached, payload: `{"capture_session_id":"capture-1","quota_scope":"node","bytes":101,"limit_bytes":100}`},
		{name: "quota not exceeded", typeID: EventTypeCaptureQuotaReached, payload: `{"capture_session_id":"capture-1","quota_scope":"agent","bytes":100,"limit_bytes":100}`},
		{name: "pipe gap", typeID: EventTypeCaptureGapDetected, payload: `{"capture_session_id":"capture-1","cause":"pipe_closed"}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := ValidateDurableEventPayload(tc.typeID, Payload(tc.payload))
			require.ErrorIs(t, err, ErrInvalid)
		})
	}
}
