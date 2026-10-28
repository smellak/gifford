package protocol

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Sample identifiers shared by the rule tables.
const (
	testEventID = "01JB0000000000000000000000"
	testAgentID = "agent-1"
	testCapture = "cap-1"
)

func validEnvelope() EventEnvelope {
	return EventEnvelope{
		EventID:          testEventID,
		SchemaVersion:    SchemaVersion,
		NodeID:           "node-a",
		SessionID:        "sess-1",
		AgentID:          testAgentID,
		AdapterType:      AdapterTypeNone,
		SourceQuality:    SourceQualityBootstrapPipePane,
		EventType:        EventTypeCaptureChunk,
		Direction:        DirectionOut,
		Stream:           StreamPTY,
		Origin:           OriginHarness,
		WallTime:         time.Date(2026, 10, 26, 12, 41, 2, 314000000, time.FixedZone("CET", 3600)),
		MonotonicNS:      883425102931,
		Sequence:         18382,
		Payload:          Payload(`{"capture_session_id":"cap-1","byte_count":24,"ansi_present":true,"raw_blob_ref":"blob://capture/cap-1/0001"}`),
		Sensitivity:      SensitivityNormal,
		RedactionApplied: false,
	}
}

// TestEnvelopeRules covers every cross-field rule of EventEnvelope.
func TestEnvelopeRules(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		valid bool
		patch func(*EventEnvelope)
	}{
		{name: "agent capture chunk passes", valid: true, patch: func(*EventEnvelope) {}},
		{name: "agent scoped needs session", patch: func(e *EventEnvelope) { e.SessionID = "" }},
		{name: "agent scoped needs agent", patch: func(e *EventEnvelope) { e.AgentID = "" }},
		{name: "agent scoped needs adapter", patch: func(e *EventEnvelope) { e.AdapterType = "" }},
		{name: "gifford scoped control passes", valid: true, patch: func(e *EventEnvelope) {
			e.EventType = EventTypeControlPause
			e.SessionID, e.AgentID, e.AdapterType = "", "", ""
			e.Direction, e.Stream, e.Origin = "", "", OriginSystem
			e.SourceQuality = SourceQualityInternal
			e.Payload = Payload(`{}`)
		}},
		{name: "gifford scoped needs system origin", patch: func(e *EventEnvelope) {
			e.EventType = EventTypeControlPause
			e.SessionID, e.AgentID, e.AdapterType = "", "", ""
			e.Direction, e.Stream = "", ""
			e.Origin = OriginHarness
			e.SourceQuality = SourceQualityInternal
			e.Payload = Payload(`{}`)
		}},
		{name: "internal quality is never agent scoped", patch: func(e *EventEnvelope) {
			e.SourceQuality = SourceQualityInternal
		}},
		{name: "native stream needs harness origin", patch: func(e *EventEnvelope) {
			e.EventType = EventTypeApprovalRequested
			e.Stream = StreamNativeEvent
			e.Origin = OriginSystem
			e.SourceQuality = SourceQualityNativeHook
			e.Payload = Payload(`{}`)
		}},
		{name: "native stream needs native quality", patch: func(e *EventEnvelope) {
			e.EventType = EventTypeApprovalRequested
			e.Stream = StreamNativeEvent
			e.Origin = OriginHarness
			e.SourceQuality = SourceQualityPTYScreen
			e.Payload = Payload(`{}`)
		}},
		{name: "native approval passes", valid: true, patch: func(e *EventEnvelope) {
			e.EventType = EventTypeApprovalRequested
			e.Stream = StreamNativeEvent
			e.Origin = OriginHarness
			e.SourceQuality = SourceQualityNativeHook
			e.Payload = Payload(`{}`)
		}},
		{name: "capture needs pty stream", patch: func(e *EventEnvelope) { e.Stream = StreamStdout }},
		{name: "capture chunk needs out direction", patch: func(e *EventEnvelope) { e.Direction = DirectionIn }},
		{name: "message control needs message id", patch: func(e *EventEnvelope) {
			e.EventType = "message.control_held"
			e.SessionID, e.AgentID, e.AdapterType = "", "", ""
			e.Direction, e.Stream = "", ""
			e.Origin = OriginSystem
			e.SourceQuality = SourceQualityInternal
			e.Payload = Payload(`{}`)
		}},
		{name: "bad payload fails", patch: func(e *EventEnvelope) {
			e.Payload = Payload(`{"capture_session_id":"cap-1","byte_count":0,"ansi_present":true,"raw_blob_ref":"blob://capture/cap-1/0001"}`)
		}},
		{name: "wrong version wraps unsupported", patch: func(e *EventEnvelope) { e.SchemaVersion = 2 }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := validEnvelope()
			tc.patch(&e)
			err := e.Validate()
			if tc.valid {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			if tc.name == "wrong version wraps unsupported" {
				require.ErrorIs(t, err, ErrUnsupportedVersion)
			} else {
				require.ErrorIs(t, err, ErrInvalid)
			}
		})
	}
}

// TestCaptureRules covers the capture payload cross-field rules.
func TestCaptureRules(t *testing.T) {
	t.Parallel()

	rotated := CaptureRotated{CaptureSessionID: testCapture, ClosedSegmentID: "seg-1", NewSegmentID: "seg-2", Bytes: 10, Compressed: true}
	require.NoError(t, rotated.Validate())
	rotated.NewSegmentID = "seg-1"
	require.ErrorIs(t, rotated.Validate(), ErrInvalid)
	rotated.NewSegmentID = "seg-2"
	compressed := int64(4)
	rotated.BytesCompressed = &compressed
	rotated.Compressed = false
	require.ErrorIs(t, rotated.Validate(), ErrInvalid)

	recreated := CapturePaneRecreated{CaptureSessionID: testCapture, OldPane: "%1", NewPane: "%2"}
	require.NoError(t, recreated.Validate())
	recreated.NewPane = "%1"
	require.ErrorIs(t, recreated.Validate(), ErrInvalid)

	reattached := CaptureReattached{CaptureSessionID: testCapture, GapFromSequence: 1, GapToSequence: 2, GapMS: 3}
	require.NoError(t, reattached.Validate())
	reattached.GapToSequence = 0
	require.ErrorIs(t, reattached.Validate(), ErrInvalid)
}

// TestManifestRules covers the manifest cross-field rules.
func TestManifestRules(t *testing.T) {
	t.Parallel()

	valid := CapabilityManifest{
		SchemaVersion: SchemaVersion, AgentID: testAgentID, Harness: HarnessCodex,
		HarnessVersion: "1.2.3", AdapterType: AdapterTypeGenericPTY,
		InputGuardProvider: InputGuardProviderPTYProxy, CaptureMode: CaptureModePTYProxy,
		Deliverable:  true,
		Capabilities: Capabilities{PTYCapture: true},
	}
	require.NoError(t, valid.Validate())

	bootstrap := valid
	bootstrap.AdapterType = AdapterTypeNone
	bootstrap.InputGuardProvider = InputGuardProviderNone
	bootstrap.CaptureMode = CaptureModeBootstrapPipePane
	bootstrap.Deliverable = false
	require.NoError(t, bootstrap.Validate())

	bootstrap.Deliverable = true
	require.ErrorIs(t, bootstrap.Validate(), ErrInvalid)
	bootstrap.Deliverable = false

	bootstrap.Capabilities = Capabilities{}
	require.ErrorIs(t, bootstrap.Validate(), ErrInvalid)
	bootstrap.Capabilities = Capabilities{PTYCapture: true}

	native := valid
	native.Capabilities = Capabilities{PTYCapture: true, StructuredEvents: true}
	require.ErrorIs(t, native.Validate(), ErrInvalid)
	native.AdapterType = AdapterTypeNativeAPI
	require.NoError(t, native.Validate())

	guardless := valid
	guardless.InputGuardProvider = InputGuardProviderNone
	require.ErrorIs(t, guardless.Validate(), ErrInvalid)
}
