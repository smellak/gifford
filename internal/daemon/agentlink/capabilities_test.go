package agentlink

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/adapters"
	"github.com/smellak/gifford/internal/protocol"
)

type recordingCapabilityWriter struct {
	event protocol.EventEnvelope
	err   error
	calls int
}

func (w *recordingCapabilityWriter) ApplyCapabilitiesChanged(
	_ context.Context,
	event protocol.EventEnvelope,
) error {
	w.calls++
	w.event = event
	return w.err
}

func TestDispatchAppliesOnlyAuthenticatedCapabilityTransition(t *testing.T) {
	t.Parallel()
	native := testNativeCapabilityManifest()
	event := testCapabilityEnvelope(t, native)
	w := &recordingCapabilityWriter{}
	client := &Client{
		opts: Options{
			NodeID: testNodeID, AgentID: native.AgentID, SessionID: testSessionID,
			CapabilityWriter: w,
		},
		ctx: context.Background(),
	}
	body, err := json.Marshal(event)
	require.NoError(t, err)
	heartbeat, err := client.dispatch(1, native, protocol.AgentFrame{
		Op: protocol.AgentOpEmitEvent, FrameID: 2, SentAt: testNow, Body: body,
	})
	require.NoError(t, err)
	require.False(t, heartbeat)
	require.Equal(t, 1, w.calls)
	require.Equal(t, event, w.event)
}

func TestDispatchCapabilityConflictIsPermanentAndPayloadFree(t *testing.T) {
	t.Parallel()
	native := testNativeCapabilityManifest()
	w := &recordingCapabilityWriter{err: errors.New("transient database error")}
	client := &Client{
		opts: Options{
			NodeID: testNodeID, AgentID: native.AgentID, SessionID: testSessionID,
			CapabilityWriter: w,
		},
		ctx: context.Background(),
	}
	event := testCapabilityEnvelope(t, native)
	body, err := json.Marshal(event)
	require.NoError(t, err)
	_, err = client.dispatch(1, native, protocol.AgentFrame{
		Op: protocol.AgentOpEmitEvent, FrameID: 2, SentAt: testNow, Body: body,
	})
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrCapabilitiesConflict)

	w.err = errors.Join(ErrCapabilitiesConflict, errors.New("private payload marker"))
	_, err = client.dispatch(1, native, protocol.AgentFrame{
		Op: protocol.AgentOpEmitEvent, FrameID: 3, SentAt: testNow, Body: body,
	})
	require.ErrorIs(t, err, ErrCapabilitiesConflict)
	require.Equal(t, ReasonCapabilitiesConflict, reasonFor(err))
	require.Contains(t, err.Error(), "code=CAPABILITIES_CONFLICT")
	require.Contains(t, err.Error(), "agent_id="+string(testAgentID))
	require.Contains(t, err.Error(), "session_id="+string(testSessionID))
	require.NotContains(t, err.Error(), "private payload marker")

	w.calls = 0
	w.err = nil
	sensitive := []byte(`{"event_id":"private payload marker"}`)
	_, err = client.dispatch(1, native, protocol.AgentFrame{
		Op: protocol.AgentOpEmitEvent, FrameID: 4, SentAt: testNow, Body: sensitive,
	})
	require.ErrorIs(t, err, ErrCapabilitiesConflict)
	require.NotContains(t, err.Error(), "private payload marker")
	require.Zero(t, w.calls)
}

func TestDispatchRejectsCapabilityReplyAndNonCanonicalPayload(t *testing.T) {
	t.Parallel()
	native := testNativeCapabilityManifest()
	event := testCapabilityEnvelope(t, native)
	w := &recordingCapabilityWriter{}
	client := &Client{
		opts: Options{
			NodeID: testNodeID, AgentID: native.AgentID, SessionID: testSessionID,
			CapabilityWriter: w,
		},
		ctx: context.Background(),
	}
	body, err := json.Marshal(event)
	require.NoError(t, err)
	replyTo := int64(1)
	_, err = client.dispatch(1, native, protocol.AgentFrame{
		Op: protocol.AgentOpEmitEvent, FrameID: 2, ReplyTo: &replyTo, SentAt: testNow, Body: body,
	})
	require.ErrorIs(t, err, ErrCapabilitiesConflict)

	var change adapters.CapabilitiesChangedEvent
	require.NoError(t, json.Unmarshal(event.Payload, &change))
	event.Payload = protocol.Payload(`{"after":{"pty_capture":true,"mcp":true},` +
		`"event_type":"adapter.capabilities_changed",` +
		`"before":{"structured_events":true,"native_cancel":true,"pty_capture":true,"mcp":true}}`)
	body, err = json.Marshal(event)
	require.NoError(t, err)
	_, err = client.dispatch(1, native, protocol.AgentFrame{
		Op: protocol.AgentOpEmitEvent, FrameID: 3, SentAt: testNow, Body: body,
	})
	require.ErrorIs(t, err, ErrCapabilitiesConflict)
	require.Zero(t, w.calls)
}

func TestCapabilityEventCannotServeAsGreeting(t *testing.T) {
	t.Parallel()
	native := testNativeCapabilityManifest()
	event := testCapabilityEnvelope(t, native)
	body, err := json.Marshal(event)
	require.NoError(t, err)
	w := &recordingCapabilityWriter{}
	client := &Client{opts: Options{CapabilityWriter: w}}
	_, err = client.validateGreeting(protocol.AgentFrame{
		Op: protocol.AgentOpEmitEvent, FrameID: 1, SentAt: testNow, Body: body,
	}, sessionRecord{manifest: native})
	require.ErrorIs(t, err, ErrInvalidGreeting)
	require.Zero(t, w.calls)
}

func testNativeCapabilityManifest() protocol.CapabilityManifest {
	return protocol.CapabilityManifest{
		SchemaVersion:      protocol.SchemaVersion,
		AgentID:            testAgentID,
		Harness:            protocol.HarnessCodex,
		HarnessVersion:     "1.2.3",
		AdapterType:        protocol.AdapterTypeNativeAPIThinPTY,
		InputGuardProvider: protocol.InputGuardProviderPTYProxy,
		CaptureMode:        protocol.CaptureModePTYProxy,
		Deliverable:        true,
		Capabilities: protocol.Capabilities{
			StructuredEvents: true,
			NativeCancel:     true,
			PTYCapture:       true,
			MCP:              true,
		},
	}
}

func testCapabilityEnvelope(
	t *testing.T,
	native protocol.CapabilityManifest,
) protocol.EventEnvelope {
	t.Helper()
	fallback := adapters.GenericManifest(native.AgentID, native.Harness, native.HarnessVersion)
	payload, err := json.Marshal(adapters.CapabilitiesChangedEvent{
		EventType: protocol.EventTypeAdapterCapabilitiesChanged,
		Before:    native.Capabilities,
		After:     fallback.Capabilities,
	})
	require.NoError(t, err)
	return protocol.EventEnvelope{
		EventID:        "01JB0000000000000000000048",
		SchemaVersion:  protocol.SchemaVersion,
		NodeID:         testNodeID,
		SessionID:      testSessionID,
		AgentID:        native.AgentID,
		Harness:        native.Harness,
		HarnessVersion: native.HarnessVersion,
		AdapterType:    native.AdapterType,
		SourceQuality:  protocol.SourceQualityProcessMetrics,
		EventType:      protocol.EventTypeAdapterCapabilitiesChanged,
		Origin:         protocol.OriginSystem,
		WallTime:       time.Date(2026, 10, 31, 1, 45, 0, 0, time.UTC),
		Payload:        protocol.Payload(payload),
		Sensitivity:    protocol.SensitivityNormal,
	}
}
