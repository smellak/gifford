package protocol

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func channelPayload() ChannelEventPayload {
	gen := int64(1)
	return ChannelEventPayload{
		DecisionID:     testDecisionID,
		ControlID:      testControlID,
		ScopeType:      ScopeTypeGlobal,
		ScopeKey:       "*",
		Plane:          PlaneAll,
		NewValue:       Value(`"ACTIVE"`),
		Version:        &gen,
		ActorType:      ActorTypeSystem,
		LoopID:         testLoop,
		LoopGeneration: &gen,
		LoopOrigin:     LoopOriginHuman,
		LoopMode:       LoopModeAuto,
		ThreadID:       testThread,
	}
}

// TestChannelEventRules covers the per-event table of §6.7.
func TestChannelEventRules(t *testing.T) {
	t.Parallel()

	gen := int64(1)
	steps := int64(3)
	restored := true
	cases := []struct {
		name  string
		event EventType
		valid bool
		patch func(*ChannelEventPayload)
	}{
		{name: "global changed passes", event: EventTypeChannelGlobalChanged, valid: true, patch: func(*ChannelEventPayload) {}},
		{name: "scope matches the name", event: EventTypeChannelNodeChanged, patch: func(*ChannelEventPayload) {}},
		{name: "global key is star", event: EventTypeChannelGlobalChanged, patch: func(p *ChannelEventPayload) {
			p.ScopeKey = testNode
		}},
		{name: "link changed passes", event: EventTypeChannelLinkChanged, valid: true, patch: func(p *ChannelEventPayload) {
			p.ScopeType = ScopeTypeLink
			p.ScopeKey = testAgentID + linkArrow + testAgent2
			p.Plane = PlaneMessage
			p.FromAgent = testAgentID
			p.ToAgent = testAgent2
		}},
		{name: "link changed needs endpoints", event: EventTypeChannelLinkChanged, patch: func(p *ChannelEventPayload) {
			p.ScopeType = ScopeTypeLink
			p.Plane = PlaneMessage
		}},
		{name: "effective changed passes", event: EventTypeChannelEffectiveChanged, valid: true, patch: func(p *ChannelEventPayload) {
			p.Plane = PlaneMessage
			p.ScopeKey = testNode
			p.ScopeType = ScopeTypeNode
		}},
		{name: "loop created passes", event: EventTypeLoopCreated, valid: true, patch: func(*ChannelEventPayload) {}},
		{name: "loop created starts auto or step", event: EventTypeLoopCreated, patch: func(p *ChannelEventPayload) {
			p.LoopMode = LoopModePaused
		}},
		{name: "mode changed passes", event: EventTypeLoopModeChanged, valid: true, patch: func(p *ChannelEventPayload) {
			p.OldValue = Value(`"AUTO"`)
		}},
		{name: "mode changed needs both values", event: EventTypeLoopModeChanged, patch: func(*ChannelEventPayload) {}},
		{name: "step granted passes", event: EventTypeLoopStepGranted, valid: true, patch: func(p *ChannelEventPayload) {
			p.Steps = &steps
		}},
		{name: "budget exhausted passes", event: EventTypeLoopBudgetExhausted, valid: true, patch: func(p *ChannelEventPayload) {
			p.BudgetDimension = BudgetDimensionMaxMessages
		}},
		{name: "pattern detected passes", event: EventTypeLoopPatternDetected, valid: true, patch: func(p *ChannelEventPayload) {
			p.PatternSignal = PatternSignalRate
		}},
		{name: "paused passes", event: EventTypeLoopPaused, valid: true, patch: func(p *ChannelEventPayload) {
			p.StateReason = LoopStateReasonBudgetExhausted
		}},
		{name: "resumed passes", event: EventTypeLoopResumed, valid: true, patch: func(*ChannelEventPayload) {}},
		{name: "generations differ", event: EventTypeLoopGenerationConflict, valid: true, patch: func(p *ChannelEventPayload) {
			two := int64(2)
			p.ExpectedGeneration = &gen
			p.ReceivedGeneration = &two
		}},
		{name: "generations equal conflict nothing", event: EventTypeLoopGenerationConflict, patch: func(p *ChannelEventPayload) {
			p.ExpectedGeneration = &gen
			p.ReceivedGeneration = &gen
		}},
		{name: "message held passes", event: EventTypeMessageControlHeld, valid: true, patch: func(p *ChannelEventPayload) {
			p.ControlState = ControlStateHeld
			p.ControlReason = DecisionReasonLinkHold
			p.Stage = DecisionStageAdmission
		}},
		{name: "message state matches event", event: EventTypeMessageControlHeld, patch: func(p *ChannelEventPayload) {
			p.ControlState = ControlStateDenied
			p.ControlReason = DecisionReasonLinkDenied
			p.Stage = DecisionStageAdmission
		}},
		{name: "approval required names approval", event: EventTypeMessageControlApprovalRequired, valid: true, patch: func(p *ChannelEventPayload) {
			p.ControlState = ControlStateApprovalRequired
			p.ControlReason = DecisionReasonLinkApproval
			p.Stage = DecisionStageAdmission
			p.ApprovalID = testApprovalID
		}},
		{name: "released passes", event: EventTypeMessageControlReleased, valid: true, patch: func(p *ChannelEventPayload) {
			p.ControlState = ControlStateAllowed
			p.ControlReason = DecisionReasonHoldReleased
		}},
		{name: "artifact held passes", event: EventTypeArtifactControlHeld, valid: true, patch: func(p *ChannelEventPayload) {
			p.ArtifactID = testArtifactID
			p.ControlState = ControlStateHeld
			p.ControlReason = DecisionReasonLinkHold
		}},
		{name: "approval requested passes", event: EventTypeApprovalRequested, valid: true, patch: func(p *ChannelEventPayload) {
			p.ApprovalID = testApprovalID
			p.ApprovalSource = ApprovalSourceLink
			p.ObjectType = ApprovalObjectTypeMessage
			p.ObjectID = testMsgID
			p.Options = []ApprovalOption{kindApproveOnce, kindReject}
		}},
		{name: "approval resolved passes", event: EventTypeApprovalResolved, valid: true, patch: func(p *ChannelEventPayload) {
			p.ApprovalID = testApprovalID
			p.Option = kindReject
			p.ApprovalState = ApprovalStateRejected
		}},
		{name: "resolved is approved or rejected", event: EventTypeApprovalResolved, patch: func(p *ChannelEventPayload) {
			p.ApprovalID = testApprovalID
			p.Option = kindReject
			p.ApprovalState = ApprovalStateExpired
		}},
		{name: "expired passes", event: EventTypeApprovalExpired, valid: true, patch: func(p *ChannelEventPayload) {
			p.ApprovalID = testApprovalID
		}},
		{name: "revalidation failed passes", event: EventTypeControlRevalidationFailed, valid: true, patch: func(p *ChannelEventPayload) {
			p.ControlReason = DecisionReasonControlChanged
		}},
		{name: "revalidation means changed", event: EventTypeControlRevalidationFailed, patch: func(p *ChannelEventPayload) {
			p.ControlReason = DecisionReasonLinkHold
		}},
		{name: "receipt passes", event: EventTypeControlRemoteReceiptReceived, valid: true, patch: func(p *ChannelEventPayload) {
			p.Receipt = ReceiptTypeControlHeld
			p.PeerNode = testNode
		}},
		{name: "ttl expired passes", event: EventTypeControlTTLExpired, valid: true, patch: func(p *ChannelEventPayload) {
			p.ScopeKey = testNode
			p.ScopeType = ScopeTypeNode
			p.Restored = &restored
			p.Reason = "TTL_EXPIRED"
		}},
		{name: "ttl expired is system", event: EventTypeControlTTLExpired, patch: func(p *ChannelEventPayload) {
			p.ScopeKey = testNode
			p.ScopeType = ScopeTypeNode
			p.Restored = &restored
			p.Reason = "TTL_EXPIRED"
			p.ActorType = ActorTypeHuman
		}},
		{name: "unknown event rejected", event: EventTypeCaptureChunk, patch: func(*ChannelEventPayload) {}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			payload := channelPayload()
			tc.patch(&payload)
			if tc.valid {
				require.NoError(t, payload.ValidateFor(tc.event))
				return
			}
			require.ErrorIs(t, payload.ValidateFor(tc.event), ErrInvalid)
		})
	}
}

// TestChannelEnvelopeRules covers the envelope hook: channel payloads are
// validated per event, and the harness approval prompt stays opaque.
func TestChannelEnvelopeRules(t *testing.T) {
	t.Parallel()

	envelope := validEnvelope()
	envelope.Origin = OriginSystem
	envelope.SourceQuality = SourceQualityInternal
	envelope.Stream = ""
	envelope.EventType = EventTypeLoopResumed
	envelope.Payload = Payload(`{"loop_id":"` + testLoop + `","version":2,"actor_type":"system"}`)
	require.NoError(t, envelope.Validate())

	bad := envelope
	bad.Payload = Payload(`{"version":2}`)
	require.ErrorIs(t, bad.Validate(), ErrInvalid)

	harness := validEnvelope()
	harness.EventType = EventTypeApprovalRequested
	harness.Stream = StreamNativeEvent
	harness.SourceQuality = SourceQualityNativeStructured
	harness.Payload = Payload(`{"prompt":"anything opaque","free":1}`)
	require.NoError(t, harness.Validate())
}
