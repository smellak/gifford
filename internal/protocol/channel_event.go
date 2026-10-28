package protocol

import "time"

// This file holds the v1.1 channel event-type constants (§6.7) and the
// ChannelEventPayload: one optional-everything struct whose ValidateFor
// enforces the per-event table, called from EventEnvelope.Validate.

// V1.1 channel event-type values.
const (
	EventTypeChannelGlobalChanged   EventType = "channel.global_changed"
	EventTypeChannelNodeChanged     EventType = "channel.node_changed"
	EventTypeChannelAgentChanged    EventType = "channel.agent_changed"
	EventTypeChannelLinkChanged     EventType = "channel.link_changed"
	EventTypeChannelThreadChanged   EventType = "channel.thread_changed"
	EventTypeChannelEffectiveChanged EventType = "channel.effective_changed"

	EventTypeLoopCreated             EventType = "loop.created"
	EventTypeLoopModeChanged         EventType = "loop.mode_changed"
	EventTypeLoopStepGranted         EventType = "loop.step_granted"
	EventTypeLoopBudgetExtended      EventType = "loop.budget_extended"
	EventTypeLoopBudgetExhausted     EventType = "loop.budget_exhausted"
	EventTypeLoopPatternDetected     EventType = "loop.pattern_detected"
	EventTypeLoopPaused              EventType = "loop.paused"
	EventTypeLoopResumed             EventType = "loop.resumed"
	EventTypeLoopStopped             EventType = "loop.stopped"
	EventTypeLoopCompleted           EventType = "loop.completed"
	EventTypeLoopGenerationConflict  EventType = "loop.generation_conflict"

	EventTypeMessageControlAccepted         EventType = "message.control_accepted"
	EventTypeMessageControlHeld             EventType = "message.control_held"
	EventTypeMessageControlApprovalRequired EventType = "message.control_approval_required"
	EventTypeMessageControlReleased         EventType = "message.control_released"
	EventTypeMessageControlDenied           EventType = "message.control_denied"

	EventTypeArtifactControlHeld   EventType = "artifact.control_held"
	EventTypeArtifactControlDenied EventType = "artifact.control_denied"

	EventTypeApprovalExpired EventType = "approval.expired"

	EventTypeControlRevalidationFailed    EventType = "control.revalidation_failed"
	EventTypeControlRemoteReceiptReceived EventType = "control.remote_receipt_received"
	EventTypeControlTTLExpired            EventType = "control.ttl_expired"
)

// channelEventTypes lists the v1.1 names with a ChannelEventPayload.
func channelEventTypes() []EventType {
	return []EventType{
		EventTypeChannelGlobalChanged,
		EventTypeChannelNodeChanged,
		EventTypeChannelAgentChanged,
		EventTypeChannelLinkChanged,
		EventTypeChannelThreadChanged,
		EventTypeChannelEffectiveChanged,
		EventTypeLoopCreated,
		EventTypeLoopModeChanged,
		EventTypeLoopStepGranted,
		EventTypeLoopBudgetExtended,
		EventTypeLoopBudgetExhausted,
		EventTypeLoopPatternDetected,
		EventTypeLoopPaused,
		EventTypeLoopResumed,
		EventTypeLoopStopped,
		EventTypeLoopCompleted,
		EventTypeLoopGenerationConflict,
		EventTypeMessageControlAccepted,
		EventTypeMessageControlHeld,
		EventTypeMessageControlApprovalRequired,
		EventTypeMessageControlReleased,
		EventTypeMessageControlDenied,
		EventTypeArtifactControlHeld,
		EventTypeArtifactControlDenied,
		EventTypeApprovalExpired,
		EventTypeControlRevalidationFailed,
		EventTypeControlRemoteReceiptReceived,
		EventTypeControlTTLExpired,
	}
}

// registerChannelPayloads adds the channel event payloads to the registry.
func registerChannelPayloads(regs *[]payloadRegistration) {
	*regs = append(*regs,
		payloadRegistration{New: func() any { return &ChannelEventPayload{} }, Types: channelEventTypes()},
	)
}

// isChannelPayloadEvent reports whether the envelope carries a channel
// payload: every v1.1 name above, plus approval.requested and
// approval.resolved when they have no stream. With stream native_event
// those two are the harness's own approval prompt and stay opaque.
func isChannelPayloadEvent(t EventType, stream Stream) bool {
	for _, registered := range channelEventTypes() {
		if t == registered {
			return true
		}
	}
	if t == EventTypeApprovalRequested || t == EventTypeApprovalResolved {
		return stream == ""
	}
	return false
}

// ChannelEventPayload is the payload of every channel event. Every field
// is optional in the schema; ValidateFor enforces the per-event table.
type ChannelEventPayload struct {
	DecisionID          ChannelDecisionID   `json:"decision_id,omitempty"`
	ControlID           ControlID           `json:"control_id,omitempty"`
	ScopeType           ScopeType           `json:"scope_type,omitempty"`
	ScopeKey            string              `json:"scope_key,omitempty" jsonschema:"maxLength=200"`
	Plane               Plane               `json:"plane,omitempty"`
	OldValue            Value               `json:"old_value,omitempty"`
	NewValue            Value               `json:"new_value,omitempty"`
	Version             *int64              `json:"version,omitempty" jsonschema:"minimum=0"`
	ActorType           ActorType           `json:"actor_type,omitempty"`
	ActorID             string              `json:"actor_id,omitempty" jsonschema:"maxLength=128"`
	Reason              string              `json:"reason,omitempty" jsonschema:"maxLength=280"`
	ExpiresAt           time.Time           `json:"expires_at,omitzero"`
	MatchedControlIDs   []ControlID         `json:"matched_control_ids,omitempty" jsonschema:"maxItems=64"`
	EffectiveOutcome    DecisionOutcome     `json:"effective_outcome,omitempty"`
	Stage               DecisionStage       `json:"stage,omitempty"`
	FromAgent           AgentID             `json:"from_agent,omitempty"`
	ToAgent             AgentID             `json:"to_agent,omitempty"`
	ThreadID            ThreadID            `json:"thread_id,omitempty"`
	LoopID              LoopID              `json:"loop_id,omitempty"`
	LoopGeneration      *int64              `json:"loop_generation,omitempty" jsonschema:"minimum=1"`
	LoopOrigin          LoopOrigin          `json:"loop_origin,omitempty"`
	LoopMode            LoopMode            `json:"loop_mode,omitempty"`
	StateReason         LoopStateReason     `json:"state_reason,omitempty"`
	Steps               *int64              `json:"steps,omitempty" jsonschema:"minimum=1"`
	BudgetDimension     BudgetDimension     `json:"budget_dimension,omitempty"`
	PatternSignal       PatternSignal       `json:"pattern_signal,omitempty"`
	ExpectedGeneration  *int64              `json:"expected_generation,omitempty" jsonschema:"minimum=1"`
	ReceivedGeneration  *int64              `json:"received_generation,omitempty" jsonschema:"minimum=1"`
	ArtifactID          OpaqueID            `json:"artifact_id,omitempty"`
	ControlState        ControlState        `json:"control_state,omitempty"`
	ControlReason       DecisionReason      `json:"control_reason,omitempty"`
	ApprovalID          ApprovalID          `json:"approval_id,omitempty"`
	ApprovalSource      ApprovalSource      `json:"approval_source,omitempty"`
	ObjectType          ApprovalObjectType  `json:"object_type,omitempty"`
	ObjectID            string              `json:"object_id,omitempty" jsonschema:"maxLength=300"`
	Options             []ApprovalOption    `json:"options,omitempty" jsonschema:"minItems=1,maxItems=8"`
	Option              ApprovalOption      `json:"option,omitempty"`
	ApprovalState       ApprovalState       `json:"approval_state,omitempty"`
	DeliveryID          OpaqueID            `json:"delivery_id,omitempty"`
	Receipt             ReceiptType         `json:"receipt,omitempty"`
	PeerNode            NodeID              `json:"peer_node,omitempty"`
	Restored            *bool               `json:"restored,omitempty"`
}

// Validate checks the schema-level shape; per-event rules need ValidateFor.
func (p ChannelEventPayload) Validate() error {
	return checkRoot(&p)
}

// ValidateFor enforces the required fields and fixed values of one event
// type. Fields not listed may still be present when they apply.
func (p ChannelEventPayload) ValidateFor(t EventType) error {
	if err := p.Validate(); err != nil {
		return err
	}
	switch t {
	case EventTypeChannelGlobalChanged, EventTypeChannelNodeChanged,
		EventTypeChannelAgentChanged, EventTypeChannelThreadChanged:
		if err := checkScopeChange(p, t); err != nil {
			return err
		}
		if p.Plane != PlaneAll {
			return &ValidationError{Path: "plane", Reason: "all for scope changes"}
		}
		return nil
	case EventTypeChannelLinkChanged:
		if err := checkScopeChange(p, t); err != nil {
			return err
		}
		if p.ScopeType != ScopeTypeLink {
			return &ValidationError{Path: "scope_type", Reason: "link for channel.link_changed"}
		}
		if p.Plane != PlaneMessage && p.Plane != PlaneArtifact {
			return &ValidationError{Path: "plane", Reason: "message or artifact for channel.link_changed"}
		}
		if p.FromAgent == "" || p.ToAgent == "" {
			return &ValidationError{Path: "from_agent", Reason: "link changes name both endpoints"}
		}
		return nil
	case EventTypeChannelEffectiveChanged:
		for _, field := range []struct {
			name    string
			present bool
		}{
			{"scope_type", p.ScopeType != ""},
			{"scope_key", p.ScopeKey != ""},
			{"plane", p.Plane != ""},
			{"version", p.Version != nil},
		} {
			if !field.present {
				return &ValidationError{Path: field.name, Reason: "required"}
			}
		}
		return nil
	case EventTypeLoopCreated:
		for _, field := range requiredLoopFields(p) {
			if !field.present {
				return &ValidationError{Path: field.name, Reason: "required"}
			}
		}
		if p.LoopMode != LoopModeAuto && p.LoopMode != LoopModeStep {
			return &ValidationError{Path: "loop_mode", Reason: "AUTO or STEP for loop.created"}
		}
		return nil
	case EventTypeLoopModeChanged, EventTypeLoopBudgetExtended:
		return checkLoopValues(p)
	case EventTypeLoopStepGranted:
		if p.LoopID == "" || p.Steps == nil || p.Version == nil || p.ActorType == "" {
			return &ValidationError{Path: "loop_id", Reason: "loop_id, steps, version, actor_type required"}
		}
		return nil
	case EventTypeLoopBudgetExhausted:
		if p.LoopID == "" || p.BudgetDimension == "" || p.Version == nil {
			return &ValidationError{Path: "loop_id", Reason: "loop_id, budget_dimension, version required"}
		}
		return nil
	case EventTypeLoopPatternDetected:
		if p.LoopID == "" || p.PatternSignal == "" || p.Version == nil {
			return &ValidationError{Path: "loop_id", Reason: "loop_id, pattern_signal, version required"}
		}
		return nil
	case EventTypeLoopPaused:
		if p.LoopID == "" || p.StateReason == "" || p.Version == nil || p.ActorType == "" {
			return &ValidationError{Path: "loop_id", Reason: "loop_id, state_reason, version, actor_type required"}
		}
		return nil
	case EventTypeLoopResumed, EventTypeLoopStopped, EventTypeLoopCompleted:
		if p.LoopID == "" || p.Version == nil || p.ActorType == "" {
			return &ValidationError{Path: "loop_id", Reason: "loop_id, version, actor_type required"}
		}
		return nil
	case EventTypeLoopGenerationConflict:
		if p.LoopID == "" || p.ExpectedGeneration == nil || p.ReceivedGeneration == nil {
			return &ValidationError{Path: "loop_id", Reason: "loop_id and both generations required"}
		}
		if *p.ExpectedGeneration == *p.ReceivedGeneration {
			return &ValidationError{Path: "received_generation", Reason: "generations differ"}
		}
		return nil
	case EventTypeMessageControlAccepted, EventTypeMessageControlHeld, EventTypeMessageControlDenied:
		return checkObjectDecision(p, t, map[EventType]ControlState{
			EventTypeMessageControlAccepted: ControlStateAllowed,
			EventTypeMessageControlHeld:     ControlStateHeld,
			EventTypeMessageControlDenied:   ControlStateDenied,
		}[t], false)
	case EventTypeMessageControlApprovalRequired:
		return checkObjectDecision(p, t, ControlStateApprovalRequired, true)
	case EventTypeMessageControlReleased:
		if p.DecisionID == "" || p.ControlState == "" || p.ControlReason == "" {
			return &ValidationError{Path: "decision_id", Reason: "decision_id, control_state, control_reason required"}
		}
		if p.ControlState != ControlStateAllowed {
			return &ValidationError{Path: "control_state", Reason: "ALLOWED for message.control_released"}
		}
		return nil
	case EventTypeArtifactControlHeld, EventTypeArtifactControlDenied:
		want := ControlStateHeld
		if t == EventTypeArtifactControlDenied {
			want = ControlStateDenied
		}
		if p.DecisionID == "" || p.ArtifactID == "" || p.ControlState == "" || p.ControlReason == "" {
			return &ValidationError{Path: "decision_id", Reason: "decision_id, artifact_id, control_state, control_reason required"}
		}
		if p.ControlState != want {
			return &ValidationError{Path: "control_state", Reason: "the event's state"}
		}
		return nil
	case EventTypeApprovalRequested:
		if p.ApprovalID == "" || p.ApprovalSource == "" || p.ObjectType == "" ||
			p.ObjectID == "" || len(p.Options) == 0 {
			return &ValidationError{Path: "approval_id", Reason: "approval_id, approval_source, object_type, object_id, options required"}
		}
		return nil
	case EventTypeApprovalResolved:
		if p.ApprovalID == "" || p.Option == "" || p.ApprovalState == "" || p.ActorType == "" {
			return &ValidationError{Path: "approval_id", Reason: "approval_id, option, approval_state, actor_type required"}
		}
		if p.ApprovalState != ApprovalStateApproved && p.ApprovalState != ApprovalStateRejected {
			return &ValidationError{Path: "approval_state", Reason: "APPROVED or REJECTED for approval.resolved"}
		}
		return nil
	case EventTypeApprovalExpired:
		if p.ApprovalID == "" {
			return &ValidationError{Path: "approval_id", Reason: "required"}
		}
		return nil
	case EventTypeControlRevalidationFailed:
		if p.DecisionID == "" || p.ControlReason == "" {
			return &ValidationError{Path: "decision_id", Reason: "decision_id, control_reason required"}
		}
		if p.ControlReason != DecisionReasonControlChanged {
			return &ValidationError{Path: "control_reason", Reason: "CONTROL_CHANGED for control.revalidation_failed"}
		}
		return nil
	case EventTypeControlRemoteReceiptReceived:
		if p.Receipt == "" || p.PeerNode == "" {
			return &ValidationError{Path: "receipt", Reason: "receipt, peer_node required"}
		}
		return nil
	case EventTypeControlTTLExpired:
		for _, field := range []struct {
			name    string
			present bool
		}{
			{"control_id", p.ControlID != ""},
			{"scope_type", p.ScopeType != ""},
			{"scope_key", p.ScopeKey != ""},
			{"plane", p.Plane != ""},
			{"version", p.Version != nil},
			{"restored", p.Restored != nil},
			{"actor_type", p.ActorType != ""},
		} {
			if !field.present {
				return &ValidationError{Path: field.name, Reason: "required"}
			}
		}
		if p.ActorType != ActorTypeSystem {
			return &ValidationError{Path: "actor_type", Reason: "system for control.ttl_expired"}
		}
		if p.Reason != "TTL_EXPIRED" {
			return &ValidationError{Path: "reason", Reason: "TTL_EXPIRED for control.ttl_expired"}
		}
		return nil
	default:
		return &ValidationError{Path: "event_type", Reason: "not a channel event"}
	}
}

// requiredLoopFields lists the loop.created requirements.
func requiredLoopFields(p ChannelEventPayload) []struct {
	name    string
	present bool
} {
	return []struct {
		name    string
		present bool
	}{
		{"loop_id", p.LoopID != ""},
		{"loop_generation", p.LoopGeneration != nil},
		{"loop_origin", p.LoopOrigin != ""},
		{"loop_mode", p.LoopMode != ""},
		{"thread_id", p.ThreadID != ""},
		{"version", p.Version != nil},
		{"actor_type", p.ActorType != ""},
	}
}

// checkScopeChange enforces the shared scope-change requirements: the
// control row, its scope, the all plane and the new value.
func checkScopeChange(p ChannelEventPayload, t EventType) error {
	for _, field := range []struct {
		name    string
		present bool
	}{
		{"control_id", p.ControlID != ""},
		{"scope_type", p.ScopeType != ""},
		{"scope_key", p.ScopeKey != ""},
		{"plane", p.Plane != ""},
		{"new_value", len(p.NewValue) != 0},
		{"version", p.Version != nil},
		{"actor_type", p.ActorType != ""},
	} {
		if !field.present {
			return &ValidationError{Path: field.name, Reason: "required"}
		}
	}
	want := map[EventType]ScopeType{
		EventTypeChannelGlobalChanged: ScopeTypeGlobal,
		EventTypeChannelNodeChanged:   ScopeTypeNode,
		EventTypeChannelAgentChanged:  ScopeTypeAgent,
		EventTypeChannelThreadChanged: ScopeTypeThread,
		EventTypeChannelLinkChanged:   ScopeTypeLink,
	}[t]
	if p.ScopeType != want {
		return &ValidationError{Path: "scope_type", Reason: "the event's scope"}
	}
	if t == EventTypeChannelGlobalChanged && p.ScopeKey != "*" {
		return &ValidationError{Path: "scope_key", Reason: "* for channel.global_changed"}
	}
	return nil
}

// checkLoopValues enforces the loop value-change requirements.
func checkLoopValues(p ChannelEventPayload) error {
	if p.LoopID == "" || len(p.OldValue) == 0 || len(p.NewValue) == 0 ||
		p.Version == nil || p.ActorType == "" {
		return &ValidationError{Path: "loop_id", Reason: "loop_id, old_value, new_value, version, actor_type required"}
	}
	return nil
}

// checkObjectDecision enforces the message decision requirements: the
// decision, its state, its reason and the gate stage, plus the approval
// for APPROVAL_REQUIRED.
func checkObjectDecision(p ChannelEventPayload, t EventType, want ControlState, approval bool) error {
	if p.DecisionID == "" || p.ControlState == "" || p.ControlReason == "" || p.Stage == "" {
		return &ValidationError{Path: "decision_id", Reason: "decision_id, control_state, control_reason, stage required"}
	}
	if p.ControlState != want {
		return &ValidationError{Path: "control_state", Reason: "the event's state"}
	}
	if approval && p.ApprovalID == "" {
		return &ValidationError{Path: "approval_id", Reason: "required for approval_required"}
	}
	return nil
}
