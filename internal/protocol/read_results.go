package protocol

import "time"

// AgentKeyboardState is the latest input-guard report metadata.
type AgentKeyboardState struct {
	DraftState        DraftState    `json:"draft_state"`
	ActivityState     ActivityState `json:"activity_state"`
	InputGuardVersion int64         `json:"input_guard_version" jsonschema:"minimum=0"`
	ObservedAt        time.Time     `json:"observed_at"`
}

// Validate checks the keyboard metadata.
func (s AgentKeyboardState) Validate() error { return checkRoot(&s) }

// AgentPhaseStatus keeps an analyst phase separate from keyboard state.
type AgentPhaseStatus struct {
	Value  AgentPhase       `json:"value"`
	Source AgentPhaseSource `json:"source"`
}

// Validate checks the phase and prevents a phase without analyst evidence.
func (s AgentPhaseStatus) Validate() error {
	if err := checkRoot(&s); err != nil {
		return err
	}
	if s.Source == AgentPhaseSourceNone && s.Value != AgentPhaseUnknown {
		return &ValidationError{Path: "value", Reason: "UNKNOWN when source is none"}
	}
	return nil
}

// AgentEffectiveChannel is the effective per-agent channel view.
type AgentEffectiveChannel struct {
	TX           bool      `json:"tx"`
	RX           bool      `json:"rx"`
	AutoDelivery bool      `json:"auto_delivery"`
	AutoContinue bool      `json:"auto_continue"`
	Mode         AgentMode `json:"mode,omitempty"`
}

// Validate checks the optional mode against the four effective flags.
func (c AgentEffectiveChannel) Validate() error {
	if err := checkRoot(&c); err != nil {
		return err
	}
	if c.Mode == "" {
		return nil
	}
	mode, ok := agentMacro(c.TX, c.RX, c.AutoDelivery, c.AutoContinue)
	if !ok || mode != c.Mode {
		return &ValidationError{Path: pathMode, Reason: "the macro matching the flags"}
	}
	return nil
}

// AgentQueueCounts are recipient queue counts. Delivering includes rows
// currently claimed or delivering; uncertain is sourced from attempts.
type AgentQueueCounts struct {
	Queued     int64 `json:"queued" jsonschema:"minimum=0"`
	Delivering int64 `json:"delivering" jsonschema:"minimum=0"`
	Uncertain  int64 `json:"uncertain" jsonschema:"minimum=0"`
}

// Validate checks the queue counts.
func (c AgentQueueCounts) Validate() error { return checkRoot(&c) }

// AgentListItem is one read-only panel agent row.
type AgentListItem struct {
	AgentID        AgentID               `json:"agent_id"`
	NodeID         NodeID                `json:"node_id"`
	Harness        Harness               `json:"harness"`
	HarnessVersion string                `json:"harness_version" jsonschema:"minLength=1,maxLength=64"`
	Mode           AgentRunMode          `json:"mode"`
	SessionState   SessionState          `json:"session_state"`
	Deliverable    bool                  `json:"deliverable"`
	Keyboard       *AgentKeyboardState   `json:"keyboard" jsonschema:"nullable"`
	Phase          AgentPhaseStatus      `json:"phase"`
	Channel        AgentEffectiveChannel `json:"channel"`
	Queue          AgentQueueCounts      `json:"queue"`
}

// Validate checks one agent row. Keyboard and phase intentionally have no
// cross-rule: a CLOSED draft never implies an IDLE analyst phase.
func (a AgentListItem) Validate() error { return checkRoot(&a) }

// AgentListResult is a stable, bounded page of agents.
type AgentListResult struct {
	Agents     []AgentListItem `json:"agents" jsonschema:"maxItems=200"`
	NextCursor PageCursor      `json:"next_cursor,omitempty"`
}

// Validate checks the agent page.
func (r AgentListResult) Validate() error { return checkRoot(&r) }

// MessageListItem is recipient-specific delivery metadata. It deliberately
// carries no subject, body, ciphertext or envelope.
type MessageListItem struct {
	MessageID  ULID               `json:"message_id"`
	FromAgent  AgentID            `json:"from_agent"`
	ToAgent    AgentID            `json:"to_agent"`
	ThreadRef  ThreadRef          `json:"thread_ref"`
	State      DeliveryQueueState `json:"state"`
	Priority   Priority           `json:"priority"`
	CreatedAt  time.Time          `json:"created_at"`
	ExpiresAt  time.Time          `json:"expires_at"`
	Attempts   int64              `json:"attempts" jsonschema:"minimum=0"`
	LastReason ReasonCode         `json:"last_reason,omitempty"`
}

// Validate checks one delivery row.
func (m MessageListItem) Validate() error {
	if err := checkRoot(&m); err != nil {
		return err
	}
	if !m.ExpiresAt.After(m.CreatedAt) {
		return &ValidationError{Path: "expires_at", Reason: "after created_at"}
	}
	return nil
}

// MessageListResult is a stable, bounded page of delivery metadata.
type MessageListResult struct {
	Messages   []MessageListItem `json:"messages" jsonschema:"maxItems=200"`
	NextCursor PageCursor        `json:"next_cursor,omitempty"`
}

// Validate checks the message page.
func (r MessageListResult) Validate() error { return checkRoot(&r) }

// DecisionListItem is the common metadata of either decision family.
type DecisionListItem struct {
	DecisionID     DecisionID         `json:"decision_id"`
	Type           DecisionType       `json:"type"`
	Stage          DecisionStage      `json:"stage,omitempty"`
	Outcome        DecisionOutcome    `json:"outcome,omitempty"`
	SelectedAction OrchestratorAction `json:"selected_action,omitempty"`
	AgentID        AgentID            `json:"agent_id"`
	MessageID      ULID               `json:"message_id"`
	CreatedAt      time.Time          `json:"created_at"`
	ReasonCodes    []ReasonCode       `json:"reason_codes" jsonschema:"minItems=1,maxItems=32,uniqueItems=true"`
}

// Validate checks the family-specific summary fields.
func (d DecisionListItem) Validate() error {
	if err := checkRoot(&d); err != nil {
		return err
	}
	return validateDecisionSummary(d.Type, d.Stage, d.Outcome, d.SelectedAction)
}

// DecisionListResult is a stable, bounded page of decision metadata.
type DecisionListResult struct {
	Decisions  []DecisionListItem `json:"decisions" jsonschema:"maxItems=200"`
	NextCursor PageCursor         `json:"next_cursor,omitempty"`
}

// Validate checks the decision page.
func (r DecisionListResult) Validate() error { return checkRoot(&r) }

// ChannelDecisionEvidence is the bounded evidence exposed for a Channel
// decision. It contains no rendered explanation or database dump.
type ChannelDecisionEvidence struct {
	ControlVersions ControlVersions  `json:"control_versions"`
	DecidingScope   ExplanationScope `json:"deciding_scope"`
}

// Validate checks Channel decision evidence.
func (e ChannelDecisionEvidence) Validate() error { return checkRoot(&e) }

// DecisionGuardEvidence is one structured orchestrator guard result.
type DecisionGuardEvidence struct {
	Guard   GuardName    `json:"guard"`
	Outcome GuardOutcome `json:"outcome"`
}

// Validate checks the guard evidence.
func (e DecisionGuardEvidence) Validate() error { return checkRoot(&e) }

// OrchestratorDecisionEvidence is the structured policy and guard evidence
// for an Orchestrator decision.
type OrchestratorDecisionEvidence struct {
	AllowedActions    []OrchestratorAction    `json:"allowed_actions" jsonschema:"minItems=1,maxItems=12,uniqueItems=true"`
	Guards            []DecisionGuardEvidence `json:"guards" jsonschema:"maxItems=32"`
	SelectedAction    OrchestratorAction      `json:"selected_action"`
	PolicyVersion     ModelMetadataValue      `json:"policy_version"`
	DecisionModel     ModelMetadataValue      `json:"decision_model,omitempty"`
	GlimmerCallID     OpaqueID                `json:"glimmer_call_id,omitempty"`
	ExecutionStatus   ExecutionStatus         `json:"execution_status"`
	ChannelDecisionID ChannelDecisionID       `json:"channel_decision_id"`
}

// Validate checks Orchestrator evidence and its selected action.
func (e OrchestratorDecisionEvidence) Validate() error {
	if err := checkRoot(&e); err != nil {
		return err
	}
	for _, action := range e.AllowedActions {
		if action == e.SelectedAction {
			return nil
		}
	}
	return &ValidationError{Path: pathSelectedAction, Reason: "must be in allowed_actions"}
}

// DecisionGetResult is one decision summary plus exactly one evidence family.
type DecisionGetResult struct {
	DecisionID           DecisionID                    `json:"decision_id"`
	Type                 DecisionType                  `json:"type"`
	Stage                DecisionStage                 `json:"stage,omitempty"`
	Outcome              DecisionOutcome               `json:"outcome,omitempty"`
	SelectedAction       OrchestratorAction            `json:"selected_action,omitempty"`
	AgentID              AgentID                       `json:"agent_id"`
	MessageID            ULID                          `json:"message_id"`
	CreatedAt            time.Time                     `json:"created_at"`
	ReasonCodes          []ReasonCode                  `json:"reason_codes" jsonschema:"minItems=1,maxItems=32,uniqueItems=true"`
	ChannelEvidence      *ChannelDecisionEvidence      `json:"channel_evidence,omitempty"`
	OrchestratorEvidence *OrchestratorDecisionEvidence `json:"orchestrator_evidence,omitempty"`
}

// Validate checks the summary and requires the matching evidence family.
func (d DecisionGetResult) Validate() error {
	if err := checkRoot(&d); err != nil {
		return err
	}
	if err := validateDecisionSummary(d.Type, d.Stage, d.Outcome, d.SelectedAction); err != nil {
		return err
	}
	switch d.Type {
	case DecisionTypeChannel:
		if d.ChannelEvidence == nil || d.OrchestratorEvidence != nil {
			return &ValidationError{Path: "channel_evidence", Reason: "required only for channel decisions"}
		}
	case DecisionTypeOrchestrator:
		if d.OrchestratorEvidence == nil || d.ChannelEvidence != nil {
			return &ValidationError{Path: "orchestrator_evidence", Reason: "required only for orchestrator decisions"}
		}
		if d.OrchestratorEvidence.SelectedAction != d.SelectedAction {
			return &ValidationError{Path: "orchestrator_evidence.selected_action", Reason: "matches selected_action"}
		}
	}
	return nil
}

func validateDecisionSummary(
	typeValue DecisionType,
	stage DecisionStage,
	outcome DecisionOutcome,
	action OrchestratorAction,
) error {
	switch typeValue {
	case DecisionTypeChannel:
		if stage != DecisionStageAdmission && stage != DecisionStageDelivery &&
			stage != DecisionStageRevalidation {
			return &ValidationError{Path: pathStage, Reason: "admission, delivery or revalidation for channel"}
		}
		if outcome == "" {
			return &ValidationError{Path: pathOutcome, Reason: reasonRequired}
		}
		if action != "" {
			return &ValidationError{Path: pathSelectedAction, Reason: "absent for channel"}
		}
	case DecisionTypeOrchestrator:
		if stage != "" || outcome != "" {
			return &ValidationError{Path: pathStage, Reason: "stage and outcome absent for orchestrator"}
		}
		if action == "" {
			return &ValidationError{Path: pathSelectedAction, Reason: reasonRequired}
		}
	}
	return nil
}

// ActiveModelProfile is the latest observation of the active profile.
type ActiveModelProfile struct {
	Profile    InferenceProfile   `json:"profile"`
	Source     ModelProfileSource `json:"source"`
	ObservedAt time.Time          `json:"observed_at"`
}

// Validate checks the active-profile observation.
func (p ActiveModelProfile) Validate() error { return checkRoot(&p) }

// ModelMetadata is one explicitly qualified deployment identifier.
type ModelMetadata struct {
	Availability MetadataAvailability `json:"availability"`
	Value        ModelMetadataValue   `json:"value,omitempty"`
}

// Validate requires a value exactly when the metadata is known.
func (m ModelMetadata) Validate() error {
	if err := checkRoot(&m); err != nil {
		return err
	}
	if (m.Availability == MetadataAvailabilityKnown) != (m.Value != "") {
		return &ValidationError{Path: "value", Reason: "present iff availability is known"}
	}
	return nil
}

// ModelDeploymentMetadata identifies a deployment without invented values.
type ModelDeploymentMetadata struct {
	BaseRevision    ModelMetadata `json:"base_revision"`
	AdapterRevision ModelMetadata `json:"adapter_revision"`
	DatasetVersion  ModelMetadata `json:"dataset_version"`
	ABVariant       ModelMetadata `json:"ab_variant"`
}

// Validate checks all deployment metadata.
func (m ModelDeploymentMetadata) Validate() error { return checkRoot(&m) }

// ModelLastCall is the latest known glimmer_calls status and latency.
type ModelLastCall struct {
	CallID     OpaqueID          `json:"call_id"`
	Status     GlimmerCallStatus `json:"status"`
	LatencyMS  int64             `json:"latency_ms" jsonschema:"minimum=0"`
	ObservedAt time.Time         `json:"observed_at"`
}

// Validate checks the latest-call metadata.
func (c ModelLastCall) Validate() error { return checkRoot(&c) }

// ModelStatusResult is read-only model status. Null observations and
// qualified metadata make absence explicit; it exposes no activation knob.
type ModelStatusResult struct {
	ActiveProfile *ActiveModelProfile     `json:"active_profile" jsonschema:"nullable"`
	LastCall      *ModelLastCall          `json:"last_call" jsonschema:"nullable"`
	Deployment    ModelDeploymentMetadata `json:"deployment"`
}

// Validate checks model status.
func (r ModelStatusResult) Validate() error { return checkRoot(&r) }
