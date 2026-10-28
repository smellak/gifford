package protocol

import (
	"fmt"
	"time"
)

// This file holds the orchestration contracts of §5.6: the request the
// Gateway sends for CHOOSE_ORCHESTRATION_ACTION and the decision it gets.

// OrchestrationAgent describes the receiver of an orchestration request.
type OrchestrationAgent struct {
	ID           AgentID      `json:"id"`
	Harness      Harness      `json:"harness,omitempty"`
	Adapter      AdapterType  `json:"adapter"`
	Capabilities Capabilities `json:"capabilities"`
}

// Validate checks the agent description.
func (a OrchestrationAgent) Validate() error {
	return checkRoot(&a)
}

// OrchestrationState carries the receiver state for the model.
type OrchestrationState struct {
	HumanDraft  DraftState     `json:"human_draft"`
	AgentPhase  AgentPhase     `json:"agent_phase"`
	PIdle       float64        `json:"p_idle" jsonschema:"minimum=0,maximum=1"`
	Attention   AttentionState `json:"attention"`
	SecondsIdle *float64       `json:"seconds_idle,omitempty" jsonschema:"minimum=0"`
}

// Validate checks the state.
func (s OrchestrationState) Validate() error {
	return checkRoot(&s)
}

// PendingObject describes one queued object for the model.
type PendingObject struct {
	Priority           Priority    `json:"priority"`
	Triage             TriageLabel `json:"triage"`
	AgeMS              int64       `json:"age_ms" jsonschema:"minimum=0"`
	SameThreadPending  *int64      `json:"same_thread_pending,omitempty" jsonschema:"minimum=0"`
	LowPriorityPending *int64      `json:"low_priority_pending,omitempty" jsonschema:"minimum=0"`
}

// Validate checks the pending object.
func (p PendingObject) Validate() error {
	return checkRoot(&p)
}

// OrchestrationPolicy carries the policy knobs for the model.
type OrchestrationPolicy struct {
	HumanDecisionAfterS int64 `json:"human_decision_after_s" jsonschema:"minimum=0"`
	BatchWindowS        int64 `json:"batch_window_s" jsonschema:"minimum=0"`
}

// Validate checks the policy.
func (p OrchestrationPolicy) Validate() error {
	return checkRoot(&p)
}

// OrchestrationChannel carries read-only channel data for the model.
type OrchestrationChannel struct {
	DecisionID     ChannelDecisionID `json:"decision_id"`
	Outcome        DecisionOutcome   `json:"outcome"`
	Global         ChannelMode       `json:"global"`
	Link           LinkMode          `json:"link"`
	Loop           *LoopSummary      `json:"loop,omitempty"`
	AutoContinue   *bool             `json:"auto_continue,omitempty"`
	PatternSignals []PatternSignal   `json:"pattern_signals,omitempty" jsonschema:"uniqueItems=true"`
}

// Validate checks the channel block.
func (c OrchestrationChannel) Validate() error {
	if err := checkRoot(&c); err != nil {
		return err
	}
	if c.Outcome != DecisionOutcomeAllow {
		return &ValidationError{Path: pathOutcome, Reason: "only ALLOW objects pass stage 0"}
	}
	if c.Global == ChannelModeOff {
		return &ValidationError{Path: "global", Reason: "OFF never reaches the model"}
	}
	if c.Link == LinkModeDeny {
		return &ValidationError{Path: "link", Reason: "DENY never reaches the model"}
	}
	return nil
}

// OrchestrationRequest is the CHOOSE_ORCHESTRATION_ACTION payload.
type OrchestrationRequest struct {
	TaskType       ModelContract        `json:"task_type"`
	Agent          OrchestrationAgent   `json:"agent"`
	State          OrchestrationState   `json:"state"`
	Message        *PendingObject       `json:"message,omitempty"`
	Artifact       *PendingObject       `json:"artifact,omitempty"`
	Policy         *OrchestrationPolicy `json:"policy,omitempty"`
	Channel        OrchestrationChannel `json:"channel"`
	AllowedActions []OrchestratorAction `json:"allowed_actions" jsonschema:"minItems=2,maxItems=12,uniqueItems=true"`
	Screen         string               `json:"screen,omitempty" jsonschema:"maxLength=8192"`
}

// Validate checks the request.
func (r OrchestrationRequest) Validate() error {
	if r.TaskType != ModelContractChooseOrchestrationAction {
		return &ValidationError{Path: "task_type", Reason: "must be CHOOSE_ORCHESTRATION_ACTION"}
	}
	if err := checkRoot(&r); err != nil {
		return err
	}
	if (r.Message == nil) == (r.Artifact == nil) {
		return &ValidationError{Path: familyMessage, Reason: "exactly one of message, artifact"}
	}
	return nil
}

// GuardResult is one deterministic guard evaluation.
type GuardResult struct {
	Guard   GuardName    `json:"guard"`
	Outcome GuardOutcome `json:"outcome"`
	Detail  string       `json:"detail,omitempty" jsonschema:"maxLength=200"`
}

// Validate checks the guard result.
func (g GuardResult) Validate() error {
	if err := checkRoot(&g); err != nil {
		return err
	}
	if g.Outcome == GuardOutcomeOverridden && g.Guard != GuardNamePIdleThreshold {
		return &ValidationError{Path: pathOutcome, Reason: "only p_idle_threshold is overridable"}
	}
	return nil
}

// OrchestratorDecision is the evaluate result and the decisions row.
type OrchestratorDecision struct {
	DecisionID      ULID                           `json:"decision_id"`
	AgentID         AgentID                        `json:"agent_id"`
	MessageID       ULID                           `json:"message_id,omitempty"`
	ArtifactID      OpaqueID                       `json:"artifact_id,omitempty"`
	StateID         ULID                           `json:"state_id"`
	AllowedActions  []OrchestratorAction           `json:"allowed_actions" jsonschema:"minItems=1,maxItems=12,uniqueItems=true"`
	SelectedAction  OrchestratorAction             `json:"selected_action"`
	Probabilities   map[OrchestratorAction]float64 `json:"probabilities,omitempty"`
	HardGuards      []GuardResult                  `json:"hard_guards"`
	PolicyVersion   string                         `json:"policy_version" jsonschema:"minLength=1,maxLength=64"`
	DecisionModel   string                         `json:"decision_model,omitempty" jsonschema:"minLength=1,maxLength=128"`
	GlimmerCallID   OpaqueID                       `json:"glimmer_call_id,omitempty"`
	Channel         OrchestrationChannel           `json:"channel"`
	CreatedAt       time.Time                      `json:"created_at"`
	ExecutionStatus ExecutionStatus                `json:"execution_status"`
}

// Validate checks the decision.
func (d OrchestratorDecision) Validate() error {
	if err := checkRoot(&d); err != nil {
		return err
	}
	if (d.MessageID == "") == (d.ArtifactID == "") {
		return &ValidationError{Path: pathMessageID, Reason: "exactly one of message_id, artifact_id"}
	}
	allowed := make(map[OrchestratorAction]bool, len(d.AllowedActions))
	for _, a := range d.AllowedActions {
		allowed[a] = true
	}
	if !allowed[d.SelectedAction] {
		return &ValidationError{Path: "selected_action", Reason: "must be in allowed_actions"}
	}
	if len(d.Probabilities) > 0 {
		if d.GlimmerCallID == "" {
			return &ValidationError{Path: pathProbabilities, Reason: "present only with glimmer_call_id"}
		}
		for action, p := range d.Probabilities {
			if !allowed[action] {
				return &ValidationError{Path: fmt.Sprintf("probabilities[%s]", action), Reason: "outside allowed_actions"}
			}
			if p < 0 || p > 1 {
				return &ValidationError{Path: fmt.Sprintf("probabilities[%s]", action), Reason: reasonRange01}
			}
		}
	} else if d.GlimmerCallID != "" {
		return &ValidationError{Path: pathProbabilities, Reason: "present iff glimmer_call_id is present"}
	}
	seen := make(map[GuardName]bool, len(d.HardGuards))
	for i, g := range d.HardGuards {
		if seen[g.Guard] {
			return &ValidationError{Path: fmt.Sprintf("hard_guards[%d]", i), Reason: "duplicate guard"}
		}
		seen[g.Guard] = true
	}
	return nil
}
