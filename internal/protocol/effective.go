package protocol

import (
	"fmt"
	"time"
)

// This file holds the effective view and explanation of §6.5: what a new
// object would get now, and why.

// EffectiveControl carries the per-scope effective values for one
// (from → to, plane) pair.
type EffectiveControl struct {
	GlobalMode       ChannelMode  `json:"global_mode"`
	FromNodeMode     ChannelMode  `json:"from_node_mode"`
	ToNodeMode       ChannelMode  `json:"to_node_mode"`
	FromTX           bool         `json:"from_tx"`
	FromAutoContinue bool         `json:"from_auto_continue"`
	ToRX             bool         `json:"to_rx"`
	ToAutoDelivery   bool         `json:"to_auto_delivery"`
	ToDeliverable    bool         `json:"to_deliverable"`
	LinkMode         LinkMode     `json:"link_mode"`
	ThreadMode       LinkMode     `json:"thread_mode,omitempty"`
	LoopMode         LoopMode     `json:"loop_mode,omitempty"`
	ObjectState      ControlState `json:"object_state,omitempty"`
}

// Validate checks the effective values.
func (c EffectiveControl) Validate() error {
	return checkRoot(&c)
}

// EffectiveView is what a new object would get now at the delivery gate.
type EffectiveView struct {
	FromAgent         AgentID           `json:"from_agent"`
	ToAgent           AgentID           `json:"to_agent"`
	Plane             Plane             `json:"plane"`
	EffectiveControl  EffectiveControl  `json:"effective_control"`
	EffectiveOutcome  DecisionOutcome   `json:"effective_outcome"`
	Blocker           ExplanationScope  `json:"blocker,omitempty"`
	ControlVersions   ControlVersions   `json:"control_versions"`
}

// Validate checks the view.
func (v EffectiveView) Validate() error {
	if err := checkRoot(&v); err != nil {
		return err
	}
	if v.Plane != PlaneMessage && v.Plane != PlaneArtifact {
		return &ValidationError{Path: "plane", Reason: "message or artifact"}
	}
	if (v.EffectiveOutcome == DecisionOutcomeAllow) != (v.Blocker == "") {
		return &ValidationError{Path: "blocker", Reason: "absent iff ALLOW"}
	}
	return nil
}

// ExplanationLine is one rendered explanation line.
type ExplanationLine struct {
	Scope     ExplanationScope `json:"scope"`
	Key       string           `json:"key" jsonschema:"minLength=1,maxLength=300"`
	Value     string           `json:"value" jsonschema:"minLength=1,maxLength=120"`
	Version   *int64           `json:"version,omitempty" jsonschema:"minimum=0"`
	Blocking  bool             `json:"blocking,omitempty"`
	ControlID ControlID        `json:"control_id,omitempty"`
	ActorType ActorType        `json:"actor_type,omitempty"`
	ActorID   string           `json:"actor_id,omitempty" jsonschema:"maxLength=128"`
	Reason    string           `json:"reason,omitempty" jsonschema:"maxLength=280"`
	ExpiresAt time.Time        `json:"expires_at,omitzero"`
}

// Validate checks the line.
func (l ExplanationLine) Validate() error {
	return checkRoot(&l)
}

// Explanation shows effective state and marks the first blocking control.
type Explanation struct {
	FromAgent      AgentID             `json:"from_agent"`
	ToAgent        AgentID             `json:"to_agent"`
	Plane          Plane               `json:"plane"`
	ObjectType     DecisionObjectType  `json:"object_type,omitempty"`
	ObjectID       OpaqueID            `json:"object_id,omitempty"`
	ThreadID       ThreadID            `json:"thread_id,omitempty"`
	LoopID         LoopID              `json:"loop_id,omitempty"`
	LoopGeneration *int64              `json:"loop_generation,omitempty" jsonschema:"minimum=1"`
	Outcome        DecisionOutcome     `json:"outcome"`
	Reason         DecisionReason      `json:"reason"`
	Blocker        ExplanationScope    `json:"blocker,omitempty"`
	Lines          []ExplanationLine   `json:"lines" jsonschema:"minItems=1,maxItems=12"`
}

// Validate checks the explanation.
func (e Explanation) Validate() error {
	if err := checkRoot(&e); err != nil {
		return err
	}
	if e.Plane != PlaneMessage && e.Plane != PlaneArtifact {
		return &ValidationError{Path: "plane", Reason: "message or artifact"}
	}
	if (e.ObjectType == "") != (e.ObjectID == "") {
		return &ValidationError{Path: "object_id", Reason: "present iff object_type present"}
	}
	if err := checkOutcomeReason(e.Outcome, e.Reason); err != nil {
		return err
	}
	blocking := -1
	for i, line := range e.Lines {
		if i > 0 && explanationScopeRank(line.Scope) < explanationScopeRank(e.Lines[i-1].Scope) {
			return &ValidationError{Path: fmt.Sprintf("lines[%d]", i), Reason: "lines follow scope order"}
		}
		if line.Blocking {
			if blocking >= 0 {
				return &ValidationError{Path: fmt.Sprintf("lines[%d]", i), Reason: "exactly one blocking line"}
			}
			blocking = i
		}
	}
	if e.Outcome == DecisionOutcomeAllow {
		if e.Blocker != "" || blocking >= 0 {
			return &ValidationError{Path: "blocker", Reason: "absent iff ALLOW with no blocking line"}
		}
	} else {
		if e.Blocker == "" {
			return &ValidationError{Path: "blocker", Reason: "a blocked outcome names its blocker"}
		}
		if blocking < 0 {
			return &ValidationError{Path: "lines", Reason: "a blocked outcome marks one blocking line"}
		}
		if e.Lines[blocking].Scope != e.Blocker {
			return &ValidationError{Path: "blocker", Reason: "the blocking line carries the blocker scope"}
		}
		for _, line := range e.Lines {
			if line.Scope == ExplanationScopeControls {
				return checkControlsLine(e)
			}
		}
	}
	if e.Blocker == ExplanationScopePolicy && e.Reason != DecisionReasonPolicyDenied {
		return &ValidationError{Path: "reason", Reason: "policy blocker means POLICY_DENIED"}
	}
	if e.Blocker == ExplanationScopeControls && e.Reason != DecisionReasonControlUnavailable {
		return &ValidationError{Path: "reason", Reason: "controls blocker means CONTROL_UNAVAILABLE"}
	}
	return nil
}

// checkControlsLine enforces the controls-line placement: when a controls
// line is present it is the last line and only a policy line may precede
// it, because unreadable controls leave nothing else known.
func checkControlsLine(e Explanation) error {
	last := e.Lines[len(e.Lines)-1]
	if last.Scope != ExplanationScopeControls {
		return &ValidationError{Path: "lines", Reason: "a controls line is the last line"}
	}
	for i, line := range e.Lines[:len(e.Lines)-1] {
		if line.Scope != ExplanationScopePolicy {
			return &ValidationError{Path: fmt.Sprintf("lines[%d]", i), Reason: "only a policy line precedes controls"}
		}
	}
	return nil
}

// checkOutcomeReason enforces the outcome/reason compatibility shared by
// DecisionRecord and Explanation.
func checkOutcomeReason(outcome DecisionOutcome, reason DecisionReason) error {
	const path = "reason"
	switch {
	case reason == DecisionReasonControlUnavailable && outcome != DecisionOutcomeHold:
		return &ValidationError{Path: path, Reason: "CONTROL_UNAVAILABLE holds everything"}
	case outcome == DecisionOutcomeAllow:
		if !isAllowReason(reason) {
			return &ValidationError{Path: path, Reason: "ALLOW needs an allow reason"}
		}
	case outcome == DecisionOutcomeApproval:
		switch reason {
		case DecisionReasonLinkApproval, DecisionReasonLoopStepTokenRequired,
			DecisionReasonAgentAutoContinueOff, DecisionReasonObjectApprovalPending:
		default:
			return &ValidationError{Path: path, Reason: "APPROVAL needs an approval reason"}
		}
	case outcome == DecisionOutcomeDeny:
		switch reason {
		case DecisionReasonObjectHeld, DecisionReasonObjectApprovalPending,
			DecisionReasonControlUnavailable, DecisionReasonControlChanged:
			return &ValidationError{Path: path, Reason: "a queued object is held, never denied"}
		}
	}
	return nil
}
