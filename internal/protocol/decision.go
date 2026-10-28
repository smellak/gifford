package protocol

import "time"

// DecisionRecord is the serialisable, inert record of one gate decision: the channel_control_decisions row, the decision_id shared by message.control_* events, and the content of explanations, panel and CLI. It is separate from channel.Decision because nobody outside internal/channel can build a Decision, records must cross processes and nodes and replay from the Event Store so anyone must be able to decode them, and internal/protocol is a leaf that cannot import internal/channel; a DecisionRecord confers no authority since no delivery function accepts it.
type DecisionRecord struct {
	DecisionID        ChannelDecisionID  `json:"decision_id"`
	ObjectType        DecisionObjectType `json:"object_type"`
	ObjectID          OpaqueID           `json:"object_id"`
	Stage             DecisionStage      `json:"stage"`
	Plane             Plane              `json:"plane"`
	FromAgent         AgentID            `json:"from_agent,omitempty"`
	ToAgent           AgentID            `json:"to_agent,omitempty"`
	Outcome           DecisionOutcome    `json:"outcome"`
	Reason            DecisionReason     `json:"reason"`
	MatchedControlIDs []ControlID        `json:"matched_control_ids" jsonschema:"maxItems=64,uniqueItems=true"`
	EffectiveControl  EffectiveControl   `json:"effective_control"`
	ControlVersions   ControlVersions    `json:"control_versions"`
	Explanation       Explanation        `json:"explanation"`
	ActorID           string             `json:"actor_id,omitempty" jsonschema:"maxLength=128"`
	CreatedAt         time.Time          `json:"created_at"`
}

// Validate checks the record: the six outcome, reason and stage rules of
// §6.5 and the coherence with its explanation. A record still confers no
// authority: no delivery function accepts one.
func (r DecisionRecord) Validate() error {
	if err := checkRoot(&r); err != nil {
		return err
	}
	if r.Outcome == DecisionOutcomeDeny {
		switch r.Stage {
		case DecisionStageAdmission, DecisionStageAcceptance, DecisionStageContinuation:
		default:
			return &ValidationError{Path: "stage", Reason: "DENY at admission, acceptance or continuation"}
		}
	}
	if r.Stage == DecisionStageRevalidation {
		if r.Outcome != DecisionOutcomeAllow && r.Outcome != DecisionOutcomeHold {
			return &ValidationError{Path: "outcome", Reason: "revalidation allows or holds"}
		}
		if r.Outcome == DecisionOutcomeHold && r.Reason != DecisionReasonControlChanged {
			return &ValidationError{Path: "reason", Reason: "a revalidation hold means CONTROL_CHANGED"}
		}
	}
	if err := checkOutcomeReason(r.Outcome, r.Reason); err != nil {
		return err
	}
	if r.Explanation.Outcome != r.Outcome || r.Explanation.Reason != r.Reason ||
		r.Explanation.Plane != r.Plane {
		return &ValidationError{Path: "explanation", Reason: "outcome, reason and plane match the record"}
	}
	if r.FromAgent != "" && r.FromAgent != r.Explanation.FromAgent {
		return &ValidationError{Path: "from_agent", Reason: "matches the explanation"}
	}
	if r.ToAgent != "" && r.ToAgent != r.Explanation.ToAgent {
		return &ValidationError{Path: "to_agent", Reason: "matches the explanation"}
	}
	return nil
}
