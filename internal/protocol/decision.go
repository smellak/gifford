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

// Decision-record rows carry no behavior; the outcome and reason rules of
// §6.5 live in the schema, the fixtures and the Explanation checks.
