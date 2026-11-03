package protocol

import "time"

// DeliveryDecisionListParams filters the payload-free delivery-decision
// register. Results are ordered by created_at and decision_id descending.
type DeliveryDecisionListParams struct {
	DecisionID ULID       `json:"decision_id,omitempty"`
	AgentID    AgentID    `json:"agent_id,omitempty"`
	MessageID  ULID       `json:"message_id,omitempty"`
	Cursor     PageCursor `json:"cursor,omitempty"`
	Limit      *int64     `json:"limit,omitempty" jsonschema:"minimum=1,maximum=200"`
}

// Validate checks the filters and page bound.
func (p DeliveryDecisionListParams) Validate() error { return checkRoot(&p) }

// EffectiveLimit returns the requested limit or the bounded default.
func (p DeliveryDecisionListParams) EffectiveLimit() int64 { return effectiveListLimit(p.Limit) }

// DeliveryDecisionListItem is one payload-free Orchestrator decision and the
// latest durable attempt it created, if any. Guards contain names and outcomes
// only; evidence references never expose local paths or screen contents.
type DeliveryDecisionListItem struct {
	DecisionID      ULID                      `json:"decision_id"`
	AgentID         AgentID                   `json:"agent_id"`
	MessageID       ULID                      `json:"message_id"`
	SelectedAction  OrchestratorAction        `json:"selected_action"`
	Mode            DeliveryDecisionMode      `json:"mode"`
	ExecutionStatus ExecutionStatus           `json:"execution_status"`
	Guards          []DecisionGuardEvidence   `json:"guards" jsonschema:"maxItems=32"`
	Delivery        *DeliveryDecisionEvidence `json:"delivery,omitempty"`
	CreatedAt       time.Time                 `json:"created_at"`
}

// Validate checks one register row.
func (i DeliveryDecisionListItem) Validate() error { return checkRoot(&i) }

// DeliveryDecisionListResult is one stable, bounded page of register rows.
type DeliveryDecisionListResult struct {
	Decisions  []DeliveryDecisionListItem `json:"decisions" jsonschema:"maxItems=200"`
	NextCursor PageCursor                 `json:"next_cursor,omitempty"`
}

// Validate checks the register page.
func (r DeliveryDecisionListResult) Validate() error { return checkRoot(&r) }
