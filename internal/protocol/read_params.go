package protocol

const (
	// DefaultListLimit is used when a read-list request omits limit.
	DefaultListLimit int64 = 50
	// MaxListLimit bounds every read-list page.
	MaxListLimit int64 = 200
)

// AgentListParams pages through agents in stable agent-id order.
type AgentListParams struct {
	Cursor PageCursor `json:"cursor,omitempty"`
	Limit  *int64     `json:"limit,omitempty" jsonschema:"minimum=1,maximum=200"`
}

// Validate checks the agent-list filters and page bound.
func (p AgentListParams) Validate() error { return checkRoot(&p) }

// EffectiveLimit returns the requested limit or the default.
func (p AgentListParams) EffectiveLimit() int64 { return effectiveListLimit(p.Limit) }

// MessageListParams filters recipient-specific delivery metadata. Results
// are ordered by creation time and message id, both descending.
type MessageListParams struct {
	AgentID AgentID            `json:"agent_id,omitempty"`
	State   DeliveryQueueState `json:"state,omitempty"`
	Cursor  PageCursor         `json:"cursor,omitempty"`
	Limit   *int64             `json:"limit,omitempty" jsonschema:"minimum=1,maximum=200"`
}

// Validate checks the message-list filters and page bound.
func (p MessageListParams) Validate() error { return checkRoot(&p) }

// EffectiveLimit returns the requested limit or the default.
func (p MessageListParams) EffectiveLimit() int64 { return effectiveListLimit(p.Limit) }

// DecisionListParams filters decision metadata. Results are ordered by
// creation time and decision id, both descending.
type DecisionListParams struct {
	AgentID   AgentID      `json:"agent_id,omitempty"`
	MessageID ULID         `json:"message_id,omitempty"`
	Type      DecisionType `json:"type,omitempty"`
	Cursor    PageCursor   `json:"cursor,omitempty"`
	Limit     *int64       `json:"limit,omitempty" jsonschema:"minimum=1,maximum=200"`
}

// Validate checks the decision-list filters and page bound.
func (p DecisionListParams) Validate() error { return checkRoot(&p) }

// EffectiveLimit returns the requested limit or the default.
func (p DecisionListParams) EffectiveLimit() int64 { return effectiveListLimit(p.Limit) }

// DecisionGetParams addresses a Channel or Orchestrator decision.
type DecisionGetParams struct {
	DecisionID DecisionID `json:"decision_id"`
}

// Validate checks the decision id.
func (p DecisionGetParams) Validate() error { return checkRoot(&p) }

func effectiveListLimit(limit *int64) int64 {
	if limit == nil {
		return DefaultListLimit
	}
	return *limit
}
