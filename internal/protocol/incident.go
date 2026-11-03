package protocol

import "time"

// IncidentKind names the five observable incident sources; it is not a new
// workflow or a channel state.
type IncidentKind string

// IncidentKind values.
const (
	IncidentKindDeliveryUncertain   IncidentKind = "DELIVERY_UNCERTAIN"
	IncidentKindApprovalPending     IncidentKind = "APPROVAL_PENDING"
	IncidentKindTmuxInstanceInvalid IncidentKind = "TMUX_INSTANCE_INVALID"
	IncidentKindDetectorUncertain   IncidentKind = "DETECTOR_UNCERTAIN"
	IncidentKindEscalated           IncidentKind = "ESCALATED"
)

// EnumValues returns the closed incident vocabulary.
func (IncidentKind) EnumValues() []string {
	return []string{
		string(IncidentKindDeliveryUncertain), string(IncidentKindApprovalPending),
		string(IncidentKindTmuxInstanceInvalid), string(IncidentKindDetectorUncertain), string(IncidentKindEscalated),
	}
}

// Valid rejects unrecognized incident kinds.
func (v IncidentKind) Valid() bool {
	switch v {
	case IncidentKindDeliveryUncertain, IncidentKindApprovalPending, IncidentKindTmuxInstanceInvalid,
		IncidentKindDetectorUncertain, IncidentKindEscalated:
		return true
	default:
		return false
	}
}

// IncidentState describes the stored source, not an inferred resolution.
type IncidentState string

// IncidentState values.
const (
	IncidentStateUncertain IncidentState = "UNCERTAIN"
	IncidentStatePending   IncidentState = "PENDING"
	IncidentStateInvalid   IncidentState = "INVALID"
	IncidentStateEscalated IncidentState = "ESCALATED"
)

// EnumValues returns the closed incident state vocabulary.
func (IncidentState) EnumValues() []string {
	return []string{string(IncidentStateUncertain), string(IncidentStatePending), string(IncidentStateInvalid), string(IncidentStateEscalated)}
}

// Valid rejects unknown incident states.
func (v IncidentState) Valid() bool {
	return v == IncidentStateUncertain || v == IncidentStatePending || v == IncidentStateInvalid || v == IncidentStateEscalated
}

// IncidentOwnerKind is independent of the component that needs repair.
type IncidentOwnerKind string

// IncidentOwnerOperator assigns attention to the node's configured operator.
const IncidentOwnerOperator IncidentOwnerKind = "operator"

// EnumValues returns the only supported owner role.
func (IncidentOwnerKind) EnumValues() []string { return []string{string(IncidentOwnerOperator)} }

// Valid reports whether the owner role is supported.
func (v IncidentOwnerKind) Valid() bool { return v == IncidentOwnerOperator }

// IncidentFixArea names a repair area, never an agent identity.
type IncidentFixArea string

// IncidentFixArea values.
const (
	IncidentFixAreaDelivery  IncidentFixArea = "delivery"
	IncidentFixAreaInstances IncidentFixArea = "instances"
	IncidentFixAreaDetector  IncidentFixArea = "detector"
	IncidentFixAreaUnknown   IncidentFixArea = "unknown"
)

// EnumValues includes an explicit unknown when origin was not persisted.
func (IncidentFixArea) EnumValues() []string {
	return []string{string(IncidentFixAreaDelivery), string(IncidentFixAreaInstances), string(IncidentFixAreaDetector), string(IncidentFixAreaUnknown)}
}

// Valid rejects unrecognized repair areas.
func (v IncidentFixArea) Valid() bool {
	return v == IncidentFixAreaDelivery || v == IncidentFixAreaInstances || v == IncidentFixAreaDetector || v == IncidentFixAreaUnknown
}

// IncidentListParams pages by opened_at, kind and source id descending.
type IncidentListParams struct {
	AgentID AgentID      `json:"agent_id,omitempty"`
	Kind    IncidentKind `json:"kind,omitempty"`
	Cursor  PageCursor   `json:"cursor,omitempty"`
	Limit   *int64       `json:"limit,omitempty" jsonschema:"minimum=1,maximum=200"`
}

// Validate checks the closed filters and pagination bounds.
func (p IncidentListParams) Validate() error { return checkRoot(&p) }

// EffectiveLimit returns the requested page size or its default.
func (p IncidentListParams) EffectiveLimit() int64 { return effectiveListLimit(p.Limit) }

// IncidentListItem is metadata from existing records, not a synthetic incident
// ledger. Null timestamps mean the source does not persist that fact. For a
// detector, opened_at is the latest stored observation, not an episode start.
type IncidentListItem struct {
	IncidentID   string            `json:"incident_id" jsonschema:"minLength=1,maxLength=300"`
	Kind         IncidentKind      `json:"kind"`
	AgentID      AgentID           `json:"agent_id,omitempty"`
	MessageID    ULID              `json:"message_id,omitempty"`
	OwnerKind    IncidentOwnerKind `json:"owner_kind"`
	OwnerID      *AgentID          `json:"owner_id" jsonschema:"nullable"`
	FixArea      IncidentFixArea   `json:"fix_area"`
	State        IncidentState     `json:"state"`
	Reason       string            `json:"reason" jsonschema:"minLength=1,maxLength=128"`
	EscalatedAt  *time.Time        `json:"escalated_at" jsonschema:"nullable"`
	NextNoticeAt *time.Time        `json:"next_notice_at" jsonschema:"nullable"`
	OpenedAt     time.Time         `json:"opened_at"`
	UpdatedAt    time.Time         `json:"updated_at"`
	ResolvedAt   *time.Time        `json:"resolved_at" jsonschema:"nullable"`
}

// Validate checks one metadata projection.
func (i IncidentListItem) Validate() error {
	if err := checkRoot(&i); err != nil {
		return err
	}
	if i.UpdatedAt.Before(i.OpenedAt) {
		return &ValidationError{Path: "updated_at", Reason: "not before opened_at"}
	}
	return nil
}

// IncidentListResult is a bounded page of observable incidents.
type IncidentListResult struct {
	Incidents  []IncidentListItem `json:"incidents" jsonschema:"maxItems=200"`
	NextCursor PageCursor         `json:"next_cursor,omitempty"`
}

// Validate checks the page's typed rows and continuation token.
func (r IncidentListResult) Validate() error { return checkRoot(&r) }
