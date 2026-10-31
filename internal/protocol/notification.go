package protocol

import "time"

const notificationPathObjectID = "object_id"

// NotificationClass identifies the durable object represented by a human
// notification. SERVICE rows are private implementation markers and are not
// part of this public read contract.
type NotificationClass string

// NotificationClass values.
const (
	NotificationClassMessage  NotificationClass = "MESSAGE"
	NotificationClassApproval NotificationClass = "APPROVAL"
)

// EnumValues returns the public notification classes in table order.
func (NotificationClass) EnumValues() []string {
	return []string{string(NotificationClassMessage), string(NotificationClassApproval)}
}

// Valid reports whether the class is public notification metadata.
func (v NotificationClass) Valid() bool {
	return v == NotificationClassMessage || v == NotificationClassApproval
}

// NotificationReason is the fixed reason that created a notification.
type NotificationReason string

// NotificationReason values.
const (
	NotificationReasonBaseline                NotificationReason = "BASELINE"
	NotificationReasonMessageAvailable        NotificationReason = "MESSAGE_AVAILABLE"
	NotificationReasonDisplayOnly             NotificationReason = "DISPLAY_ONLY"
	NotificationReasonChannelHeld             NotificationReason = "CHANNEL_HELD"
	NotificationReasonChannelApprovalRequired NotificationReason = "CHANNEL_APPROVAL_REQUIRED"
	NotificationReasonApprovalPending         NotificationReason = "APPROVAL_PENDING"
)

// EnumValues returns notification reasons in lifecycle order.
func (NotificationReason) EnumValues() []string {
	return []string{
		string(NotificationReasonBaseline),
		string(NotificationReasonMessageAvailable),
		string(NotificationReasonDisplayOnly),
		string(NotificationReasonChannelHeld),
		string(NotificationReasonChannelApprovalRequired),
		string(NotificationReasonApprovalPending),
	}
}

// Valid reports whether the reason is fixed notification metadata.
func (v NotificationReason) Valid() bool {
	switch v {
	case NotificationReasonBaseline, NotificationReasonMessageAvailable,
		NotificationReasonDisplayOnly, NotificationReasonChannelHeld,
		NotificationReasonChannelApprovalRequired, NotificationReasonApprovalPending:
		return true
	default:
		return false
	}
}

// NotificationState is the durable notification lifecycle state.
type NotificationState string

// NotificationState values.
const (
	NotificationStateBaselined NotificationState = "BASELINED"
	NotificationStatePending   NotificationState = "PENDING"
	NotificationStateRendered  NotificationState = "RENDERED"
)

// EnumValues returns notification states in lifecycle order.
func (NotificationState) EnumValues() []string {
	return []string{
		string(NotificationStateBaselined),
		string(NotificationStatePending),
		string(NotificationStateRendered),
	}
}

// Valid reports whether the state is known.
func (v NotificationState) Valid() bool {
	return v == NotificationStateBaselined || v == NotificationStatePending ||
		v == NotificationStateRendered
}

// NotificationAdapter identifies the adapter recorded for a notification.
type NotificationAdapter string

// NotificationAdapter values.
const (
	NotificationAdapterNone NotificationAdapter = "NONE"
	NotificationAdapterTMUX NotificationAdapter = "TMUX"
)

// EnumValues returns notification adapters in table order.
func (NotificationAdapter) EnumValues() []string {
	return []string{string(NotificationAdapterNone), string(NotificationAdapterTMUX)}
}

// Valid reports whether the adapter is known.
func (v NotificationAdapter) Valid() bool {
	return v == NotificationAdapterNone || v == NotificationAdapterTMUX
}

// NotificationFailureReason is a fixed adapter failure reason. It cannot
// carry command output, paths or free-form diagnostics.
type NotificationFailureReason string

// NotificationFailureReason values.
const (
	NotificationFailureAdapterDisabled    NotificationFailureReason = "ADAPTER_DISABLED"
	NotificationFailureAdapterUnavailable NotificationFailureReason = "ADAPTER_UNAVAILABLE"
	NotificationFailureUnsupportedFlags   NotificationFailureReason = "UNSUPPORTED_FLAGS"
	NotificationFailureClientUnavailable  NotificationFailureReason = "CLIENT_UNAVAILABLE"
	NotificationFailureAdapterTimeout     NotificationFailureReason = "ADAPTER_TIMEOUT"
	NotificationFailureInvalidTemplate    NotificationFailureReason = "INVALID_TEMPLATE"
	NotificationFailureAdapterFailed      NotificationFailureReason = "ADAPTER_FAILED"
)

// EnumValues returns fixed adapter failures in table order.
func (NotificationFailureReason) EnumValues() []string {
	return []string{
		string(NotificationFailureAdapterDisabled),
		string(NotificationFailureAdapterUnavailable),
		string(NotificationFailureUnsupportedFlags),
		string(NotificationFailureClientUnavailable),
		string(NotificationFailureAdapterTimeout),
		string(NotificationFailureInvalidTemplate),
		string(NotificationFailureAdapterFailed),
	}
}

// Valid reports whether the failure reason is fixed metadata.
func (v NotificationFailureReason) Valid() bool {
	switch v {
	case NotificationFailureAdapterDisabled, NotificationFailureAdapterUnavailable,
		NotificationFailureUnsupportedFlags, NotificationFailureClientUnavailable,
		NotificationFailureAdapterTimeout, NotificationFailureInvalidTemplate,
		NotificationFailureAdapterFailed:
		return true
	default:
		return false
	}
}

// NotificationListParams filters notification metadata. Results are ordered
// by creation time and notification id, both descending.
type NotificationListParams struct {
	State  NotificationState `json:"state,omitempty"`
	Cursor PageCursor        `json:"cursor,omitempty"`
	Limit  *int64            `json:"limit,omitempty" jsonschema:"minimum=1,maximum=200"`
}

// Validate checks the notification-list filter and page bound.
func (p NotificationListParams) Validate() error { return checkRoot(&p) }

// EffectiveLimit returns the requested limit or the default.
func (p NotificationListParams) EffectiveLimit() int64 { return effectiveListLimit(p.Limit) }

// NotificationListItem is one metadata-only human notification. It has no
// field capable of carrying message text or an approval request note.
type NotificationListItem struct {
	NotificationID  OpaqueID                  `json:"notification_id"`
	Class           NotificationClass         `json:"class"`
	ObjectID        OpaqueID                  `json:"object_id"`
	SenderAgent     AgentID                   `json:"sender_agent,omitempty"`
	RecipientAgents []AgentID                 `json:"recipient_agents" jsonschema:"maxItems=32,uniqueItems=true"`
	Priority        Priority                  `json:"priority,omitempty"`
	Reason          NotificationReason        `json:"reason"`
	State           NotificationState         `json:"state"`
	Adapter         NotificationAdapter       `json:"adapter"`
	Attempts        int64                     `json:"attempts" jsonschema:"minimum=0"`
	LastErrorReason NotificationFailureReason `json:"last_error_reason,omitempty"`
	CreatedAt       time.Time                 `json:"created_at"`
	UpdatedAt       time.Time                 `json:"updated_at"`
	LastAttemptAt   *time.Time                `json:"last_attempt_at" jsonschema:"nullable"`
	NextAttemptAt   *time.Time                `json:"next_attempt_at" jsonschema:"nullable"`
	RenderedAt      *time.Time                `json:"rendered_at" jsonschema:"nullable"`
}

// Validate checks one public notification row and its class-specific reason.
func (n NotificationListItem) Validate() error {
	if err := checkRoot(&n); err != nil {
		return err
	}
	switch n.Class {
	case NotificationClassMessage:
		if !ULID(n.ObjectID).Valid() {
			return &ValidationError{Path: notificationPathObjectID, Reason: "a message id"}
		}
		if n.Reason == NotificationReasonApprovalPending {
			return &ValidationError{Path: pathReason, Reason: "a message notification reason"}
		}
	case NotificationClassApproval:
		if !ApprovalID(n.ObjectID).Valid() {
			return &ValidationError{Path: notificationPathObjectID, Reason: "an approval id"}
		}
		if n.Reason != NotificationReasonBaseline && n.Reason != NotificationReasonApprovalPending {
			return &ValidationError{Path: pathReason, Reason: "an approval notification reason"}
		}
	}
	if n.Reason == NotificationReasonBaseline {
		if n.State != NotificationStateBaselined || n.Adapter != NotificationAdapterNone {
			return &ValidationError{Path: "state", Reason: "BASELINED with adapter NONE"}
		}
	} else if n.State == NotificationStateBaselined {
		return &ValidationError{Path: pathReason, Reason: "BASELINE when state is BASELINED"}
	}
	if (n.State == NotificationStateRendered) != (n.RenderedAt != nil) {
		return &ValidationError{Path: "rendered_at", Reason: "present iff state is RENDERED"}
	}
	return nil
}

// NotificationListResult is a stable, bounded page plus the total number of
// pending public notifications at the same read snapshot.
type NotificationListResult struct {
	Notifications []NotificationListItem `json:"notifications" jsonschema:"maxItems=200"`
	PendingCount  int64                  `json:"pending_count" jsonschema:"minimum=0"`
	NextCursor    PageCursor             `json:"next_cursor,omitempty"`
}

// Validate checks the notification page.
func (r NotificationListResult) Validate() error { return checkRoot(&r) }
