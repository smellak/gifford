package notifications

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/smellak/gifford/internal/protocol"
)

const (
	// FixedPrefix makes every rendered notice recognizable and prevents a
	// metadata template from becoming an argv option.
	FixedPrefix = "[GIFFORD] "
	// DefaultTemplate contains metadata only. Message bodies and approval
	// request_note values are neither queried nor accepted by this type.
	DefaultTemplate = FixedPrefix + "{class} {reason} from {sender} for {recipient_count} recipient(s) id {id}"
	// MaxTemplateRunes bounds configured template text.
	MaxTemplateRunes = 256
	// MaxNoticeRunes bounds rendered notification text.
	MaxNoticeRunes = 512
	// DefaultDuration is how long tmux keeps one notice visible.
	DefaultDuration = 5 * time.Second
	// DefaultTimeout bounds the tmux subprocess.
	DefaultTimeout = time.Second
)

var (
	// ErrInvalidOptions means the service or adapter cannot uphold its durable
	// and privacy invariants.
	ErrInvalidOptions = errors.New("notifications: invalid options")
	// ErrInvalidRecord means a durable row is outside the writer's own schema
	// contract. The error never includes the row contents.
	ErrInvalidRecord = errors.New("notifications: invalid durable record")
)

// Class identifies the durable object that caused a notice.
type Class string

const (
	// ClassMessage identifies a local message notice.
	ClassMessage Class = "MESSAGE"
	// ClassApproval identifies a pending-approval notice.
	ClassApproval Class = "APPROVAL"
	classService  Class = "SERVICE"
)

func (v Class) valid() bool {
	return v == ClassMessage || v == ClassApproval || v == classService
}

// Reason is source metadata, never free-form display text.
type Reason string

const (
	// ReasonBaseline identifies an existing object captured at first start.
	ReasonBaseline Reason = "BASELINE"
	// ReasonMessageAvailable identifies an accepted message.
	ReasonMessageAvailable Reason = "MESSAGE_AVAILABLE"
	// ReasonDisplayOnly identifies a message with a display-only recipient.
	ReasonDisplayOnly Reason = "DISPLAY_ONLY"
	// ReasonChannelHeld identifies a retained channel hold.
	ReasonChannelHeld Reason = "CHANNEL_HELD"
	// ReasonChannelApprovalRequired identifies a retained channel approval gate.
	ReasonChannelApprovalRequired Reason = "CHANNEL_APPROVAL_REQUIRED"
	// ReasonApprovalPending identifies an unresolved approval request.
	ReasonApprovalPending Reason = "APPROVAL_PENDING"
)

func (v Reason) valid() bool {
	switch v {
	case ReasonBaseline, ReasonMessageAvailable, ReasonDisplayOnly,
		ReasonChannelHeld, ReasonChannelApprovalRequired, ReasonApprovalPending:
		return true
	default:
		return false
	}
}

// State is the notification outbox state. BASELINED rows deliberately never
// produce an event or external effect.
type State string

const (
	// StateBaselined marks a historical row that is never rendered.
	StateBaselined State = "BASELINED"
	// StatePending marks a durable row awaiting external rendering.
	StatePending State = "PENDING"
	// StateRendered marks a row whose adapter call succeeded.
	StateRendered State = "RENDERED"
)

func (v State) valid() bool {
	return v == StateBaselined || v == StatePending || v == StateRendered
}

// AdapterKind is the adapter recorded with the durable notice.
type AdapterKind string

const (
	// AdapterNone records that no external adapter applies.
	AdapterNone AdapterKind = "NONE"
	// AdapterTMUX records the explicit tmux display-message adapter.
	AdapterTMUX AdapterKind = "TMUX"
)

func (v AdapterKind) valid() bool { return v == AdapterNone || v == AdapterTMUX }

// FailureReason is a fixed machine reason. Adapter diagnostics, paths and
// external output are never persisted or logged.
type FailureReason string

const (
	// FailureAdapterDisabled means notification rendering is not configured.
	FailureAdapterDisabled FailureReason = "ADAPTER_DISABLED"
	// FailureAdapterUnavailable means the tmux command cannot run.
	FailureAdapterUnavailable FailureReason = "ADAPTER_UNAVAILABLE"
	// FailureUnsupportedFlags means tmux lacks the required literal-mode flags.
	FailureUnsupportedFlags FailureReason = "UNSUPPORTED_FLAGS"
	// FailureClientUnavailable means the configured tmux client is absent.
	FailureClientUnavailable FailureReason = "CLIENT_UNAVAILABLE"
	// FailureAdapterTimeout means the bounded tmux call timed out.
	FailureAdapterTimeout FailureReason = "ADAPTER_TIMEOUT"
	// FailureInvalidTemplate means metadata rendering violated its closed contract.
	FailureInvalidTemplate FailureReason = "INVALID_TEMPLATE"
	// FailureAdapterFailed is the fixed fallback for an unclassified adapter error.
	FailureAdapterFailed FailureReason = "ADAPTER_FAILED"
)

func (v FailureReason) valid() bool {
	switch v {
	case FailureAdapterDisabled, FailureAdapterUnavailable, FailureUnsupportedFlags,
		FailureClientUnavailable, FailureAdapterTimeout, FailureInvalidTemplate,
		FailureAdapterFailed:
		return true
	default:
		return false
	}
}

// Notification is the metadata-only registry row supplied to an adapter.
type Notification struct {
	ID              protocol.OpaqueID
	Class           Class
	ObjectID        protocol.OpaqueID
	Sender          protocol.AgentID
	LocalRecipients []protocol.AgentID
	Priority        protocol.Priority
	Reason          Reason
	State           State
	Adapter         AdapterKind
	Attempts        int
	CreatedAt       time.Time
	UpdatedAt       time.Time
	LastAttemptAt   *time.Time
	NextAttemptAt   *time.Time
	RenderedAt      *time.Time
	LastError       FailureReason
}

func (n Notification) validate() error {
	if !n.ID.Valid() || !n.Class.valid() || !n.ObjectID.Valid() || !n.Reason.valid() ||
		!n.State.valid() || !n.Adapter.valid() || n.Attempts < 0 || n.CreatedAt.IsZero() ||
		n.UpdatedAt.IsZero() || n.Sender != "" && !n.Sender.Valid() ||
		n.Priority != "" && !n.Priority.Valid() || n.LastError != "" && !n.LastError.valid() ||
		len(n.LocalRecipients) > 32 {
		return ErrInvalidRecord
	}
	if n.Class == ClassMessage && !protocol.ULID(n.ObjectID).Valid() {
		return ErrInvalidRecord
	}
	if n.Class == ClassApproval && !protocol.ApprovalID(n.ObjectID).Valid() {
		return ErrInvalidRecord
	}
	if n.Class == classService && (n.Reason != ReasonBaseline || n.State != StateBaselined) {
		return ErrInvalidRecord
	}
	if n.Reason == ReasonBaseline {
		if n.State != StateBaselined || n.Adapter != AdapterNone {
			return ErrInvalidRecord
		}
	} else if n.State == StateBaselined {
		return ErrInvalidRecord
	}
	switch n.Class {
	case ClassMessage:
		if n.Reason != ReasonBaseline && n.Reason != ReasonMessageAvailable &&
			n.Reason != ReasonDisplayOnly && n.Reason != ReasonChannelHeld &&
			n.Reason != ReasonChannelApprovalRequired {
			return ErrInvalidRecord
		}
	case ClassApproval:
		if n.Reason != ReasonBaseline && n.Reason != ReasonApprovalPending {
			return ErrInvalidRecord
		}
	case classService:
	default:
		return ErrInvalidRecord
	}
	seen := make(map[protocol.AgentID]struct{}, len(n.LocalRecipients))
	for _, recipient := range n.LocalRecipients {
		if !recipient.Valid() {
			return ErrInvalidRecord
		}
		if _, ok := seen[recipient]; ok {
			return ErrInvalidRecord
		}
		seen[recipient] = struct{}{}
	}
	return nil
}

// EventPayload is the strict metadata payload for human_notification.created
// and human_notification.rendered. It has no field capable of carrying body
// text or an approval request_note.
type EventPayload struct {
	NotificationID protocol.OpaqueID `json:"notification_id"`
	Class          Class             `json:"class"`
	ObjectID       protocol.OpaqueID `json:"object_id"`
	Reason         Reason            `json:"reason"`
	State          State             `json:"state"`
	Adapter        AdapterKind       `json:"adapter"`
}

func (p EventPayload) validateFor(eventType protocol.EventType) error {
	if !p.NotificationID.Valid() || !p.Class.valid() || p.Class == classService ||
		!p.ObjectID.Valid() || !p.Reason.valid() || !p.Adapter.valid() {
		return ErrInvalidRecord
	}
	if p.Class == ClassMessage && !protocol.ULID(p.ObjectID).Valid() ||
		p.Class == ClassApproval && !protocol.ApprovalID(p.ObjectID).Valid() {
		return ErrInvalidRecord
	}
	if p.Reason == ReasonBaseline ||
		p.Class == ClassApproval && p.Reason != ReasonApprovalPending ||
		p.Class == ClassMessage && p.Reason == ReasonApprovalPending {
		return ErrInvalidRecord
	}
	switch eventType {
	case protocol.EventTypeHumanNotificationCreated:
		if p.State != StatePending {
			return ErrInvalidRecord
		}
	case protocol.EventTypeHumanNotificationRendered:
		if p.State != StateRendered {
			return ErrInvalidRecord
		}
	default:
		return ErrInvalidRecord
	}
	return nil
}

func marshalEventPayload(eventType protocol.EventType, payload EventPayload) (protocol.Payload, error) {
	if err := payload.validateFor(eventType); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal notification event payload: %w", err)
	}
	return protocol.Payload(raw), nil
}

// DecodeEventPayload strictly decodes a payload emitted by this package.
// It is used by readers and tests without accepting unknown future fields.
func DecodeEventPayload(eventType protocol.EventType, raw protocol.Payload) (EventPayload, error) {
	var payload EventPayload
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&payload); err != nil {
		return EventPayload{}, fmt.Errorf("decode notification event payload: %w", ErrInvalidRecord)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return EventPayload{}, fmt.Errorf("decode notification event payload: %w", ErrInvalidRecord)
	}
	if err := payload.validateFor(eventType); err != nil {
		return EventPayload{}, err
	}
	return payload, nil
}

// Adapter performs the external, non-transactional render operation.
type Adapter interface {
	Name() AdapterKind
	Render(context.Context, Notification) error
}

// AdapterError carries only a fixed reason and retry policy.
type AdapterError struct {
	Reason    FailureReason
	Retryable bool
}

func (e *AdapterError) Error() string { return string(e.Reason) }

type disabledAdapter struct{}

// DisabledAdapter returns the adapter used when notifications configuration
// is absent. It records a fixed failure and never performs an external action.
func DisabledAdapter() Adapter { return disabledAdapter{} }

func (disabledAdapter) Name() AdapterKind { return AdapterNone }

func (disabledAdapter) Render(context.Context, Notification) error {
	return &AdapterError{Reason: FailureAdapterDisabled}
}

var templateFields = map[string]struct{}{
	"id": {}, "class": {}, "sender": {}, "recipient_count": {}, "priority": {}, "reason": {},
}

// ValidateTemplate accepts only bounded literal metadata templates with the
// fixed prefix and a closed placeholder vocabulary.
func ValidateTemplate(template string) error {
	if !strings.HasPrefix(template, FixedPrefix) || utf8.RuneCountInString(template) > MaxTemplateRunes {
		return fmt.Errorf("notification template: %w", ErrInvalidOptions)
	}
	lower := strings.ToLower(template)
	for _, forbidden := range []string{"body", "request_note", "secret", "token", "password", "credential"} {
		if strings.Contains(lower, forbidden) {
			return fmt.Errorf("notification template: %w", ErrInvalidOptions)
		}
	}
	for _, r := range template {
		if r < 0x20 || r == 0x7f || r == '/' || r == '\\' {
			return fmt.Errorf("notification template: %w", ErrInvalidOptions)
		}
	}
	for rest := template; ; {
		open := strings.IndexByte(rest, '{')
		closeIndex := strings.IndexByte(rest, '}')
		if open < 0 && closeIndex < 0 {
			break
		}
		if open < 0 || closeIndex < 0 || closeIndex < open {
			return fmt.Errorf("notification template: %w", ErrInvalidOptions)
		}
		field := rest[open+1 : closeIndex]
		if _, ok := templateFields[field]; !ok {
			return fmt.Errorf("notification template: %w", ErrInvalidOptions)
		}
		rest = rest[closeIndex+1:]
	}
	return nil
}

func renderTemplate(template string, notification Notification) (string, error) {
	if err := ValidateTemplate(template); err != nil {
		return "", &AdapterError{Reason: FailureInvalidTemplate}
	}
	sender := "unknown"
	if notification.Sender != "" {
		sender = string(notification.Sender)
	}
	priority := "NONE"
	if notification.Priority != "" {
		priority = string(notification.Priority)
	}
	notice := strings.NewReplacer(
		"{id}", string(notification.ID),
		"{class}", string(notification.Class),
		"{sender}", sender,
		"{recipient_count}", fmt.Sprintf("%d", len(notification.LocalRecipients)),
		"{priority}", priority,
		"{reason}", string(notification.Reason),
	).Replace(template)
	if !strings.HasPrefix(notice, FixedPrefix) || strings.HasPrefix(notice, "-") ||
		utf8.RuneCountInString(notice) > MaxNoticeRunes {
		return "", &AdapterError{Reason: FailureInvalidTemplate}
	}
	return notice, nil
}
