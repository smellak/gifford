package protocol

import (
	"fmt"
	"time"
)

// BridgeReminderSchemaV1 is the bridge response-reminder wire schema.
const BridgeReminderSchemaV1 = "gifford.bridge.response-reminder/v1"

// BridgeReminderExpectationID identifies one response expectation.
type BridgeReminderExpectationID string

// JSONPattern returns the fixed response-expectation grammar.
func (BridgeReminderExpectationID) JSONPattern() string { return `^rsp-[0-9a-f]{32}$` }

// Valid reports whether the identifier uses the canonical lowercase form.
func (v BridgeReminderExpectationID) Valid() bool {
	const prefix = "rsp-"
	value := string(v)
	if len(value) != len(prefix)+32 || value[:len(prefix)] != prefix {
		return false
	}
	for _, c := range value[len(prefix):] {
		if c < '0' || (c > '9' && c < 'a') || c > 'f' {
			return false
		}
	}
	return true
}

// BridgeReminderCancelReason is the bounded reason attached to cancellation
// and tombstone requests.
type BridgeReminderCancelReason string

// Bridge reminder cancellation reasons.
const (
	BridgeReminderCancelExpectationResolved BridgeReminderCancelReason = "EXPECTATION_RESOLVED"
	BridgeReminderCancelModeSwitch          BridgeReminderCancelReason = "MODE_SWITCH"
	BridgeReminderCancelRollback            BridgeReminderCancelReason = "ROLLBACK"
)

// EnumValues returns cancellation reasons in wire order.
func (BridgeReminderCancelReason) EnumValues() []string {
	return []string{
		string(BridgeReminderCancelExpectationResolved),
		string(BridgeReminderCancelModeSwitch),
		string(BridgeReminderCancelRollback),
	}
}

// Valid reports whether the reason is part of the closed wire set.
func (v BridgeReminderCancelReason) Valid() bool {
	switch v {
	case BridgeReminderCancelExpectationResolved, BridgeReminderCancelModeSwitch,
		BridgeReminderCancelRollback:
		return true
	default:
		return false
	}
}

// BridgeReminderTerminalState is the closed state set a tombstone receipt may
// expose. It is intentionally narrower than the full reminder lifecycle.
type BridgeReminderTerminalState string

// Terminal reminder states.
const (
	BridgeReminderTerminalDelivered BridgeReminderTerminalState = "DELIVERED"
	BridgeReminderTerminalExpired   BridgeReminderTerminalState = "EXPIRED"
	BridgeReminderTerminalCancelled BridgeReminderTerminalState = "CANCELLED"
	BridgeReminderTerminalUncertain BridgeReminderTerminalState = "UNCERTAIN"
)

// EnumValues returns terminal reminder states in lifecycle order.
func (BridgeReminderTerminalState) EnumValues() []string {
	return []string{
		string(BridgeReminderTerminalDelivered),
		string(BridgeReminderTerminalExpired),
		string(BridgeReminderTerminalCancelled),
		string(BridgeReminderTerminalUncertain),
	}
}

// Valid reports whether the state is terminal.
func (v BridgeReminderTerminalState) Valid() bool {
	switch v {
	case BridgeReminderTerminalDelivered, BridgeReminderTerminalExpired,
		BridgeReminderTerminalCancelled, BridgeReminderTerminalUncertain:
		return true
	default:
		return false
	}
}

// BridgeReminderDisposition distinguishes a newly accepted operation from an
// exact replay of an existing reminder PUT.
type BridgeReminderDisposition string

// Bridge reminder dispositions.
const (
	BridgeReminderDispositionCreated   BridgeReminderDisposition = "CREATED"
	BridgeReminderDispositionDuplicate BridgeReminderDisposition = "DUPLICATE"
)

// EnumValues returns reminder dispositions in wire order.
func (BridgeReminderDisposition) EnumValues() []string {
	return []string{
		string(BridgeReminderDispositionCreated),
		string(BridgeReminderDispositionDuplicate),
	}
}

// Valid reports whether the disposition is known.
func (v BridgeReminderDisposition) Valid() bool {
	return v == BridgeReminderDispositionCreated || v == BridgeReminderDispositionDuplicate
}

// BridgeReminderTombstoneMarker is true on every v1.11 terminal tombstone
// receipt. Naming the scalar lets the schema generator publish that constant.
type BridgeReminderTombstoneMarker bool

// BridgeReminderTombstoneParams installs a durable terminal key. SourceNodeID
// is additionally required to match the serving node at the authenticated
// bridge boundary.
type BridgeReminderTombstoneParams struct {
	Schema        string                      `json:"schema" jsonschema:"enum=gifford.bridge.response-reminder/v1"`
	SourceNodeID  NodeID                      `json:"source_node_id"`
	ExpectationID BridgeReminderExpectationID `json:"expectation_id"`
	Generation    int64                       `json:"generation" jsonschema:"minimum=1"`
	Reason        BridgeReminderCancelReason  `json:"reason"`
}

// Validate checks the versioned tombstone payload. The serving Hub performs
// the node-local equality check because that identity is not part of a schema.
func (p BridgeReminderTombstoneParams) Validate() error {
	if err := checkRoot(&p); err != nil {
		return err
	}
	if p.Schema != BridgeReminderSchemaV1 {
		return &ValidationError{Path: "schema", Reason: "unsupported schema"}
	}
	return nil
}

// BridgeReminderReceiptV111 is the contracts-v1.11 receipt. Tombstone is true
// only after the durable anti-creation record commits; that receipt is always
// terminal and remains byte-stable across exact retries.
type BridgeReminderReceiptV111 struct {
	ReminderID        ULID                          `json:"reminder_id"`
	GiffordMessageID  ULID                          `json:"gifford_message_id"`
	Disposition       BridgeReminderDisposition     `json:"disposition"`
	State             BridgeReminderTerminalState   `json:"state"`
	AcceptedAt        string                        `json:"accepted_at" jsonschema:"format=date-time"`
	ExpiresAt         string                        `json:"expires_at" jsonschema:"format=date-time"`
	DeliveryAttemptID *string                       `json:"delivery_attempt_id" jsonschema:"nullable"`
	Tombstone         BridgeReminderTombstoneMarker `json:"tombstone"`
}

// Validate checks receipt scalars and the tombstone terminal invariant.
func (r BridgeReminderReceiptV111) Validate() error {
	if err := checkRoot(&r); err != nil {
		return err
	}
	accepted, err := time.Parse(time.RFC3339Nano, r.AcceptedAt)
	if err != nil {
		return &ValidationError{Path: "accepted_at", Reason: "must be RFC3339"}
	}
	expires, err := time.Parse(time.RFC3339Nano, r.ExpiresAt)
	if err != nil {
		return &ValidationError{Path: "expires_at", Reason: "must be RFC3339"}
	}
	if expires.Before(accepted) {
		return &ValidationError{Path: "expires_at", Reason: "precedes accepted_at"}
	}
	if r.DeliveryAttemptID != nil && !OpaqueID(*r.DeliveryAttemptID).Valid() {
		return &ValidationError{Path: "delivery_attempt_id", Reason: "invalid opaque id"}
	}
	if !bool(r.Tombstone) {
		return &ValidationError{Path: "tombstone", Reason: "must be true"}
	}
	if !r.State.Valid() {
		return &ValidationError{Path: durablePathState, Reason: fmt.Sprintf("tombstone cannot be %s", r.State)}
	}
	return nil
}
