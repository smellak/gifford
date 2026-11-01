package agentlink

import (
	"context"
	"time"

	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
)

// State is the externally visible lifecycle of one agent link.
type State string

const (
	// StateDown means no authenticated connection is available.
	StateDown State = "DOWN"
	// StateConnecting means the link is validating or greeting a connection.
	StateConnecting State = "CONNECTING"
	// StateUp means the socket peer and greeting are authenticated.
	StateUp State = "UP"
	// StateStopped means the owner stopped the link.
	StateStopped State = "STOPPED"
)

// Reason is the stable reason a link most recently went down.
type Reason string

// Reason values describe why the last authenticated connection ended.
const (
	ReasonNone                 Reason = ""
	ReasonInsecureSocket       Reason = "INSECURE_SOCKET"
	ReasonPeerMismatch         Reason = "PEER_MISMATCH"
	ReasonSessionUnavailable   Reason = "SESSION_UNAVAILABLE"
	ReasonInvalidGreeting      Reason = "INVALID_GREETING"
	ReasonHeartbeatTimeout     Reason = "HEARTBEAT_TIMEOUT"
	ReasonConnectionLost       Reason = "CONNECTION_LOST"
	ReasonGuardWriteFailed     Reason = "GUARD_WRITE_FAILED"
	ReasonCapabilitiesConflict Reason = "CAPABILITIES_CONFLICT"
	ReasonUnsupported          Reason = "UNSUPPORTED_PLATFORM"
	ReasonStopped              Reason = "STOPPED"
)

// Snapshot is a race-safe copy of a link's current state.
type Snapshot struct {
	AgentID         protocol.AgentID
	SessionID       protocol.OpaqueID
	State           State
	Reason          Reason
	ConnectedAt     time.Time
	LastHeartbeatAt time.Time
	// LastReport is the session high-water report. HasReport is true only
	// after the current connection confirms that version or a newer one.
	LastReport protocol.InputGuardReport
	HasReport  bool
}

// DeliveryResult is a Proxy delivery report with its frame correlation.
// Results are published even when the caller that sent the order has stopped
// waiting for its reply.
type DeliveryResult struct {
	FrameID int64
	ReplyTo int64
	Result  protocol.DeliveryResult
}

// GuardWriter is the node's sole input_guards writer.
type GuardWriter interface {
	WriteInputGuard(context.Context, protocol.InputGuardReport) (bool, error)
}

// CapabilityWriter atomically applies one authenticated capability transition.
type CapabilityWriter interface {
	ApplyCapabilitiesChanged(context.Context, protocol.EventEnvelope) error
}

// DurableEventReceipt is the terminal storage result of one durable Proxy event.
// The authenticated link decides static NOT_ALLOWED cases; the writer may
// also return it for durable per-session limits after checking exact replay.
type DurableEventReceipt struct {
	Status protocol.EmitEventStatus
	Reason protocol.EmitEventRejectReason
}

// DurableEventWriter stores one validated general or native event. The link calls it
// outside the socket reader so database pressure cannot block heartbeats,
// input guards or delivery results.
type DurableEventWriter interface {
	StoreDurableEvent(context.Context, protocol.EventEnvelope) (DurableEventReceipt, error)
}

// ObservationSink takes the state observations of one authenticated Proxy.
// The link calls Submit from its own goroutine, never from the socket reader,
// and only with a report that validates and names the link's agent and
// session. A report Submit refuses is dropped; a later one replaces it. The
// sink is optional: without one, a valid observation is dropped.
type ObservationSink interface {
	Submit(protocol.StateObservationReport) error
}

// CapabilityLineage verifies that a native registration is the immutable
// predecessor of a session's durable degraded manifest. The caller supplies
// its own query handle so Hub registration can keep the check in its existing
// transaction.
type CapabilityLineage interface {
	MatchesCapabilitiesChanged(
		context.Context,
		registry.Querier,
		protocol.OpaqueID,
		protocol.CapabilityManifest,
		protocol.CapabilityManifest,
	) (bool, error)
}
