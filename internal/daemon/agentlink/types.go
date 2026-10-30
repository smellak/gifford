package agentlink

import (
	"context"
	"time"

	"github.com/smellak/gifford/internal/protocol"
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
	ReasonNone               Reason = ""
	ReasonInsecureSocket     Reason = "INSECURE_SOCKET"
	ReasonPeerMismatch       Reason = "PEER_MISMATCH"
	ReasonSessionUnavailable Reason = "SESSION_UNAVAILABLE"
	ReasonInvalidGreeting    Reason = "INVALID_GREETING"
	ReasonHeartbeatTimeout   Reason = "HEARTBEAT_TIMEOUT"
	ReasonConnectionLost     Reason = "CONNECTION_LOST"
	ReasonGuardWriteFailed   Reason = "GUARD_WRITE_FAILED"
	ReasonResultQueueFull    Reason = "RESULT_QUEUE_FULL"
	ReasonUnsupported        Reason = "UNSUPPORTED_PLATFORM"
	ReasonStopped            Reason = "STOPPED"
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
