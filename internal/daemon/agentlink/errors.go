package agentlink

import (
	"errors"
	"fmt"

	"github.com/smellak/gifford/internal/protocol"
)

var (
	// ErrInvalidOptions means the link cannot be constructed safely.
	ErrInvalidOptions = errors.New("agentlink: invalid options")
	// ErrInsecureSocket means the socket or its parent directory is not private.
	ErrInsecureSocket = errors.New("agentlink: socket is not private")
	// ErrPeerMismatch means the socket peer is not the process of the live Hub session.
	ErrPeerMismatch = errors.New("agentlink: socket peer does not own the session")
	// ErrSessionUnavailable means the Hub session is not live and deliverable.
	ErrSessionUnavailable = errors.New("agentlink: session unavailable")
	// ErrInvalidGreeting means the Proxy greeting does not match the Hub session.
	ErrInvalidGreeting = errors.New("agentlink: invalid greeting")
	// ErrHeartbeatTimeout means the Proxy stopped sending its liveness heartbeat.
	ErrHeartbeatTimeout = errors.New("agentlink: heartbeat timeout")
	// ErrLinkDown means an operation had no authenticated live connection.
	ErrLinkDown = errors.New("agentlink: link down")
	// ErrUnexpectedFrame means an authenticated Proxy sent a frame for another identity.
	ErrUnexpectedFrame = errors.New("agentlink: unexpected frame")
	// ErrDeliveryResultQueueFull means the durable-result consumer stopped
	// keeping up. The link disconnects instead of growing memory or silently
	// discarding a delivery result.
	ErrDeliveryResultQueueFull = errors.New("agentlink: delivery result queue full")
	// ErrUnsupportedPlatform means peer process credentials cannot be verified here.
	ErrUnsupportedPlatform = errors.New("agentlink: peer verification unsupported")
	// ErrRemote means the Proxy answered an operation with an error frame.
	ErrRemote = errors.New("agentlink: proxy error")
)

// RemoteError is a validated error frame returned by the Proxy. Its message is
// one of the Proxy's fixed protocol messages and never includes delivery text.
type RemoteError struct {
	Code    protocol.FrameErrorCode
	Message string
}

// Error implements error.
func (e *RemoteError) Error() string {
	return fmt.Sprintf("%v: %s", ErrRemote, e.Code)
}

// Unwrap makes errors.Is(err, ErrRemote) true.
func (e *RemoteError) Unwrap() error { return ErrRemote }
