package registry

import "errors"

// Registry sentinels. Use errors.Is to match them.
var (
	// ErrInvalidArgument means a caller-supplied argument breaks its rules.
	ErrInvalidArgument = errors.New("invalid argument")
	// ErrNoUsableKey means the agent has no signing key to use: none,
	// or the stored rows do not verify, are expired, revoked, suspended,
	// or ambiguously duplicated. Reads fail closed.
	ErrNoUsableKey = errors.New("no usable signing key")
	// ErrUnrelatedProcess means the registering process does not descend
	// from a live registered session of the agent.
	ErrUnrelatedProcess = errors.New("process does not descend from a registered session")
	// ErrBlocked means the agent is blocked and only a Review unblocks it.
	ErrBlocked = errors.New("agent is blocked")
	// ErrRevoked means the agent holds a revoked key, which no Review
	// restores.
	ErrRevoked = errors.New("agent key revoked")
	// ErrNotBlocked means a Review was requested for an agent with no
	// suspended key to resolve.
	ErrNotBlocked = errors.New("agent is not blocked")
	// ErrNoRegisteredAncestor means no ancestor of the process matches a
	// live registered session.
	ErrNoRegisteredAncestor = errors.New("no registered ancestor session")
)
