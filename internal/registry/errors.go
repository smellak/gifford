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
	// ErrPeerNotFound means the named remote node has not been registered.
	ErrPeerNotFound = errors.New("peer not found")
	// ErrPeerNotTrusted means directory data came from a peer that is not
	// currently trusted.
	ErrPeerNotTrusted = errors.New("peer is not trusted")
	// ErrPeerRevoked means a peer reached its terminal trust state.
	ErrPeerRevoked = errors.New("peer is revoked")
	// ErrNodeKeyMismatch means a node id was presented with different key
	// material from the key already pinned for it.
	ErrNodeKeyMismatch = errors.New("node signing key mismatch")
	// ErrKeyConflict means a key id was presented with different signed
	// content from the record already stored under that id.
	ErrKeyConflict = errors.New("agent key record conflict")
	// ErrAgentConflict means two nodes claim the same global agent id.
	ErrAgentConflict = errors.New("agent id belongs to another node")
)
