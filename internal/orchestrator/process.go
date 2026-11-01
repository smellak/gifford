package orchestrator

import "github.com/smellak/gifford/internal/protocol"

// ProcessToken distinguishes a process from a later process that reused its
// pid. StartTime is the raw Linux /proc starttime field.
type ProcessToken struct {
	PID       int64
	StartTime int64
}

// ProcessNode is one observed descendant and the exact process identity of
// its parent. Executable is the resolved /proc/<pid>/exe target, never argv[0]
// or the truncated comm field.
type ProcessNode struct {
	Token      ProcessToken
	Parent     ProcessToken
	Executable string
}

// ProcessTree is one fail-closed observation of the authenticated gifford run
// process and everything below it. Parent edges stay in the observation so the
// delivery guard can distinguish one measured auxiliary from tools below it.
type ProcessTree struct {
	BootID      string
	Root        ProcessToken
	Descendants []ProcessNode
}

// ProcessInspector returns one process-tree observation below the stored
// session process. A read error is a hard guard failure, never an empty tree.
type ProcessInspector interface {
	Inspect(protocol.ProcessIdentity) (ProcessTree, error)
}

// RegisteredEndpointProvider exposes only the exact process identity of the
// live Gifford endpoint registered for an agent session. It grants no trust to
// the endpoint's ancestors or children.
type RegisteredEndpointProvider interface {
	RegisteredEndpointProcess(
		protocol.AgentID,
		protocol.OpaqueID,
	) (pid int64, startTime int64, ok bool)
}
