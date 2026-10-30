package orchestrator

import "github.com/smellak/gifford/internal/protocol"

// ProcessToken distinguishes a process from a later process that reused its
// pid. StartTime is the raw Linux /proc starttime field.
type ProcessToken struct {
	PID       int64
	StartTime int64
}

// ProcessTree is one fail-closed observation of the authenticated gifford run
// process and everything below it. DirectChildren is kept separate because a
// session binds exactly one harness child; Descendants also includes tools and
// wrappers below that child.
type ProcessTree struct {
	BootID         string
	Root           ProcessToken
	DirectChildren []ProcessToken
	Descendants    []ProcessToken
}

// ProcessInspector returns one process-tree observation below the stored
// session process. A read error is a hard guard failure, never an empty tree.
type ProcessInspector interface {
	Inspect(protocol.ProcessIdentity) (ProcessTree, error)
}
