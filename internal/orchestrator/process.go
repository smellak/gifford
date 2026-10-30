package orchestrator

import "github.com/smellak/gifford/internal/protocol"

// ProcessToken distinguishes a process from a later process that reused its
// pid. StartTime is the raw Linux /proc starttime field.
type ProcessToken struct {
	PID       int64
	StartTime int64
}

// ProcessInspector returns every descendant below the stored session
// process. A read error is a hard guard failure, never an empty process set.
type ProcessInspector interface {
	Descendants(protocol.ProcessIdentity) ([]ProcessToken, error)
}
