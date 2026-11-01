package orchestrator

import (
	"errors"
	"fmt"

	"github.com/smellak/gifford/internal/protocol"
)

// ProcessToken distinguishes a process from a later process that reused its
// pid. StartTime is the raw Linux /proc starttime field.
type ProcessToken struct {
	PID       int64
	StartTime int64
}

// ProcessNode is one observed descendant and the exact process identity of
// its parent. Executable is the stable resolved /proc/<pid>/exe target, never
// argv[0] or the truncated comm field. It is empty when the target could not
// be read twice without changing, which keeps the node conservatively
// untrusted.
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

// processObservationIncompleteError marks a descendant traversal that raced
// with process or thread teardown. Callers must reject that observation for
// the current delivery, but it does not prove the durable binding is corrupt.
type processObservationIncompleteError struct {
	pid   int64
	cause error
}

func (e *processObservationIncompleteError) Error() string {
	return fmt.Sprintf("process guard: observation incomplete at pid %d: %v", e.pid, e.cause)
}

func (e *processObservationIncompleteError) Unwrap() error { return e.cause }

func newProcessObservationIncompleteError(pid int64, cause error) error {
	return &processObservationIncompleteError{pid: pid, cause: cause}
}

func isProcessObservationIncomplete(err error) bool {
	var incomplete *processObservationIncompleteError
	return errors.As(err, &incomplete)
}

// ProcessInspector returns one process-tree observation below the stored
// session process. Root observation errors are hard failures. Descendant
// teardown makes the whole observation transiently incomplete; an unstable
// executable remains as an untrusted node with an empty Executable.
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
