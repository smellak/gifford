package analyst

import (
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/screenproj"
)

// ErrInvalidObservation means the observation failed its validation; it
// names a field and a rule, never content.
var ErrInvalidObservation = errors.New("invalid observation")

// Bounds of an observation.
const (
	maxScreenRows  = 10000
	maxScreenCols  = 10000
	maxNativeEvent = 256
	maxChildren    = 64
	// maxIdleSeconds bounds seconds_since_last_output, a year.
	maxIdleSeconds = 365 * 24 * 3600
)

// Observation is what the Analyst sees of one agent at one moment.
type Observation struct {
	AgentID        protocol.AgentID
	Harness        protocol.Harness
	HarnessVersion string
	ObservedAt     time.Time
	// Screen is the reconstructed pane as text rows, top to bottom.
	Screen   []string
	Activity Activity
	// ChildProcesses are the new child processes. Nil means the process
	// source was not observed; empty means none started.
	ChildProcesses []string
	// ExitCode is set once the agent process has exited.
	ExitCode     *int
	NativeEvents []NativeEvent
	Previous     *PreviousState
	// Cols is the pane's width in cells, which the redactor's wrap model
	// needs.
	Cols int
	// Cursor is the terminal cursor on Screen, as captured with it.
	Cursor screenproj.Cursor
	// Guard is a copy of the input guard's last report for the agent
	// (report_input_guard), read when Screen was captured: a snapshot,
	// never the guard itself. Nil means none was read, and the PTY route
	// then sends nothing.
	Guard *protocol.InputGuardReport
	// GuardVersion is the input_guard_version Screen was captured under.
	// A Guard of any other version is stale.
	GuardVersion int64
}

// Activity is the output seen in the windows before ObservedAt.
type Activity struct {
	BytesLast500ms         int64
	BytesLast2s            int64
	BytesLast10s           int64
	SecondsSinceLastOutput float64
}

// NativeEvent is one structured event from the harness adapter.
type NativeEvent struct {
	Type protocol.EventType
	At   time.Time
}

// PreviousState is the last phase the Analyst reported.
type PreviousState struct {
	Phase protocol.AgentPhase
	Since time.Time
}

func obsErr(field, reason string) error {
	return fmt.Errorf("%w: %s: %s", ErrInvalidObservation, field, reason)
}

// Validate checks the observation.
func (o Observation) Validate() error {
	switch {
	case !o.AgentID.Valid():
		return obsErr("agent_id", "not an agent id")
	case !o.Harness.Valid():
		return obsErr("harness", "unknown harness")
	case len(o.HarnessVersion) > 64:
		return obsErr("harness_version", "too long")
	case o.ObservedAt.IsZero():
		return obsErr("observed_at", "required")
	case len(o.Screen) > maxScreenRows:
		return obsErr("screen", "too many rows")
	case o.Cols < 0 || o.Cols > maxScreenCols:
		return obsErr("cols", "out of range")
	case o.GuardVersion < 0:
		return obsErr("guard_version", "negative")
	case len(o.ChildProcesses) > maxChildren:
		return obsErr("child_processes", "too many")
	case len(o.NativeEvents) > maxNativeEvent:
		return obsErr("native_events", "too many")
	}
	a := o.Activity
	if a.BytesLast500ms < 0 || a.BytesLast500ms > a.BytesLast2s || a.BytesLast2s > a.BytesLast10s {
		return obsErr("activity", "byte windows must be non-negative and nested")
	}
	if math.IsNaN(a.SecondsSinceLastOutput) || math.IsInf(a.SecondsSinceLastOutput, 0) || a.SecondsSinceLastOutput < 0 || a.SecondsSinceLastOutput > maxIdleSeconds {
		return obsErr("activity.seconds_since_last_output", "must be a number in range")
	}
	for _, e := range o.NativeEvents {
		if !e.Type.Valid() {
			return obsErr("native_events.type", "unknown event type")
		}
		if e.At.IsZero() || e.At.After(o.ObservedAt) {
			return obsErr("native_events.at", "must be set and not after observed_at")
		}
	}
	if p := o.Previous; p != nil {
		if !p.Phase.Valid() {
			return obsErr("previous.phase", "unknown phase")
		}
		if p.Since.IsZero() || p.Since.After(o.ObservedAt) {
			return obsErr("previous.since", "must be set and not after observed_at")
		}
	}
	return nil
}
