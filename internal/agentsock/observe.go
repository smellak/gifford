//go:build unix

package agentsock

import (
	"sync"

	"github.com/smellak/gifford/internal/protocol"
)

// lastObservation is the last state observation the socket wrote, on any
// connection. One connection is served at a time, so only its writer sets it.
type lastObservation struct {
	mu     sync.Mutex
	report protocol.StateObservationReport
	ok     bool
}

func (l *lastObservation) get() (protocol.StateObservationReport, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.report, l.ok
}

func (l *lastObservation) set(report protocol.StateObservationReport) {
	l.mu.Lock()
	l.report, l.ok = report, true
	l.mu.Unlock()
}

// nextObservation is the observation to write now, or false for none. The
// first one of a connection is the last one written again, unchanged, when it
// belongs to this agent and session. Any other is a new cut, with the
// connection's ids and the registered harness, written only when it
// validates and moves on from the last one.
func (c *conn) nextObservation(first bool) (protocol.StateObservationReport, bool) {
	last, hasLast := c.last.get()
	hasLast = hasLast && last.AgentID == c.agent && last.SessionID == c.sessionID
	if first && hasLast {
		return last, true
	}
	r, ok := c.opts.Observations.Observe()
	if !ok {
		return protocol.StateObservationReport{}, false
	}
	manifest := c.opts.Register.Manifest
	r.AgentID, r.SessionID = c.agent, c.sessionID
	r.Harness, r.HarnessVersion = manifest.Harness, manifest.HarnessVersion
	if r.Validate() != nil || hasLast && !movesOn(last, r) {
		return protocol.StateObservationReport{}, false
	}
	return r, true
}

// movesOn reports whether next is a new observation after last, of the same
// session: a new id and a higher observation generation, no earlier time,
// and no output generation, event generation or guard version below last's.
func movesOn(last, next protocol.StateObservationReport) bool {
	return next.ObservationID != last.ObservationID &&
		next.ObservationGeneration > last.ObservationGeneration &&
		next.OutputGeneration >= last.OutputGeneration &&
		next.EventGeneration >= last.EventGeneration &&
		next.InputGuardVersion >= last.InputGuardVersion &&
		!next.ObservedAt.Before(last.ObservedAt)
}

// supportObservations records that the Core takes state observations on
// this connection. It is the reader's, and only the first announcement
// counts: a later one changes nothing.
func (c *conn) supportObservations() {
	if c.announced {
		return
	}
	c.announced = true
	close(c.observing)
}
