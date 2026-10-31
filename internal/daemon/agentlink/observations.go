package agentlink

import (
	"time"

	"github.com/smellak/gifford/internal/protocol"
)

// observationHead is the evidence of an observation that only rises within a
// session: its generations, its guard version and its time.
type observationHead struct {
	observedAt                        time.Time
	observation, output, event, guard int64
}

func headOf(report protocol.StateObservationReport) observationHead {
	return observationHead{
		observedAt: report.ObservedAt, observation: report.ObservationGeneration,
		output: report.OutputGeneration, event: report.EventGeneration,
		guard: report.InputGuardVersion,
	}
}

// newerThan is the state producer's order: next goes back on no generation
// and no guard version, and moves on in one of them or in time. A refresh of
// the same evidence is newer; a fresher id that rolls any evidence back is
// not.
func (next observationHead) newerThan(current observationHead) bool {
	if next.observation < current.observation || next.output < current.output ||
		next.event < current.event || next.guard < current.guard {
		return false
	}
	return next.observation > current.observation || next.output > current.output ||
		next.event > current.event || next.guard > current.guard ||
		next.observedAt.After(current.observedAt)
}

// offerObservation keeps report as the only one waiting for the sink and
// never waits itself, so a busy sink cannot stall the socket reader. A report
// not newer than the highest evidence taken on this session is ignored,
// whether a report is waiting, the sink is busy or the connection is new, so
// a later report never erases evidence the sink has not seen. A newer report
// replaces the one waiting: the sink only needs the newest.
func (c *Client) offerObservation(report protocol.StateObservationReport) {
	if c.opts.Observations == nil {
		return
	}
	head := headOf(report)
	c.observationMu.Lock()
	if c.observationSeen && !head.newerThan(c.observationHigh) {
		c.observationMu.Unlock()
		return
	}
	c.observationHigh, c.observationSeen = head, true
	c.observation = &report
	c.observationMu.Unlock()
	select {
	case c.observationWake <- struct{}{}:
	default:
	}
}

// forwardObservations is the only caller of the sink. The link persists no
// observation, and a refused one is dropped without its contents.
func (c *Client) forwardObservations() {
	defer c.wg.Done()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-c.observationWake:
		}
		c.observationMu.Lock()
		report := c.observation
		c.observation = nil
		c.observationMu.Unlock()
		if report != nil {
			_ = c.opts.Observations.Submit(*report)
		}
	}
}
