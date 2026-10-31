package agentlink

import "github.com/smellak/gifford/internal/protocol"

// offerObservation keeps report as the only one waiting for the sink and
// never waits itself, so a busy sink cannot stall the socket reader. A report
// still waiting is replaced: the sink only needs the newest.
func (c *Client) offerObservation(report protocol.StateObservationReport) {
	if c.opts.Observations == nil {
		return
	}
	c.observationMu.Lock()
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
