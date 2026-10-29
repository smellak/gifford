package gateway

import (
	"sync"
	"time"

	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/protocol"
)

// Design limits of the gateway (PRD §9.7).
const (
	// breakerThreshold consecutive endpoint failures open the breaker.
	breakerThreshold = 3
	// breakerOpenFor is how long an open breaker refuses calls.
	breakerOpenFor = 30 * time.Second
)

// contractTimeout is the deadline of one call of a contract.
func contractTimeout(c protocol.ModelContract) time.Duration {
	switch c {
	case protocol.ModelContractTriageMessage:
		return 5 * time.Second
	default:
		// ANALYZE_AGENT_STATE and CHOOSE_ORCHESTRATION_ACTION.
		return 3 * time.Second
	}
}

// Clock tells the time and fires timers, so tests drive deadlines and
// the breaker without waiting.
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
}

// SystemClock is the real clock: internal/clock for the time, the
// runtime for timers.
type SystemClock struct{ clock.System }

// After fires once d has elapsed.
func (SystemClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// outcome is what a finished call tells the breaker.
type outcome int

const (
	// outcomeNeutral says nothing about the endpoint: the call never
	// reached it.
	outcomeNeutral outcome = iota
	outcomeSuccess
	outcomeFailure
)

// breaker opens after breakerThreshold consecutive endpoint failures and
// refuses calls for breakerOpenFor. Then one probe call goes through:
// success closes it, failure opens it again.
type breaker struct {
	mu        sync.Mutex
	failures  int
	openUntil time.Time
	probing   bool
}

// allow reports whether a call may reach the endpoint now, and whether
// it is the probe of an open breaker.
func (b *breaker) allow(now time.Time) (ok, probe bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.failures < breakerThreshold {
		return true, false
	}
	if now.Before(b.openUntil) || b.probing {
		return false, false
	}
	b.probing = true
	return true, true
}

// done records the outcome of an allowed call.
func (b *breaker) done(now time.Time, probe bool, o outcome) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if probe {
		b.probing = false
	}
	switch o {
	case outcomeSuccess:
		b.failures = 0
	case outcomeFailure:
		b.failures++
		if b.failures >= breakerThreshold {
			b.openUntil = now.Add(breakerOpenFor)
		}
	case outcomeNeutral:
	}
}

// inFlight holds the agents with an Analyst call running.
type inFlight struct {
	mu     sync.Mutex
	agents map[protocol.AgentID]bool
}

// acquire claims agent, reporting false if it already has a call.
func (f *inFlight) acquire(agent protocol.AgentID) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.agents[agent] {
		return false
	}
	if f.agents == nil {
		f.agents = make(map[protocol.AgentID]bool)
	}
	f.agents[agent] = true
	return true
}

func (f *inFlight) release(agent protocol.AgentID) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.agents, agent)
}
