package agentsock

import (
	"time"

	"github.com/smellak/gifford/internal/clock"
)

// Clock tells the time and fires timers, so tests drive the greeting
// deadline and the heartbeat without waiting.
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
}

// SystemClock is the real clock: internal/clock for the time, the
// runtime for timers.
type SystemClock struct{ clock.System }

// After fires once d has elapsed.
func (SystemClock) After(d time.Duration) <-chan time.Time { return time.After(d) }
