package agentlink

import (
	"time"

	"github.com/smellak/gifford/internal/clock"
)

// Clock supplies wall time and reconnect/liveness timers. Tests can advance
// both without sleeping.
type Clock interface {
	clock.Clock
	After(time.Duration) <-chan time.Time
}

// SystemClock is the production clock.
type SystemClock struct{ clock.System }

// After fires once d has elapsed.
func (SystemClock) After(d time.Duration) <-chan time.Time { return time.After(d) }
