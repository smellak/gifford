package fuzz

import (
	"sync/atomic"
	"time"
)

// epoch is where every scenario's clock starts.
var epoch = time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)

// virtualClock is the session's clock in a scenario: it moves only when
// the human's script says time passed, so a long pause costs no real
// time. Safe for concurrent use.
type virtualClock struct{ elapsed atomic.Int64 }

// Now is the epoch plus the time the script let pass.
func (c *virtualClock) Now() time.Time { return epoch.Add(time.Duration(c.elapsed.Load())) }

// advance lets d pass.
func (c *virtualClock) advance(d time.Duration) { c.elapsed.Add(int64(d)) }
