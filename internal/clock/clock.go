// Package clock is the only place that reads the wall clock. Everything else
// receives a Clock so the race fuzzer can drive time (PRD §9.1); forbidigo
// rejects time.Now outside this package.
package clock

import "time"

// Clock tells the time.
type Clock interface {
	Now() time.Time
}

// System is the real clock.
type System struct{}

// Now returns the current wall-clock time.
func (System) Now() time.Time { return time.Now() }
