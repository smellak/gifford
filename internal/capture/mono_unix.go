//go:build unix

package capture

import "golang.org/x/sys/unix"

// sysMono reads CLOCK_MONOTONIC.
type sysMono struct{}

// MonotonicNS returns the monotonic clock in nanoseconds.
func (sysMono) MonotonicNS() int64 {
	var ts unix.Timespec
	_ = unix.ClockGettime(unix.CLOCK_MONOTONIC, &ts)
	return ts.Nano()
}

// SystemMono is the production monotonic source.
var SystemMono = sysMono{}
