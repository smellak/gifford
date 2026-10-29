//go:build !unix

package capture

// sysMono is unreachable off unix: mutating entry points fail closed
// at the lock files before any timestamp is taken.
type sysMono struct{}

// MonotonicNS reports zero off unix.
func (sysMono) MonotonicNS() int64 {
	return 0
}

// SystemMono is the production monotonic source.
var SystemMono = sysMono{}
