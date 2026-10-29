package capture

// Mono is the one-method monotonic source behind event timestamps.
type Mono interface {
	MonotonicNS() int64
}
