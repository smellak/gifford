package gateway

import (
	"fmt"
	"sync/atomic"

	"github.com/smellak/gifford/internal/protocol"
)

// ModelSelectionProvider publishes one immutable ANALYZE_AGENT_STATE tuple.
// Callers copy the value once at the start of work and never reread it midway.
type ModelSelectionProvider interface {
	CurrentModelSelection() protocol.ModelSelection
}

// Selector is the process-wide, lock-free publication point for a committed
// model selection. A writer publishes only after its database transaction has
// committed.
type Selector struct {
	current atomic.Pointer[protocol.ModelSelection]
}

// NewSelector returns a selector initialized with a validated durable tuple.
func NewSelector(initial protocol.ModelSelection) (*Selector, error) {
	if err := initial.Validate(); err != nil {
		return nil, fmt.Errorf("new model selector: %w", err)
	}
	s := &Selector{}
	s.current.Store(cloneSelection(initial))
	return s, nil
}

// CurrentModelSelection returns a value copy of the current immutable tuple.
func (s *Selector) CurrentModelSelection() protocol.ModelSelection {
	if s == nil {
		return protocol.ModelSelection{}
	}
	current := s.current.Load()
	if current == nil {
		return protocol.ModelSelection{}
	}
	return *current
}

// Publish atomically exposes a committed newer generation. Equal generations
// are accepted only when the complete tuple is unchanged.
func (s *Selector) Publish(next protocol.ModelSelection) error {
	if s == nil {
		return fmt.Errorf("publish model selection: nil selector")
	}
	if err := next.Validate(); err != nil {
		return fmt.Errorf("publish model selection: %w", err)
	}
	for {
		current := s.current.Load()
		if current != nil && next.Generation <= current.Generation {
			if next == *current {
				return nil
			}
			return fmt.Errorf("publish model selection: generation did not advance")
		}
		candidate := cloneSelection(next)
		if s.current.CompareAndSwap(current, candidate) {
			return nil
		}
	}
}

func cloneSelection(selection protocol.ModelSelection) *protocol.ModelSelection {
	cloned := selection
	return &cloned
}
