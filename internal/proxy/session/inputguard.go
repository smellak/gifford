//go:build unix

package session

import (
	"math"

	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy/draft"
)

// InputGuard reads the input guard for the agent socket, settled
// first: the published draft state, generation and version, the guard's
// activity and the last human key. It carries no agent or session id, and it has no field
// a human byte could reach.
func (s *Session) InputGuard() protocol.InputGuardReport {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.settleLocked()
	return protocol.InputGuardReport{
		DraftState:        draftState(s.snap.State),
		ActivityState:     activityState(s.guard.Activity()),
		DraftGeneration:   generation(s.snap.Generation),
		InputGuardVersion: s.snap.Version,
		LastHumanKeyAt:    s.lastKey,
		DetectionSource:   protocol.InputGuardProviderPTYProxy,
		Confidence:        1,
	}
}

// GuardChanged signals each new guard version. The loop never waits on
// it: a signal nobody took yet stands for every later change.
func (s *Session) GuardChanged() <-chan struct{} { return s.changed }

// draftState names a guard state on the wire. A state the wire does not
// know is UNKNOWN, never a guess.
func draftState(st draft.State) protocol.DraftState {
	switch st {
	case draft.StateClosed:
		return protocol.DraftStateClosed
	case draft.StateOpen:
		return protocol.DraftStateOpen
	case draft.StatePasting:
		return protocol.DraftStatePasting
	case draft.StateSubmitting:
		return protocol.DraftStateSubmitting
	default:
		return protocol.DraftStateUnknown
	}
}

// activityState names a guard activity on the wire.
func activityState(a draft.Activity) protocol.ActivityState {
	switch a {
	case draft.ActivityActive:
		return protocol.ActivityStateActive
	case draft.ActivityPaused:
		return protocol.ActivityStatePaused
	case draft.ActivityIdle:
		return protocol.ActivityStateIdle
	default:
		return protocol.ActivityStateUnknown
	}
}

// generation is a draft generation on the wire, which counts in int64.
func generation(g uint64) int64 {
	if g > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(g)
}
