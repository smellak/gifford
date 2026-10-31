//go:build unix

package session

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy/draft"
)

func TestInputGuardStateNames(t *testing.T) {
	t.Parallel()
	for st, want := range map[draft.State]protocol.DraftState{
		draft.StateClosed:     protocol.DraftStateClosed,
		draft.StateOpen:       protocol.DraftStateOpen,
		draft.StatePasting:    protocol.DraftStatePasting,
		draft.StateSubmitting: protocol.DraftStateSubmitting,
		draft.StateUnknown:    protocol.DraftStateUnknown,
		"bogus":               protocol.DraftStateUnknown,
	} {
		require.Equal(t, want, draftState(st), st)
	}
	for a, want := range map[draft.Activity]protocol.ActivityState{
		draft.ActivityActive: protocol.ActivityStateActive,
		draft.ActivityPaused: protocol.ActivityStatePaused,
		draft.ActivityIdle:   protocol.ActivityStateIdle,
		"bogus":              protocol.ActivityStateUnknown,
	} {
		require.Equal(t, want, activityState(a), a)
	}
}

// guardSignalled reports whether a guard change is pending.
func guardSignalled(s *Session) bool {
	select {
	case <-s.GuardChanged():
		return true
	default:
		return false
	}
}

// waitChunks waits until the loop fully processed n input chunks.
func waitChunks(t *testing.T, s *Session, n uint64) {
	t.Helper()
	waitSnapshot(t, s, "input never processed", func(snap guardSnapshot) bool {
		return snap.Chunks >= n
	})
}

// TestInputGuardReadsTheGuard reads the guard as the agent socket does:
// the published state, generation and version, the activity and the
// last human key, and a change signal on every new version only.
func TestInputGuardReadsTheGuard(t *testing.T) {
	t.Parallel()

	r := startGuardSession(t, "input-guard")
	at := r.clock.Now()
	g := r.session.InputGuard()
	require.Equal(t, protocol.InputGuardReport{
		DraftState: protocol.DraftStateClosed, ActivityState: protocol.ActivityStateIdle,
		DetectionSource: protocol.InputGuardProviderPTYProxy, Confidence: 1,
	}, g, "no key yet")
	require.False(t, guardSignalled(r.session))

	writeKeys(t, r.master, "q")
	waitChunks(t, r.session, 1)
	g = r.session.InputGuard()
	require.Equal(t, protocol.DraftStateOpen, g.DraftState)
	require.Equal(t, protocol.ActivityStateActive, g.ActivityState)
	require.Equal(t, int64(1), g.DraftGeneration)
	require.Equal(t, int64(1), g.InputGuardVersion)
	require.True(t, at.Equal(g.LastHumanKeyAt))
	require.True(t, guardSignalled(r.session), "a new version signals")

	// More text moves no version, so it signals nothing.
	writeKeys(t, r.master, "k")
	waitChunks(t, r.session, 2)
	require.False(t, guardSignalled(r.session))

	r.clock.advance(2 * time.Second)
	g = r.session.InputGuard()
	require.Equal(t, protocol.DraftStateOpen, g.DraftState, "silence must not close the draft")
	require.Equal(t, protocol.ActivityStatePaused, g.ActivityState)
	require.Equal(t, int64(1), g.DraftGeneration)
	require.Equal(t, int64(2), g.InputGuardVersion)
	require.True(t, guardSignalled(r.session), "the clock-only activity transition signals")
	require.False(t, guardSignalled(r.session))

	writeKeys(t, r.master, "\x03")
	waitChunks(t, r.session, 3)
	g = r.session.InputGuard()
	require.Equal(t, protocol.DraftStateClosed, g.DraftState)
	require.Equal(t, int64(3), g.InputGuardVersion)
	require.True(t, r.clock.Now().Equal(g.LastHumanKeyAt))
	require.True(t, guardSignalled(r.session))

	endDeliverySession(t, r.master, r.done)
}

// TestGuardChangedNeverBlocks moves the guard many times while nobody
// takes the signal: the loop never waits on it.
func TestGuardChangedNeverBlocks(t *testing.T) {
	t.Parallel()

	r := startGuardSession(t, "guard-changed")
	const toggles = 200
	for i := range toggles {
		writeKeys(t, r.master, "a")
		waitChunks(t, r.session, uint64(2*i+1))
		writeKeys(t, r.master, "\x03")
		waitChunks(t, r.session, uint64(2*i+2))
	}
	require.Equal(t, int64(2*toggles), r.session.InputGuard().InputGuardVersion)
	require.True(t, guardSignalled(r.session), "one signal stands for them all")
	require.False(t, guardSignalled(r.session))

	endDeliverySession(t, r.master, r.done)
}
