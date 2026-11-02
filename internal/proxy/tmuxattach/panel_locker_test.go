package tmuxattach

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/tmuxlock"
)

func TestPanelLockerPassesExactBindingAndRefusesEveryNonAcquiredOutcome(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("unprovisioned")
	for name, outcome := range map[string]tmuxlock.LockOutcome{
		"deferred":      tmuxlock.LockDeferred,
		"unprovisioned": tmuxlock.LockUnprovisioned,
		"invalid":       tmuxlock.LockInvalid,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			var got tmuxlock.PanelBinding
			locker, err := newPanelLocker(25*time.Millisecond,
				func(_ context.Context, binding tmuxlock.PanelBinding, wait time.Duration) (
					*tmuxlock.PanelGuard, tmuxlock.LockOutcome, error,
				) {
					got = binding
					require.Equal(t, 25*time.Millisecond, wait)
					if outcome == tmuxlock.LockUnprovisioned {
						return nil, outcome, wantErr
					}
					return nil, outcome, nil
				})
			require.NoError(t, err)
			lock, err := locker.Lock(t.Context(), testTarget)
			require.Nil(t, lock)
			require.Equal(t, tmuxlock.PanelBinding{
				OwnerUID: testServer.OwnerUID, Socket: testServer.Socket,
				PaneID: string(testTarget.Pane),
			}, got)
			if outcome == tmuxlock.LockUnprovisioned {
				require.ErrorIs(t, err, wantErr)
			} else {
				require.ErrorIs(t, err, ErrPanelLockNotAcquired)
			}
		})
	}
}

func TestPanelLockerValidatesConstructionAndTargetBeforeAcquire(t *testing.T) {
	t.Parallel()
	_, err := NewPanelLocker(-time.Nanosecond)
	require.ErrorIs(t, err, ErrInvalidOptions)
	_, err = newPanelLocker(0, nil)
	require.ErrorIs(t, err, ErrInvalidOptions)

	called := false
	locker, err := newPanelLocker(0, func(
		context.Context, tmuxlock.PanelBinding, time.Duration,
	) (*tmuxlock.PanelGuard, tmuxlock.LockOutcome, error) {
		called = true
		return nil, tmuxlock.LockInvalid, nil
	})
	require.NoError(t, err)
	lock, err := locker.Lock(t.Context(), Target{})
	require.Nil(t, lock)
	require.ErrorIs(t, err, ErrInvalidOptions)
	require.False(t, called)
}
