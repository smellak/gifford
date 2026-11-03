package tmuxattach

import (
	"context"
	"errors"
	"os"
	"path/filepath"
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

func TestPanelLockerRefusesEffectWhenProtocolRootExists(t *testing.T) {
	t.Parallel()

	called := false
	locker, err := newPanelLocker(0, func(
		context.Context, tmuxlock.PanelBinding, time.Duration,
	) (*tmuxlock.PanelGuard, tmuxlock.LockOutcome, error) {
		called = true
		return nil, tmuxlock.LockInvalid, nil
	})
	require.NoError(t, err)
	locker.protocolRoot = filepath.Join(t.TempDir(), "pane-locks")
	require.NoError(t, os.Mkdir(locker.protocolRoot, 0o700))

	lock, err := locker.Lock(t.Context(), testTarget)
	require.Nil(t, lock)
	require.ErrorIs(t, err, ErrPanelLockProtocolRequired)
	require.False(t, called)
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

type recordingPaneLockReporter struct {
	binding tmuxlock.LiveBinding
	reason  tmuxlock.GuardReason
	calls   int
}

func (r *recordingPaneLockReporter) ReportPaneLockIncident(
	_ context.Context,
	binding tmuxlock.LiveBinding,
	reason tmuxlock.GuardReason,
) error {
	r.binding, r.reason = binding, reason
	r.calls++
	return nil
}

func TestCooperativePanelLockerReportsOnlyInvalidAcquisition(t *testing.T) {
	t.Parallel()
	target := testTarget
	target.Authority = PaneAuthority{
		Host: "node-a", BootID: "0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0",
		PanePID: 4242, PaneStart: 123,
	}
	reporter := &recordingPaneLockReporter{}
	policy := tmuxlock.RecordPolicy{
		PeerOwner: testPeerWriter, PeerWriters: []string{testPeerWriter},
	}
	var got tmuxlock.LiveBinding
	locker, err := newCooperativePanelLocker(
		"/etc/example/pane-mode", policy, 20*time.Millisecond, reporter,
		func(_ context.Context, modePath string, gotPolicy tmuxlock.RecordPolicy,
			binding tmuxlock.LiveBinding, wait time.Duration,
		) (
			*tmuxlock.PanelGuard, tmuxlock.LockOutcome, error,
		) {
			require.Equal(t, "/etc/example/pane-mode", modePath)
			require.Equal(t, policy, gotPolicy)
			require.Equal(t, 20*time.Millisecond, wait)
			got = binding
			return nil, tmuxlock.LockInvalid, &tmuxlock.GuardError{
				Reason: tmuxlock.GuardRegisterInvalid, Invalid: true,
			}
		},
	)
	require.NoError(t, err)
	lock, err := locker.Lock(t.Context(), target)
	require.Nil(t, lock)
	require.Error(t, err)
	require.Equal(t, target.liveBinding(), got)
	require.Equal(t, 1, reporter.calls)
	require.Equal(t, got, reporter.binding)
	require.Equal(t, tmuxlock.GuardRegisterInvalid, reporter.reason)

	reporter.calls = 0
	locker.owned = func(context.Context, string, tmuxlock.RecordPolicy, tmuxlock.LiveBinding, time.Duration) (
		*tmuxlock.PanelGuard, tmuxlock.LockOutcome, error,
	) {
		return nil, tmuxlock.LockDeferred, &tmuxlock.GuardError{Reason: tmuxlock.GuardPending}
	}
	lock, err = locker.Lock(t.Context(), target)
	require.Nil(t, lock)
	require.Error(t, err)
	require.Zero(t, reporter.calls)
}
