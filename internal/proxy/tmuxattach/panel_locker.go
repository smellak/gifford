package tmuxattach

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/smellak/gifford/internal/tmuxlock"
)

// ErrPanelLockNotAcquired means the shared, pre-provisioned pane lock did
// not grant exclusive ownership. It is always a no-effect outcome.
var ErrPanelLockNotAcquired = errors.New("tmux attach: panel lock not acquired")

type panelLockAcquire func(
	context.Context,
	tmuxlock.PanelBinding,
	time.Duration,
) (*tmuxlock.PanelGuard, tmuxlock.LockOutcome, error)

// PanelLocker adapts the shared E1 lock to the transport's narrow Locker
// interface. It never falls back to process-local or unlocked delivery.
type PanelLocker struct {
	wait    time.Duration
	acquire panelLockAcquire
}

// NewPanelLocker constructs the cross-process pane locker. A zero wait makes
// contention defer immediately; a positive wait remains bounded by both wait
// and the caller's context.
func NewPanelLocker(wait time.Duration) (*PanelLocker, error) {
	return newPanelLocker(wait, tmuxlock.AcquirePanelLock)
}

func newPanelLocker(wait time.Duration, acquire panelLockAcquire) (*PanelLocker, error) {
	if wait < 0 || acquire == nil {
		return nil, ErrInvalidOptions
	}
	return &PanelLocker{wait: wait, acquire: acquire}, nil
}

// Lock acquires the exact owner/socket/pane binding selected from the durable
// registry. Every non-acquired E1 outcome is returned as an error before tmux
// is called.
func (l *PanelLocker) Lock(ctx context.Context, target Target) (Lock, error) {
	if l == nil || ctx == nil || target.validate() != nil {
		return nil, ErrInvalidOptions
	}
	guard, outcome, err := l.acquire(ctx, tmuxlock.PanelBinding{
		OwnerUID: target.Server.OwnerUID,
		Socket:   target.Server.Socket,
		PaneID:   string(target.Pane),
	}, l.wait)
	if err != nil {
		return nil, fmt.Errorf("acquire shared panel lock (%s): %w", outcome, err)
	}
	if outcome != tmuxlock.LockAcquired || guard == nil {
		return nil, fmt.Errorf("acquire shared panel lock (%s): %w",
			outcome, ErrPanelLockNotAcquired)
	}
	return panelLock{guard: guard}, nil
}

type panelLock struct {
	guard *tmuxlock.PanelGuard
}

func (l panelLock) Unlock() error {
	if err := l.guard.Close(); err != nil {
		return fmt.Errorf("release shared panel lock: %w", err)
	}
	return nil
}
