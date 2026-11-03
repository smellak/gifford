package tmuxattach

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/smellak/gifford/internal/tmuxlock"
)

// ErrPanelLockNotAcquired means the shared, pre-provisioned pane lock did
// not grant exclusive ownership. It is always a no-effect outcome.
var ErrPanelLockNotAcquired = errors.New("tmux attach: panel lock not acquired")

// ErrPanelLockProtocolRequired means the cooperative root is present but the
// node was started without its required protocol configuration. Delivery must
// remain effect-free even though the hub itself may serve unrelated traffic.
var ErrPanelLockProtocolRequired = errors.New("tmux attach: pane-lock protocol configuration required")

type panelLockAcquire func(
	context.Context,
	tmuxlock.PanelBinding,
	time.Duration,
) (*tmuxlock.PanelGuard, tmuxlock.LockOutcome, error)

type ownedPanelLockAcquire func(
	context.Context,
	string,
	tmuxlock.RecordPolicy,
	tmuxlock.LiveBinding,
	time.Duration,
) (*tmuxlock.PanelGuard, tmuxlock.LockOutcome, error)

// PanelLockIncidentReporter persists every invalid cooperative result before
// it is returned to the delivery path.
type PanelLockIncidentReporter interface {
	ReportPaneLockIncident(context.Context, tmuxlock.LiveBinding, tmuxlock.GuardReason) error
}

// PanelLocker adapts the shared E1 lock to the transport's narrow Locker
// interface. It never falls back to process-local or unlocked delivery.
type PanelLocker struct {
	wait     time.Duration
	modePath string
	policy   tmuxlock.RecordPolicy
	acquire  panelLockAcquire
	owned    ownedPanelLockAcquire
	reporter PanelLockIncidentReporter
	// protocolRoot is set only on the public legacy constructor. Test seams and
	// the cooperative constructor enforce their own explicit authority model.
	protocolRoot string
	mu           sync.Mutex
	active       map[tmuxlock.LiveBinding]*tmuxlock.PanelGuard
}

// NewPanelLocker constructs the cross-process pane locker. A zero wait makes
// contention defer immediately; a positive wait remains bounded by both wait
// and the caller's context.
func NewPanelLocker(wait time.Duration) (*PanelLocker, error) {
	locker, err := newPanelLocker(wait, tmuxlock.AcquirePanelLock)
	if err != nil {
		return nil, err
	}
	locker.protocolRoot = tmuxlock.DefaultRoot
	return locker, nil
}

func newPanelLocker(wait time.Duration, acquire panelLockAcquire) (*PanelLocker, error) {
	if wait < 0 || acquire == nil {
		return nil, ErrInvalidOptions
	}
	return &PanelLocker{wait: wait, acquire: acquire}, nil
}

// NewCooperativePanelLocker constructs the protocol-aware locker and lifecycle
// guard. The same value must be installed as Transport's Locker and Lifecycle.
func NewCooperativePanelLocker(
	modePath string,
	policy tmuxlock.RecordPolicy,
	wait time.Duration,
	reporter PanelLockIncidentReporter,
) (*PanelLocker, error) {
	return newCooperativePanelLocker(
		modePath, policy, wait, reporter, tmuxlock.AcquireOwnedPanelLock,
	)
}

func newCooperativePanelLocker(
	modePath string,
	policy tmuxlock.RecordPolicy,
	wait time.Duration,
	reporter PanelLockIncidentReporter,
	acquire ownedPanelLockAcquire,
) (*PanelLocker, error) {
	if wait < 0 || modePath == "" || policy.Validate() != nil || reporter == nil || acquire == nil {
		return nil, ErrInvalidOptions
	}
	return &PanelLocker{
		wait: wait, modePath: modePath,
		policy: tmuxlock.RecordPolicy{
			PeerOwner: policy.PeerOwner, PeerWriters: append([]string(nil), policy.PeerWriters...),
		},
		owned: acquire, reporter: reporter,
		active: make(map[tmuxlock.LiveBinding]*tmuxlock.PanelGuard),
	}, nil
}

// Lock acquires the exact owner/socket/pane binding selected from the durable
// registry. Every non-acquired E1 outcome is returned as an error before tmux
// is called.
func (l *PanelLocker) Lock(ctx context.Context, target Target) (Lock, error) {
	if l == nil || ctx == nil || target.validate() != nil {
		return nil, ErrInvalidOptions
	}
	if l.owned != nil {
		return l.lockCooperative(ctx, target)
	}
	if l.acquire == nil {
		return nil, ErrInvalidOptions
	}
	if err := l.requireConfiguredProtocolWhenRootExists(); err != nil {
		return nil, err
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
	// Close the creation race between the first check and acquisition. A
	// successful legacy lock is still discarded if the protocol root appeared.
	if err := l.requireConfiguredProtocolWhenRootExists(); err != nil {
		if closeErr := guard.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("release shared panel lock: %w", closeErr))
		}
		return nil, err
	}
	return panelLock{guard: guard}, nil
}

func (l *PanelLocker) requireConfiguredProtocolWhenRootExists() error {
	if l.protocolRoot == "" {
		return nil
	}
	_, err := os.Lstat(l.protocolRoot)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect pane-lock root: %w",
			errors.Join(ErrPanelLockProtocolRequired, err))
	}
	return ErrPanelLockProtocolRequired
}

func (l *PanelLocker) lockCooperative(ctx context.Context, target Target) (Lock, error) {
	binding := target.liveBinding()
	if err := binding.Validate(); err != nil {
		return nil, ErrInvalidOptions
	}
	guard, outcome, err := l.owned(ctx, l.modePath, l.policy, binding, l.wait)
	if err != nil {
		if reportErr := l.reportInvalid(ctx, binding, err); reportErr != nil {
			err = errors.Join(err, reportErr)
		}
		return nil, fmt.Errorf("acquire cooperative panel lock (%s): %w", outcome, err)
	}
	if outcome != tmuxlock.LockAcquired || guard == nil {
		return nil, fmt.Errorf("acquire cooperative panel lock (%s): %w",
			outcome, ErrPanelLockNotAcquired)
	}
	l.mu.Lock()
	if _, exists := l.active[binding]; exists {
		l.mu.Unlock()
		_ = guard.Close()
		return nil, fmt.Errorf("cooperative panel lock already active: %w", ErrPanelLockNotAcquired)
	}
	l.active[binding] = guard
	l.mu.Unlock()
	return &cooperativePanelLock{owner: l, guard: guard, binding: binding}, nil
}

func (l *PanelLocker) reportInvalid(
	ctx context.Context,
	binding tmuxlock.LiveBinding,
	err error,
) error {
	reason, invalid, ok := tmuxlock.GuardFailure(err)
	if !ok || !invalid {
		return nil
	}
	if reportErr := l.reporter.ReportPaneLockIncident(ctx, binding, reason); reportErr != nil {
		return fmt.Errorf("report pane-lock incident: %w", reportErr)
	}
	return nil
}

// Check validates the authority records under the exact flock retained by
// Lock. It runs after the delivery guard has freshly revalidated the pane.
func (l *PanelLocker) Check(ctx context.Context, target Target, _ PaneObservation) error {
	if l == nil || l.owned == nil || ctx == nil {
		return ErrInvalidOptions
	}
	binding := target.liveBinding()
	l.mu.Lock()
	guard := l.active[binding]
	l.mu.Unlock()
	if guard == nil {
		return ErrInvalidOptions
	}
	err := guard.Validate(ctx, binding)
	if err == nil {
		return nil
	}
	if reportErr := l.reportInvalid(ctx, binding, err); reportErr != nil {
		return errors.Join(err, reportErr)
	}
	return fmt.Errorf("validate cooperative panel lock: %w", err)
}

// DuplicateLockFile returns a caller-owned descriptor for the cooperative
// lock currently held for target. It is available only after Lock and remains
// tied to that exact open file description until the caller closes it.
func (l *PanelLocker) DuplicateLockFile(target Target) (*os.File, error) {
	if l == nil || l.owned == nil || target.validate() != nil {
		return nil, ErrInvalidOptions
	}
	binding := target.liveBinding()
	l.mu.Lock()
	guard := l.active[binding]
	l.mu.Unlock()
	if guard == nil {
		return nil, ErrInvalidOptions
	}
	file, err := guard.DuplicateFile()
	if err != nil {
		return nil, fmt.Errorf("duplicate cooperative panel lock: %w", err)
	}
	return file, nil
}

// Begin is a no-op; durable attempt state belongs to the journal lifecycle.
func (*PanelLocker) Begin(context.Context, Target) error { return nil }

// Step is a no-op; durable attempt state belongs to the journal lifecycle.
func (*PanelLocker) Step(context.Context, Target, PaneStep) error { return nil }

// EffectBegin is a no-op; common logging belongs to the journal lifecycle.
func (*PanelLocker) EffectBegin(context.Context, Target, PaneStep) error { return nil }

// Child is a no-op; durable attempt state belongs to the journal lifecycle.
func (*PanelLocker) Child(context.Context, Target, PaneStep, PaneChild) error { return nil }

// EffectEnd is a no-op; common logging belongs to the journal lifecycle.
func (*PanelLocker) EffectEnd(context.Context, Target, PaneStep, PaneEffectOutcome) error {
	return nil
}

// Finish is a no-op; durable attempt state belongs to the journal lifecycle.
func (*PanelLocker) Finish(context.Context, Target, Result) error { return nil }

type panelLock struct {
	guard *tmuxlock.PanelGuard
}

func (l panelLock) Unlock() error {
	if err := l.guard.Close(); err != nil {
		return fmt.Errorf("release shared panel lock: %w", err)
	}
	return nil
}

type cooperativePanelLock struct {
	owner   *PanelLocker
	guard   *tmuxlock.PanelGuard
	binding tmuxlock.LiveBinding
}

func (l *cooperativePanelLock) Unlock() error {
	if l == nil || l.owner == nil || l.guard == nil {
		return nil
	}
	l.owner.mu.Lock()
	if l.owner.active[l.binding] == l.guard {
		delete(l.owner.active, l.binding)
	}
	l.owner.mu.Unlock()
	if err := l.guard.Close(); err != nil {
		return fmt.Errorf("release cooperative panel lock: %w", err)
	}
	l.guard = nil
	return nil
}
