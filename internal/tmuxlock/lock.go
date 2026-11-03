package tmuxlock

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"
)

// ErrUnprovisioned means the pre-created lock path is absent or fails its
// ownership, type, mode, link-count, size, or ACL contract.
var ErrUnprovisioned = errors.New("panel lock unprovisioned")

// LockOutcome is the fixed telemetry classification of an acquisition.
type LockOutcome string

// Lock outcomes.
const (
	// LockAcquired means the returned guard holds the exclusive lock.
	LockAcquired LockOutcome = "ACQUIRED"
	// LockDeferred means another writer retained the lock for the bounded wait.
	LockDeferred LockOutcome = "DEFERRED"
	// LockUnprovisioned means no trusted, pre-created lock inode was available.
	LockUnprovisioned LockOutcome = "UNPROVISIONED"
	// LockInvalid means the binding, root, context, or wait was invalid.
	LockInvalid LockOutcome = "INVALID"
)

// PanelGuard retains the exact locked open-file description. The caller must
// keep it until its durable terminal delivery-attempt update has completed.
type PanelGuard struct {
	mu     sync.Mutex
	fd     int
	device uint64
	inode  uint64
	close  func() error
	check  func(context.Context, LiveBinding) error
	closed bool
}

// DuplicateFile returns a caller-owned descriptor for the same open file
// description that retains the exclusive claim. The caller must close it;
// keeping it alive does not replace closing the guard.
func (guard *PanelGuard) DuplicateFile() (*os.File, error) {
	if guard == nil {
		return nil, ErrInvalidBinding
	}
	guard.mu.Lock()
	defer guard.mu.Unlock()
	if guard.closed || guard.fd < 0 {
		return nil, ErrInvalidBinding
	}
	return duplicatePanelLockFile(guard.fd)
}

// AcquirePanelLock opens the existing lock inode and waits at most wait for an
// exclusive claim. Every non-acquired outcome is no-effect: callers must not
// fall back to an unlocked write.
func AcquirePanelLock(
	ctx context.Context,
	binding PanelBinding,
	wait time.Duration,
) (*PanelGuard, LockOutcome, error) {
	return acquireAt(ctx, DefaultRoot, binding, wait)
}

func acquireAt(
	ctx context.Context,
	root string,
	binding PanelBinding,
	wait time.Duration,
) (*PanelGuard, LockOutcome, error) {
	if ctx == nil {
		return nil, LockInvalid, fmt.Errorf("panel lock context: %w", ErrInvalidBinding)
	}
	if wait < 0 {
		return nil, LockInvalid, fmt.Errorf("panel lock wait: %w", ErrInvalidBinding)
	}
	parts, err := deriveParts(binding)
	if err != nil {
		return nil, LockInvalid, err
	}
	// Root-owned tmux servers are never eligible delivery targets. Derivation
	// remains total for UID 0 so independent implementations can share vectors.
	if binding.OwnerUID == 0 {
		return nil, LockInvalid, fmt.Errorf("panel owner: %w", ErrInvalidBinding)
	}
	if err := ctx.Err(); err != nil {
		return nil, LockDeferred, fmt.Errorf("panel lock context: %w", err)
	}
	return acquirePlatform(ctx, root, binding.OwnerUID, parts, wait)
}

// Close releases the claim and closes its descriptor. It is idempotent.
func (guard *PanelGuard) Close() error {
	if guard == nil {
		return nil
	}
	guard.mu.Lock()
	defer guard.mu.Unlock()
	if guard.closed {
		return nil
	}
	guard.closed = true
	if guard.close != nil {
		return guard.close()
	}
	fd := guard.fd
	guard.fd = -1
	return releasePanelLock(fd)
}
