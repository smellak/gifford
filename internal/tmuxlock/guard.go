package tmuxlock

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"
)

// GuardReason is the closed, content-free reason a cooperative pane cannot
// authorize an effect.
type GuardReason string

// Cooperative guard reasons.
const (
	GuardModeInvalid          GuardReason = "mode-invalid"
	GuardInventoryInvalid     GuardReason = "inventory-invalid"
	GuardPanelInvalid         GuardReason = "panel-invalid"
	GuardRegisterInvalid      GuardReason = "register-invalid"
	GuardStateInvalid         GuardReason = "state-invalid"
	GuardPending              GuardReason = "pending"
	GuardOwnerCopyMismatch    GuardReason = "owner-copy-mismatch"
	GuardStateBindingMismatch GuardReason = "state-binding-mismatch"
	GuardBindingMismatch      GuardReason = "binding-mismatch"
	GuardForeignOwner         GuardReason = "foreign-owner"
	GuardPhaseBlocked         GuardReason = "phase-blocked"
	GuardGenerationExhausted  GuardReason = "generation-exhausted"
	GuardNotIncorporated      GuardReason = "not-incorporated"
)

// GuardError classifies a cooperative no-effect result. Invalid results must
// be reported as incidents; ordinary deferrals remain retryable only after
// their external condition changes.
type GuardError struct {
	Reason  GuardReason
	Invalid bool
}

func (e *GuardError) Error() string {
	if e == nil {
		return "pane-lock guard failed"
	}
	return "pane-lock guard: " + string(e.Reason)
}

func guardInvalid(reason GuardReason) error {
	return &GuardError{Reason: reason, Invalid: true}
}

func guardDeferred(reason GuardReason) error {
	return &GuardError{Reason: reason}
}

// GuardFailure returns the closed reason and whether it is an incident.
func GuardFailure(err error) (GuardReason, bool, bool) {
	var failure *GuardError
	if !errors.As(err, &failure) {
		return "", false, false
	}
	return failure.Reason, failure.Invalid, true
}

// AcquireOwnedPanelLock validates required mode and the published inventory,
// then retains the exact pane flock and authority-record descriptors.
func AcquireOwnedPanelLock(
	ctx context.Context,
	modePath string,
	policy RecordPolicy,
	binding LiveBinding,
	wait time.Duration,
) (*PanelGuard, LockOutcome, error) {
	return AcquireOwnedPanelLockAt(ctx, modePath, DefaultRoot, 0, 0, policy, binding, wait)
}

// AcquireOwnedPanelLockAt is the explicit-authority form used by the node's
// synthetic wiring. Production uses AcquireOwnedPanelLock, which fixes the
// root and mode owner to the system contract.
func AcquireOwnedPanelLockAt(
	ctx context.Context,
	modePath string,
	rootPath string,
	modeUID uint32,
	modeGID uint32,
	policy RecordPolicy,
	binding LiveBinding,
	wait time.Duration,
) (*PanelGuard, LockOutcome, error) {
	return acquireOwnedWithModeAt(
		ctx, modePath, rootPath, modeUID, modeGID, policy, binding, wait,
	)
}

func acquireOwnedWithModeAt(
	ctx context.Context,
	modePath string,
	rootPath string,
	modeUID uint32,
	modeGID uint32,
	policy RecordPolicy,
	binding LiveBinding,
	wait time.Duration,
) (*PanelGuard, LockOutcome, error) {
	if ctx == nil || wait < 0 || !validManagedPath(modePath) || !validManagedPath(rootPath) {
		return nil, LockInvalid, fmt.Errorf("owned panel lock: %w", ErrInvalidBinding)
	}
	if err := validateLiveBinding(binding); err != nil {
		return nil, LockInvalid, err
	}
	validatedPolicy, err := validateRecordPolicy(policy)
	if err != nil {
		return nil, LockInvalid, err
	}
	if err := ctx.Err(); err != nil {
		return nil, LockDeferred, fmt.Errorf("owned panel lock context: %w", err)
	}
	if err := requireProtocolModePlatform(modePath, rootPath, modeUID, modeGID); err != nil {
		return nil, LockInvalid, guardInvalid(GuardModeInvalid)
	}
	guard, outcome, err := acquireOwnedAt(ctx, rootPath, validatedPolicy, binding, wait)
	if err != nil || guard == nil {
		return guard, outcome, err
	}
	check := guard.check
	guard.check = func(checkCtx context.Context, current LiveBinding) error {
		if err := requireProtocolModePlatform(
			modePath, rootPath, modeUID, modeGID,
		); err != nil {
			return guardInvalid(GuardModeInvalid)
		}
		return check(checkCtx, current)
	}
	return guard, outcome, nil
}

// ValidateProtocolMode verifies the root-owned required-mode marker and the
// managed root before a node enables cooperative delivery.
func ValidateProtocolMode(modePath string) error {
	return ValidateProtocolModeAt(modePath, DefaultRoot, 0, 0)
}

// ValidateProtocolModeAt validates an isolated cooperative root and its mode
// owner. Production callers use ValidateProtocolMode and therefore retain the
// root-owned system contract.
func ValidateProtocolModeAt(modePath, rootPath string, modeUID, modeGID uint32) error {
	if !validManagedPath(modePath) {
		return fmt.Errorf("validate pane-lock mode: %w", ErrInvalidBinding)
	}
	if !validManagedPath(rootPath) {
		return fmt.Errorf("validate pane-lock root: %w", ErrInvalidBinding)
	}
	if err := requireProtocolModePlatform(modePath, rootPath, modeUID, modeGID); err != nil {
		return fmt.Errorf("validate pane-lock mode: %w", err)
	}
	return nil
}

// Validate checks the authority records and live binding while the exact pane
// flock retained by this guard is still held.
func (guard *PanelGuard) Validate(ctx context.Context, binding LiveBinding) error {
	if guard == nil || ctx == nil {
		return guardInvalid(GuardPanelInvalid)
	}
	guard.mu.Lock()
	defer guard.mu.Unlock()
	if guard.closed || guard.check == nil {
		return guardInvalid(GuardPanelInvalid)
	}
	return guard.check(ctx, binding)
}

func validManagedPath(value string) bool {
	if value == "" || value == "/" || value[0] != '/' || path.Clean(value) != value ||
		strings.IndexByte(value, 0) >= 0 || strings.ContainsAny(value, "\r\n") {
		return false
	}
	for _, component := range strings.Split(strings.TrimPrefix(value, "/"), "/") {
		if component == "" || component == "." || component == ".." {
			return false
		}
	}
	return true
}
