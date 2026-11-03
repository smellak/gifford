package tmuxlock

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// RecoveryOutcome is the content-free result of an explicit recovery call.
type RecoveryOutcome string

const (
	// RecoveryUnchanged means the exact request needed no durable write.
	RecoveryUnchanged RecoveryOutcome = "unchanged"
	// RecoveryConfirmed means visible evidence confirmed the attempted effect.
	RecoveryConfirmed RecoveryOutcome = "confirmed"
	// RecoveryNoEffect means complete evidence proved that no effect occurred.
	RecoveryNoEffect RecoveryOutcome = "no_effect"
	// RecoveryRepaired means a corrupt state copy was durably replaced.
	RecoveryRepaired RecoveryOutcome = "repaired"
)

// EvidenceVerdict keeps an unavailable proof distinct from a negative one.
type EvidenceVerdict string

const (
	// EvidenceUnknown means the proof could not be obtained.
	EvidenceUnknown EvidenceVerdict = "unknown"
	// EvidenceYes means the observed proposition was true.
	EvidenceYes EvidenceVerdict = "yes"
	// EvidenceNo means the observed proposition was false.
	EvidenceNo EvidenceVerdict = "no"
)

// RecordEvidence is the bounded, content-free description of a state record.
// It never includes record bytes.
type RecordEvidence struct {
	Size          int64
	MagicValid    EvidenceVerdict
	ChecksumValid EvidenceVerdict
	Version       *int
}

// RecoveryEvidence contains only the structured proofs safe for telemetry.
type RecoveryEvidence struct {
	WriterExited  EvidenceVerdict
	ChildExited   EvidenceVerdict
	BindingEqual  EvidenceVerdict
	ComposerEmpty EvidenceVerdict
	LineVisible   EvidenceVerdict
	Record        *RecordEvidence
}

// RecoveryResult is the idempotent acknowledgement returned by NodeWriter.
type RecoveryResult struct {
	Changed  bool
	Outcome  RecoveryOutcome
	Evidence RecoveryEvidence
}

// RecoveryProcess is one process identity read from the durable state.
type RecoveryProcess struct {
	PID   int64
	Start uint64
}

// ReconcileSnapshot is the minimum durable state an observer needs to obtain
// fresh process and pane proofs. It deliberately contains no screen bytes.
type ReconcileSnapshot struct {
	Writer     RecoveryProcess
	Child      RecoveryProcess
	RecordedAt time.Time
}

// ReconcileObservation is a fresh, content-free observation made by the node
// while NodeWriter retains the exact pane flock.
type ReconcileObservation struct {
	BindingEqual     EvidenceVerdict
	LineVisible      EvidenceVerdict
	ComposerEmpty    EvidenceVerdict
	NoticeAbsent     EvidenceVerdict
	WriterExited     EvidenceVerdict
	ChildExited      EvidenceVerdict
	DrainWaitElapsed EvidenceVerdict
}

// ReconcileObserver reads tmux and process evidence for snapshot. The client
// cannot supply this callback or any of its verdicts.
type ReconcileObserver func(context.Context, ReconcileSnapshot) (ReconcileObservation, error)

// Reconcile conditionally resolves one uncertain attempt from fresh evidence.
func (w *NodeWriter) Reconcile(
	ctx context.Context,
	binding LiveBinding,
	expectedAttempt string,
	observe ReconcileObserver,
) (RecoveryResult, error) {
	result := emptyRecoveryResult()
	if w == nil || ctx == nil || binding.Host != w.host ||
		!validHex(expectedAttempt, 32) || observe == nil {
		return result, fmt.Errorf("reconcile pane: %w", ErrInvalidBinding)
	}
	if err := validateLiveBinding(binding); err != nil {
		return result, err
	}
	if err := ctx.Err(); err != nil {
		return result, fmt.Errorf("reconcile pane context: %w", err)
	}
	if err := w.requireMode(); err != nil {
		return result, err
	}
	return w.reconcilePlatform(ctx, binding, expectedAttempt, observe)
}

// Repair replaces only an invalid or incoherent state copy. The transaction
// identity is generated inside NodeWriter after it owns the pane flock.
func (w *NodeWriter) Repair(ctx context.Context, binding LiveBinding) (RecoveryResult, error) {
	result := emptyRecoveryResult()
	if w == nil || ctx == nil || binding.Host != w.host {
		return result, fmt.Errorf("repair pane: %w", ErrInvalidBinding)
	}
	if err := validateLiveBinding(binding); err != nil {
		return result, err
	}
	if err := ctx.Err(); err != nil {
		return result, fmt.Errorf("repair pane context: %w", err)
	}
	if err := w.requireMode(); err != nil {
		return result, err
	}
	return w.repairPlatform(ctx, binding)
}

func (w *NodeWriter) requireMode() error {
	if err := requireProtocolModePlatform(
		w.modePath, w.root, w.hooks.modeUID, w.hooks.modeGID,
	); err != nil {
		return fmt.Errorf("validate pane-lock mode: %w", ErrWriterInvalid)
	}
	return nil
}

func emptyRecoveryResult() RecoveryResult {
	return RecoveryResult{
		Outcome: RecoveryUnchanged,
		Evidence: RecoveryEvidence{
			WriterExited: EvidenceUnknown, ChildExited: EvidenceUnknown,
			BindingEqual: EvidenceUnknown, ComposerEmpty: EvidenceUnknown,
			LineVisible: EvidenceUnknown,
		},
	}
}

func evidenceVerdict(value bool) EvidenceVerdict {
	if value {
		return EvidenceYes
	}
	return EvidenceNo
}

func publicRecoveryError(operation string, err error) error {
	if errors.Is(err, errReconcileState) || errors.Is(err, errRepairState) ||
		errors.Is(err, errInvalidRecord) {
		return fmt.Errorf("%s pane state: %w", operation, ErrWriterInvalid)
	}
	return err
}
