package tmuxlock

import (
	"errors"
	"fmt"
	"time"
)

var errReconcileState = errors.New("pane state cannot be reconciled")

type reconcileOutcome uint8

const (
	reconcileUnchanged reconcileOutcome = iota
	reconcileConfirmed
	reconcileNoEffect
)

// reconcileEvidence is the content-free verdict produced from a fresh pane
// observation. The screen itself must not cross this boundary or enter
// telemetry.
type reconcileEvidence struct {
	BindingMatches   bool
	EffectVisible    bool
	ComposerEmpty    bool
	NoticeAbsent     bool
	WriterExited     bool
	ChildExited      bool
	DrainWaitElapsed bool
}

type reconcileRecordStore interface {
	ReadRegister() (registerRecord, error)
	ReadState() (stateRecord, error)
	WriteState(stateRecord) error
}

// reconcileObserver obtains process and pane evidence for the exact state read
// under the caller's held pane lock. Keeping it behind this internal callback
// prevents a control request from supplying trusted screen verdicts.
type reconcileObserver func(stateRecord) (reconcileEvidence, error)

func reconcilePane(
	store reconcileRecordStore,
	expectedAttempt string,
	observe reconcileObserver,
	timestamp time.Time,
) (reconcileOutcome, error) {
	if store == nil || !validHex(expectedAttempt, 32) || observe == nil {
		return reconcileUnchanged, errReconcileState
	}
	register, err := store.ReadRegister()
	if err != nil {
		return reconcileUnchanged, fmt.Errorf("read pane register for reconcile: %w", err)
	}
	if validateRegister(register) != nil || register.Pending != nil {
		return reconcileUnchanged, errReconcileState
	}
	current, err := store.ReadState()
	if err != nil {
		return reconcileUnchanged, fmt.Errorf("read pane state for reconcile: %w", err)
	}
	if validateState(current) != nil || !recordsAgree(register, current) {
		return reconcileUnchanged, errReconcileState
	}
	if current.Attempt != expectedAttempt {
		return reconcileUnchanged, errReconcileState
	}
	evidence, err := observe(current)
	if err != nil {
		return reconcileUnchanged, fmt.Errorf("observe pane for reconcile: %w", err)
	}
	next, outcome, err := reconcileState(current, evidence, timestamp)
	if err != nil || outcome == reconcileUnchanged {
		return outcome, err
	}
	if err := store.WriteState(next); err != nil {
		return reconcileUnchanged, fmt.Errorf("write reconciled pane state: %w", err)
	}
	return outcome, nil
}

func reconcileState(
	current stateRecord,
	evidence reconcileEvidence,
	timestamp time.Time,
) (stateRecord, reconcileOutcome, error) {
	if current.Phase != phaseRunning && current.Phase != phaseUncertain {
		return current, reconcileUnchanged, errReconcileState
	}
	if err := validateState(current); err != nil || timestamp.IsZero() {
		return current, reconcileUnchanged, errReconcileState
	}
	if !evidence.BindingMatches {
		return current, reconcileUnchanged, nil
	}

	var outcome reconcileOutcome
	var phase string
	var result string
	switch {
	case evidence.EffectVisible:
		outcome = reconcileConfirmed
		phase = phaseDone
		result = "CONFIRMADO"
	case current.Step == 0 && evidence.ComposerEmpty && evidence.NoticeAbsent &&
		evidence.WriterExited && evidence.ChildExited && evidence.DrainWaitElapsed:
		outcome = reconcileNoEffect
		phase = phaseIdle
		result = "SIN_EFECTO"
	default:
		return current, reconcileUnchanged, nil
	}
	if current.Gen+1 >= maxSafeInteger {
		return current, reconcileUnchanged, errReconcileState
	}

	next := current
	next.Gen++
	next.Phase = phase
	next.Result = result
	next.Timestamp = formatRecordTime(timestamp)
	next.Writer = "reconcile"
	if err := validateState(next); err != nil {
		return current, reconcileUnchanged, errReconcileState
	}
	return next, outcome, nil
}
