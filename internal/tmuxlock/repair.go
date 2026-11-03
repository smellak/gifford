package tmuxlock

import (
	"errors"
	"fmt"
	"time"
)

var errRepairState = errors.New("pane state cannot be repaired")

type repairOutcome uint8

const (
	repairNoop repairOutcome = iota
	repairApplied
)

// repairRecordStore is deliberately smaller than a filesystem. Implementors
// keep the pane flock for the whole call and make each write durable before it
// returns.
type repairRecordStore interface {
	ReadRegister() (registerRecord, error)
	ReadState() (stateRecord, error)
	WriteRegister(registerRecord) error
	WriteState(stateRecord) error
}

func repairState(
	store repairRecordStore,
	txID string,
	timestamp time.Time,
) (repairOutcome, error) {
	if store == nil || !validHex(txID, 32) || timestamp.IsZero() {
		return repairNoop, errRepairState
	}
	register, err := store.ReadRegister()
	if err != nil || validateRegister(register) != nil {
		return repairNoop, errRepairState
	}
	state, stateErr := store.ReadState()
	if stateErr != nil && !errors.Is(stateErr, errInvalidRecord) {
		return repairNoop, fmt.Errorf("read pane state for repair: %w", stateErr)
	}
	if stateErr == nil && validateState(state) != nil {
		stateErr = errInvalidRecord
	}
	if register.Pending != nil {
		return resumeRepair(store, register, state, stateErr, timestamp)
	}
	if stateErr == nil && recordsAgree(register, state) {
		if repairedState(state) {
			return repairNoop, nil
		}
		return repairNoop, errRepairState
	}
	if stateErr == nil && !repairOriginState(state) {
		return repairNoop, errRepairState
	}
	if register.Epoch == maxRecordEpoch {
		return repairNoop, errRepairState
	}

	fromEpoch := register.Epoch
	fromGen := uint64(0)
	if stateErr == nil {
		fromEpoch = state.Epoch
		fromGen = state.Gen
	}
	pending := &pendingRecord{
		FromEpoch: fromEpoch, FromGen: fromGen, Operation: operationRepair,
		ToEpoch: register.Epoch + 1, ToOwner: register.Owner,
		ToOwnerGen: register.OwnerGen, TxID: txID,
	}
	register.Epoch = pending.ToEpoch
	register.Pending = pending
	if err := store.WriteRegister(register); err != nil {
		return repairNoop, fmt.Errorf("write pending pane repair: %w", err)
	}
	return finishRepair(store, register, timestamp)
}

func resumeRepair(
	store repairRecordStore,
	register registerRecord,
	state stateRecord,
	stateErr error,
	timestamp time.Time,
) (repairOutcome, error) {
	pending := register.Pending
	if pending.Operation != operationRepair || pending.ToEpoch != register.Epoch ||
		pending.ToOwner != register.Owner || pending.ToOwnerGen != register.OwnerGen {
		return repairNoop, errRepairState
	}
	if stateErr == nil && state.TxID == pending.TxID {
		if !recordCopiesAgree(register, state) || !repairedState(state) {
			return repairNoop, errRepairState
		}
		register.Pending = nil
		if err := store.WriteRegister(register); err != nil {
			return repairNoop, fmt.Errorf("complete pending pane repair: %w", err)
		}
		return repairApplied, nil
	}
	if stateErr == nil && ((state.Epoch != pending.FromEpoch || state.Gen != pending.FromGen) ||
		!repairOriginState(state)) {
		return repairNoop, errRepairState
	}
	return finishRepair(store, register, timestamp)
}

func finishRepair(
	store repairRecordStore,
	register registerRecord,
	timestamp time.Time,
) (repairOutcome, error) {
	pending := register.Pending
	next := initialState(register.Binding, register.Owner, timestamp)
	next.Epoch = register.Epoch
	next.OwnerGen = register.OwnerGen
	next.Phase = phaseUncertain
	next.Result = resultRepaired
	next.Step = -1
	next.TxID = pending.TxID
	next.Writer = operationRepair
	if err := validateState(next); err != nil {
		return repairNoop, errRepairState
	}
	if err := store.WriteState(next); err != nil {
		return repairNoop, fmt.Errorf("write repaired pane state: %w", err)
	}
	register.Pending = nil
	if err := store.WriteRegister(register); err != nil {
		return repairNoop, fmt.Errorf("clear pending pane repair: %w", err)
	}
	return repairApplied, nil
}

func recordsAgree(register registerRecord, state stateRecord) bool {
	return register.Pending == nil && recordCopiesAgree(register, state)
}

func recordCopiesAgree(register registerRecord, state stateRecord) bool {
	return register.Binding == state.binding() &&
		register.Epoch == state.Epoch && register.Owner == state.Owner &&
		register.OwnerGen == state.OwnerGen
}

func repairedState(state stateRecord) bool {
	return state.Phase == phaseUncertain && state.Result == resultRepaired &&
		state.Writer == operationRepair && state.Gen == 0 && state.Step == -1 && state.Attempt == "-" &&
		state.Operation == "-" && state.WriterPID == 0 && state.WriterStart == "0" &&
		state.ChildPID == 0 && state.ChildStart == "0" && validHex(state.TxID, 32)
}

func repairOriginState(state stateRecord) bool {
	return state.Phase == phaseIdle || state.Phase == phaseDone || state.Phase == phaseDeferred
}
