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
	if err != nil || validateRegisterShape(register) != nil {
		return repairNoop, errRepairState
	}
	state, stateErr := store.ReadState()
	if stateErr != nil && !errors.Is(stateErr, errInvalidRecord) {
		return repairNoop, fmt.Errorf("read pane state for repair: %w", stateErr)
	}
	if stateErr == nil && validateStateShape(state) != nil {
		stateErr = errInvalidRecord
	}
	if rebinding, ok := store.(rebindingStore); ok {
		if target, evidence, previous := rebinding.Rebinding(state, stateErr == nil); previous {
			return rebindRepair(store, register, state, stateErr, target, evidence, txID, timestamp)
		}
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
	if stateErr == nil && repairedAheadOfRegister(register, state) {
		// A finished repair whose register write was lost or overwritten.
		// Adopting the state's epoch keeps the pane parked, and a reconcile
		// with evidence can then free it.
		register.Epoch = state.Epoch
		if err := store.WriteRegister(register); err != nil {
			return repairNoop, fmt.Errorf("complete pane repair register: %w", err)
		}
		return repairApplied, nil
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
	// A state that still binds the pane's previous incarnation is a rebind
	// whose register write landed: its writer and child were proven gone
	// before that write, so any phase may finish.
	rebound := stateErr == nil && state.binding() != register.Binding
	if stateErr == nil && ((state.Epoch != pending.FromEpoch || state.Gen != pending.FromGen) ||
		!rebound && !repairOriginState(state)) {
		return repairNoop, errRepairState
	}
	return finishRepair(store, register, timestamp)
}

// rebindEvidence is what the node proved about the incarnation of a pane
// that a record still binds: its pane process is gone, and so are the
// writer and the child its state names.
type rebindEvidence struct {
	PaneGone     bool
	WriterExited bool
	ChildExited  bool
}

// rebindingStore is a repair store whose register still binds a previous
// incarnation of the live pane: tmux recreated the pane with the same
// socket and pane id. Rebinding returns the live binding and the evidence
// about the previous incarnation; previous is false for a register that
// binds the live pane.
type rebindingStore interface {
	Rebinding(state stateRecord, stateValid bool) (target recordBinding, evidence rebindEvidence, previous bool)
}

// rebindRepair repairs a record that still binds a previous incarnation of
// the live pane. It runs only once the node proved that incarnation gone,
// together with the writer and child of any write it left in flight, then
// binds the record to the live pane and parks it repaired, like any repair:
// a reconcile with evidence frees it. A state the node cannot read leaves
// nothing to prove a writer gone, so it is refused.
func rebindRepair(
	store repairRecordStore,
	register registerRecord,
	state stateRecord,
	stateErr error,
	target recordBinding,
	evidence rebindEvidence,
	txID string,
	timestamp time.Time,
) (repairOutcome, error) {
	if register.Pending != nil || register.Epoch == maxRecordEpoch || stateErr != nil ||
		!recordsAgree(register, state) || !evidence.PaneGone {
		return repairNoop, errRepairState
	}
	inFlight := state.Phase == phaseRunning || state.Phase == phaseUncertain
	if inFlight && (!evidence.WriterExited || !evidence.ChildExited) {
		return repairNoop, errRepairState
	}
	pending := &pendingRecord{
		FromEpoch: state.Epoch, FromGen: state.Gen, Operation: operationRepair,
		ToEpoch: register.Epoch + 1, ToOwner: register.Owner,
		ToOwnerGen: register.OwnerGen, TxID: txID,
	}
	register.Binding = target
	register.Epoch = pending.ToEpoch
	register.Pending = pending
	if err := store.WriteRegister(register); err != nil {
		return repairNoop, fmt.Errorf("write pending pane rebind: %w", err)
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
	if err := validateStateShape(next); err != nil {
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

// repairedAheadOfRegister reports a repaired state exactly one epoch ahead
// of a register with no pending operation that agrees on everything else.
func repairedAheadOfRegister(register registerRecord, state stateRecord) bool {
	return register.Pending == nil && repairedState(state) && register.Epoch < maxRecordEpoch &&
		state.Epoch == register.Epoch+1 && register.Binding == state.binding() &&
		register.Owner == state.Owner && register.OwnerGen == state.OwnerGen
}

func repairOriginState(state stateRecord) bool {
	return state.Phase == phaseIdle || state.Phase == phaseDone || state.Phase == phaseDeferred
}
