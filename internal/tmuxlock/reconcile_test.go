package tmuxlock

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestReconcileStateConfirmed(t *testing.T) {
	t.Parallel()
	current := uncertainState(3)
	next, outcome, err := reconcileState(current, reconcileEvidence{
		BindingMatches: true,
		EffectVisible:  true,
	}, recordTestTime())
	require.NoError(t, err)
	require.Equal(t, reconcileConfirmed, outcome)
	require.Equal(t, phaseDone, next.Phase)
	require.Equal(t, "CONFIRMADO", next.Result)
	require.Equal(t, "reconcile", next.Writer)
	require.Equal(t, current.Gen+1, next.Gen)
	require.Equal(t, current.Attempt, next.Attempt)
	require.Equal(t, current.Operation, next.Operation)
	require.Equal(t, current.Step, next.Step)
	require.Equal(t, current.WriterPID, next.WriterPID)
	require.Equal(t, current.ChildPID, next.ChildPID)
}

func TestReconcileStateConfirmsRunningAttempt(t *testing.T) {
	t.Parallel()
	current := uncertainState(1)
	current.Phase = phaseRunning
	next, outcome, err := reconcileState(current, reconcileEvidence{
		BindingMatches: true,
		EffectVisible:  true,
	}, recordTestTime())
	require.NoError(t, err)
	require.Equal(t, reconcileConfirmed, outcome)
	require.Equal(t, phaseDone, next.Phase)
	require.Equal(t, "CONFIRMADO", next.Result)
}

func TestReconcileStateDoesNotConfirmChangedBinding(t *testing.T) {
	t.Parallel()
	current := uncertainState(1)
	next, outcome, err := reconcileState(current, reconcileEvidence{
		EffectVisible: true,
	}, recordTestTime())
	require.NoError(t, err)
	require.Equal(t, reconcileUnchanged, outcome)
	require.Equal(t, current, next)
}

func TestReconcileStateNoEffect(t *testing.T) {
	t.Parallel()
	for _, phase := range []string{phaseRunning, phaseUncertain} {
		t.Run(phase, func(t *testing.T) {
			t.Parallel()
			current := uncertainState(0)
			current.Phase = phase
			next, outcome, err := reconcileState(current, reconcileEvidence{
				BindingMatches:   true,
				ComposerEmpty:    true,
				NoticeAbsent:     true,
				WriterExited:     true,
				ChildExited:      true,
				DrainWaitElapsed: true,
			}, recordTestTime())
			require.NoError(t, err)
			require.Equal(t, reconcileNoEffect, outcome)
			require.Equal(t, phaseIdle, next.Phase)
			require.Equal(t, "SIN_EFECTO", next.Result)
			require.Equal(t, "reconcile", next.Writer)
			require.Equal(t, current.Gen+1, next.Gen)
		})
	}
}

func TestReconcileStateKeepsUncertainWithoutCompleteProof(t *testing.T) {
	t.Parallel()
	complete := reconcileEvidence{
		BindingMatches:   true,
		ComposerEmpty:    true,
		NoticeAbsent:     true,
		WriterExited:     true,
		ChildExited:      true,
		DrainWaitElapsed: true,
	}
	mutants := map[string]func(*stateRecord, *reconcileEvidence){
		"unknown step":       func(state *stateRecord, _ *reconcileEvidence) { state.Step = -1 },
		"effect launched":    func(state *stateRecord, _ *reconcileEvidence) { state.Step = 1 },
		"binding changed":    func(_ *stateRecord, proof *reconcileEvidence) { proof.BindingMatches = false },
		"composer busy":      func(_ *stateRecord, proof *reconcileEvidence) { proof.ComposerEmpty = false },
		"notice present":     func(_ *stateRecord, proof *reconcileEvidence) { proof.NoticeAbsent = false },
		"writer alive":       func(_ *stateRecord, proof *reconcileEvidence) { proof.WriterExited = false },
		"child alive":        func(_ *stateRecord, proof *reconcileEvidence) { proof.ChildExited = false },
		"drain wait pending": func(_ *stateRecord, proof *reconcileEvidence) { proof.DrainWaitElapsed = false },
	}
	for name, mutate := range mutants {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			current := uncertainState(0)
			proof := complete
			mutate(&current, &proof)
			next, outcome, err := reconcileState(current, proof, recordTestTime())
			require.NoError(t, err)
			require.Equal(t, reconcileUnchanged, outcome)
			require.Equal(t, current, next)
		})
	}
}

func TestReconcileStateRejectsTerminalPhase(t *testing.T) {
	t.Parallel()
	current := uncertainState(0)
	current.Phase = phaseDone
	next, outcome, err := reconcileState(current, reconcileEvidence{
		BindingMatches: true,
		EffectVisible:  true,
	}, recordTestTime())
	require.ErrorIs(t, err, errReconcileState)
	require.Equal(t, reconcileUnchanged, outcome)
	require.Equal(t, current, next)
}

func TestReconcilePanePersistsOnlyTerminalState(t *testing.T) {
	t.Parallel()
	store := newRepairStore()
	store.state = uncertainState(1)
	outcome, err := reconcilePane(store, store.state.Attempt, fixedReconcileEvidence(reconcileEvidence{
		BindingMatches: true,
		EffectVisible:  true,
	}), recordTestTime())
	require.NoError(t, err)
	require.Equal(t, reconcileConfirmed, outcome)
	require.Equal(t, []string{repairWriteState}, store.writes)
	require.Equal(t, phaseDone, store.state.Phase)
	require.Equal(t, "CONFIRMADO", store.state.Result)
}

func TestReconcilePaneDoesNotWriteWithoutProof(t *testing.T) {
	t.Parallel()
	store := newRepairStore()
	store.state = uncertainState(1)
	before := store.state
	outcome, err := reconcilePane(store, store.state.Attempt,
		fixedReconcileEvidence(reconcileEvidence{BindingMatches: true}), recordTestTime())
	require.NoError(t, err)
	require.Equal(t, reconcileUnchanged, outcome)
	require.Empty(t, store.writes)
	require.Equal(t, before, store.state)
}

func TestReconcilePaneRejectsPendingTransaction(t *testing.T) {
	t.Parallel()
	store := newRepairStore()
	store.state = uncertainState(1)
	store.register.Pending = &pendingRecord{
		FromEpoch: 0, FromGen: store.state.Gen, Operation: operationRepair, ToEpoch: 1,
		ToOwner: nodeOwner, ToOwnerGen: 0, TxID: repairTestTxID,
	}
	store.register.Epoch = 1
	outcome, err := reconcilePane(store, store.state.Attempt, fixedReconcileEvidence(reconcileEvidence{
		BindingMatches: true,
		EffectVisible:  true,
	}), recordTestTime())
	require.ErrorIs(t, err, errReconcileState)
	require.Equal(t, reconcileUnchanged, outcome)
	require.Empty(t, store.writes)
}

func TestReconcilePaneDoesNotPublishFailedWrite(t *testing.T) {
	t.Parallel()
	store := newRepairStore()
	store.state = uncertainState(1)
	before := store.state
	store.failAt = 1
	outcome, err := reconcilePane(store, store.state.Attempt, fixedReconcileEvidence(reconcileEvidence{
		BindingMatches: true,
		EffectVisible:  true,
	}), recordTestTime())
	require.ErrorIs(t, err, errRepairWrite)
	require.Equal(t, reconcileUnchanged, outcome)
	require.Equal(t, before, store.state)
}

func TestReconcilePaneRejectsStaleAttempt(t *testing.T) {
	t.Parallel()
	store := newRepairStore()
	store.state = uncertainState(1)
	before := store.state
	observed := false
	outcome, err := reconcilePane(store, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		func(stateRecord) (reconcileEvidence, error) {
			observed = true
			return reconcileEvidence{BindingMatches: true, EffectVisible: true}, nil
		}, recordTestTime())
	require.ErrorIs(t, err, errReconcileState)
	require.Equal(t, reconcileUnchanged, outcome)
	require.False(t, observed, "stale attempts are rejected before process or screen observation")
	require.Empty(t, store.writes)
	require.Equal(t, before, store.state)
}

func TestReconcilePaneRejectsInvalidStateBeforeObservation(t *testing.T) {
	t.Parallel()
	store := newRepairStore()
	store.state = uncertainState(1)
	store.state.Writer = "NOT-CANONICAL"
	observed := false
	outcome, err := reconcilePane(store, store.state.Attempt,
		func(stateRecord) (reconcileEvidence, error) {
			observed = true
			return reconcileEvidence{BindingMatches: true, EffectVisible: true}, nil
		}, recordTestTime())
	require.ErrorIs(t, err, errReconcileState)
	require.Equal(t, reconcileUnchanged, outcome)
	require.False(t, observed, "invalid state is rejected before process or screen observation")
	require.Empty(t, store.writes)
}

func fixedReconcileEvidence(evidence reconcileEvidence) reconcileObserver {
	return func(stateRecord) (reconcileEvidence, error) { return evidence, nil }
}

func uncertainState(step int) stateRecord {
	state := initialState(neutralRecordBinding(), nodeOwner,
		time.Date(2026, 11, 2, 21, 20, 0, 0, time.UTC))
	state.Attempt = "11111111111111111111111111111111"
	state.Operation = "2222222222222222222222222222222222222222222222222222222222222222"
	state.ChildPID = 3334
	state.ChildStart = "555556"
	state.Gen = 3
	state.Phase = phaseUncertain
	state.Step = step
	state.Writer = nodeOwner
	state.WriterPID = 3333
	state.WriterStart = "555555"
	return state
}

func recordTestTime() time.Time {
	return time.Date(2026, 11, 2, 21, 25, 0, 0, time.UTC)
}
