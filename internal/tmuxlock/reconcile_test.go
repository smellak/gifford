package tmuxlock

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	testAttemptID   = "11111111111111111111111111111111"
	testOperationID = "2222222222222222222222222222222222222222222222222222222222222222"
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
	require.Equal(t, resultConfirmed, next.Result)
	require.Equal(t, operationReconcile, next.Writer)
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
	require.Equal(t, resultConfirmed, next.Result)
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
			require.Equal(t, resultNoEffect, next.Result)
			require.Equal(t, operationReconcile, next.Writer)
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
		"binding changed":    func(_ *stateRecord, proof *reconcileEvidence) { proof.BindingMatches = false },
		"composer busy":      func(_ *stateRecord, proof *reconcileEvidence) { proof.ComposerEmpty = false },
		"notice present":     func(_ *stateRecord, proof *reconcileEvidence) { proof.NoticeAbsent = false },
		"writer alive":       func(_ *stateRecord, proof *reconcileEvidence) { proof.WriterExited = false },
		"child alive":        func(_ *stateRecord, proof *reconcileEvidence) { proof.ChildExited = false },
		"drain wait pending": func(_ *stateRecord, proof *reconcileEvidence) { proof.DrainWaitElapsed = false },
	}
	for _, step := range []int{0, 2} {
		for name, mutate := range mutants {
			t.Run(fmt.Sprintf("step %d %s", step, name), func(t *testing.T) {
				t.Parallel()
				current := uncertainState(0)
				current.Step = step
				proof := complete
				mutate(&current, &proof)
				next, outcome, err := reconcileState(current, proof, recordTestTime())
				require.NoError(t, err)
				require.Equal(t, reconcileUnchanged, outcome)
				require.Equal(t, current, next)
			})
		}
	}
}

// An attempt that typed its notice frees the pane once the pane holds
// nothing of it, even when the notice line has left the screen: the box is
// empty, no writer or child remains and the drain wait is over.
func TestReconcileStateFreesClearPaneAfterLaunchedEffect(t *testing.T) {
	t.Parallel()
	for step := 1; step <= 4; step++ {
		t.Run(fmt.Sprintf("step %d", step), func(t *testing.T) {
			t.Parallel()
			current := uncertainState(0)
			current.Step = step
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
			require.Equal(t, resultNoEffect, next.Result)
			require.Equal(t, operationReconcile, next.Writer)
			require.Equal(t, step, next.Step)
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
	require.Equal(t, resultConfirmed, store.state.Result)
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

func TestReconcilePaneTerminalReplayIsUnchangedWithoutObservation(t *testing.T) {
	t.Parallel()
	store := newRepairStore()
	store.state = uncertainState(1)
	store.state.Phase = phaseDone
	store.state.Result = resultConfirmed
	store.state.Writer = operationReconcile
	observed := false
	outcome, err := reconcilePane(store, store.state.Attempt,
		func(stateRecord) (reconcileEvidence, error) {
			observed = true
			return reconcileEvidence{}, nil
		}, recordTestTime())
	require.NoError(t, err)
	require.Equal(t, reconcileUnchanged, outcome)
	require.False(t, observed)
	require.Empty(t, store.writes)
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

func TestReconcilePaneFreesRepairedRecordWithDash(t *testing.T) {
	t.Parallel()
	store := repairedReconcileStore(t)
	// A visible notice line names no attempt of a repaired record, so it
	// neither confirms the record nor keeps it parked.
	outcome, err := reconcilePane(store, "-", fixedReconcileEvidence(reconcileEvidence{
		BindingMatches: true, EffectVisible: true, ComposerEmpty: true,
		WriterExited: true, ChildExited: true, DrainWaitElapsed: true,
	}), recordTestTime())
	require.NoError(t, err)
	require.Equal(t, reconcileNoEffect, outcome)
	require.Equal(t, []string{repairWriteState}, store.writes)
	require.Equal(t, phaseIdle, store.state.Phase)
	require.Equal(t, resultNoEffect, store.state.Result)
	require.Equal(t, operationReconcile, store.state.Writer)
	require.Equal(t, uint64(1), store.state.Gen)
	require.Equal(t, 0, store.state.Step)
	require.Equal(t, "-", store.state.Attempt)
	require.Equal(t, repairTestTxID, store.state.TxID)

	store.resetWrites()
	observed := false
	replayed, err := reconcilePane(store, "-", func(stateRecord) (reconcileEvidence, error) {
		observed = true
		return reconcileEvidence{}, nil
	}, recordTestTime())
	require.NoError(t, err)
	require.Equal(t, reconcileUnchanged, replayed)
	require.False(t, observed, "the reconciled record replays without observing the pane")
	require.Empty(t, store.writes)
}

func TestReconcilePaneKeepsRepairedRecordWithoutCompleteProof(t *testing.T) {
	t.Parallel()
	complete := reconcileEvidence{
		BindingMatches: true, ComposerEmpty: true,
		WriterExited: true, ChildExited: true, DrainWaitElapsed: true,
	}
	for name, missing := range map[string]func(*reconcileEvidence){
		"binding":  func(evidence *reconcileEvidence) { evidence.BindingMatches = false },
		"composer": func(evidence *reconcileEvidence) { evidence.ComposerEmpty = false },
		"writer":   func(evidence *reconcileEvidence) { evidence.WriterExited = false },
		"child":    func(evidence *reconcileEvidence) { evidence.ChildExited = false },
		"drain":    func(evidence *reconcileEvidence) { evidence.DrainWaitElapsed = false },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			store := repairedReconcileStore(t)
			before := store.state
			evidence := complete
			missing(&evidence)
			outcome, err := reconcilePane(store, "-", fixedReconcileEvidence(evidence), recordTestTime())
			require.NoError(t, err)
			require.Equal(t, reconcileUnchanged, outcome)
			require.Empty(t, store.writes)
			require.Equal(t, before, store.state)
		})
	}
}

func TestReconcilePaneRejectsDashOutsideRepairedRecord(t *testing.T) {
	t.Parallel()
	idle := newRepairStore()
	uncertain := newRepairStore()
	uncertain.state = uncertainState(0)
	uncertain.state.Attempt = "-"
	for name, store := range map[string]*repairStore{"idle": idle, "uncertain": uncertain} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			observed := false
			outcome, err := reconcilePane(store, "-", func(stateRecord) (reconcileEvidence, error) {
				observed = true
				return reconcileEvidence{BindingMatches: true, EffectVisible: true}, nil
			}, recordTestTime())
			require.ErrorIs(t, err, errReconcileState)
			require.Equal(t, reconcileUnchanged, outcome)
			require.False(t, observed, "dash is rejected before process or screen observation")
			require.Empty(t, store.writes)
		})
	}
}

func TestReconcilePaneRejectsOldAttemptForRepairedRecord(t *testing.T) {
	t.Parallel()
	store := repairedReconcileStore(t)
	before := store.state
	observed := false
	outcome, err := reconcilePane(store, testAttemptID, func(stateRecord) (reconcileEvidence, error) {
		observed = true
		return reconcileEvidence{BindingMatches: true, EffectVisible: true}, nil
	}, recordTestTime())
	require.ErrorIs(t, err, errReconcileState)
	require.Equal(t, reconcileUnchanged, outcome)
	require.False(t, observed, "an old attempt cannot clear a repaired record")
	require.Equal(t, before, store.state)
}

func repairedReconcileStore(t *testing.T) *repairStore {
	t.Helper()
	store := newRepairStore()
	store.stateErr = errInvalidRecord
	outcome, err := repairState(store, repairTestTxID, recordTestTime())
	require.NoError(t, err)
	require.Equal(t, repairApplied, outcome)
	require.True(t, repairedState(store.state))
	store.resetWrites()
	return store
}

func fixedReconcileEvidence(evidence reconcileEvidence) reconcileObserver {
	return func(stateRecord) (reconcileEvidence, error) { return evidence, nil }
}

func uncertainState(step int) stateRecord {
	state := initialState(neutralRecordBinding(), nodeOwner,
		time.Date(2026, 11, 2, 21, 20, 0, 0, time.UTC))
	state.Attempt = testAttemptID
	state.Operation = testOperationID
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
