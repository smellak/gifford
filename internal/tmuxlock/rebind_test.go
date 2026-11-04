package tmuxlock

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// rebindStore is a repair store whose register still binds a previous
// incarnation of the live pane, with fixed evidence about that incarnation.
type rebindStore struct {
	*repairStore
	target   recordBinding
	evidence rebindEvidence
}

func (store *rebindStore) Rebinding(stateRecord, bool) (recordBinding, rebindEvidence, bool) {
	if store.register.Binding == store.target {
		return recordBinding{}, rebindEvidence{}, false
	}
	return store.target, store.evidence, true
}

// newRebindStore holds a coherent record for a pane that tmux recreated:
// same host, owner, socket and pane id, another pane process.
func newRebindStore(state stateRecord) *rebindStore {
	base := newRepairStore()
	base.register.Epoch, base.register.OwnerGen = 4, 2
	state.Epoch, state.OwnerGen = 4, 2
	base.state = state
	target := base.register.Binding
	target.PanePID, target.PaneStart = 525252, "1234567"
	return &rebindStore{
		repairStore: base, target: target,
		evidence: rebindEvidence{PaneGone: true, WriterExited: true, ChildExited: true},
	}
}

func doneState() stateRecord {
	state := uncertainState(4)
	state.Phase = phaseDone
	state.Result = resultConfirmed
	return state
}

// A record bound to a pane that tmux recreated is bound to the live pane
// and parked repaired one epoch later, keeping its owner; a reconcile with
// evidence frees it like any repaired record.
func TestRepairRebindsRecordOfRecreatedPane(t *testing.T) {
	t.Parallel()
	for name, state := range map[string]stateRecord{
		"finished":                    doneState(),
		"uncertain, writer gone":      uncertainState(2),
		"in flight, writer gone":      func() stateRecord { s := uncertainState(3); s.Phase = phaseRunning; return s }(),
		"idle after a repair release": initialState(neutralRecordBinding(), nodeOwner, recordTestTime()),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			store := newRebindStore(state)
			outcome, err := repairState(store, repairTestTxID, recordTestTime())
			require.NoError(t, err)
			require.Equal(t, repairApplied, outcome)
			require.Equal(t, []string{repairWriteRecord, repairWriteState, repairWriteRecord}, store.writes)
			require.Nil(t, store.register.Pending)
			require.Equal(t, store.target, store.register.Binding)
			require.Equal(t, store.target, store.state.binding())
			require.Equal(t, uint32(5), store.register.Epoch)
			require.Equal(t, uint32(5), store.state.Epoch)
			require.Equal(t, nodeOwner, store.register.Owner)
			require.Equal(t, uint32(2), store.register.OwnerGen)
			require.True(t, recordsAgree(store.register, store.state))
			require.True(t, repairedState(store.state))

			store.resetWrites()
			replayed, err := repairState(store, repairOtherTxID, recordTestTime())
			require.NoError(t, err)
			require.Equal(t, repairNoop, replayed, "a repeated repair changes nothing")
			require.Empty(t, store.writes)
		})
	}
}

// Nothing is rebound while the previous incarnation, or a writer or child
// of a write it left in flight, may still be running.
func TestRepairRefusesRebindWithoutProof(t *testing.T) {
	t.Parallel()
	for name, test := range map[string]struct {
		state    stateRecord
		evidence rebindEvidence
	}{
		"pane process alive": {doneState(), rebindEvidence{WriterExited: true, ChildExited: true}},
		"writer alive":       {uncertainState(2), rebindEvidence{PaneGone: true, ChildExited: true}},
		"child alive":        {uncertainState(3), rebindEvidence{PaneGone: true, WriterExited: true}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			store := newRebindStore(test.state)
			store.evidence = test.evidence
			before := store.register
			_, err := repairState(store, repairTestTxID, recordTestTime())
			require.ErrorIs(t, err, errRepairState)
			require.Empty(t, store.writes)
			require.Equal(t, before, store.register)
		})
	}
}

// A finished write needs no writer proof: its writer may still be the
// running node, which holds nothing in the pane.
func TestRepairRebindsFinishedRecordWithLiveWriter(t *testing.T) {
	t.Parallel()
	store := newRebindStore(doneState())
	store.evidence = rebindEvidence{PaneGone: true}
	outcome, err := repairState(store, repairTestTxID, recordTestTime())
	require.NoError(t, err)
	require.Equal(t, repairApplied, outcome)
}

// Records that are not one coherent pair, or that carry a pending
// operation, are never rebound.
func TestRepairRefusesRebindOfIncoherentRecord(t *testing.T) {
	t.Parallel()
	for name, mutate := range map[string]func(*rebindStore){
		"unreadable state": func(store *rebindStore) { store.stateErr = errInvalidRecord },
		"copies disagree":  func(store *rebindStore) { store.state.Epoch++ },
		"pending transfer": func(store *rebindStore) {
			store.register.Pending = &pendingRecord{
				FromEpoch: 4, FromGen: 3, Operation: operationTransfer,
				ToEpoch: 4, ToOwner: nodeOwner, ToOwnerGen: 3, TxID: repairOtherTxID,
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			store := newRebindStore(doneState())
			mutate(store)
			_, err := repairState(store, repairTestTxID, recordTestTime())
			require.ErrorIs(t, err, errRepairState)
			require.Empty(t, store.writes)
		})
	}
}

// A rebind interrupted after its register write finishes on the next
// repair, from any phase the previous incarnation left: the proof was made
// before that write.
func TestRepairFinishesInterruptedRebind(t *testing.T) {
	t.Parallel()
	store := newRebindStore(uncertainState(2))
	store.failAt = 2
	_, err := repairState(store, repairTestTxID, recordTestTime())
	require.ErrorIs(t, err, errRepairWrite)
	require.Equal(t, store.target, store.register.Binding)
	require.NotNil(t, store.register.Pending)
	require.Equal(t, phaseUncertain, store.state.Phase, "the state still binds the previous incarnation")

	store.resetWrites()
	outcome, err := repairState(store, repairOtherTxID, recordTestTime())
	require.NoError(t, err)
	require.Equal(t, repairApplied, outcome)
	require.Nil(t, store.register.Pending)
	require.True(t, recordsAgree(store.register, store.state))
	require.True(t, repairedState(store.state))
	require.Equal(t, store.target, store.state.binding())
}
