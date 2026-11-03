package tmuxlock

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var errRepairWrite = errors.New("injected durable write failure")

const (
	repairTestTxID    = "33333333333333333333333333333333"
	repairOtherTxID   = "44444444444444444444444444444444"
	repairWriteState  = "state"
	repairWriteRecord = "register"
)

func TestRepairStateCommitsThreeDurableRecords(t *testing.T) {
	t.Parallel()
	store := newRepairStore()
	store.stateErr = errInvalidRecord
	txID := repairTestTxID
	outcome, err := repairState(store, txID, recordTestTime())
	require.NoError(t, err)
	require.Equal(t, repairApplied, outcome)
	require.Equal(t, []string{repairWriteRecord, repairWriteState, repairWriteRecord}, store.writes)
	require.Nil(t, store.register.Pending)
	require.Equal(t, uint32(1), store.register.Epoch)
	require.Equal(t, uint32(1), store.state.Epoch)
	require.Equal(t, uint64(0), store.state.Gen)
	require.Equal(t, phaseUncertain, store.state.Phase)
	require.Equal(t, resultRepaired, store.state.Result)
	require.Equal(t, -1, store.state.Step)
	require.Equal(t, txID, store.state.TxID)
	require.Equal(t, "-", store.state.Attempt)
	require.Equal(t, "-", store.state.Operation)
	require.Zero(t, store.state.WriterPID)
	require.Zero(t, store.state.ChildPID)
}

func TestRepairStateRepeatedAfterLostResponseIsNoop(t *testing.T) {
	t.Parallel()
	store := newRepairStore()
	store.stateErr = errInvalidRecord
	first, err := repairState(store, repairTestTxID, recordTestTime())
	require.NoError(t, err)
	require.Equal(t, repairApplied, first)

	epoch := store.register.Epoch
	state := store.state
	store.resetWrites()
	second, err := repairState(store, repairOtherTxID,
		recordTestTime().Add(time.Minute))
	require.NoError(t, err)
	require.Equal(t, repairNoop, second)
	require.Empty(t, store.writes)
	require.Equal(t, epoch, store.register.Epoch)
	require.Equal(t, state, store.state)
}

func TestRepairStateReplacesInvalidAuthorityCopy(t *testing.T) {
	t.Parallel()
	store := newRepairStore()
	store.register.OwnerGen = 7
	store.state.Owner = "other-owner"
	outcome, err := repairState(store, repairTestTxID, recordTestTime())
	require.NoError(t, err)
	require.Equal(t, repairApplied, outcome)
	require.Equal(t, nodeOwner, store.state.Owner)
	require.Equal(t, store.register.Epoch, store.state.Epoch)
	require.Equal(t, store.register.OwnerGen, store.state.OwnerGen)
}

func TestRepairStateUsesCodecValidation(t *testing.T) {
	t.Parallel()
	store := newRepairStore()
	store.state.Writer = "NOT-CANONICAL"
	outcome, err := repairState(store, repairTestTxID, recordTestTime())
	require.NoError(t, err)
	require.Equal(t, repairApplied, outcome)
	require.Equal(t, []string{repairWriteRecord, repairWriteState, repairWriteRecord}, store.writes)
	require.Equal(t, operationRepair, store.state.Writer)
}

func TestRepairStateRecoversAfterEachInterruptedCommit(t *testing.T) {
	t.Parallel()
	for name, scenario := range map[string]struct {
		failAt     int
		wantWrites []string
	}{
		repairWriteState: {
			failAt:     2,
			wantWrites: []string{repairWriteState, repairWriteRecord},
		},
		"clear pending": {
			failAt:     3,
			wantWrites: []string{repairWriteRecord},
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			store := newRepairStore()
			store.stateErr = errInvalidRecord
			store.failAt = scenario.failAt
			firstTxID := repairTestTxID
			outcome, err := repairState(store, firstTxID, recordTestTime())
			require.ErrorIs(t, err, errRepairWrite)
			require.Equal(t, repairNoop, outcome)
			require.NotNil(t, store.register.Pending)
			require.Equal(t, uint32(1), store.register.Epoch)

			store.resetWrites()
			outcome, err = repairState(store, repairOtherTxID,
				recordTestTime().Add(time.Minute))
			require.NoError(t, err)
			require.Equal(t, repairApplied, outcome)
			require.Nil(t, store.register.Pending)
			require.Equal(t, uint32(1), store.register.Epoch)
			require.Equal(t, firstTxID, store.state.TxID)
			require.Equal(t, scenario.wantWrites, store.writes)
		})
	}
}

func TestRepairStateRecoversAfterEachLostWriteAcknowledgement(t *testing.T) {
	t.Parallel()
	for name, scenario := range map[string]struct {
		failAt      int
		wantOutcome repairOutcome
		wantWrites  []string
	}{
		"pending": {
			failAt: 1, wantOutcome: repairApplied,
			wantWrites: []string{repairWriteState, repairWriteRecord},
		},
		repairWriteState: {
			failAt: 2, wantOutcome: repairApplied,
			wantWrites: []string{repairWriteRecord},
		},
		"clear pending": {
			failAt: 3, wantOutcome: repairNoop,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			store := newRepairStore()
			store.stateErr = errInvalidRecord
			store.failAt = scenario.failAt
			store.applyFailedWrite = true
			outcome, err := repairState(store, repairTestTxID, recordTestTime())
			require.ErrorIs(t, err, errRepairWrite)
			require.Equal(t, repairNoop, outcome)

			store.resetWrites()
			outcome, err = repairState(store, repairOtherTxID,
				recordTestTime().Add(time.Minute))
			require.NoError(t, err)
			require.Equal(t, scenario.wantOutcome, outcome)
			require.Equal(t, scenario.wantWrites, store.writes)
			require.Nil(t, store.register.Pending)
			require.Equal(t, uint32(1), store.register.Epoch)
			require.Equal(t, repairTestTxID, store.state.TxID)
		})
	}
}

func TestRepairStateResumesFromEachAllowedReadableOrigin(t *testing.T) {
	t.Parallel()
	for _, phase := range []string{phaseIdle, phaseDone, phaseDeferred} {
		t.Run(phase, func(t *testing.T) {
			t.Parallel()
			store := newRepairStore()
			store.state.Phase = phase
			store.register.Epoch = 1
			store.register.Pending = &pendingRecord{
				FromEpoch: store.state.Epoch, FromGen: store.state.Gen,
				Operation: operationRepair, ToEpoch: 1, ToOwner: nodeOwner,
				ToOwnerGen: store.register.OwnerGen, TxID: repairTestTxID,
			}

			outcome, err := repairState(store, repairOtherTxID, recordTestTime())
			require.NoError(t, err)
			require.Equal(t, repairApplied, outcome)
			require.Equal(t, []string{repairWriteState, repairWriteRecord}, store.writes)
			require.Nil(t, store.register.Pending)
			require.Equal(t, repairTestTxID, store.state.TxID)
		})
	}
}

func TestRepairStateRejectsForeignPendingOperation(t *testing.T) {
	t.Parallel()
	store := newRepairStore()
	store.register.Pending = &pendingRecord{
		FromEpoch: store.state.Epoch, FromGen: store.state.Gen,
		Operation: operationTransfer, ToEpoch: store.register.Epoch,
		ToOwner: nodeOwner, ToOwnerGen: store.register.OwnerGen, TxID: repairTestTxID,
	}
	outcome, err := repairState(store, repairOtherTxID, recordTestTime())
	require.ErrorIs(t, err, errRepairState)
	require.Equal(t, repairNoop, outcome)
	require.Empty(t, store.writes)
}

func TestRepairStateRejectsValidRecord(t *testing.T) {
	t.Parallel()
	store := newRepairStore()
	beforeRegister := store.register
	beforeState := store.state
	outcome, err := repairState(store, repairTestTxID, recordTestTime())
	require.ErrorIs(t, err, errRepairState)
	require.Equal(t, repairNoop, outcome)
	require.Empty(t, store.writes)
	require.Equal(t, beforeRegister, store.register)
	require.Equal(t, beforeState, store.state)
}

func TestRepairStateRejectsUnrecoverableReadableOrigin(t *testing.T) {
	t.Parallel()
	store := newRepairStore()
	store.state.Owner = "other-owner"
	store.state.Phase = phaseUncertain
	outcome, err := repairState(store, repairTestTxID, recordTestTime())
	require.ErrorIs(t, err, errRepairState)
	require.Equal(t, repairNoop, outcome)
	require.Empty(t, store.writes)
}

func TestRepairStateRejectsMismatchedPendingOrigin(t *testing.T) {
	t.Parallel()
	store := newRepairStore()
	store.register.Epoch = 1
	store.register.Pending = &pendingRecord{
		FromEpoch: 0, FromGen: 99, Operation: operationRepair, ToEpoch: 1,
		ToOwner: nodeOwner, ToOwnerGen: 0, TxID: repairTestTxID,
	}
	outcome, err := repairState(store, repairOtherTxID, recordTestTime())
	require.ErrorIs(t, err, errRepairState)
	require.Equal(t, repairNoop, outcome)
	require.Empty(t, store.writes)
}

func TestRepairStateRejectsMutatedPendingTarget(t *testing.T) {
	t.Parallel()
	store := newRepairStore()
	store.stateErr = errInvalidRecord
	store.failAt = 3
	_, err := repairState(store, repairTestTxID, recordTestTime())
	require.ErrorIs(t, err, errRepairWrite)
	require.NotNil(t, store.register.Pending)
	store.state.Gen = 1
	store.resetWrites()

	outcome, err := repairState(store, repairOtherTxID, recordTestTime())
	require.ErrorIs(t, err, errRepairState)
	require.Equal(t, repairNoop, outcome)
	require.Empty(t, store.writes)
}

type repairStore struct {
	register         registerRecord
	state            stateRecord
	stateErr         error
	writes           []string
	failAt           int
	written          int
	applyFailedWrite bool
}

func newRepairStore() *repairStore {
	binding := neutralRecordBinding()
	return &repairStore{
		register: registerRecord{Binding: binding, Owner: nodeOwner},
		state: initialState(binding, nodeOwner,
			time.Date(2026, 11, 2, 21, 20, 0, 0, time.UTC)),
	}
}

func (store *repairStore) ReadRegister() (registerRecord, error) {
	return cloneRegister(store.register), nil
}

func (store *repairStore) ReadState() (stateRecord, error) {
	return store.state, store.stateErr
}

func (store *repairStore) WriteRegister(value registerRecord) error {
	failed := store.write(repairWriteRecord)
	if failed && !store.applyFailedWrite {
		return errRepairWrite
	}
	store.register = cloneRegister(value)
	if failed {
		return errRepairWrite
	}
	return nil
}

func (store *repairStore) WriteState(value stateRecord) error {
	failed := store.write(repairWriteState)
	if failed && !store.applyFailedWrite {
		return errRepairWrite
	}
	store.state = value
	store.stateErr = nil
	if failed {
		return errRepairWrite
	}
	return nil
}

func (store *repairStore) write(name string) bool {
	store.written++
	store.writes = append(store.writes, name)
	return store.failAt > 0 && store.written == store.failAt
}

func (store *repairStore) resetWrites() {
	store.writes = nil
	store.failAt = 0
	store.written = 0
	store.applyFailedWrite = false
}

func cloneRegister(value registerRecord) registerRecord {
	if value.Pending != nil {
		pending := *value.Pending
		value.Pending = &pending
	}
	return value
}
