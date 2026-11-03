//go:build linux

package tmuxlock

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestNodeWriterReconcileObservesAndCommitsUnderPaneFlock(t *testing.T) {
	t.Parallel()
	fixture := newTransferFixture(t)
	writer := fixture.writer(t, writerHooks{})
	_, err := writer.Take(t.Context(), fixture.live)
	require.NoError(t, err)
	register, state := fixture.records(t)
	state.Attempt = testAttemptID
	state.Operation = testOperationID
	state.Phase = phaseUncertain
	state.Step = 1
	state.Writer = nodeOwner
	state.WriterPID = 3333
	state.WriterStart = "555555"
	state.ChildPID = 3334
	state.ChildStart = "555556"
	fixture.writeRecords(t, register, state)

	result, err := writer.Reconcile(
		t.Context(), fixture.live, state.Attempt,
		func(_ context.Context, snapshot ReconcileSnapshot) (ReconcileObservation, error) {
			require.Equal(t, RecoveryProcess{PID: 3333, Start: 555555}, snapshot.Writer)
			require.Equal(t, RecoveryProcess{PID: 3334, Start: 555556}, snapshot.Child)
			other, openErr := os.OpenFile(fixture.lock, os.O_RDWR, 0)
			require.NoError(t, openErr)
			defer func() { require.NoError(t, other.Close()) }()
			lockErr := unix.Flock(int(other.Fd()), unix.LOCK_EX|unix.LOCK_NB)
			require.ErrorIs(t, lockErr, unix.EWOULDBLOCK)
			return completeReconcileObservation(true), nil
		},
	)
	require.NoError(t, err)
	require.True(t, result.Changed)
	require.Equal(t, RecoveryConfirmed, result.Outcome)
	require.Equal(t, EvidenceYes, result.Evidence.BindingEqual)
	require.Equal(t, EvidenceYes, result.Evidence.LineVisible)

	_, state = fixture.records(t)
	require.Equal(t, phaseDone, state.Phase)
	require.Equal(t, resultConfirmed, state.Result)
	require.Equal(t, operationReconcile, state.Writer)

	replayed, err := writer.Reconcile(
		t.Context(), fixture.live, state.Attempt,
		func(context.Context, ReconcileSnapshot) (ReconcileObservation, error) {
			t.Fatal("terminal replay must not observe the pane")
			return ReconcileObservation{}, nil
		},
	)
	require.NoError(t, err)
	require.False(t, replayed.Changed)
	require.Equal(t, RecoveryUnchanged, replayed.Outcome)
}

func TestNodeWriterReconcileRevalidatesModeBeforeCommit(t *testing.T) {
	t.Parallel()
	fixture := newTransferFixture(t)
	writer := fixture.writer(t, writerHooks{})
	_, err := writer.Take(t.Context(), fixture.live)
	require.NoError(t, err)
	register, state := fixture.records(t)
	state.Attempt = testAttemptID
	state.Operation = testOperationID
	state.Phase = phaseUncertain
	state.Step = 1
	state.Writer = nodeOwner
	fixture.writeRecords(t, register, state)
	before := readTransferPath(t, fixture.state)

	_, err = writer.Reconcile(t.Context(), fixture.live, state.Attempt,
		func(context.Context, ReconcileSnapshot) (ReconcileObservation, error) {
			require.NoError(t, os.WriteFile(fixture.modePath, []byte("optional\n"), 0o644))
			return completeReconcileObservation(true), nil
		})
	require.ErrorIs(t, err, ErrWriterInvalid)
	require.Equal(t, before, readTransferPath(t, fixture.state))
}

func TestNodeWriterRepairReplacesWrongSizedStateAndIsIdempotent(t *testing.T) {
	t.Parallel()
	fixture := newTransferFixture(t)
	writer := fixture.writer(t, writerHooks{})
	_, err := writer.Take(t.Context(), fixture.live)
	require.NoError(t, err)
	registerBefore, _ := fixture.records(t)
	require.NoError(t, os.WriteFile(fixture.state, []byte("broken"), 0o660))

	result, err := writer.Repair(t.Context(), fixture.live)
	require.NoError(t, err)
	require.True(t, result.Changed)
	require.Equal(t, RecoveryRepaired, result.Outcome)
	require.Equal(t, EvidenceYes, result.Evidence.BindingEqual)
	require.NotNil(t, result.Evidence.Record)
	require.Equal(t, int64(len("broken")), result.Evidence.Record.Size)
	require.Equal(t, EvidenceNo, result.Evidence.Record.MagicValid)
	require.Equal(t, EvidenceNo, result.Evidence.Record.ChecksumValid)
	require.Nil(t, result.Evidence.Record.Version)

	info, err := os.Stat(fixture.state)
	require.NoError(t, err)
	require.Equal(t, int64(recordSize), info.Size())
	register, state := fixture.records(t)
	require.Nil(t, register.Pending)
	require.Equal(t, registerBefore.Epoch+1, register.Epoch)
	require.Equal(t, register.Epoch, state.Epoch)
	require.Equal(t, register.Owner, state.Owner)
	require.Equal(t, register.OwnerGen, state.OwnerGen)
	require.True(t, repairedState(state))

	replayed, err := writer.Repair(t.Context(), fixture.live)
	require.NoError(t, err)
	require.False(t, replayed.Changed)
	require.Equal(t, RecoveryUnchanged, replayed.Outcome)
	require.NotNil(t, replayed.Evidence.Record)
	require.Equal(t, EvidenceYes, replayed.Evidence.Record.MagicValid)
	require.Equal(t, EvidenceYes, replayed.Evidence.Record.ChecksumValid)
	require.NotNil(t, replayed.Evidence.Record.Version)
	require.Equal(t, 3, *replayed.Evidence.Record.Version)
}

func TestNodeWriterReconcileOwnConfirmsNodeAttempt(t *testing.T) {
	t.Parallel()
	fixture := newTransferFixture(t)
	writer := fixture.writer(t, writerHooks{})
	_, err := writer.Take(t.Context(), fixture.live)
	require.NoError(t, err)
	register, state := fixture.records(t)
	state.Attempt = testAttemptID
	state.Operation = testOperationID
	state.Phase = phaseUncertain
	state.Step = 3
	state.Writer = nodeOwner
	state.WriterPID = 3333
	state.WriterStart = "555555"
	fixture.writeRecords(t, register, state)
	observe := func(context.Context, ReconcileSnapshot) (ReconcileObservation, error) {
		return completeReconcileObservation(true), nil
	}

	_, err = writer.Reconcile(t.Context(), fixture.live, ownAttempt, observe)
	require.ErrorIs(t, err, ErrInvalidBinding, "a client cannot name the own attempt")
	result, err := writer.ReconcileOwn(t.Context(), fixture.live, observe)
	require.NoError(t, err)
	require.True(t, result.Changed)
	require.Equal(t, RecoveryConfirmed, result.Outcome)
	_, state = fixture.records(t)
	require.Equal(t, phaseDone, state.Phase)
	require.Equal(t, resultConfirmed, state.Result)
	require.Equal(t, testAttemptID, state.Attempt)

	again, err := writer.ReconcileOwn(t.Context(), fixture.live,
		func(context.Context, ReconcileSnapshot) (ReconcileObservation, error) {
			t.Fatal("a finished record must not be observed again")
			return ReconcileObservation{}, nil
		})
	require.NoError(t, err)
	require.False(t, again.Changed)
	require.Equal(t, RecoveryUnchanged, again.Outcome)
}

func TestNodeWriterReconcileWithDashFreesRepairedRecord(t *testing.T) {
	t.Parallel()
	fixture := newTransferFixture(t)
	writer := fixture.writer(t, writerHooks{})
	_, err := writer.Take(t.Context(), fixture.live)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(fixture.state, []byte("broken"), 0o660))
	_, err = writer.Repair(t.Context(), fixture.live)
	require.NoError(t, err)
	_, err = writer.Return(t.Context(), fixture.live)
	require.ErrorIs(t, err, ErrWriterInvalid, "a repaired record cannot originate a transfer")

	result, err := writer.Reconcile(t.Context(), fixture.live, "-",
		func(_ context.Context, snapshot ReconcileSnapshot) (ReconcileObservation, error) {
			require.Equal(t, RecoveryProcess{}, snapshot.Writer)
			require.Equal(t, RecoveryProcess{}, snapshot.Child)
			return completeReconcileObservation(true), nil
		})
	require.NoError(t, err)
	require.True(t, result.Changed)
	require.Equal(t, RecoveryNoEffect, result.Outcome)
	_, state := fixture.records(t)
	require.Equal(t, phaseIdle, state.Phase)
	require.Equal(t, resultNoEffect, state.Result)
	require.Equal(t, operationReconcile, state.Writer)

	returned, err := writer.Return(t.Context(), fixture.live)
	require.NoError(t, err)
	require.True(t, returned.Changed)
}

func TestNodeWriterRepairAdoptsRepairedStateAfterRegisterRewrite(t *testing.T) {
	t.Parallel()
	fixture := newTransferFixture(t)
	writer := fixture.writer(t, writerHooks{})
	_, err := writer.Take(t.Context(), fixture.live)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(fixture.state, []byte("broken"), 0o660))
	_, err = writer.Repair(t.Context(), fixture.live)
	require.NoError(t, err)
	register, state := fixture.records(t)
	require.True(t, repairedState(state))
	register.Epoch--
	fixture.writeRecords(t, register, state)

	_, err = writer.Reconcile(t.Context(), fixture.live, "-",
		func(context.Context, ReconcileSnapshot) (ReconcileObservation, error) {
			t.Fatal("records that disagree must not be observed")
			return ReconcileObservation{}, nil
		})
	require.Error(t, err)

	repaired, err := writer.Repair(t.Context(), fixture.live)
	require.NoError(t, err)
	require.True(t, repaired.Changed)
	register, adopted := fixture.records(t)
	require.Equal(t, state, adopted)
	require.Equal(t, state.Epoch, register.Epoch)

	result, err := writer.Reconcile(t.Context(), fixture.live, "-",
		func(context.Context, ReconcileSnapshot) (ReconcileObservation, error) {
			return completeReconcileObservation(false), nil
		})
	require.NoError(t, err)
	require.Equal(t, RecoveryNoEffect, result.Outcome)
}

func TestNodeWriterRepairRecoversEveryDurableBoundary(t *testing.T) {
	t.Parallel()
	stop := errors.New("stop after durable boundary")
	for _, boundary := range []writerBoundary{boundaryRegister, boundaryState, boundaryClear} {
		t.Run(string(boundary), func(t *testing.T) {
			t.Parallel()
			fixture := newTransferFixture(t)
			stable := fixture.writer(t, writerHooks{})
			_, err := stable.Take(t.Context(), fixture.live)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(fixture.state, []byte("invalid"), 0o660))
			crashing := fixture.writer(t, writerHooks{
				boundary: func(got writerBoundary) error {
					if got == boundary {
						return stop
					}
					return nil
				},
			})
			_, err = crashing.Repair(t.Context(), fixture.live)
			require.ErrorIs(t, err, stop)
			_, err = stable.Repair(t.Context(), fixture.live)
			require.NoError(t, err)
			register, state := fixture.records(t)
			require.Nil(t, register.Pending)
			require.True(t, repairedState(state))
			require.True(t, recordsCoherent(register, state))
		})
	}
}

func completeReconcileObservation(lineVisible bool) ReconcileObservation {
	return ReconcileObservation{
		BindingEqual: EvidenceYes, LineVisible: evidenceVerdict(lineVisible),
		ComposerEmpty: EvidenceYes, NoticeAbsent: evidenceVerdict(!lineVisible),
		WriterExited: EvidenceYes, ChildExited: EvidenceYes, DrainWaitElapsed: EvidenceYes,
	}
}
