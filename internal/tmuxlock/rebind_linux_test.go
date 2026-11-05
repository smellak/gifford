//go:build linux

package tmuxlock

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// rebindReceiptPolicy is the record policy the fixture's records use.
var rebindReceiptPolicy = RecordPolicy{PeerOwner: testPeerWriter, PeerWriters: []string{testPeerWriter}}

// recreatedPane is the fixture's pane after tmux recreated it: same host,
// owner, socket and pane id, another pane process.
func recreatedPane(fixture transferFixture) LiveBinding {
	live := fixture.live
	live.PanePID, live.PaneStart = recreatedPanePID, recreatedPaneStart
	return live
}

// The recreated pane's process.
const (
	recreatedPanePID   = 525252
	recreatedPaneStart = 1234567
)

// processStarts answers start times for the pids it knows and reports every
// other pid as gone, as /proc does. The recreated pane's process always runs.
func processStarts(alive map[int64]uint64) func(int64) (uint64, error) {
	return func(pid int64) (uint64, error) {
		if pid == recreatedPanePID {
			return recreatedPaneStart, nil
		}
		if start, ok := alive[pid]; ok {
			return start, nil
		}
		return 0, fmt.Errorf("read process %d stat: %w", pid, os.ErrNotExist)
	}
}

// A pane that tmux recreated keeps the record of its previous process, which
// no read of the live pane accepts. Repair binds it to the live pane once
// that process is gone and parks it repaired; reconcile then frees it.
func TestNodeWriterRepairRebindsRecreatedPaneAndReconcileFreesIt(t *testing.T) {
	t.Parallel()
	fixture := newTransferFixture(t)
	writer := fixture.writer(t, writerHooks{processStart: processStarts(nil)})
	_, err := writer.Take(t.Context(), fixture.live)
	require.NoError(t, err)
	before, _ := fixture.records(t)
	live := recreatedPane(fixture)

	_, err = ReadPanelReceipt(fixture.root, live, rebindReceiptPolicy)
	require.ErrorIs(t, err, errInvalidRecord, "the live pane rejects the old record")
	_, err = writer.Reconcile(t.Context(), live, "-",
		func(context.Context, ReconcileSnapshot) (ReconcileObservation, error) {
			t.Fatal("only repair may read a record of another pane process")
			return ReconcileObservation{}, nil
		})
	require.Error(t, err)

	result, err := writer.Repair(t.Context(), live)
	require.NoError(t, err)
	require.True(t, result.Changed)
	require.Equal(t, RecoveryRepaired, result.Outcome)
	require.Equal(t, EvidenceNo, result.Evidence.BindingEqual, "the record bound another pane process")
	register, state := fixture.records(t)
	want, err := recordBindingForLive(live)
	require.NoError(t, err)
	require.Equal(t, want, register.Binding)
	require.Equal(t, before.Epoch+1, register.Epoch)
	require.Equal(t, before.Owner, register.Owner)
	require.Equal(t, before.OwnerGen, register.OwnerGen)
	require.True(t, repairedState(state))

	reconciled, err := writer.Reconcile(t.Context(), live, "-",
		func(context.Context, ReconcileSnapshot) (ReconcileObservation, error) {
			return completeReconcileObservation(false), nil
		})
	require.NoError(t, err)
	require.True(t, reconciled.Changed)
	receipt, err := ReadPanelReceipt(fixture.root, live, rebindReceiptPolicy)
	require.NoError(t, err)
	require.Equal(t, phaseIdle, receipt.Phase)
}

// While the previous pane process may still run, repair rebinds nothing and
// leaves both records as they were.
func TestNodeWriterRepairNeverRebindsLivePane(t *testing.T) {
	t.Parallel()
	fixture := newTransferFixture(t)
	writer := fixture.writer(t, writerHooks{processStart: processStarts(map[int64]uint64{
		fixture.live.PanePID: uint64(fixture.live.PaneStart),
	})})
	_, err := writer.Take(t.Context(), fixture.live)
	require.NoError(t, err)
	register, state := fixture.records(t)

	_, err = writer.Repair(t.Context(), recreatedPane(fixture))
	require.Error(t, err)
	afterRegister, afterState := fixture.records(t)
	require.Equal(t, register, afterRegister)
	require.Equal(t, state, afterState)
}

// Reconcile never reads a record of another pane process: its evidence
// would describe the live pane, not the one the attempt wrote to.
func TestNodeWriterReconcileNeverReadsRecreatedPaneRecord(t *testing.T) {
	t.Parallel()
	fixture := newTransferFixture(t)
	writer := fixture.writer(t, writerHooks{processStart: processStarts(nil)})
	_, err := writer.Take(t.Context(), fixture.live)
	require.NoError(t, err)
	register, state := fixture.records(t)
	uncertain := uncertainState(2)
	state.Phase, state.Step, state.Attempt, state.Operation = phaseUncertain, 2, uncertain.Attempt, uncertain.Operation
	state.Writer, state.WriterPID, state.WriterStart = nodeOwner, uncertain.WriterPID, uncertain.WriterStart
	state.ChildPID, state.ChildStart, state.Gen = uncertain.ChildPID, uncertain.ChildStart, uncertain.Gen
	fixture.writeRecords(t, register, state)

	_, err = writer.Reconcile(t.Context(), recreatedPane(fixture), state.Attempt,
		func(context.Context, ReconcileSnapshot) (ReconcileObservation, error) {
			t.Fatal("reconcile observed the live pane for an attempt on its previous process")
			return ReconcileObservation{}, nil
		})
	require.Error(t, err)
	_, after := fixture.records(t)
	require.Equal(t, state, after)
}

// A start time that cannot be read is no proof the process is gone.
func TestNodeWriterRepairNeverRebindsOnUnreadableProcess(t *testing.T) {
	t.Parallel()
	fixture := newTransferFixture(t)
	writer := fixture.writer(t, writerHooks{processStart: func(pid int64) (uint64, error) {
		if pid == recreatedPanePID {
			return recreatedPaneStart, nil
		}
		return 0, os.ErrPermission
	}})
	_, err := writer.Take(t.Context(), fixture.live)
	require.NoError(t, err)
	register, _ := fixture.records(t)

	_, err = writer.Repair(t.Context(), recreatedPane(fixture))
	require.Error(t, err)
	after, _ := fixture.records(t)
	require.Equal(t, register, after)
}

// A reused pid is another process: its start time differs from the one the
// record keeps, so the previous pane process is gone.
func TestNodeWriterRepairRebindsAfterPidReuse(t *testing.T) {
	t.Parallel()
	fixture := newTransferFixture(t)
	writer := fixture.writer(t, writerHooks{processStart: processStarts(map[int64]uint64{
		fixture.live.PanePID: uint64(fixture.live.PaneStart) + 1,
	})})
	_, err := writer.Take(t.Context(), fixture.live)
	require.NoError(t, err)

	result, err := writer.Repair(t.Context(), recreatedPane(fixture))
	require.NoError(t, err)
	require.Equal(t, RecoveryRepaired, result.Outcome)
}

// Every proof speaks about the pane tmux shows now. A target that names
// another boot, which would read every old process as gone, or a pane
// process that is not running rebinds nothing.
func TestNodeWriterRepairNeverRebindsToATargetThatIsNotLive(t *testing.T) {
	t.Parallel()
	for name, target := range map[string]func(transferFixture) LiveBinding{
		"another boot": func(fixture transferFixture) LiveBinding {
			live := recreatedPane(fixture)
			live.BootID = "1f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0"
			return live
		},
		"no such process": func(fixture transferFixture) LiveBinding {
			live := recreatedPane(fixture)
			live.PanePID = recreatedPanePID + 1
			return live
		},
		"another start time": func(fixture transferFixture) LiveBinding {
			live := recreatedPane(fixture)
			live.PaneStart = recreatedPaneStart + 1
			return live
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			fixture := newTransferFixture(t)
			writer := fixture.writer(t, writerHooks{processStart: processStarts(nil)})
			_, err := writer.Take(t.Context(), fixture.live)
			require.NoError(t, err)
			register, state := fixture.records(t)

			_, err = writer.Repair(t.Context(), target(fixture))
			require.Error(t, err)
			afterRegister, afterState := fixture.records(t)
			require.Equal(t, register, afterRegister)
			require.Equal(t, state, afterState)
		})
	}
}

func TestPreviousIncarnationNamesOnlyTheSamePane(t *testing.T) {
	t.Parallel()
	previous := neutralRecordBinding()
	live := previous
	live.PanePID, live.PaneStart = previous.PanePID+1, "1"
	require.True(t, previousIncarnation(previous, live))
	require.False(t, previousIncarnation(previous, previous), "the live pane itself")
	for name, mutate := range map[string]func(*recordBinding){
		"other host":   func(b *recordBinding) { b.Host = "host-8" },
		"other owner":  func(b *recordBinding) { b.UID++ },
		"other socket": func(b *recordBinding) { b.SocketSHA = strings.Repeat("e", len(b.SocketSHA)) },
		"other pane":   func(b *recordBinding) { b.Pane++ },
	} {
		other := live
		mutate(&other)
		require.False(t, previousIncarnation(previous, other), name)
	}
}
