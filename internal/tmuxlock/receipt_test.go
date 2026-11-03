package tmuxlock

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const receiptTestWriter = "receipt_writer"

func receiptTestBinding() LiveBinding {
	return LiveBinding{
		PanelBinding: PanelBinding{
			OwnerUID: 1113, Socket: "/run/pane-receipt-test/node.sock", PaneID: testPaneTwelve,
		},
		Host: neutralHost, BootID: provisionTestBootID,
		PanePID: 424242, PaneStart: 987654,
	}
}

func receiptTestPolicy() RecordPolicy {
	return RecordPolicy{PeerOwner: testPeerOwner, PeerWriters: []string{receiptTestWriter}}
}

func writeReceiptTree(t *testing.T, root string, binding LiveBinding, register registerRecord, state stateRecord) PanelPaths {
	t.Helper()
	policy, err := validateRecordPolicy(receiptTestPolicy())
	require.NoError(t, err)
	paths, err := derivePanelPathsAt(root, binding.PanelBinding)
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(paths.SocketDir, 0o700))
	rawRegister, err := encodeRegisterRecord(policy, register)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(paths.Register, rawRegister, 0o600))
	rawState, err := encodeStateRecord(policy, state)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(paths.State, rawState, 0o600))
	return paths
}

func TestReadPanelReceiptReturnsRegisterAndState(t *testing.T) {
	t.Parallel()

	binding := receiptTestBinding()
	want, err := recordBindingForLive(binding)
	require.NoError(t, err)
	timestamp := time.Date(2026, time.November, 3, 9, 0, 0, 0, time.UTC)
	paths := writeReceiptTree(t, t.TempDir(), binding,
		registerRecord{Binding: want, Owner: testPeerOwner},
		initialState(want, testPeerOwner, timestamp))

	receipt, err := ReadPanelReceipt(paths.Root, binding, receiptTestPolicy())
	require.NoError(t, err)
	require.Equal(t, testPeerOwner, receipt.Owner)
	require.Equal(t, uint32(0), receipt.OwnerGen)
	require.Equal(t, uint32(0), receipt.Epoch)
	require.Nil(t, receipt.Pending)
	require.Equal(t, phaseIdle, receipt.Phase)
	require.Equal(t, uint64(0), receipt.Gen)
	require.Equal(t, "-", receipt.TxID)
	require.Equal(t, "-", receipt.Attempt)
	require.Zero(t, receipt.Step)
}

// The receipt names the attempt reconcile expects and the step it reached,
// apart from the transaction of the last record write.
func TestReadPanelReceiptNamesAttemptAndStep(t *testing.T) {
	t.Parallel()

	binding := receiptTestBinding()
	want, err := recordBindingForLive(binding)
	require.NoError(t, err)
	timestamp := time.Date(2026, time.November, 3, 9, 0, 0, 0, time.UTC)
	state := initialState(want, nodeOwner, timestamp)
	state.Attempt, state.Operation = testAttemptID, testOperationID
	state.ChildPID, state.ChildStart = 3334, "555556"
	state.Gen, state.Phase, state.Step = 3, phaseUncertain, 2
	state.Writer, state.WriterPID, state.WriterStart = nodeOwner, 3333, "555555"
	paths := writeReceiptTree(t, t.TempDir(), binding,
		registerRecord{Binding: want, Owner: nodeOwner}, state)

	receipt, err := ReadPanelReceipt(paths.Root, binding, receiptTestPolicy())
	require.NoError(t, err)
	require.Equal(t, phaseUncertain, receipt.Phase)
	require.Equal(t, testAttemptID, receipt.Attempt)
	require.Equal(t, 2, receipt.Step)
	require.NotEqual(t, receipt.Attempt, receipt.TxID)
}

func TestReadPanelReceiptReturnsPending(t *testing.T) {
	t.Parallel()

	binding := receiptTestBinding()
	want, err := recordBindingForLive(binding)
	require.NoError(t, err)
	timestamp := time.Date(2026, time.November, 3, 9, 0, 0, 0, time.UTC)
	pending := &pendingRecord{
		FromEpoch: 0, FromGen: 3, Operation: operationTransfer, ToEpoch: 0,
		ToOwner: nodeOwner, ToOwnerGen: 1, TxID: "33333333333333333333333333333333",
	}
	paths := writeReceiptTree(t, t.TempDir(), binding,
		registerRecord{Binding: want, Owner: nodeOwner, OwnerGen: 1, Pending: pending},
		initialState(want, testPeerOwner, timestamp))

	receipt, err := ReadPanelReceipt(paths.Root, binding, receiptTestPolicy())
	require.NoError(t, err)
	require.Equal(t, nodeOwner, receipt.Owner)
	require.Equal(t, uint32(1), receipt.OwnerGen)
	require.NotNil(t, receipt.Pending)
	require.Equal(t, operationTransfer, receipt.Pending.Operation)
	require.Equal(t, nodeOwner, receipt.Pending.ToOwner)
	require.Equal(t, uint32(1), receipt.Pending.ToOwnerGen)
	require.Equal(t, "33333333333333333333333333333333", receipt.Pending.TxID)
	require.Equal(t, phaseIdle, receipt.Phase)
}

func TestReadPanelReceiptRejectsMismatchAndCorruption(t *testing.T) {
	t.Parallel()

	binding := receiptTestBinding()
	want, err := recordBindingForLive(binding)
	require.NoError(t, err)
	timestamp := time.Date(2026, time.November, 3, 9, 0, 0, 0, time.UTC)
	state := initialState(want, testPeerOwner, timestamp)

	root := t.TempDir()
	paths := writeReceiptTree(t, root, binding,
		registerRecord{Binding: want, Owner: testPeerOwner}, state)

	foreign := want
	foreign.Host = "other-host"
	otherState := initialState(foreign, testPeerOwner, timestamp)
	policy, err := validateRecordPolicy(receiptTestPolicy())
	require.NoError(t, err)
	rawOther, err := encodeStateRecord(policy, otherState)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(paths.State, rawOther, 0o600))
	_, err = ReadPanelReceipt(root, binding, receiptTestPolicy())
	require.ErrorIs(t, err, errInvalidRecord)

	rawState, err := encodeStateRecord(policy, state)
	require.NoError(t, err)
	rawState[100] ^= 0xff
	require.NoError(t, os.WriteFile(paths.State, rawState, 0o600))
	_, err = ReadPanelReceipt(root, binding, receiptTestPolicy())
	require.ErrorIs(t, err, errInvalidRecord)

	require.NoError(t, os.Remove(paths.State))
	_, err = ReadPanelReceipt(root, binding, receiptTestPolicy())
	require.Error(t, err)
}

func TestReadPanelReceiptRejectsBadPolicy(t *testing.T) {
	t.Parallel()

	_, err := ReadPanelReceipt(t.TempDir(), receiptTestBinding(), RecordPolicy{})
	require.Error(t, err)
}

func TestDerivePanelPathsAtKeepsDefaultRoot(t *testing.T) {
	t.Parallel()

	binding := PanelBinding{OwnerUID: 1113, Socket: "/run/x/node.sock", PaneID: "%3"}
	def, err := DerivePanelPaths(binding)
	require.NoError(t, err)
	require.Equal(t, DefaultRoot, def.Root)
	require.Equal(t, DefaultRoot+"/1113", def.OwnerDir[:len(DefaultRoot)+5])
	require.True(t, filepath.IsAbs(def.Register))
	require.True(t, filepath.IsAbs(def.State))

	custom, err := derivePanelPathsAt(t.TempDir(), binding)
	require.NoError(t, err)
	require.Equal(t, def.Lock[len(DefaultRoot):], custom.Lock[len(custom.Root):])
	require.Equal(t, def.Register[len(DefaultRoot):], custom.Register[len(custom.Root):])
	require.Equal(t, def.State[len(DefaultRoot):], custom.State[len(custom.Root):])
}
