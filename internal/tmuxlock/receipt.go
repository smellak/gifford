package tmuxlock

import (
	"fmt"
	"os"
)

// ReceiptPending is the content-free pending transfer or repair named by a
// pane register. It never includes record bytes.
type ReceiptPending struct {
	Operation  string `json:"op"`
	FromEpoch  uint32 `json:"from_epoch"`
	FromGen    uint64 `json:"from_gen"`
	ToEpoch    uint32 `json:"to_epoch"`
	ToOwner    string `json:"to_owner"`
	ToOwnerGen uint32 `json:"to_owner_gen"`
	TxID       string `json:"txid"`
}

// PanelReceipt is the content-free receipt of one pane's register and state:
// the register copy (owner, owner generation, epoch, pending) plus the
// state copy (phase, generation, transaction). It never includes record
// bytes.
type PanelReceipt struct {
	Owner    string          `json:"owner"`
	OwnerGen uint32          `json:"owner_gen"`
	Epoch    uint32          `json:"epoch"`
	Pending  *ReceiptPending `json:"pending,omitempty"`
	Phase    string          `json:"phase"`
	Gen      uint64          `json:"gen"`
	TxID     string          `json:"txid"`
}

// ReadPanelReceipt reads the register and state of one pane under root and
// returns its receipt. It verifies the record checksums and that both
// records name the requested live binding, the same checks the node guard
// applies before trusting them. It never writes and never locks: a torn
// read fails the checksum instead of surfacing as a receipt.
func ReadPanelReceipt(root string, binding LiveBinding, policy RecordPolicy) (PanelReceipt, error) {
	validated, err := validateRecordPolicy(policy)
	if err != nil {
		return PanelReceipt{}, fmt.Errorf("read pane receipt: %w", err)
	}
	if err := validateLiveBinding(binding); err != nil {
		return PanelReceipt{}, fmt.Errorf("read pane receipt: %w", err)
	}
	paths, err := derivePanelPathsAt(root, binding.PanelBinding)
	if err != nil {
		return PanelReceipt{}, fmt.Errorf("read pane receipt: %w", err)
	}
	rawRegister, err := readReceiptRecord(paths.Register)
	if err != nil {
		return PanelReceipt{}, fmt.Errorf("read pane register: %w", err)
	}
	register, err := decodeRegisterRecord(validated, rawRegister)
	if err != nil {
		return PanelReceipt{}, fmt.Errorf("read pane register: %w", errInvalidRecord)
	}
	rawState, err := readReceiptRecord(paths.State)
	if err != nil {
		return PanelReceipt{}, fmt.Errorf("read pane state: %w", err)
	}
	state, err := decodeStateRecord(validated, rawState)
	if err != nil {
		return PanelReceipt{}, fmt.Errorf("read pane state: %w", errInvalidRecord)
	}
	want, err := recordBindingForLive(binding)
	if err != nil {
		return PanelReceipt{}, fmt.Errorf("read pane receipt: %w", err)
	}
	if register.Binding != want || state.binding() != want {
		return PanelReceipt{}, fmt.Errorf("read pane receipt: %w", errInvalidRecord)
	}
	if register.Pending == nil &&
		(register.Owner != state.Owner || register.OwnerGen != state.OwnerGen ||
			register.Epoch != state.Epoch) {
		return PanelReceipt{}, fmt.Errorf("read pane receipt: %w", errInvalidRecord)
	}
	receipt := PanelReceipt{
		Owner: register.Owner, OwnerGen: register.OwnerGen, Epoch: register.Epoch,
		Phase: state.Phase, Gen: state.Gen, TxID: state.TxID,
	}
	if register.Pending != nil {
		pending := register.Pending
		receipt.Pending = &ReceiptPending{
			Operation: pending.Operation, FromEpoch: pending.FromEpoch,
			FromGen: pending.FromGen, ToEpoch: pending.ToEpoch,
			ToOwner: pending.ToOwner, ToOwnerGen: pending.ToOwnerGen,
			TxID: pending.TxID,
		}
	}
	return receipt, nil
}

func readReceiptRecord(path string) ([]byte, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read pane receipt record: %w", err)
	}
	if len(raw) != recordSize {
		return nil, errInvalidRecord
	}
	return raw, nil
}
