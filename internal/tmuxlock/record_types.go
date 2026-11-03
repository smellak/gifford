package tmuxlock

import (
	"fmt"
	"time"
)

// LiveBinding is the registry's complete pane identity. PanelBinding selects
// the stable file layout; the remaining fields prevent pid or pane-id reuse
// from inheriting authority.
type LiveBinding struct {
	PanelBinding
	Host      string
	BootID    string
	PanePID   int64
	PaneStart int64
}

// Validate reports whether the live identity has canonical in-memory form.
func (binding LiveBinding) Validate() error {
	return validateLiveBinding(binding)
}

func validateLiveBinding(binding LiveBinding) error {
	if binding.OwnerUID == 0 || !hostPattern.MatchString(binding.Host) ||
		binding.PanePID <= 0 || binding.PanePID > maxRecordPID || binding.PaneStart <= 0 ||
		!bootIDPattern.MatchString(binding.BootID) {
		return fmt.Errorf("pane live binding: %w", ErrInvalidBinding)
	}
	if _, err := deriveParts(binding.PanelBinding); err != nil {
		return err
	}
	return nil
}

func initialState(binding recordBinding, owner string, timestamp time.Time) stateRecord {
	return stateRecord{
		Attempt: "-", BootID: binding.BootID, ChildPID: 0, ChildStart: "0",
		Epoch: 0, Gen: 0, Host: binding.Host, Operation: "-", Owner: owner,
		OwnerGen: 0, Pane: binding.Pane, PanePID: binding.PanePID,
		PaneStart: binding.PaneStart, Phase: phaseIdle, Result: "-",
		SocketSHA: binding.SocketSHA, Step: 0, Timestamp: formatRecordTime(timestamp),
		TxID: "-", UID: binding.UID, Version: 3, Writer: "provision",
		WriterPID: 0, WriterStart: "0",
	}
}

func formatRecordTime(value time.Time) string {
	return value.UTC().Format("2006-01-02T15:04:05.000000000Z")
}
