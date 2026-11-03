//go:build !linux

package tmuxlock

import "errors"

var errJournalUnsupported = errors.New("pane journal requires linux")

// OpenStateFile is unavailable off Linux: the journal needs the same secure
// walk and positional data sync as the lock.
func OpenStateFile(PanelBinding) (StateFile, error) {
	return nil, errJournalUnsupported
}

// OpenStateFileAt is unavailable off Linux for the same reason as
// OpenStateFile.
func OpenStateFileAt(string, PanelBinding) (StateFile, error) {
	return nil, errJournalUnsupported
}

// CurrentWriter is unavailable off Linux.
func CurrentWriter() (Writer, error) {
	return Writer{}, errJournalUnsupported
}

// ProcessStart is unavailable off Linux.
func ProcessStart(int64) (uint64, error) {
	return 0, errJournalUnsupported
}

// BootID is unavailable off Linux.
func BootID() (string, error) {
	return "", errJournalUnsupported
}
