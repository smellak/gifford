//go:build !unix

package capture

import (
	"errors"
	"os"
)

// adoptStream is unavailable off unix: the ingest fails closed before
// touching the stream.
func adoptStream(*os.File) (*os.File, error) {
	return nil, errors.ErrUnsupported
}

// pollReadable is unavailable off unix.
func pollReadable(uintptr, int) (bool, error) {
	return false, errors.ErrUnsupported
}

// pollInterrupted is unreachable off unix: pollReadable always fails.
func pollInterrupted(error) bool {
	return false
}

// readReady is unavailable off unix.
func readReady(int, []byte) (int, bool, error) {
	return 0, false, errors.ErrUnsupported
}
