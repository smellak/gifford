//go:build !unix

package capture

import (
	"errors"
	"os"
	"time"
)

// Locks are unavailable off unix: entry points fail closed before any
// acquisition.
func ensureLock(*os.Root, string) error {
	return errors.ErrUnsupported
}

func withLock(*os.Root, string, bool, time.Duration, func() error) error {
	return errors.ErrUnsupported
}

func tryLock(*os.Root, string, bool) (*os.File, error) {
	return nil, errors.ErrUnsupported
}

func releaseLock(*os.File) error {
	return errors.ErrUnsupported
}

func lockLive(*os.Root, string) (bool, error) {
	return false, errors.ErrUnsupported
}

// lockPollInterval mirrors the unix retry cadence so the shared takeLock
// loop compiles off unix; the ingest still fails closed earlier.
const lockPollInterval = 10 * time.Millisecond
