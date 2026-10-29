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

func lockLive(*os.Root, string) (bool, error) {
	return false, errors.ErrUnsupported
}
