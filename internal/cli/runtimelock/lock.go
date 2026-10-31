// Package runtimelock owns the process-wide claim on a Gifford runtime
// directory. The hub and offline maintenance commands share this lock so
// SQLite is never opened by both at once.
package runtimelock

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

const lockName = "hub.lock"

// ErrHeld means another process currently owns the runtime lock.
var ErrHeld = errors.New("runtime lock held")

// Lock is one held exclusive claim on the runtime directory.
type Lock struct {
	file *os.File
}

type heldError struct {
	runtimeDir string
	cause      error
}

func (e *heldError) Error() string {
	return fmt.Sprintf("another hub may be serving %s: %v", e.runtimeDir, e.cause)
}

func (e *heldError) Unwrap() []error {
	return []error{ErrHeld, e.cause}
}

// Acquire opens (creating) the lock file and takes a non-blocking exclusive
// lock on it. The caller must keep the returned Lock open for its entire use
// of the node database.
func Acquire(runtimeDir string) (*Lock, error) {
	path := filepath.Join(runtimeDir, lockName)
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open runtime lock: %w", err)
	}
	if err := tryLockFile(file); err != nil {
		_ = file.Close()
		return nil, &heldError{runtimeDir: runtimeDir, cause: err}
	}
	return &Lock{file: file}, nil
}

// Close releases the claim and closes the lock file.
func (l *Lock) Close() error {
	err := unlockFile(l.file)
	closeErr := l.file.Close()
	if closeErr != nil {
		closeErr = fmt.Errorf("close runtime lock: %w", closeErr)
	}
	return errors.Join(err, closeErr)
}
