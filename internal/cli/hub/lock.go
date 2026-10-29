package hub

import (
	"fmt"
	"os"
	"path/filepath"
)

// lockName is the runtime-dir file proving one hub owns the node. It is
// taken before the event store opens and held until shutdown, so a
// second hub refuses without creating or migrating gifford.db. The
// kernel releases the lock when the holder dies, so a crash never
// leaves a stale claim behind: only the lock file remains, and it is
// re-locked on the next start.
const lockName = "hub.lock"

// runtimeLock is one held exclusive claim on the runtime directory.
type runtimeLock struct {
	file *os.File
}

// acquireRuntimeLock opens (creating) the lock file and takes a
// non-blocking exclusive lock on it. Failure means another hub holds
// the node.
func acquireRuntimeLock(runtimeDir string) (*runtimeLock, error) {
	path := filepath.Join(runtimeDir, lockName)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open runtime lock: %w", err)
	}
	if err := tryLockFile(f); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("another hub may be serving %s: %w", runtimeDir, err)
	}
	return &runtimeLock{file: f}, nil
}

// Close releases the claim and closes the lock file.
func (l *runtimeLock) Close() error {
	err := unlockFile(l.file)
	if cerr := l.file.Close(); err == nil {
		err = cerr
	}
	return err
}
