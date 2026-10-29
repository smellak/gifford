//go:build unix

package capture

import (
	"errors"
	"fmt"
	"os"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// lockPollInterval retries a LOCK_NB acquisition every 10 ms.
const lockPollInterval = 10 * time.Millisecond

// ensureLock creates an empty lock file once; an existing one is
// checked. Lock files are coordination, never published data.
func ensureLock(root *os.Root, name string) error {
	f, err := root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err == nil {
		if cerr := f.Close(); cerr != nil {
			return fmt.Errorf("%w: lock %s: %w", ErrUnsafePath, name, cerr)
		}
		return nil
	}
	if !errors.Is(err, os.ErrExist) {
		return fmt.Errorf("%w: lock %s: %w", ErrUnsafePath, name, err)
	}
	if _, err := openChecked(root, name, false); err != nil {
		return err
	}
	return nil
}

// withLock holds the flock of an existing lock file for fn: exclusive
// for writers, shared for the liveness probe. It polls every 10 ms up
// to bound and reports ErrBusy past it.
func withLock(root *os.Root, name string, exclusive bool, bound time.Duration, fn func() error) error {
	f, err := openChecked(root, name, false)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	how := unix.LOCK_SH
	if exclusive {
		how = unix.LOCK_EX
	}
	fd := int(f.Fd())
	for i := 0; ; i++ {
		if err := unix.Flock(fd, how|unix.LOCK_NB); err == nil {
			break
		} else if !errors.Is(err, syscall.EWOULDBLOCK) {
			return fmt.Errorf("%w: lock %s: %w", ErrUnsafePath, name, err)
		} else if time.Duration(i+1)*lockPollInterval >= bound {
			return fmt.Errorf("%w: lock %s", ErrBusy, name)
		}
		time.Sleep(lockPollInterval)
	}
	defer func() { _ = unix.Flock(fd, unix.LOCK_UN) }()
	return fn()
}

// tryLock opens an existing lock file read-write and takes it at
// once, non-blocking: exclusive for writers, shared for probes. The
// file passes the §4.3 checks, so the holder rewrites its line on the
// same descriptor. The caller holds the file until releaseLock.
func tryLock(root *os.Root, name string, exclusive bool) (*os.File, error) {
	fail := func(err error) (*os.File, error) {
		return nil, fmt.Errorf("%w: %s: %w", ErrUnsafePath, name, err)
	}
	fi, err := root.Lstat(name)
	if err != nil {
		return fail(err)
	}
	if err := checkFile(fi); err != nil {
		return fail(err)
	}
	f, err := root.OpenFile(name, os.O_RDWR, 0)
	if err != nil {
		return fail(err)
	}
	gfi, err := f.Stat()
	if err != nil || !os.SameFile(fi, gfi) {
		_ = f.Close()
		return fail(fmt.Errorf("entry swapped"))
	}
	how := unix.LOCK_SH
	if exclusive {
		how = unix.LOCK_EX
	}
	if err := unix.Flock(int(f.Fd()), how|unix.LOCK_NB); err != nil {
		_ = f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("%w: lock %s", ErrBusy, name)
		}
		return nil, fmt.Errorf("%w: lock %s: %w", ErrUnsafePath, name, err)
	}
	return f, nil
}

// releaseLock drops the flock and closes the file.
func releaseLock(f *os.File) error {
	_ = unix.Flock(int(f.Fd()), unix.LOCK_UN)
	if err := f.Close(); err != nil {
		return fmt.Errorf("release lock: %w", err)
	}
	return nil
}

// lockLive probes an existing ingest.lock read-only: EWOULDBLOCK means
// an exclusive holder exists now. A missing file means none.
func lockLive(root *os.Root, name string) (bool, error) {
	f, err := root.Open(name)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, fmt.Errorf("%w: lock %s: %w", ErrUnsafePath, name, err)
	}
	defer func() { _ = f.Close() }()
	if err := unix.Flock(int(f.Fd()), unix.LOCK_SH|unix.LOCK_NB); err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return true, nil
		}
		return false, fmt.Errorf("%w: lock %s: %w", ErrUnsafePath, name, err)
	}
	_ = unix.Flock(int(f.Fd()), unix.LOCK_UN)
	return false, nil
}
