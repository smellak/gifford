//go:build linux

package tmuxlock

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

const lockRetryInterval = 10 * time.Millisecond

func acquirePlatform(
	ctx context.Context,
	root string,
	ownerUID uint32,
	parts lockPathParts,
	wait time.Duration,
) (*PanelGuard, LockOutcome, error) {
	fd, stat, err := openProvisionedLock(root, ownerUID, parts)
	if err != nil {
		return nil, LockUnprovisioned, fmt.Errorf("open panel lock: %w: %w", ErrUnprovisioned, err)
	}
	acquired, err := waitForPanelLock(ctx, fd, wait)
	if err != nil {
		_ = unix.Close(fd)
		return nil, LockDeferred, err
	}
	if !acquired {
		_ = unix.Close(fd)
		return nil, LockDeferred, nil
	}
	if err := validateLockFile(fd, ownerUID, stat.Uid); err != nil {
		_ = releasePanelLock(fd)
		return nil, LockUnprovisioned, fmt.Errorf(
			"revalidate panel lock: %w: %w", ErrUnprovisioned, err,
		)
	}
	return &PanelGuard{
		fd:     fd,
		device: stat.Dev,
		inode:  stat.Ino,
	}, LockAcquired, nil
}

func openProvisionedLock(
	root string,
	ownerUID uint32,
	parts lockPathParts,
) (int, unix.Stat_t, error) {
	components, err := managedRootComponents(root)
	if err != nil {
		return -1, unix.Stat_t{}, err
	}
	current, err := unix.Open(
		"/", unix.O_PATH|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0,
	)
	if err != nil {
		return -1, unix.Stat_t{}, fmt.Errorf("open filesystem root: %w", err)
	}
	validateSystemAncestors := root == DefaultRoot
	if validateSystemAncestors {
		if err := validateTrustedAncestor(current); err != nil {
			_ = unix.Close(current)
			return -1, unix.Stat_t{}, err
		}
	}
	for index, component := range components {
		next, openErr := openAtSecure(
			current, component,
			unix.O_PATH|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC,
		)
		_ = unix.Close(current)
		if openErr != nil {
			return -1, unix.Stat_t{}, fmt.Errorf("open managed root: %w", openErr)
		}
		current = next
		if validateSystemAncestors && index < len(components)-1 {
			if err := validateTrustedAncestor(current); err != nil {
				_ = unix.Close(current)
				return -1, unix.Stat_t{}, err
			}
		}
	}
	expectedOwner := effectiveUID()
	if err := validateManagedDirectory(current, ownerUID, expectedOwner); err != nil {
		_ = unix.Close(current)
		return -1, unix.Stat_t{}, err
	}
	for _, component := range []string{parts.owner, parts.hash} {
		next, openErr := openAtSecure(
			current, component,
			unix.O_PATH|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC,
		)
		_ = unix.Close(current)
		if openErr != nil {
			return -1, unix.Stat_t{}, fmt.Errorf("open lock directory: %w", openErr)
		}
		current = next
		if err := validateManagedDirectory(current, ownerUID, expectedOwner); err != nil {
			_ = unix.Close(current)
			return -1, unix.Stat_t{}, err
		}
	}
	fd, err := openAtSecure(current, parts.file, unix.O_RDWR|unix.O_NOFOLLOW|unix.O_CLOEXEC)
	_ = unix.Close(current)
	if err != nil {
		return -1, unix.Stat_t{}, fmt.Errorf("open lock file: %w", err)
	}
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		_ = unix.Close(fd)
		return -1, unix.Stat_t{}, fmt.Errorf("stat lock file: %w", err)
	}
	if err := validateLockFileStat(stat, expectedOwner); err != nil {
		_ = unix.Close(fd)
		return -1, unix.Stat_t{}, err
	}
	if err := validateFileACL(fd, ownerUID); err != nil {
		_ = unix.Close(fd)
		return -1, unix.Stat_t{}, err
	}
	return fd, stat, nil
}

func managedRootComponents(root string) ([]string, error) {
	if root == "" || root == "/" || root[0] != '/' || path.Clean(root) != root ||
		strings.IndexByte(root, 0) >= 0 || strings.ContainsAny(root, "\r\n") {
		return nil, errors.New("invalid managed root")
	}
	components := strings.Split(strings.TrimPrefix(root, "/"), "/")
	for _, component := range components {
		if component == "" || component == "." || component == ".." {
			return nil, errors.New("invalid managed root component")
		}
	}
	return components, nil
}

func openAtSecure(parent int, name string, flags uint64) (int, error) {
	how := &unix.OpenHow{
		Flags:   flags,
		Resolve: unix.RESOLVE_BENEATH | unix.RESOLVE_NO_SYMLINKS,
	}
	fd, err := unix.Openat2(parent, name, how)
	if err == nil {
		return fd, nil
	}
	if !errors.Is(err, unix.ENOSYS) && !errors.Is(err, unix.EINVAL) {
		return -1, fmt.Errorf("openat2: %w", err)
	}
	fd, err = unix.Openat(
		parent, name,
		int(flags), //nolint:gosec // flags are the package's representable open(2) constants
		0,
	)
	if err != nil {
		return -1, fmt.Errorf("openat: %w", err)
	}
	return fd, nil
}

func effectiveUID() uint32 {
	// Linux exposes geteuid(2) as an int in x/sys even though uid_t is uint32.
	return uint32(os.Geteuid()) //nolint:gosec // kernel uid_t cannot overflow uint32
}

func validateManagedDirectory(fd int, ownerUID uint32, expectedOwner uint32) error {
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return fmt.Errorf("stat managed directory: %w", err)
	}
	if err := validateManagedDirectoryStat(stat, expectedOwner); err != nil {
		return err
	}
	return validateDirectoryACL(fd, ownerUID)
}

func validateTrustedAncestor(fd int) error {
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return fmt.Errorf("stat trusted ancestor: %w", err)
	}
	return validateTrustedAncestorStat(stat)
}

func validateTrustedAncestorStat(stat unix.Stat_t) error {
	if stat.Mode&unix.S_IFMT != unix.S_IFDIR || stat.Uid != 0 || stat.Mode&0o022 != 0 {
		return errors.New("unsafe trusted ancestor attributes")
	}
	return nil
}

func validateManagedDirectoryStat(stat unix.Stat_t, expectedOwner uint32) error {
	if stat.Mode&unix.S_IFMT != unix.S_IFDIR || stat.Uid != expectedOwner ||
		stat.Mode&0o7777 != 0o710 {
		return errors.New("unsafe managed directory attributes")
	}
	return nil
}

func validateLockFile(fd int, ownerUID uint32, expectedOwner uint32) error {
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return fmt.Errorf("stat lock file: %w", err)
	}
	if err := validateLockFileStat(stat, expectedOwner); err != nil {
		return err
	}
	return validateFileACL(fd, ownerUID)
}

func validateLockFileStat(stat unix.Stat_t, expectedOwner uint32) error {
	if stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Uid != expectedOwner ||
		stat.Mode&0o7777 != 0o660 || stat.Nlink != 1 || stat.Size != 0 {
		return errors.New("unsafe lock file attributes")
	}
	return nil
}

func waitForPanelLock(ctx context.Context, fd int, wait time.Duration) (bool, error) {
	if err := tryPanelLock(fd); err == nil {
		return true, nil
	} else if !lockContended(err) {
		return false, fmt.Errorf("take panel lock: %w", err)
	}
	if wait == 0 {
		return false, nil
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	interval := min(lockRetryInterval, wait)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return false, fmt.Errorf("wait for panel lock: %w", ctx.Err())
		case <-timer.C:
			return false, nil
		case <-ticker.C:
			if err := tryPanelLock(fd); err == nil {
				return true, nil
			} else if !lockContended(err) {
				return false, fmt.Errorf("take panel lock: %w", err)
			}
		}
	}
}

func tryPanelLock(fd int) error {
	if err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return fmt.Errorf("flock panel lock: %w", err)
	}
	return nil
}

func lockContended(err error) bool {
	return errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN)
}

func releasePanelLock(fd int) error {
	unlockErr := unix.Flock(fd, unix.LOCK_UN)
	if unlockErr != nil {
		unlockErr = fmt.Errorf("unlock panel lock: %w", unlockErr)
	}
	closeErr := unix.Close(fd)
	if closeErr != nil {
		closeErr = fmt.Errorf("close panel lock: %w", closeErr)
	}
	return errors.Join(unlockErr, closeErr)
}
