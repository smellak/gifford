//go:build linux

package controlapi

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"golang.org/x/sys/unix"
)

const panePolicyACLName = "system.posix_acl_access"

func loadPaneAdminUIDs(path string) ([]uint32, error) {
	return loadPaneAdminUIDsFor(path, 0, 0, true)
}

func loadPaneAdminUIDsFor(
	path string,
	expectedUID uint32,
	expectedGID uint32,
	checkAncestors bool,
) ([]uint32, error) {
	fd, err := openPanePolicy(path, checkAncestors)
	if err != nil {
		return nil, err
	}
	defer func() { _ = unix.Close(fd) }()
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return nil, fmt.Errorf("stat pane operation policy: %w", err)
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Uid != expectedUID ||
		stat.Gid != expectedGID || stat.Nlink != 1 || stat.Size < 0 ||
		stat.Size > maxPaneAdminUIDBytes || stat.Mode&0o7000 != 0 ||
		stat.Mode&0o400 == 0 || stat.Mode&0o022 != 0 {
		return nil, ErrPaneOperationPolicyInvalid
	}
	if _, err := unix.Fgetxattr(fd, panePolicyACLName, nil); err == nil {
		return nil, ErrPaneOperationPolicyInvalid
	} else if !errors.Is(err, unix.ENODATA) && !errors.Is(err, unix.EOPNOTSUPP) {
		return nil, fmt.Errorf("read pane operation policy ACL: %w", err)
	}
	raw := make([]byte, int(stat.Size))
	for offset := 0; offset < len(raw); {
		read, err := unix.Pread(fd, raw[offset:], int64(offset))
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("read pane operation policy: %w", err)
		}
		if read == 0 {
			return nil, ErrPaneOperationPolicyInvalid
		}
		offset += read
	}
	return parsePaneAdminUIDs(raw)
}

func openPanePolicy(path string, checkAncestors bool) (int, error) {
	components := strings.Split(strings.TrimPrefix(path, "/"), "/")
	current, err := unix.Open("/", unix.O_PATH|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, fmt.Errorf("open pane policy root: %w", err)
	}
	if checkAncestors {
		if err := validatePanePolicyAncestor(current); err != nil {
			_ = unix.Close(current)
			return -1, err
		}
	}
	for _, component := range components[:len(components)-1] {
		next, openErr := openPanePolicyAt(current, component,
			unix.O_PATH|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC)
		_ = unix.Close(current)
		if openErr != nil {
			return -1, openErr
		}
		current = next
		if checkAncestors {
			if err := validatePanePolicyAncestor(current); err != nil {
				_ = unix.Close(current)
				return -1, err
			}
		}
	}
	fd, err := openPanePolicyAt(current, components[len(components)-1],
		unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC)
	_ = unix.Close(current)
	return fd, err
}

func openPanePolicyAt(parent int, name string, flags uint64) (int, error) {
	how := &unix.OpenHow{
		Flags: flags,
		Resolve: unix.RESOLVE_BENEATH | unix.RESOLVE_NO_SYMLINKS |
			unix.RESOLVE_NO_MAGICLINKS,
	}
	fd, err := unix.Openat2(parent, name, how)
	if err == nil {
		return fd, nil
	}
	if !errors.Is(err, unix.ENOSYS) && !errors.Is(err, unix.EINVAL) {
		return -1, fmt.Errorf("open pane operation policy: %w", err)
	}
	fd, err = unix.Openat(parent, name, int(flags), 0) //nolint:gosec // closed flags above
	if err != nil {
		return -1, fmt.Errorf("open pane operation policy: %w", err)
	}
	return fd, nil
}

func validatePanePolicyAncestor(fd int) error {
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return fmt.Errorf("stat pane policy ancestor: %w", err)
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFDIR || stat.Uid != 0 || stat.Mode&0o022 != 0 {
		return ErrPaneOperationPolicyInvalid
	}
	return nil
}

func effectivePanePolicyOwner() (uint32, uint32) {
	return uint32(os.Geteuid()), uint32(os.Getegid()) //nolint:gosec // uid_t and gid_t are uint32
}
