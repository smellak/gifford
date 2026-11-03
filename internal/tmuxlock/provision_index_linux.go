//go:build linux

package tmuxlock

import (
	"errors"
	"fmt"
	"slices"

	"github.com/smellak/gifford/internal/clock"
	"golang.org/x/sys/unix"
)

// Index file names below their directories.
const (
	ownerIndexName    = "index"
	ownerIndexTemp    = "index.tmp"
	enrolledIndexName = "enrolled-uids"
	enrolledIndexTemp = "enrolled-uids.tmp"
)

// Index file modes: the owner index is readable by its uid through a
// read-only named entry and writable only by the node; the enrolled set is
// world-readable because an unenrolled writer must read it to learn it is
// not enrolled. No base validator covers index files yet, so provision pins
// both shapes here, derived from the frozen index contract.
const (
	ownerIndexMode    = 0o640
	enrolledIndexMode = 0o644
)

// publishPanelIndexes publishes the provisioned panel to the owner index and
// the owner uid to the enrolled set, in Alta order (panel files first, then
// U/index, then enrolled-uids), while the caller holds the root index lock.
// Publication is convergent: entries already listed keep their place and no
// file is rewritten, so repeating provision after a crash completes the Alta
// instead of duplicating it. Every sequence number only moves forward.
func publishPanelIndexes(
	rootFD, ownerFD int,
	parts lockPathParts,
	ownerUID uint32,
	clk clock.Clock,
) (IndexPublication, error) {
	pane := mustPaneNumber(parts)
	entry := panelIndexEntry{Pane: pane, SocketSHA: parts.hash}

	ownerSeq, err := publishOwnerIndex(ownerFD, ownerUID, entry)
	if err != nil {
		return IndexPublication{}, err
	}
	// The directory fsync makes the owner index durable; the publication
	// timestamp is taken after it.
	if err := fsyncDir(ownerFD); err != nil {
		return IndexPublication{}, err
	}
	publishedAt := clk.Now()

	enrolledSeq, err := publishEnrolledIndex(rootFD, ownerUID)
	if err != nil {
		return IndexPublication{}, err
	}
	if err := fsyncDir(rootFD); err != nil {
		return IndexPublication{}, err
	}
	return IndexPublication{
		PanelKey:    parts.owner + "/" + parts.hash + "/" + parts.file,
		OwnerSeq:    ownerSeq,
		EnrolledSeq: enrolledSeq,
		PublishedAt: publishedAt,
	}, nil
}

// publishOwnerIndex lists one panel in the owner index, creating the file on
// first use. It returns the sequence the file now carries.
func publishOwnerIndex(dirFD int, ownerUID uint32, entry panelIndexEntry) (uint64, error) {
	seq, entries, err := readPanelIndex(dirFD)
	if err != nil {
		return 0, err
	}
	position, found := slices.BinarySearchFunc(entries, entry, func(left, right panelIndexEntry) int {
		switch {
		case panelEntryLess(left, right):
			return -1
		case panelEntryLess(right, left):
			return 1
		default:
			return 0
		}
	})
	if found {
		return seq, nil
	}
	entries = slices.Insert(entries, position, entry)
	next := seq + 1
	if seq == 0 {
		next = 1
	}
	if next > maxSafeInteger {
		return 0, errors.New("owner index sequence exhausted")
	}
	raw, err := encodePanelIndex(next, entries)
	if err != nil {
		return 0, fmt.Errorf("encode owner index: %w", err)
	}
	if len(raw) > maxPanelIndexSize {
		return 0, errors.New("owner index exceeds size limit")
	}
	if err := writeTempRename(dirFD, ownerIndexName, ownerIndexTemp, raw); err != nil {
		return 0, err
	}
	if err := installOwnerIndexShape(dirFD, ownerUID); err != nil {
		return 0, err
	}
	return next, nil
}

// publishEnrolledIndex lists one uid in the enrolled set, creating the file
// on first use. It returns the sequence the file now carries.
func publishEnrolledIndex(dirFD int, ownerUID uint32) (uint64, error) {
	seq, entries, err := readEnrolledIndex(dirFD)
	if err != nil {
		return 0, err
	}
	position, found := slices.BinarySearch(entries, ownerUID)
	if found {
		return seq, nil
	}
	entries = slices.Insert(entries, position, ownerUID)
	next := seq + 1
	if seq == 0 {
		next = 1
	}
	if next > maxSafeInteger {
		return 0, errors.New("enrolled index sequence exhausted")
	}
	raw, err := encodeEnrolledIndex(next, entries)
	if err != nil {
		return 0, fmt.Errorf("encode enrolled index: %w", err)
	}
	if len(raw) > maxEnrolledBytes {
		return 0, errors.New("enrolled index exceeds size limit")
	}
	if err := writeTempRename(dirFD, enrolledIndexName, enrolledIndexTemp, raw); err != nil {
		return 0, err
	}
	if err := installEnrolledIndexShape(dirFD); err != nil {
		return 0, err
	}
	return next, nil
}

// readPanelIndex reads and strict-decodes the owner index, or reports an
// empty unpublished index when the file does not exist yet.
func readPanelIndex(dirFD int) (uint64, []panelIndexEntry, error) {
	raw, err := readIndexFile(dirFD, ownerIndexName, maxPanelIndexSize)
	if err != nil {
		return 0, nil, err
	}
	if raw == nil {
		return 0, nil, nil
	}
	seq, entries, err := decodePanelIndex(raw)
	if err != nil {
		return 0, nil, fmt.Errorf("owner index: %w", err)
	}
	return seq, entries, nil
}

// readEnrolledIndex reads and strict-decodes the enrolled set, or reports an
// empty unpublished set when the file does not exist yet.
func readEnrolledIndex(dirFD int) (uint64, []uint32, error) {
	raw, err := readIndexFile(dirFD, enrolledIndexName, maxEnrolledBytes)
	if err != nil {
		return 0, nil, err
	}
	if raw == nil {
		return 0, nil, nil
	}
	seq, entries, err := decodeEnrolledIndex(raw)
	if err != nil {
		return 0, nil, fmt.Errorf("enrolled index: %w", err)
	}
	return seq, entries, nil
}

// readIndexFile returns the content of an index file capped at limit bytes,
// or nil when the file does not exist yet. Anything present must have the
// exact published size: indexes are replaced whole, never appended.
func readIndexFile(dirFD int, name string, limit int64) ([]byte, error) {
	fd, err := openAtSecure(dirFD, name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC)
	if err != nil {
		if errors.Is(err, unix.ENOENT) {
			return nil, nil
		}
		return nil, fmt.Errorf("open index file: %w", err)
	}
	defer func() { _ = unix.Close(fd) }()
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return nil, fmt.Errorf("stat index file: %w", err)
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Size > limit {
		return nil, fmt.Errorf("index file %s: unsafe size", name)
	}
	raw := make([]byte, stat.Size)
	offset := 0
	for offset < len(raw) {
		count, err := unix.Read(fd, raw[offset:])
		if err != nil {
			return nil, fmt.Errorf("read index file: %w", err)
		}
		if count == 0 {
			break
		}
		offset += count
	}
	if offset != len(raw) {
		return nil, fmt.Errorf("index file %s: short read", name)
	}
	return raw, nil
}

// writeTempRename installs index bytes durably: exclusive temp write,
// file sync, atomic rename over the target. A stale temp from an
// interrupted publication is unlinked once and retried; anything else
// fails closed without touching the published file.
func writeTempRename(dirFD int, target, temp string, raw []byte) error {
	fd, err := openAtSecure(
		dirFD, temp,
		unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC,
	)
	if err != nil {
		if !errors.Is(err, unix.EEXIST) {
			return fmt.Errorf("create index temp: %w", err)
		}
		if err := unix.Unlinkat(dirFD, temp, 0); err != nil {
			return fmt.Errorf("clear stale index temp: %w", err)
		}
		fd, err = openAtSecure(
			dirFD, temp,
			unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC,
		)
		if err != nil {
			return fmt.Errorf("recreate index temp: %w", err)
		}
	}
	written := 0
	for written < len(raw) {
		count, err := unix.Write(fd, raw[written:])
		if err != nil {
			_ = unix.Close(fd)
			return fmt.Errorf("write index temp: %w", err)
		}
		if count == 0 {
			_ = unix.Close(fd)
			return errors.New("short write index temp")
		}
		written += count
	}
	// Temps are created mode 000; make the owner able to reopen the file
	// after the rename. The shape installer restricts it further.
	if err := unix.Fchmod(fd, 0o600); err != nil {
		_ = unix.Close(fd)
		return fmt.Errorf("chmod index temp: %w", err)
	}
	if err := unix.Fdatasync(fd); err != nil {
		_ = unix.Close(fd)
		return fmt.Errorf("sync index temp: %w", err)
	}
	_ = unix.Close(fd)
	if err := unix.Renameat(dirFD, temp, dirFD, target); err != nil {
		return fmt.Errorf("publish index: %w", err)
	}
	return nil
}

// installOwnerIndexShape restricts a freshly published owner index to its
// published shape: owner-only write mode with a read-only named entry for
// the owner uid, so writers can read their own index but only the node
// publishes it.
func installOwnerIndexShape(dirFD int, ownerUID uint32) error {
	fd, err := openAtSecure(dirFD, ownerIndexName, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC)
	if err != nil {
		return fmt.Errorf("open owner index: %w", err)
	}
	defer func() { _ = unix.Close(fd) }()
	if err := unix.Fchmod(fd, ownerIndexMode); err != nil {
		return fmt.Errorf("chmod owner index: %w", err)
	}
	readable := aclRead
	raw := encodeACL([]aclEntry{
		{aclUserObject, aclRead | aclWrite, aclUndefinedID},
		{aclNamedUser, readable, ownerUID},
		{aclGroupObject, 0, aclUndefinedID},
		{aclMask, readable, aclUndefinedID},
		{aclOther, 0, aclUndefinedID},
	})
	if err := unix.Fsetxattr(fd, accessACLName, raw, 0); err != nil {
		return fmt.Errorf("set owner index ACL: %w", err)
	}
	return validateOwnerIndexShape(fd, ownerUID)
}

// installEnrolledIndexShape restricts a freshly published enrolled set to
// its published shape: world-readable, no extended ACL, so every writer can
// learn whether it is enrolled.
func installEnrolledIndexShape(dirFD int) error {
	fd, err := openAtSecure(dirFD, enrolledIndexName, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC)
	if err != nil {
		return fmt.Errorf("open enrolled index: %w", err)
	}
	defer func() { _ = unix.Close(fd) }()
	if err := unix.Fchmod(fd, enrolledIndexMode); err != nil {
		return fmt.Errorf("chmod enrolled index: %w", err)
	}
	return validateNoFileACL(fd)
}

// validateOwnerIndexShape checks the published owner index attributes: a
// regular file with the owner index mode, a single link, and the exact
// read-only named ACL for the owner uid.
func validateOwnerIndexShape(fd int, ownerUID uint32) error {
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return fmt.Errorf("stat owner index: %w", err)
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Uid != effectiveUID() ||
		stat.Mode&0o7777 != ownerIndexMode || stat.Nlink != 1 {
		return errors.New("unsafe owner index attributes")
	}
	acl, err := readAccessACL(fd)
	if err != nil {
		return fmt.Errorf("read owner index ACL: %w", err)
	}
	if acl.userObject != aclRead|aclWrite || acl.groupObject != 0 || acl.mask != aclRead ||
		acl.other != 0 || len(acl.groups) != 0 || len(acl.users) != 1 ||
		acl.users[ownerUID] != aclRead {
		return errors.New("owner index ACL mismatch")
	}
	return nil
}
