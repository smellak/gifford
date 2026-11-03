//go:build linux

package tmuxlock

import (
	"encoding/binary"
	"errors"
	"fmt"

	"golang.org/x/sys/unix"
)

// Provision index-lock name: the single global lock serializing panel
// publications. The Alta commit order publishes panel files first and the
// indexes afterwards under this lock; provision holds it across the whole
// call so concurrent altas of different panels cannot interleave.
const provisionIndexLockName = "index.lock"

// Provision file modes: panel files are owner-managed 0660 with the exact
// contract ACL, the global index lock is owner-only 0600 with no extended
// ACL. Register files use the same file ACL as lock and state records: the
// base panel validators accept exactly this shape, and the Alta order keeps
// authority in the record content, never in a read-only bit.
const (
	provisionFileMode      = 0o660
	provisionIndexLockMode = 0o600
	provisionCreateMode    = 0o700
	provisionDirMode       = 0o710
)

// provisionPlatform installs or revalidates one panel under an absolute,
// already validated root. Every path below the root is opened through
// directory file descriptors with NOFOLLOW resolution, mirroring the panel
// guard walk, so a symlink planted between components cannot redirect the
// install. Files are created with O_CREAT|O_EXCL; EEXIST means another
// provision won the race and the existing files are validated strictly
// instead. Each created file is fdatasynced before the next step, and the
// socket directory is fsynced after the three files.
func provisionPlatform(spec ProvisionSpec, parts lockPathParts, content panelContent) (ProvisionResult, error) {
	result := ProvisionResult{
		OwnerDir:  spec.Root + "/" + parts.owner,
		SocketDir: spec.Root + "/" + parts.owner + "/" + parts.hash,
		IndexLock: spec.Root + "/" + provisionIndexLockName,
	}
	result.Lock = result.SocketDir + "/" + parts.file
	pane := parts.file[:len(parts.file)-len(".lock")]
	result.State = result.SocketDir + "/" + pane + ".state"
	result.Register = result.SocketDir + "/" + pane + ".reg"

	rootFD, err := openProvisionRoot(spec.Root)
	if err != nil {
		return ProvisionResult{}, err
	}
	defer func() { _ = unix.Close(rootFD) }()

	indexFD, err := ensureIndexLock(rootFD)
	if err != nil {
		_ = unix.Close(rootFD)
		return ProvisionResult{}, err
	}
	defer func() { _ = unix.Close(indexFD) }()
	if err := unix.Flock(indexFD, unix.LOCK_EX); err != nil {
		return ProvisionResult{}, fmt.Errorf("lock provision index: %w", err)
	}
	defer func() { _ = unix.Flock(indexFD, unix.LOCK_UN) }()

	ownerUID := spec.Binding.OwnerUID
	ownerFD, ownerCreated, err := ensureManagedDir(rootFD, parts.owner, ownerUID)
	if err != nil {
		return ProvisionResult{}, err
	}
	defer func() { _ = unix.Close(ownerFD) }()
	if ownerCreated {
		if err := fsyncDir(rootFD); err != nil {
			return ProvisionResult{}, err
		}
	}

	socketFD, socketCreated, err := ensureManagedDir(ownerFD, parts.hash, ownerUID)
	if err != nil {
		return ProvisionResult{}, err
	}
	defer func() { _ = unix.Close(socketFD) }()
	if socketCreated {
		if err := fsyncDir(ownerFD); err != nil {
			return ProvisionResult{}, err
		}
	}

	created, err := ensurePanelFiles(socketFD, parts, ownerUID, content)
	if err != nil {
		return ProvisionResult{}, err
	}
	if created {
		if err := fsyncDir(socketFD); err != nil {
			return ProvisionResult{}, err
		}
	}
	result.Created = created
	return result, nil
}

// openProvisionRoot walks the absolute root from the filesystem root without
// following symlinks and validates it as a managed root owned by the current
// effective user, exactly like the panel guard walk. System ancestors are
// only enforced for the production root; tests use private roots.
func openProvisionRoot(root string) (int, error) {
	components, err := managedRootComponents(root)
	if err != nil {
		return -1, err
	}
	// Unlike the guard walk, the root descriptor stays readable: provision
	// fsyncs through it after creating entries, which O_PATH forbids.
	current, err := unix.Open(
		"/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0,
	)
	if err != nil {
		return -1, fmt.Errorf("open filesystem root: %w", err)
	}
	validateAncestors := root == DefaultRoot
	if validateAncestors {
		if err := validateTrustedAncestor(current); err != nil {
			_ = unix.Close(current)
			return -1, err
		}
	}
	for index, component := range components {
		next, openErr := openAtSecure(
			current, component,
			unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC,
		)
		_ = unix.Close(current)
		if openErr != nil {
			return -1, fmt.Errorf("open managed root: %w", openErr)
		}
		current = next
		if validateAncestors && index < len(components)-1 {
			if err := validateTrustedAncestor(current); err != nil {
				_ = unix.Close(current)
				return -1, err
			}
		}
	}
	if err := validateManagedRoot(current, effectiveUID(), effectiveGID()); err != nil {
		_ = unix.Close(current)
		return -1, err
	}
	return current, nil
}

// ensureManagedDir creates or revalidates one managed directory below
// parentFD: owner and socket directories are 0710 with the exact owner
// directory ACL (traversal for the owner uid, nothing else). New directories
// start at 0700, receive the ACL, then move to 0710, so the intermediate mode
// is always more restrictive than the final one.
func ensureManagedDir(parentFD int, name string, ownerUID uint32) (int, bool, error) {
	if err := unix.Mkdirat(parentFD, name, provisionCreateMode); err != nil {
		if !errors.Is(err, unix.EEXIST) {
			return -1, false, fmt.Errorf("create managed directory: %w", err)
		}
		fd, err := openAtSecure(
			parentFD, name,
			unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC,
		)
		if err != nil {
			return -1, false, fmt.Errorf("open managed directory: %w", err)
		}
		if err := validateManagedDirectory(fd, ownerUID, effectiveUID()); err != nil {
			_ = unix.Close(fd)
			return -1, false, fmt.Errorf("validate managed directory: %w", err)
		}
		return fd, false, nil
	}
	fd, err := openAtSecure(
		parentFD, name,
		unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC,
	)
	if err != nil {
		return -1, false, fmt.Errorf("open managed directory: %w", err)
	}
	if err := installDirectoryACL(fd, ownerUID); err != nil {
		_ = unix.Close(fd)
		return -1, false, err
	}
	if err := unix.Fchmod(fd, provisionDirMode); err != nil {
		_ = unix.Close(fd)
		return -1, false, fmt.Errorf("chmod managed directory: %w", err)
	}
	if err := validateManagedDirectory(fd, ownerUID, effectiveUID()); err != nil {
		_ = unix.Close(fd)
		return -1, false, fmt.Errorf("validate managed directory: %w", err)
	}
	return fd, true, nil
}

// ensureIndexLock creates or revalidates the global publication lock at the
// root: an empty, owner-only file without extended ACL. The caller holds
// LOCK_EX on the returned descriptor for the whole provision.
func ensureIndexLock(rootFD int) (int, error) {
	fd, err := openAtSecure(
		rootFD, provisionIndexLockName,
		unix.O_RDWR|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC,
	)
	if err == nil {
		if err := installIndexLock(fd); err != nil {
			_ = unix.Close(fd)
			return -1, err
		}
		return fd, nil
	}
	if !errors.Is(err, unix.EEXIST) {
		return -1, fmt.Errorf("create provision index lock: %w", err)
	}
	fd, err = openAtSecure(
		rootFD, provisionIndexLockName,
		unix.O_RDWR|unix.O_NOFOLLOW|unix.O_CLOEXEC,
	)
	if err != nil {
		return -1, fmt.Errorf("open provision index lock: %w", err)
	}
	if err := validateIndexLock(fd); err != nil {
		_ = unix.Close(fd)
		return -1, err
	}
	return fd, nil
}

// installIndexLock restricts a freshly created index lock to its final shape:
// owner-only mode, empty content made durable, no extended ACL.
func installIndexLock(fd int) error {
	if err := unix.Fchmod(fd, provisionIndexLockMode); err != nil {
		return fmt.Errorf("chmod provision index lock: %w", err)
	}
	if err := unix.Fdatasync(fd); err != nil {
		return fmt.Errorf("sync provision index lock: %w", err)
	}
	return validateIndexLock(fd)
}

// validateIndexLock checks the shape provision guarantees for the global
// publication lock: regular file, owned by the effective user, owner-only
// mode, single link, empty, and no extended ACL.
func validateIndexLock(fd int) error {
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return fmt.Errorf("stat provision index lock: %w", err)
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Uid != effectiveUID() ||
		stat.Mode&0o7777 != provisionIndexLockMode || stat.Nlink != 1 || stat.Size != 0 {
		return errors.New("unsafe provision index lock attributes")
	}
	return validateNoFileACL(fd)
}

// validateNoFileACL requires a regular file to carry no extended ACL at all.
// Unlike validateNoExtendedACL, which opens a directory descriptor, this
// reads the xattrs straight off the already NOFOLLOW-opened file descriptor,
// so it works on non-directories. Missing attributes (ENODATA) are the
// expected state; anything else fails closed.
func validateNoFileACL(fd int) error {
	for _, name := range []string{accessACLName, defaultACLName} {
		_, err := unix.Fgetxattr(fd, name, nil)
		if errors.Is(err, unix.ENODATA) {
			continue
		}
		if err != nil {
			return fmt.Errorf("read panel file ACL: %w", err)
		}
		return errors.New("panel file has an extended ACL")
	}
	return nil
}

// ensurePanelFiles installs or revalidates the three panel files in Alta
// order: empty lock, provision state, register. The three files are created
// with O_EXCL and made durable one by one; pre-existing files are validated
// strictly (attributes, ACL and content) and cross-checked for coherence with
// the request. A partial install from an interrupted earlier run is completed
// by this same call, never deleted.
func ensurePanelFiles(dirFD int, parts lockPathParts, ownerUID uint32, content panelContent) (bool, error) {
	pane := parts.file[:len(parts.file)-len(".lock")]
	files := []struct {
		name    string
		content []byte
	}{
		{parts.file, content.lock},
		{pane + ".state", content.state},
		{pane + ".reg", content.register},
	}
	created := false
	installed := make([][]byte, 0, len(files))
	for _, file := range files {
		raw, fileCreated, err := ensurePanelFile(dirFD, file.name, file.content, ownerUID)
		if err != nil {
			return false, err
		}
		created = created || fileCreated
		installed = append(installed, raw)
	}
	if len(installed[0]) != 0 {
		return false, errors.New("provision lock file is not empty")
	}
	state, err := decodeStateRecord(installed[1])
	if err != nil {
		return false, fmt.Errorf("provision state: %w", err)
	}
	register, err := decodeRegisterRecord(installed[2])
	if err != nil {
		return false, fmt.Errorf("provision register: %w", err)
	}
	if err := checkPanelCoherence(content, register, state); err != nil {
		return false, err
	}
	return created, nil
}

// ensurePanelFile creates one panel file with the exact contract shape or,
// when it already exists, reads it back for strict validation. It returns
// the installed bytes and whether this call created them.
func ensurePanelFile(dirFD int, name string, content []byte, ownerUID uint32) ([]byte, bool, error) {
	fd, err := openAtSecure(
		dirFD, name,
		unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC,
	)
	if err == nil {
		raw := content
		if writeErr := installPanelFile(fd, name, raw, ownerUID); writeErr != nil {
			_ = unix.Close(fd)
			return nil, false, writeErr
		}
		_ = unix.Close(fd)
		return raw, true, nil
	}
	if !errors.Is(err, unix.EEXIST) {
		return nil, false, fmt.Errorf("create panel file: %w", err)
	}
	fd, err = openAtSecure(dirFD, name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC)
	if err != nil {
		return nil, false, fmt.Errorf("open panel file: %w", err)
	}
	defer func() { _ = unix.Close(fd) }()
	return readPanelFile(fd, name, ownerUID)
}

// installPanelFile writes a new panel file to its final shape: restrictive
// creation mode first, then the owner file ACL, then the durable content.
// The final fstat rejects any mode drift the xattr write may have caused.
func installPanelFile(fd int, name string, content []byte, ownerUID uint32) error {
	if err := unix.Fchmod(fd, provisionFileMode); err != nil {
		return fmt.Errorf("chmod panel file: %w", err)
	}
	written := 0
	for written < len(content) {
		count, err := unix.Write(fd, content[written:])
		if err != nil {
			return fmt.Errorf("write panel file: %w", err)
		}
		if count == 0 {
			return errors.New("short write panel file")
		}
		written += count
	}
	if err := unix.Fdatasync(fd); err != nil {
		return fmt.Errorf("sync panel file: %w", err)
	}
	if err := installFileACL(fd, ownerUID); err != nil {
		return fmt.Errorf("acl panel file %s: %w", name, err)
	}
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return fmt.Errorf("stat panel file: %w", err)
	}
	if err := validateProvisionedFileStat(stat, int64(len(content))); err != nil {
		return fmt.Errorf("panel file %s: %w", name, err)
	}
	return nil
}

// readPanelFile validates a pre-existing panel file by descriptor (regular,
// owner, mode, single link, expected size, exact file ACL) and returns its
// content for strict decoding by the caller.
func readPanelFile(fd int, name string, ownerUID uint32) ([]byte, bool, error) {
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return nil, false, fmt.Errorf("stat panel file: %w", err)
	}
	size, err := panelFileSize(name)
	if err != nil {
		return nil, false, err
	}
	if err := validateProvisionedFileStat(stat, size); err != nil {
		return nil, false, fmt.Errorf("panel file %s: %w", name, err)
	}
	if err := validateFileACL(fd, ownerUID); err != nil {
		return nil, false, fmt.Errorf("panel file %s: %w", name, err)
	}
	raw := make([]byte, size)
	offset := 0
	for offset < len(raw) {
		count, err := unix.Read(fd, raw[offset:])
		if err != nil {
			return nil, false, fmt.Errorf("read panel file: %w", err)
		}
		if count == 0 {
			break
		}
		offset += count
	}
	if int64(offset) != size {
		return nil, false, fmt.Errorf("panel file %s: short read", name)
	}
	return raw, false, nil
}

// panelFileSize is the exact on-disk size provision guarantees: empty locks,
// full 1024-byte authority records.
func panelFileSize(name string) (int64, error) {
	switch {
	case len(name) > len(".lock") && name[len(name)-len(".lock"):] == ".lock":
		return 0, nil
	case len(name) > len(".state") && name[len(name)-len(".state"):] == ".state",
		len(name) > len(".reg") && name[len(name)-len(".reg"):] == ".reg":
		return recordSize, nil
	default:
		return 0, fmt.Errorf("unknown panel file %q", name)
	}
}

// validateProvisionedFileStat checks the descriptor attributes provision
// guarantees for panel files: regular file owned by the effective user, the
// panel file mode, a single link, and the exact size for the file kind.
func validateProvisionedFileStat(stat unix.Stat_t, size int64) error {
	if stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Uid != effectiveUID() ||
		stat.Mode&0o7777 != provisionFileMode || stat.Nlink != 1 || stat.Size != size {
		return errors.New("unsafe panel file attributes")
	}
	return nil
}

// aclEntry is one POSIX ACL entry in wire order.
type aclEntry struct {
	tag         uint16
	permissions uint16
	id          uint32
}

// encodeACL renders versioned little-endian ACL bytes in tag-rank order,
// the same layout parseAccessACL accepts. Named users go sorted ascending.
func encodeACL(entries []aclEntry) []byte {
	raw := make([]byte, 4+8*len(entries))
	binary.LittleEndian.PutUint32(raw[:4], accessACLVersion)
	for index, entry := range entries {
		offset := 4 + 8*index
		binary.LittleEndian.PutUint16(raw[offset:offset+2], entry.tag)
		binary.LittleEndian.PutUint16(raw[offset+2:offset+4], entry.permissions)
		binary.LittleEndian.PutUint32(raw[offset+4:offset+8], entry.id)
	}
	return raw
}

// installDirectoryACL installs the exact owner directory ACL: full control for
// the file owner, traversal for the owner uid, nothing else.
func installDirectoryACL(fd int, ownerUID uint32) error {
	traverse := aclExecute
	raw := encodeACL([]aclEntry{
		{aclUserObject, aclRead | aclWrite | aclExecute, aclUndefinedID},
		{aclNamedUser, traverse, ownerUID},
		{aclGroupObject, 0, aclUndefinedID},
		{aclMask, traverse, aclUndefinedID},
		{aclOther, 0, aclUndefinedID},
	})
	if err := unix.Fsetxattr(fd, accessACLName, raw, 0); err != nil {
		return fmt.Errorf("set directory ACL: %w", err)
	}
	return nil
}

// installFileACL installs the exact panel file ACL: read-write for the file
// owner and for the owner uid, nothing else. Register, state and lock files
// all share this shape; authority lives in the record content.
func installFileACL(fd int, ownerUID uint32) error {
	writable := aclRead | aclWrite
	raw := encodeACL([]aclEntry{
		{aclUserObject, writable, aclUndefinedID},
		{aclNamedUser, writable, ownerUID},
		{aclGroupObject, 0, aclUndefinedID},
		{aclMask, writable, aclUndefinedID},
		{aclOther, 0, aclUndefinedID},
	})
	if err := unix.Fsetxattr(fd, accessACLName, raw, 0); err != nil {
		return fmt.Errorf("set file ACL: %w", err)
	}
	return nil
}

// fsyncDir persists directory entries created under dirFD.
func fsyncDir(dirFD int) error {
	if err := unix.Fsync(dirFD); err != nil {
		return fmt.Errorf("sync managed directory: %w", err)
	}
	return nil
}
