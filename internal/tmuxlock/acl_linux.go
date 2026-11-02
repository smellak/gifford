//go:build linux

package tmuxlock

import (
	"encoding/binary"
	"errors"
	"fmt"

	"golang.org/x/sys/unix"
)

const (
	accessACLName    = "system.posix_acl_access"
	defaultACLName   = "system.posix_acl_default"
	accessACLVersion = uint32(2)
	accessACLMaxSize = 4096
	aclUndefinedID   = ^uint32(0)

	aclUserObject  = uint16(0x01)
	aclNamedUser   = uint16(0x02)
	aclGroupObject = uint16(0x04)
	aclNamedGroup  = uint16(0x08)
	aclMask        = uint16(0x10)
	aclOther       = uint16(0x20)

	aclRead    = uint16(0x04)
	aclWrite   = uint16(0x02)
	aclExecute = uint16(0x01)
)

type accessACL struct {
	userObject  uint16
	groupObject uint16
	mask        uint16
	other       uint16
	users       map[uint32]uint16
	groups      map[uint32]uint16
}

func validateDirectoryACL(fd int, ownerUID uint32) error {
	aclFD, err := openDirectoryACLDescriptor(fd)
	if err != nil {
		return err
	}
	defer func() { _ = unix.Close(aclFD) }()

	acl, err := readAccessACL(aclFD)
	if err != nil {
		return err
	}
	if acl.userObject != aclRead|aclWrite|aclExecute || acl.groupObject != 0 ||
		acl.mask != aclExecute || acl.other != 0 || len(acl.groups) != 0 {
		return errors.New("directory ACL base entries mismatch")
	}
	if acl.users[ownerUID] != aclExecute {
		return errors.New("directory ACL lacks owner traversal")
	}
	for _, permissions := range acl.users {
		if permissions != aclExecute {
			return errors.New("directory ACL grants non-traversal access")
		}
	}
	return nil
}

func validateNoExtendedACL(fd int) error {
	aclFD, err := openDirectoryACLDescriptor(fd)
	if err != nil {
		return err
	}
	defer func() { _ = unix.Close(aclFD) }()

	for _, name := range []string{accessACLName, defaultACLName} {
		_, err = unix.Fgetxattr(aclFD, name, nil)
		if errors.Is(err, unix.ENODATA) {
			continue
		}
		if err != nil {
			return fmt.Errorf("read managed root ACL: %w", err)
		}
		return errors.New("managed root has an extended ACL")
	}
	return nil
}

func openDirectoryACLDescriptor(fd int) (int, error) {
	aclFD, err := unix.Openat(
		fd, ".", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0,
	)
	if err != nil {
		return -1, fmt.Errorf("open directory ACL descriptor: %w", err)
	}

	var pathStat unix.Stat_t
	if err := unix.Fstat(fd, &pathStat); err != nil {
		_ = unix.Close(aclFD)
		return -1, fmt.Errorf("stat directory path descriptor: %w", err)
	}
	var aclStat unix.Stat_t
	if err := unix.Fstat(aclFD, &aclStat); err != nil {
		_ = unix.Close(aclFD)
		return -1, fmt.Errorf("stat directory ACL descriptor: %w", err)
	}
	if pathStat.Dev != aclStat.Dev || pathStat.Ino != aclStat.Ino {
		_ = unix.Close(aclFD)
		return -1, errors.New("directory changed while opening ACL descriptor")
	}
	return aclFD, nil
}

func validateFileACL(fd int, ownerUID uint32) error {
	acl, err := readAccessACL(fd)
	if err != nil {
		return err
	}
	writable := aclRead | aclWrite
	if acl.userObject != writable || acl.groupObject != 0 || acl.mask != writable ||
		acl.other != 0 || len(acl.groups) != 0 || len(acl.users) != 1 ||
		acl.users[ownerUID] != writable {
		return errors.New("lock file ACL mismatch")
	}
	return nil
}

func readAccessACL(fd int) (accessACL, error) {
	size, err := unix.Fgetxattr(fd, accessACLName, nil)
	if err != nil {
		return accessACL{}, fmt.Errorf("read ACL size: %w", err)
	}
	if size < 4 || size > accessACLMaxSize || (size-4)%8 != 0 {
		return accessACL{}, errors.New("invalid ACL size")
	}
	raw := make([]byte, size)
	read, err := unix.Fgetxattr(fd, accessACLName, raw)
	if err != nil {
		return accessACL{}, fmt.Errorf("read ACL: %w", err)
	}
	if read != size {
		return accessACL{}, errors.New("ACL changed while reading")
	}
	return parseAccessACL(raw)
}

func parseAccessACL(raw []byte) (accessACL, error) {
	if len(raw) < 4 || (len(raw)-4)%8 != 0 ||
		binary.LittleEndian.Uint32(raw[:4]) != accessACLVersion {
		return accessACL{}, errors.New("invalid ACL encoding")
	}
	acl := accessACL{
		users:  make(map[uint32]uint16),
		groups: make(map[uint32]uint16),
	}
	seenBase := make(map[uint16]bool, 4)
	lastRank := -1
	var lastUserID uint32
	var lastGroupID uint32
	seenUser := false
	seenGroup := false
	for offset := 4; offset < len(raw); offset += 8 {
		tag := binary.LittleEndian.Uint16(raw[offset : offset+2])
		permissions := binary.LittleEndian.Uint16(raw[offset+2 : offset+4])
		id := binary.LittleEndian.Uint32(raw[offset+4 : offset+8])
		if permissions & ^(aclRead|aclWrite|aclExecute) != 0 {
			return accessACL{}, errors.New("invalid ACL permissions")
		}
		rank, ok := aclTagRank(tag)
		if !ok || rank < lastRank {
			return accessACL{}, errors.New("invalid ACL entry order")
		}
		lastRank = rank
		switch tag {
		case aclUserObject, aclGroupObject, aclMask, aclOther:
			if id != aclUndefinedID || seenBase[tag] {
				return accessACL{}, errors.New("duplicate or identified ACL base entry")
			}
			seenBase[tag] = true
			switch tag {
			case aclUserObject:
				acl.userObject = permissions
			case aclGroupObject:
				acl.groupObject = permissions
			case aclMask:
				acl.mask = permissions
			case aclOther:
				acl.other = permissions
			}
		case aclNamedUser:
			if id == aclUndefinedID || (seenUser && id <= lastUserID) {
				return accessACL{}, errors.New("invalid named-user ACL entry")
			}
			seenUser = true
			lastUserID = id
			acl.users[id] = permissions
		case aclNamedGroup:
			if id == aclUndefinedID || (seenGroup && id <= lastGroupID) {
				return accessACL{}, errors.New("invalid named-group ACL entry")
			}
			seenGroup = true
			lastGroupID = id
			acl.groups[id] = permissions
		}
	}
	for _, tag := range []uint16{aclUserObject, aclGroupObject, aclMask, aclOther} {
		if !seenBase[tag] {
			return accessACL{}, errors.New("missing ACL base entry")
		}
	}
	return acl, nil
}

func aclTagRank(tag uint16) (int, bool) {
	switch tag {
	case aclUserObject:
		return 0, true
	case aclNamedUser:
		return 1, true
	case aclGroupObject:
		return 2, true
	case aclNamedGroup:
		return 3, true
	case aclMask:
		return 4, true
	case aclOther:
		return 5, true
	default:
		return 0, false
	}
}
