//go:build linux

package tmuxlock

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

const panelLockHelperEnv = "GIFFORD_TMUXLOCK_HELPER"

type panelLockFixture struct {
	root     string
	binding  PanelBinding
	parts    lockPathParts
	ownerDir string
	hashDir  string
	lockPath string
}

type testACLEntry struct {
	tag         uint16
	permissions uint16
	id          uint32
}

func TestAcquirePanelLockValidProvisioning(t *testing.T) {
	t.Parallel()
	fixture := newPanelLockFixture(t)
	guard, outcome, err := acquireAt(t.Context(), fixture.root, fixture.binding, 0)
	require.NoError(t, err)
	require.Equal(t, LockAcquired, outcome)
	require.NotNil(t, guard)

	flags, err := unix.FcntlInt(uintptr(guard.fd), unix.F_GETFD, 0)
	require.NoError(t, err)
	require.NotZero(t, flags&unix.FD_CLOEXEC)
	require.NotZero(t, guard.device)
	require.NotZero(t, guard.inode)
	require.NoError(t, guard.Close())
	require.NoError(t, guard.Close())
}

func TestAcquirePanelLockAcceptsTraverseOnlyPeers(t *testing.T) {
	t.Parallel()
	fixture := newPanelLockFixture(t)
	peerUID := fixture.binding.OwnerUID + 1
	setDirectoryACL(t, fixture.ownerDir, fixture.binding.OwnerUID, peerUID)
	setDirectoryACL(t, fixture.hashDir, fixture.binding.OwnerUID, peerUID)

	guard, outcome, err := acquireAt(t.Context(), fixture.root, fixture.binding, 0)
	require.NoError(t, err)
	require.Equal(t, LockAcquired, outcome)
	require.NoError(t, guard.Close())
}

func TestAcquirePanelLockExcludesAnotherProcess(t *testing.T) {
	fixture := newPanelLockFixture(t)
	guard, outcome, err := acquireAt(t.Context(), fixture.root, fixture.binding, 0)
	require.NoError(t, err)
	require.Equal(t, LockAcquired, outcome)
	defer func() { require.NoError(t, guard.Close()) }()

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestPanelLockProcessHelper$")
	command.Env = append(os.Environ(),
		panelLockHelperEnv+"=1",
		"GIFFORD_TMUXLOCK_ROOT="+fixture.root,
		"GIFFORD_TMUXLOCK_UID="+strconv.FormatUint(uint64(fixture.binding.OwnerUID), 10),
		"GIFFORD_TMUXLOCK_SOCKET="+fixture.binding.Socket,
		"GIFFORD_TMUXLOCK_PANE="+fixture.binding.PaneID,
		"GIFFORD_TMUXLOCK_FD="+strconv.Itoa(guard.fd),
		"GIFFORD_TMUXLOCK_DEVICE="+strconv.FormatUint(guard.device, 10),
		"GIFFORD_TMUXLOCK_INODE="+strconv.FormatUint(guard.inode, 10),
	)
	require.NoError(t, command.Run())
	require.NoError(t, ctx.Err())
}

func TestPanelLockProcessHelper(t *testing.T) {
	if os.Getenv(panelLockHelperEnv) != "1" {
		return
	}
	owner, err := strconv.ParseUint(os.Getenv("GIFFORD_TMUXLOCK_UID"), 10, 32)
	require.NoError(t, err)
	parentFD, err := strconv.Atoi(os.Getenv("GIFFORD_TMUXLOCK_FD"))
	require.NoError(t, err)
	wantDevice, err := strconv.ParseUint(os.Getenv("GIFFORD_TMUXLOCK_DEVICE"), 10, 64)
	require.NoError(t, err)
	wantInode, err := strconv.ParseUint(os.Getenv("GIFFORD_TMUXLOCK_INODE"), 10, 64)
	require.NoError(t, err)
	var inherited unix.Stat_t
	if err := unix.Fstat(parentFD, &inherited); err == nil {
		require.NotEqual(t, [2]uint64{wantDevice, wantInode}, [2]uint64{inherited.Dev, inherited.Ino})
	}
	binding := PanelBinding{
		OwnerUID: uint32(owner),
		Socket:   os.Getenv("GIFFORD_TMUXLOCK_SOCKET"),
		PaneID:   os.Getenv("GIFFORD_TMUXLOCK_PANE"),
	}
	guard, outcome, err := acquireAt(
		t.Context(), os.Getenv("GIFFORD_TMUXLOCK_ROOT"), binding, 30*time.Millisecond,
	)
	require.NoError(t, err)
	require.Nil(t, guard)
	require.Equal(t, LockDeferred, outcome)
}

func TestAcquirePanelLockReopensSameInode(t *testing.T) {
	t.Parallel()
	fixture := newPanelLockFixture(t)
	guard, outcome, err := acquireAt(t.Context(), fixture.root, fixture.binding, 0)
	require.NoError(t, err)
	require.Equal(t, LockAcquired, outcome)
	defer func() { require.NoError(t, guard.Close()) }()

	fd, stat, err := openProvisionedLock(fixture.root, fixture.binding.OwnerUID, fixture.parts)
	require.NoError(t, err)
	defer func() { require.NoError(t, unix.Close(fd)) }()
	require.Equal(t, [2]uint64{guard.device, guard.inode}, [2]uint64{stat.Dev, stat.Ino})
	require.Error(t, tryPanelLock(fd))

	other, otherOutcome, err := acquireAt(t.Context(), fixture.root, fixture.binding, 20*time.Millisecond)
	require.NoError(t, err)
	require.Nil(t, other)
	require.Equal(t, LockDeferred, otherOutcome)
}

func TestAcquirePanelLockRejectsSymlinks(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		mutate func(*testing.T, panelLockFixture) string
	}{
		{
			name: "managed root",
			mutate: func(t *testing.T, fixture panelLockFixture) string {
				realRoot := fixture.root + "-real"
				require.NoError(t, os.Rename(fixture.root, realRoot))
				t.Cleanup(func() { _ = os.RemoveAll(realRoot) })
				require.NoError(t, os.Symlink(realRoot, fixture.root))
				return fixture.root
			},
		},
		{
			name: "owner directory",
			mutate: func(t *testing.T, fixture panelLockFixture) string {
				realDirectory := fixture.ownerDir + "-real"
				require.NoError(t, os.Rename(fixture.ownerDir, realDirectory))
				require.NoError(t, os.Symlink(realDirectory, fixture.ownerDir))
				return fixture.root
			},
		},
		{
			name: "hash directory",
			mutate: func(t *testing.T, fixture panelLockFixture) string {
				realDirectory := fixture.hashDir + "-real"
				require.NoError(t, os.Rename(fixture.hashDir, realDirectory))
				require.NoError(t, os.Symlink(realDirectory, fixture.hashDir))
				return fixture.root
			},
		},
		{
			name: "lock file",
			mutate: func(t *testing.T, fixture panelLockFixture) string {
				realFile := fixture.lockPath + "-real"
				require.NoError(t, os.Rename(fixture.lockPath, realFile))
				require.NoError(t, os.Symlink(realFile, fixture.lockPath))
				return fixture.root
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			fixture := newPanelLockFixture(t)
			root := test.mutate(t, fixture)
			assertUnprovisioned(t, root, fixture.binding)
		})
	}
}

func TestAcquirePanelLockRejectsUnsafeManagedRoot(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		mutate func(*testing.T, panelLockFixture)
	}{
		{
			name: "extended ACL",
			mutate: func(t *testing.T, fixture panelLockFixture) {
				setRootExtendedACL(t, fixture.root, fixture.binding.OwnerUID)
				info, err := os.Stat(fixture.root)
				require.NoError(t, err)
				require.Equal(t, os.FileMode(0o711), info.Mode().Perm())
			},
		},
		{
			name: "default ACL",
			mutate: func(t *testing.T, fixture panelLockFixture) {
				setRootDefaultACL(t, fixture.root, fixture.binding.OwnerUID)
			},
		},
		{
			name: "group writable",
			mutate: func(t *testing.T, fixture panelLockFixture) {
				require.NoError(t, os.Chmod(fixture.root, 0o731))
			},
		},
		{
			name: "other writable",
			mutate: func(t *testing.T, fixture panelLockFixture) {
				require.NoError(t, os.Chmod(fixture.root, 0o713))
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			fixture := newPanelLockFixture(t)
			test.mutate(t, fixture)
			assertUnprovisioned(t, fixture.root, fixture.binding)
		})
	}
}

func TestAcquirePanelLockRejectsUnsafeLeaf(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		mutate func(*testing.T, panelLockFixture)
	}{
		{
			name: "missing",
			mutate: func(t *testing.T, fixture panelLockFixture) {
				require.NoError(t, os.Remove(fixture.lockPath))
			},
		},
		{
			name: "hard linked",
			mutate: func(t *testing.T, fixture panelLockFixture) {
				require.NoError(t, os.Link(fixture.lockPath, fixture.lockPath+".other"))
			},
		},
		{
			name: "nonempty",
			mutate: func(t *testing.T, fixture panelLockFixture) {
				require.NoError(t, os.WriteFile(fixture.lockPath, []byte("x"), 0o600))
			},
		},
		{
			name: "wrong mode",
			mutate: func(t *testing.T, fixture panelLockFixture) {
				require.NoError(t, os.Chmod(fixture.lockPath, 0o600))
			},
		},
		{
			name: "wrong ACL",
			mutate: func(t *testing.T, fixture panelLockFixture) {
				setFileACL(t, fixture.lockPath, fixture.binding.OwnerUID+1)
			},
		},
		{
			name: "directory",
			mutate: func(t *testing.T, fixture panelLockFixture) {
				require.NoError(t, os.Remove(fixture.lockPath))
				require.NoError(t, os.Mkdir(fixture.lockPath, 0o700))
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			fixture := newPanelLockFixture(t)
			test.mutate(t, fixture)
			assertUnprovisioned(t, fixture.root, fixture.binding)
		})
	}
}

func TestValidateProvisionedStats(t *testing.T) {
	t.Parallel()
	ancestor := unix.Stat_t{Mode: unix.S_IFDIR | 0o755, Uid: 0}
	require.NoError(t, validateTrustedAncestorStat(ancestor))
	for _, mutate := range []func(*unix.Stat_t){
		func(stat *unix.Stat_t) { stat.Uid = 1 },
		func(stat *unix.Stat_t) { stat.Mode = unix.S_IFREG | 0o755 },
		func(stat *unix.Stat_t) { stat.Mode = unix.S_IFDIR | 0o775 },
		func(stat *unix.Stat_t) { stat.Mode = unix.S_IFDIR | 0o757 },
	} {
		bad := ancestor
		mutate(&bad)
		require.Error(t, validateTrustedAncestorStat(bad))
	}

	uid := uint32(os.Geteuid())
	gid := uint32(os.Getegid())
	root := unix.Stat_t{Mode: unix.S_IFDIR | 0o711, Uid: uid, Gid: gid, Nlink: 2}
	require.NoError(t, validateManagedRootStat(root, uid, gid))
	for _, mutate := range []func(*unix.Stat_t){
		func(stat *unix.Stat_t) { stat.Uid++ },
		func(stat *unix.Stat_t) { stat.Gid++ },
		func(stat *unix.Stat_t) { stat.Mode = unix.S_IFREG | 0o711 },
		func(stat *unix.Stat_t) { stat.Mode = unix.S_IFDIR | 0o710 },
		func(stat *unix.Stat_t) { stat.Mode = unix.S_IFDIR | 0o731 },
		func(stat *unix.Stat_t) { stat.Mode = unix.S_IFDIR | 0o713 },
		func(stat *unix.Stat_t) { stat.Mode = unix.S_IFDIR | 0o1711 },
		func(stat *unix.Stat_t) { stat.Nlink = 1 },
	} {
		bad := root
		mutate(&bad)
		require.Error(t, validateManagedRootStat(bad, uid, gid))
	}

	directory := unix.Stat_t{Mode: unix.S_IFDIR | 0o710, Uid: uid}
	require.NoError(t, validateManagedDirectoryStat(directory, uid))
	for _, mutate := range []func(*unix.Stat_t){
		func(stat *unix.Stat_t) { stat.Uid++ },
		func(stat *unix.Stat_t) { stat.Mode = unix.S_IFREG | 0o710 },
		func(stat *unix.Stat_t) { stat.Mode = unix.S_IFDIR | 0o700 },
		func(stat *unix.Stat_t) { stat.Mode = unix.S_IFDIR | 0o1710 },
	} {
		bad := directory
		mutate(&bad)
		require.Error(t, validateManagedDirectoryStat(bad, uid))
	}

	file := unix.Stat_t{Mode: unix.S_IFREG | 0o660, Uid: uid, Nlink: 1}
	require.NoError(t, validateLockFileStat(file, uid))
	for _, mutate := range []func(*unix.Stat_t){
		func(stat *unix.Stat_t) { stat.Uid++ },
		func(stat *unix.Stat_t) { stat.Mode = unix.S_IFDIR | 0o660 },
		func(stat *unix.Stat_t) { stat.Mode = unix.S_IFREG | 0o600 },
		func(stat *unix.Stat_t) { stat.Mode = unix.S_IFREG | 0o1660 },
		func(stat *unix.Stat_t) { stat.Nlink = 2 },
		func(stat *unix.Stat_t) { stat.Size = 1 },
	} {
		bad := file
		mutate(&bad)
		require.Error(t, validateLockFileStat(bad, uid))
	}
}

func TestParseAccessACLRejectsMalformedEncoding(t *testing.T) {
	t.Parallel()
	valid := encodeTestACL([]testACLEntry{
		{aclUserObject, aclRead | aclWrite | aclExecute, aclUndefinedID},
		{aclNamedUser, aclExecute, 1000},
		{aclGroupObject, 0, aclUndefinedID},
		{aclMask, aclExecute, aclUndefinedID},
		{aclOther, 0, aclUndefinedID},
	})
	parsed, err := parseAccessACL(valid)
	require.NoError(t, err)
	require.Equal(t, aclExecute, parsed.users[1000])

	tests := [][]byte{
		nil,
		{2, 0, 0},
		append([]byte(nil), valid[:len(valid)-1]...),
		encodeTestACL([]testACLEntry{
			{aclNamedUser, aclExecute, 1000},
			{aclUserObject, aclRead | aclWrite | aclExecute, aclUndefinedID},
			{aclGroupObject, 0, aclUndefinedID},
			{aclMask, aclExecute, aclUndefinedID},
			{aclOther, 0, aclUndefinedID},
		}),
		encodeTestACL([]testACLEntry{
			{aclUserObject, aclRead | aclWrite | aclExecute, aclUndefinedID},
			{aclNamedUser, aclExecute, 1000},
			{aclNamedUser, aclExecute, 1000},
			{aclGroupObject, 0, aclUndefinedID},
			{aclMask, aclExecute, aclUndefinedID},
			{aclOther, 0, aclUndefinedID},
		}),
	}
	for _, raw := range tests {
		_, err := parseAccessACL(raw)
		require.Error(t, err)
	}
}

func newPanelLockFixture(t *testing.T) panelLockFixture {
	t.Helper()
	ownerUID := uint32(60001)
	if ownerUID == uint32(os.Geteuid()) {
		ownerUID++
	}
	binding := PanelBinding{
		OwnerUID: ownerUID,
		Socket:   "/tmp/tmux-60001/panel-lock-test",
		PaneID:   testPaneTwelve,
	}
	parts, err := deriveParts(binding)
	require.NoError(t, err)
	root := t.TempDir()
	setManagedRoot(t, root)
	ownerDir := filepath.Join(root, parts.owner)
	require.NoError(t, os.Mkdir(ownerDir, 0o700))
	setDirectoryACL(t, ownerDir, ownerUID)
	hashDir := filepath.Join(ownerDir, parts.hash)
	require.NoError(t, os.Mkdir(hashDir, 0o700))
	setDirectoryACL(t, hashDir, ownerUID)
	lockPath := filepath.Join(hashDir, parts.file)
	file, err := os.OpenFile(lockPath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	require.NoError(t, err)
	require.NoError(t, file.Close())
	setFileACL(t, lockPath, ownerUID)
	return panelLockFixture{
		root:     root,
		binding:  binding,
		parts:    parts,
		ownerDir: ownerDir,
		hashDir:  hashDir,
		lockPath: lockPath,
	}
}

func setManagedRoot(t *testing.T, path string) {
	t.Helper()
	require.NoError(t, os.Chmod(path, 0o711))
	for _, name := range []string{accessACLName, defaultACLName} {
		err := unix.Removexattr(path, name)
		if !errors.Is(err, unix.ENODATA) {
			require.NoError(t, err)
		}
	}
}

func setRootExtendedACL(t *testing.T, path string, ownerUID uint32) {
	t.Helper()
	setTestACL(t, path, encodeTestACL([]testACLEntry{
		{aclUserObject, aclRead | aclWrite | aclExecute, aclUndefinedID},
		{aclNamedUser, aclExecute, ownerUID},
		{aclGroupObject, 0, aclUndefinedID},
		{aclMask, aclExecute, aclUndefinedID},
		{aclOther, aclExecute, aclUndefinedID},
	}))
}

func setRootDefaultACL(t *testing.T, path string, ownerUID uint32) {
	t.Helper()
	err := unix.Setxattr(path, defaultACLName, encodeTestACL([]testACLEntry{
		{aclUserObject, aclRead | aclWrite | aclExecute, aclUndefinedID},
		{aclNamedUser, aclExecute, ownerUID},
		{aclGroupObject, 0, aclUndefinedID},
		{aclMask, aclExecute, aclUndefinedID},
		{aclOther, aclExecute, aclUndefinedID},
	}), 0)
	if errors.Is(err, unix.ENOTSUP) || errors.Is(err, unix.EOPNOTSUPP) {
		t.Skip("test filesystem does not support POSIX ACLs")
	}
	require.NoError(t, err)
}

func setDirectoryACL(t *testing.T, path string, ownerUID uint32, peers ...uint32) {
	t.Helper()
	users := append([]uint32{ownerUID}, peers...)
	sort.Slice(users, func(left, right int) bool { return users[left] < users[right] })
	entries := make([]testACLEntry, 0, 1+len(users)+3)
	entries = append(entries, testACLEntry{aclUserObject, aclRead | aclWrite | aclExecute, aclUndefinedID})
	for _, uid := range users {
		entries = append(entries, testACLEntry{aclNamedUser, aclExecute, uid})
	}
	entries = append(entries,
		testACLEntry{aclGroupObject, 0, aclUndefinedID},
		testACLEntry{aclMask, aclExecute, aclUndefinedID},
		testACLEntry{aclOther, 0, aclUndefinedID},
	)
	setTestACL(t, path, encodeTestACL(entries))
}

func setFileACL(t *testing.T, path string, ownerUID uint32) {
	t.Helper()
	writable := aclRead | aclWrite
	setTestACL(t, path, encodeTestACL([]testACLEntry{
		{aclUserObject, writable, aclUndefinedID},
		{aclNamedUser, writable, ownerUID},
		{aclGroupObject, 0, aclUndefinedID},
		{aclMask, writable, aclUndefinedID},
		{aclOther, 0, aclUndefinedID},
	}))
}

func setTestACL(t *testing.T, path string, raw []byte) {
	t.Helper()
	err := unix.Setxattr(path, accessACLName, raw, 0)
	if errors.Is(err, unix.ENOTSUP) || errors.Is(err, unix.EOPNOTSUPP) {
		t.Skip("test filesystem does not support POSIX ACLs")
	}
	require.NoError(t, err)
}

func encodeTestACL(entries []testACLEntry) []byte {
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

func assertUnprovisioned(t *testing.T, root string, binding PanelBinding) {
	t.Helper()
	guard, outcome, err := acquireAt(t.Context(), root, binding, 0)
	require.Nil(t, guard)
	require.Equal(t, LockUnprovisioned, outcome)
	require.ErrorIs(t, err, ErrUnprovisioned, "error: %v", err)
}
