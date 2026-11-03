//go:build linux

package tmuxlock

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"golang.org/x/sys/unix"
)

type inodeIdentity struct {
	device uint64
	inode  uint64
}

type guardTree struct {
	rootPath  string
	parts     lockPathParts
	policy    validatedRecordPolicy
	root      int
	enrolled  int
	uidDir    int
	index     int
	socketDir int
	lock      int
	state     int
	register  int
	ids       map[string]inodeIdentity
}

func newGuardTree(root string, parts lockPathParts, policy validatedRecordPolicy) *guardTree {
	return &guardTree{
		rootPath: root, parts: parts, policy: policy,
		root: -1, enrolled: -1, uidDir: -1, index: -1,
		socketDir: -1, lock: -1, state: -1, register: -1,
		ids: make(map[string]inodeIdentity),
	}
}

func acquireOwnedAt(
	ctx context.Context,
	root string,
	policy validatedRecordPolicy,
	live LiveBinding,
	wait time.Duration,
) (*PanelGuard, LockOutcome, error) {
	parts, err := deriveParts(live.PanelBinding)
	if err != nil {
		return nil, LockInvalid, err
	}
	target, err := recordBindingForLive(live)
	if err != nil {
		return nil, LockInvalid, err
	}
	tree := newGuardTree(root, parts, policy)
	fail := func(outcome LockOutcome, cause error) (*PanelGuard, LockOutcome, error) {
		_ = tree.close()
		return nil, outcome, cause
	}
	tree.root, err = openCooperativeRoot(root)
	if err != nil {
		return fail(LockInvalid, guardInvalid(GuardInventoryInvalid))
	}
	if err := tree.remember("root", tree.root); err != nil {
		return fail(LockInvalid, guardInvalid(GuardInventoryInvalid))
	}
	tree.enrolled, err = openAtSecure(tree.root, "enrolled-uids",
		unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC)
	if err != nil || validateCooperativeFile(
		tree.enrolled, 0o644, -1, maxEnrolledBytes, fileACLNone, live.OwnerUID,
	) != nil {
		return fail(LockInvalid, guardInvalid(GuardInventoryInvalid))
	}
	rawEnrolled, err := readVariable(tree.enrolled, maxEnrolledBytes)
	if err != nil {
		return fail(LockInvalid, guardInvalid(GuardInventoryInvalid))
	}
	_, enrolled, err := decodeEnrolledIndex(rawEnrolled)
	if err != nil {
		return fail(LockInvalid, guardInvalid(GuardInventoryInvalid))
	}
	if !slices.Contains(enrolled, live.OwnerUID) {
		return fail(LockDeferred, guardDeferred(GuardNotIncorporated))
	}
	tree.uidDir, err = openAtSecure(tree.root, parts.owner,
		unix.O_PATH|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC)
	if err != nil || validateCooperativeDirectory(tree.uidDir, live.OwnerUID) != nil {
		return fail(LockInvalid, guardInvalid(GuardInventoryInvalid))
	}
	if err := tree.remember("uid", tree.uidDir); err != nil {
		return fail(LockInvalid, guardInvalid(GuardInventoryInvalid))
	}
	tree.index, err = openAtSecure(tree.uidDir, "index",
		unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC)
	if err != nil || validateCooperativeFile(
		tree.index, 0o640, -1, maxPanelIndexSize, fileACLRead, live.OwnerUID,
	) != nil {
		return fail(LockInvalid, guardInvalid(GuardInventoryInvalid))
	}
	rawIndex, err := readVariable(tree.index, maxPanelIndexSize)
	if err != nil {
		return fail(LockInvalid, guardInvalid(GuardInventoryInvalid))
	}
	_, entries, err := decodePanelIndex(rawIndex)
	if err != nil {
		return fail(LockInvalid, guardInvalid(GuardInventoryInvalid))
	}
	entry := panelIndexEntry{Pane: target.Pane, SocketSHA: target.SocketSHA}
	if !slices.Contains(entries, entry) {
		return fail(LockDeferred, guardDeferred(GuardNotIncorporated))
	}
	tree.socketDir, err = openAtSecure(tree.uidDir, parts.hash,
		unix.O_PATH|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC)
	if err != nil || validateCooperativeDirectory(tree.socketDir, live.OwnerUID) != nil {
		return fail(LockInvalid, guardInvalid(GuardPanelInvalid))
	}
	if err := tree.remember("socket", tree.socketDir); err != nil {
		return fail(LockInvalid, guardInvalid(GuardPanelInvalid))
	}
	tree.lock, err = openAtSecure(tree.socketDir, parts.file,
		unix.O_RDWR|unix.O_NOFOLLOW|unix.O_CLOEXEC)
	if err != nil || validateCooperativeFile(
		tree.lock, 0o660, 0, 0, fileACLWrite, live.OwnerUID,
	) != nil {
		return fail(LockInvalid, guardInvalid(GuardPanelInvalid))
	}
	if err := tree.remember("lock", tree.lock); err != nil {
		return fail(LockInvalid, guardInvalid(GuardPanelInvalid))
	}
	acquired, err := waitForPanelLock(ctx, tree.lock, wait)
	if err != nil {
		return fail(LockDeferred, err)
	}
	if !acquired {
		return fail(LockDeferred, nil)
	}
	pane := parts.file[:len(parts.file)-len(".lock")]
	tree.state, err = openAtSecure(tree.socketDir, pane+".state",
		unix.O_RDWR|unix.O_NOFOLLOW|unix.O_CLOEXEC)
	if err != nil || validateCooperativeFile(
		tree.state, 0o660, recordSize, 0, fileACLWrite, live.OwnerUID,
	) != nil {
		return fail(LockInvalid, guardInvalid(GuardPanelInvalid))
	}
	if err := tree.remember("state", tree.state); err != nil {
		return fail(LockInvalid, guardInvalid(GuardPanelInvalid))
	}
	tree.register, err = openAtSecure(tree.socketDir, pane+".reg",
		unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC)
	if err != nil || validateCooperativeFile(
		tree.register, 0o640, recordSize, 0, fileACLRead, live.OwnerUID,
	) != nil {
		return fail(LockInvalid, guardInvalid(GuardPanelInvalid))
	}
	if err := tree.remember("register", tree.register); err != nil {
		return fail(LockInvalid, guardInvalid(GuardPanelInvalid))
	}
	if err := tree.revalidatePanel(); err != nil {
		return fail(LockInvalid, guardInvalid(GuardPanelInvalid))
	}
	identity := tree.ids["lock"]
	return &PanelGuard{
		fd: tree.lock, device: identity.device, inode: identity.inode,
		close: tree.close,
		check: func(checkCtx context.Context, current LiveBinding) error {
			return validateOwnedRecords(checkCtx, tree, current)
		},
	}, LockAcquired, nil
}

func validateOwnedRecords(ctx context.Context, tree *guardTree, live LiveBinding) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("validate pane-lock context: %w", err)
	}
	if err := validateLiveBinding(live); err != nil {
		return guardInvalid(GuardBindingMismatch)
	}
	if err := tree.revalidatePanel(); err != nil {
		return guardInvalid(GuardPanelInvalid)
	}
	target, err := recordBindingForLive(live)
	if err != nil {
		return guardInvalid(GuardBindingMismatch)
	}
	rawRegister, err := readFixed(tree.register, recordSize)
	if err != nil {
		return guardInvalid(GuardRegisterInvalid)
	}
	register, err := decodeRegisterRecord(tree.policy, rawRegister)
	if err != nil {
		return guardInvalid(GuardRegisterInvalid)
	}
	rawState, err := readFixed(tree.state, recordSize)
	if err != nil {
		return guardInvalid(GuardStateInvalid)
	}
	state, err := decodeStateRecord(tree.policy, rawState)
	if err != nil {
		return guardInvalid(GuardStateInvalid)
	}
	if register.Pending != nil {
		if !pendingStateCoherent(register, state) {
			return guardInvalid(GuardOwnerCopyMismatch)
		}
		return guardDeferred(GuardPending)
	}
	if register.Owner != state.Owner || register.OwnerGen != state.OwnerGen ||
		register.Epoch != state.Epoch {
		return guardInvalid(GuardOwnerCopyMismatch)
	}
	if register.Binding != state.binding() {
		return guardInvalid(GuardStateBindingMismatch)
	}
	if register.Binding != target {
		return guardInvalid(GuardBindingMismatch)
	}
	if register.Owner != nodeOwner {
		return guardDeferred(GuardForeignOwner)
	}
	if state.Phase == phaseRunning || state.Phase == phaseUncertain {
		return guardDeferred(GuardPhaseBlocked)
	}
	if state.Gen > maxSafeInteger-generationHeadroom {
		return guardInvalid(GuardGenerationExhausted)
	}
	return nil
}

func pendingStateCoherent(register registerRecord, state stateRecord) bool {
	pending := register.Pending
	if pending == nil || register.Owner != pending.ToOwner ||
		register.OwnerGen != pending.ToOwnerGen || register.Epoch != pending.ToEpoch {
		return false
	}
	originPhase := slices.Contains([]string{phaseIdle, phaseDone, phaseDeferred}, state.Phase)
	switch pending.Operation {
	case operationTransfer:
		if pending.ToEpoch != pending.FromEpoch || state.binding() != register.Binding {
			return false
		}
		origin := state.Epoch == pending.FromEpoch && state.Gen == pending.FromGen &&
			state.Owner != pending.ToOwner && state.OwnerGen+1 == pending.ToOwnerGen && originPhase
		complete := transferStateComplete(*pending, state)
		return origin || complete
	case operationRepair:
		if pending.ToEpoch != pending.FromEpoch+1 || state.Owner != pending.ToOwner ||
			state.OwnerGen != pending.ToOwnerGen {
			return false
		}
		origin := state.Epoch == pending.FromEpoch && state.Gen == pending.FromGen && originPhase
		complete := state.binding() == register.Binding && state.Epoch == pending.ToEpoch &&
			state.Gen == 0 && state.TxID == pending.TxID && state.Phase == phaseUncertain &&
			state.Result == resultRepaired && state.Step == -1
		return origin || complete
	default:
		return false
	}
}

func transferStateComplete(pending pendingRecord, state stateRecord) bool {
	return state.Epoch == pending.ToEpoch && state.Gen == pending.FromGen+1 &&
		state.Owner == pending.ToOwner && state.OwnerGen == pending.ToOwnerGen &&
		state.TxID == pending.TxID && state.Phase == phaseIdle && state.Attempt == "-" &&
		state.Operation == "-" && state.Result == "-" && state.Step == 0 &&
		state.Writer == operationTransfer && state.WriterPID == 0 && state.WriterStart == "0" &&
		state.ChildPID == 0 && state.ChildStart == "0"
}

func openCooperativeRoot(root string) (int, error) {
	components, err := managedRootComponents(root)
	if err != nil {
		return -1, err
	}
	current, err := unix.Open("/", unix.O_PATH|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
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
		flags := uint64(unix.O_PATH | unix.O_DIRECTORY | unix.O_NOFOLLOW | unix.O_CLOEXEC)
		if index == len(components)-1 {
			flags = unix.O_RDONLY | unix.O_DIRECTORY | unix.O_NOFOLLOW | unix.O_CLOEXEC
		}
		next, openErr := openAtSecure(current, component, flags)
		_ = unix.Close(current)
		if openErr != nil {
			return -1, openErr
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

func requireProtocolModePlatform(
	modePath string,
	rootPath string,
	expectedUID uint32,
	expectedGID uint32,
) error {
	components, err := managedRootComponents(modePath)
	if err != nil {
		return errInvalidRecord
	}
	current, err := unix.Open("/", unix.O_PATH|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("open mode path root: %w", err)
	}
	for index, component := range components {
		if index == len(components)-1 {
			break
		}
		next, openErr := openAtSecure(current, component,
			unix.O_PATH|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC)
		_ = unix.Close(current)
		if openErr != nil {
			return errInvalidRecord
		}
		current = next
	}
	fd, err := openAtSecure(current, components[len(components)-1],
		unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC)
	_ = unix.Close(current)
	if err != nil {
		return errInvalidRecord
	}
	defer func() { _ = unix.Close(fd) }()
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return fmt.Errorf("stat pane-lock mode file: %w", err)
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Uid != expectedUID ||
		stat.Gid != expectedGID || stat.Mode&0o7777 != 0o644 || stat.Nlink != 1 || stat.Size != 9 {
		return errInvalidRecord
	}
	if err := validateNoAccessACL(fd); err != nil {
		return errInvalidRecord
	}
	raw, err := readFixed(fd, 9)
	if err != nil || !bytes.Equal(raw, []byte("required\n")) {
		return errInvalidRecord
	}
	root, err := openCooperativeRoot(rootPath)
	if err != nil {
		return errInvalidRecord
	}
	if err := unix.Close(root); err != nil {
		return fmt.Errorf("close pane-lock root: %w", err)
	}
	return nil
}

func validateCooperativeDirectory(fd int, ownerUID uint32) error {
	if err := validateManagedDirectory(fd, ownerUID, effectiveUID()); err != nil {
		return err
	}
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return fmt.Errorf("stat managed directory: %w", err)
	}
	if stat.Gid != effectiveGID() {
		return errors.New("managed directory group mismatch")
	}
	return validateNoDefaultACL(fd)
}

type fileACLKind uint8

const (
	fileACLNone fileACLKind = iota
	fileACLRead
	fileACLWrite
)

func validateCooperativeFile(
	fd int,
	mode uint32,
	exactSize int64,
	maxSize int64,
	acl fileACLKind,
	ownerUID uint32,
) error {
	if fd < 0 {
		return errors.New("pane-lock file is unavailable")
	}
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return fmt.Errorf("stat pane-lock file: %w", err)
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Uid != effectiveUID() ||
		stat.Gid != effectiveGID() || stat.Mode&0o7777 != mode || stat.Nlink != 1 ||
		exactSize >= 0 && stat.Size != exactSize || maxSize > 0 && stat.Size > maxSize {
		return errors.New("unsafe pane-lock file attributes")
	}
	switch acl {
	case fileACLNone:
		return validateNoAccessACL(fd)
	case fileACLRead:
		return validateReadFileACL(fd, ownerUID)
	case fileACLWrite:
		return validateFileACL(fd, ownerUID)
	default:
		return errors.New("unknown pane-lock ACL kind")
	}
}

func (tree *guardTree) remember(key string, fd int) error {
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return fmt.Errorf("stat pane-lock inode: %w", err)
	}
	tree.ids[key] = inodeIdentity{device: stat.Dev, inode: stat.Ino}
	return nil
}

func (tree *guardTree) close() error {
	if tree == nil {
		return nil
	}
	var errs []error
	if tree.lock >= 0 {
		if err := unix.Flock(tree.lock, unix.LOCK_UN); err != nil {
			errs = append(errs, fmt.Errorf("unlock pane lock: %w", err))
		}
	}
	for _, fd := range []int{
		tree.register, tree.state, tree.lock, tree.socketDir,
		tree.index, tree.uidDir, tree.enrolled, tree.root,
	} {
		if fd >= 0 {
			if err := unix.Close(fd); err != nil {
				errs = append(errs, fmt.Errorf("close pane-lock descriptor: %w", err))
			}
		}
	}
	tree.register, tree.state, tree.lock = -1, -1, -1
	return errors.Join(errs...)
}

func (tree *guardTree) revalidatePanel() error {
	root, err := openCooperativeRoot(tree.rootPath)
	if err != nil {
		return err
	}
	defer func() { _ = unix.Close(root) }()
	if err := compareIdentity(root, tree.ids["root"]); err != nil {
		return err
	}
	type identityCheck struct {
		parent int
		name   string
		flags  uint64
		key    string
	}
	checks := make([]identityCheck, 0, 5)
	checks = append(checks,
		identityCheck{root, tree.parts.owner, unix.O_PATH | unix.O_DIRECTORY | unix.O_NOFOLLOW | unix.O_CLOEXEC, "uid"},
		identityCheck{tree.uidDir, tree.parts.hash, unix.O_PATH | unix.O_DIRECTORY | unix.O_NOFOLLOW | unix.O_CLOEXEC, "socket"},
		identityCheck{tree.socketDir, tree.parts.file, unix.O_RDWR | unix.O_NOFOLLOW | unix.O_CLOEXEC, "lock"},
	)
	pane := tree.parts.file[:len(tree.parts.file)-len(".lock")]
	checks = append(checks,
		identityCheck{tree.socketDir, pane + ".state", unix.O_RDONLY | unix.O_NOFOLLOW | unix.O_CLOEXEC, "state"},
		identityCheck{tree.socketDir, pane + ".reg", unix.O_RDONLY | unix.O_NOFOLLOW | unix.O_CLOEXEC, "register"},
	)
	for _, check := range checks {
		fd, err := openAtSecure(check.parent, check.name, check.flags)
		if err != nil {
			return err
		}
		err = compareIdentity(fd, tree.ids[check.key])
		_ = unix.Close(fd)
		if err != nil {
			return err
		}
	}
	return nil
}

func compareIdentity(fd int, expected inodeIdentity) error {
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return fmt.Errorf("stat pane-lock identity: %w", err)
	}
	if stat.Dev != expected.device || stat.Ino != expected.inode {
		return errors.New("pane-lock path changed inode")
	}
	return nil
}

func readVariable(fd int, limit int) ([]byte, error) {
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return nil, fmt.Errorf("stat pane-lock file: %w", err)
	}
	if stat.Size < 0 || stat.Size > int64(limit) {
		return nil, errInvalidRecord
	}
	return readFixed(fd, int(stat.Size))
}

func readFixed(fd int, size int) ([]byte, error) {
	raw := make([]byte, size)
	for offset := 0; offset < len(raw); {
		read, err := unix.Pread(fd, raw[offset:], int64(offset))
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("read pane-lock file: %w", err)
		}
		if read == 0 {
			return nil, errors.New("unexpected end of pane-lock file")
		}
		offset += read
	}
	return raw, nil
}
