//go:build linux

package tmuxlock

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"golang.org/x/sys/unix"
)

func (w *NodeWriter) transferPlatform(
	ctx context.Context,
	live LiveBinding,
	source string,
	target string,
) (OwnershipResult, error) {
	parts, err := deriveParts(live.PanelBinding)
	if err != nil {
		return OwnershipResult{}, err
	}
	targetBinding, err := recordBindingForLive(live)
	if err != nil {
		return OwnershipResult{}, err
	}
	tree, err := openTransferPanel(ctx, w.root, w.policy, live, parts, targetBinding, false)
	if err != nil {
		return OwnershipResult{}, err
	}
	defer func() { _ = tree.close() }()
	rawRegister, err := readFixed(tree.register, recordSize)
	if err != nil {
		return OwnershipResult{}, fmt.Errorf("read pane register: %w", err)
	}
	register, err := decodeRegisterRecord(w.policy, rawRegister)
	if err != nil || register.Binding != targetBinding {
		return OwnershipResult{}, fmt.Errorf("decode pane register: %w", ErrWriterInvalid)
	}
	state, stateErr := w.decodeState(tree.state)
	if stateErr != nil {
		return OwnershipResult{}, fmt.Errorf("decode pane state: %w", ErrWriterInvalid)
	}
	if register.Pending != nil {
		pending := register.Pending
		if pending.Operation != operationTransfer || pending.ToOwner != target {
			return OwnershipResult{}, fmt.Errorf("another pane operation is pending: %w", ErrWriterDeferred)
		}
		register, _, err = w.recoverTransfer(tree, register, state, source)
		if err != nil {
			return OwnershipResult{}, err
		}
		return OwnershipResult{Changed: true, Owner: register.Owner, OwnerGen: register.OwnerGen}, nil
	}
	if !recordsCoherent(register, state) {
		return OwnershipResult{}, fmt.Errorf("pane records disagree: %w", ErrWriterInvalid)
	}
	if register.Owner == target {
		return OwnershipResult{Owner: register.Owner, OwnerGen: register.OwnerGen}, nil
	}
	if register.Owner != source {
		return OwnershipResult{}, fmt.Errorf("pane owner is not configured source: %w", ErrWriterDeferred)
	}
	if state.Phase == phaseRunning || state.Phase == phaseUncertain {
		return OwnershipResult{}, fmt.Errorf("pane state cannot originate transfer: %w", ErrWriterInvalid)
	}
	if register.OwnerGen == maxRecordEpoch || state.Gen == maxSafeInteger {
		return OwnershipResult{}, fmt.Errorf("pane generation exhausted: %w", ErrWriterInvalid)
	}
	txID, err := w.newTxID()
	if err != nil {
		return OwnershipResult{}, err
	}
	pending := &pendingRecord{
		FromEpoch: register.Epoch, FromGen: state.Gen, Operation: operationTransfer,
		ToEpoch: register.Epoch, ToOwner: target, ToOwnerGen: register.OwnerGen + 1,
		TxID: txID,
	}
	register.Owner = target
	register.OwnerGen = pending.ToOwnerGen
	register.Pending = pending
	if err := w.writeRegister(tree.register, register, boundaryRegister); err != nil {
		return OwnershipResult{}, err
	}
	register, state, err = w.recoverTransfer(tree, register, state, source)
	if err != nil {
		return OwnershipResult{}, err
	}
	return OwnershipResult{Changed: true, Owner: register.Owner, OwnerGen: register.OwnerGen}, nil
}

func openTransferPanel(
	ctx context.Context,
	rootPath string,
	policy validatedRecordPolicy,
	live LiveBinding,
	parts lockPathParts,
	target recordBinding,
	allowInvalidState bool,
) (*guardTree, error) {
	tree := newGuardTree(rootPath, parts, policy)
	fail := func(cause error) (*guardTree, error) {
		_ = tree.close()
		if errors.Is(cause, context.Canceled) || errors.Is(cause, context.DeadlineExceeded) ||
			errors.Is(cause, ErrWriterDeferred) {
			return nil, cause
		}
		return nil, fmt.Errorf("open pane-lock writer tree: %w: %w", ErrWriterUnprovisioned, cause)
	}
	var err error
	tree.root, err = openCooperativeRoot(rootPath)
	if err != nil {
		return fail(err)
	}
	if err := tree.remember("root", tree.root); err != nil {
		return fail(err)
	}
	tree.enrolled, err = openAtSecure(tree.root, "enrolled-uids",
		unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC)
	if err != nil || validateCooperativeFile(
		tree.enrolled, 0o644, -1, maxEnrolledBytes, fileACLNone, live.OwnerUID,
	) != nil {
		return fail(ErrWriterInvalid)
	}
	rawEnrolled, err := readVariable(tree.enrolled, maxEnrolledBytes)
	if err != nil {
		return fail(err)
	}
	_, enrolled, err := decodeEnrolledIndex(rawEnrolled)
	if err != nil || !slices.Contains(enrolled, live.OwnerUID) {
		return fail(ErrWriterInvalid)
	}
	tree.uidDir, err = openAtSecure(tree.root, parts.owner,
		unix.O_PATH|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC)
	if err != nil || validateCooperativeDirectory(tree.uidDir, live.OwnerUID) != nil {
		return fail(ErrWriterInvalid)
	}
	if err := tree.remember("uid", tree.uidDir); err != nil {
		return fail(err)
	}
	tree.index, err = openAtSecure(tree.uidDir, "index",
		unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC)
	if err != nil || validateCooperativeFile(
		tree.index, 0o640, -1, maxPanelIndexSize, fileACLRead, live.OwnerUID,
	) != nil {
		return fail(ErrWriterInvalid)
	}
	rawIndex, err := readVariable(tree.index, maxPanelIndexSize)
	if err != nil {
		return fail(err)
	}
	_, entries, err := decodePanelIndex(rawIndex)
	entry := panelIndexEntry{Pane: target.Pane, SocketSHA: target.SocketSHA}
	if err != nil || !slices.Contains(entries, entry) {
		return fail(ErrWriterInvalid)
	}
	tree.socketDir, err = openAtSecure(tree.uidDir, parts.hash,
		unix.O_PATH|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC)
	if err != nil || validateCooperativeDirectory(tree.socketDir, live.OwnerUID) != nil {
		return fail(ErrWriterInvalid)
	}
	if err := tree.remember("socket", tree.socketDir); err != nil {
		return fail(err)
	}
	pane := parts.file[:len(parts.file)-len(".lock")]
	for _, file := range []struct {
		name  string
		field *int
		mode  uint32
		size  int64
		acl   fileACLKind
	}{
		{parts.file, &tree.lock, 0o660, 0, fileACLWrite},
		{pane + ".state", &tree.state, 0o660, recordSize, fileACLWrite},
		{pane + ".reg", &tree.register, 0o640, recordSize, fileACLRead},
	} {
		exactSize := file.size
		if allowInvalidState && file.field == &tree.state {
			exactSize = -1
		}
		*file.field, err = openAtSecure(tree.socketDir, file.name,
			unix.O_RDWR|unix.O_NOFOLLOW|unix.O_CLOEXEC)
		if err != nil || validateCooperativeFile(
			*file.field, file.mode, exactSize, 0, file.acl, live.OwnerUID,
		) != nil {
			return fail(ErrWriterInvalid)
		}
		key := "register"
		if file.field == &tree.lock {
			key = "lock"
		} else if file.field == &tree.state {
			key = "state"
		}
		if err := tree.remember(key, *file.field); err != nil {
			return fail(err)
		}
	}
	acquired, err := waitForPanelLock(ctx, tree.lock, 0)
	if err != nil {
		return fail(err)
	}
	if !acquired {
		return fail(ErrWriterDeferred)
	}
	if err := tree.revalidatePanel(); err != nil {
		return fail(err)
	}
	return tree, nil
}

func (w *NodeWriter) recoverTransfer(
	tree *guardTree,
	register registerRecord,
	state stateRecord,
	expectedSource string,
) (registerRecord, stateRecord, error) {
	pending := register.Pending
	if pending == nil || pending.Operation != operationTransfer ||
		register.Owner != pending.ToOwner || register.OwnerGen != pending.ToOwnerGen ||
		register.Epoch != pending.ToEpoch || pending.ToEpoch != pending.FromEpoch ||
		pending.ToOwnerGen == 0 || state.binding() != register.Binding {
		return registerRecord{}, stateRecord{}, fmt.Errorf("invalid pending transfer: %w", ErrWriterInvalid)
	}
	sourceOK := state.Owner != pending.ToOwner && state.OwnerGen+1 == pending.ToOwnerGen
	if expectedSource != "" {
		sourceOK = sourceOK && state.Owner == expectedSource
	}
	origin := sourceOK && state.Epoch == pending.FromEpoch && state.Gen == pending.FromGen &&
		slices.Contains([]string{phaseIdle, phaseDone, phaseDeferred}, state.Phase)
	copyComplete := transferStateComplete(*pending, state)
	if !copyComplete {
		if !origin || pending.FromGen == maxSafeInteger {
			return registerRecord{}, stateRecord{}, fmt.Errorf("pending transfer state mismatch: %w", ErrWriterInvalid)
		}
		state.Attempt = "-"
		state.ChildPID = 0
		state.ChildStart = "0"
		state.Epoch = pending.ToEpoch
		state.Gen = pending.FromGen + 1
		state.Operation = "-"
		state.Owner = pending.ToOwner
		state.OwnerGen = pending.ToOwnerGen
		state.Phase = phaseIdle
		state.Result = "-"
		state.Step = 0
		state.Timestamp = formatRecordTime(w.hooks.now())
		state.TxID = pending.TxID
		state.Writer = operationTransfer
		state.WriterPID = 0
		state.WriterStart = "0"
		if err := w.writeState(tree.state, state); err != nil {
			return registerRecord{}, stateRecord{}, err
		}
	}
	register.Pending = nil
	if err := w.writeRegister(tree.register, register, boundaryClear); err != nil {
		return registerRecord{}, stateRecord{}, err
	}
	return register, state, nil
}

func (w *NodeWriter) writeRegister(fd int, record registerRecord, boundary writerBoundary) error {
	raw, err := encodeRegisterRecord(w.policy, record)
	if err != nil {
		return fmt.Errorf("encode pane register: %w", err)
	}
	if err := durableWrite(fd, raw); err != nil {
		return fmt.Errorf("write pane register: %w", err)
	}
	return w.crossed(boundary)
}

func (w *NodeWriter) writeState(fd int, record stateRecord) error {
	raw, err := encodeStateRecord(w.policy, record)
	if err != nil {
		return fmt.Errorf("encode pane state: %w", err)
	}
	if err := durableWrite(fd, raw); err != nil {
		return fmt.Errorf("write pane state: %w", err)
	}
	return w.crossed(boundaryState)
}

func (w *NodeWriter) decodeState(fd int) (stateRecord, error) {
	raw, err := readFixed(fd, recordSize)
	if err != nil {
		return stateRecord{}, err
	}
	return decodeStateRecord(w.policy, raw)
}

func recordsCoherent(register registerRecord, state stateRecord) bool {
	return register.Pending == nil && register.Binding == state.binding() &&
		register.Owner == state.Owner && register.OwnerGen == state.OwnerGen &&
		register.Epoch == state.Epoch
}

func durableWrite(fd int, raw []byte) error {
	for offset := 0; offset < len(raw); {
		written, err := unix.Pwrite(fd, raw[offset:], int64(offset))
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			return fmt.Errorf("write pane-lock file: %w", err)
		}
		if written <= 0 {
			return errors.New("pane-lock pwrite made no progress")
		}
		offset += written
	}
	if err := unix.Ftruncate(fd, int64(len(raw))); err != nil {
		return fmt.Errorf("truncate pane-lock file: %w", err)
	}
	if err := unix.Fdatasync(fd); err != nil {
		return fmt.Errorf("sync pane-lock file: %w", err)
	}
	return nil
}
