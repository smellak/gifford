//go:build linux

package tmuxlock

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"golang.org/x/sys/unix"
)

type lockedRecoveryStore struct {
	ctx     context.Context
	writer  *NodeWriter
	tree    *guardTree
	target  recordBinding
	binding EvidenceVerdict
	record  *RecordEvidence
}

func (w *NodeWriter) reconcilePlatform(
	ctx context.Context,
	live LiveBinding,
	expectedAttempt string,
	observe ReconcileObserver,
) (RecoveryResult, error) {
	result := emptyRecoveryResult()
	store, closeStore, err := w.openRecoveryStore(ctx, live, false)
	if err != nil {
		return result, err
	}
	defer closeStore()
	var observation ReconcileObservation
	observed := false
	outcome, err := reconcilePane(store, expectedAttempt, func(state stateRecord) (reconcileEvidence, error) {
		snapshot, snapshotErr := reconcileSnapshot(state)
		if snapshotErr != nil {
			return reconcileEvidence{}, snapshotErr
		}
		observation, snapshotErr = observe(ctx, snapshot)
		if snapshotErr != nil {
			return reconcileEvidence{}, snapshotErr
		}
		observed = true
		return reconcileEvidence{
			BindingMatches:   observation.BindingEqual == EvidenceYes,
			EffectVisible:    observation.LineVisible == EvidenceYes,
			ComposerEmpty:    observation.ComposerEmpty == EvidenceYes,
			NoticeAbsent:     observation.NoticeAbsent == EvidenceYes,
			WriterExited:     observation.WriterExited == EvidenceYes,
			ChildExited:      observation.ChildExited == EvidenceYes,
			DrainWaitElapsed: observation.DrainWaitElapsed == EvidenceYes,
		}, nil
	}, w.hooks.now())
	result.Evidence.BindingEqual = store.binding
	if observed {
		result.Evidence.WriterExited = observation.WriterExited
		result.Evidence.ChildExited = observation.ChildExited
		result.Evidence.BindingEqual = observation.BindingEqual
		result.Evidence.ComposerEmpty = observation.ComposerEmpty
		result.Evidence.LineVisible = observation.LineVisible
	}
	if err != nil {
		return result, publicRecoveryError(operationReconcile, err)
	}
	switch outcome {
	case reconcileConfirmed:
		result.Changed, result.Outcome = true, RecoveryConfirmed
	case reconcileNoEffect:
		result.Changed, result.Outcome = true, RecoveryNoEffect
	case reconcileUnchanged:
		result.Outcome = RecoveryUnchanged
	}
	return result, nil
}

func (w *NodeWriter) repairPlatform(
	ctx context.Context,
	live LiveBinding,
) (RecoveryResult, error) {
	result := emptyRecoveryResult()
	store, closeStore, err := w.openRecoveryStore(ctx, live, true)
	if err != nil {
		return result, err
	}
	defer closeStore()
	txID, err := w.newTxID()
	if err != nil {
		return result, err
	}
	outcome, err := repairState(store, txID, w.hooks.now())
	result.Evidence.BindingEqual = store.binding
	result.Evidence.Record = cloneRecordEvidence(store.record)
	if err != nil {
		return result, publicRecoveryError("repair", err)
	}
	if outcome == repairApplied {
		result.Changed, result.Outcome = true, RecoveryRepaired
	}
	return result, nil
}

func (w *NodeWriter) openRecoveryStore(
	ctx context.Context,
	live LiveBinding,
	allowInvalidState bool,
) (*lockedRecoveryStore, func(), error) {
	parts, err := deriveParts(live.PanelBinding)
	if err != nil {
		return nil, func() {}, err
	}
	target, err := recordBindingForLive(live)
	if err != nil {
		return nil, func() {}, err
	}
	tree, err := openTransferPanel(
		ctx, w.root, w.policy, live, parts, target, allowInvalidState,
	)
	if err != nil {
		return nil, func() {}, err
	}
	store := &lockedRecoveryStore{
		ctx: ctx, writer: w, tree: tree, target: target, binding: EvidenceUnknown,
	}
	return store, func() { _ = tree.close() }, nil
}

func (s *lockedRecoveryStore) ReadRegister() (registerRecord, error) {
	raw, err := readFixed(s.tree.register, recordSize)
	if err != nil {
		return registerRecord{}, err
	}
	record, err := decodeRegisterRecord(s.writer.policy, raw)
	if err != nil {
		return registerRecord{}, err
	}
	s.binding = evidenceVerdict(record.Binding == s.target)
	if record.Binding != s.target {
		return registerRecord{}, errInvalidRecord
	}
	return record, nil
}

func (s *lockedRecoveryStore) ReadState() (stateRecord, error) {
	record, evidence, err := readStateEvidence(s.tree.state, s.writer.policy)
	s.record = evidence
	return record, err
}

func (s *lockedRecoveryStore) WriteRegister(record registerRecord) error {
	if err := s.prepareWrite(); err != nil {
		return err
	}
	boundary := boundaryClear
	if record.Pending != nil {
		boundary = boundaryRegister
	}
	return s.writer.writeRegister(s.tree.register, record, boundary)
}

func (s *lockedRecoveryStore) WriteState(record stateRecord) error {
	if err := s.prepareWrite(); err != nil {
		return err
	}
	return s.writer.writeState(s.tree.state, record)
}

func (s *lockedRecoveryStore) prepareWrite() error {
	if err := s.ctx.Err(); err != nil {
		return fmt.Errorf("pane recovery context: %w", err)
	}
	if err := s.writer.requireMode(); err != nil {
		return err
	}
	if err := s.tree.revalidatePanel(); err != nil {
		return fmt.Errorf("revalidate pane-lock files: %w", ErrWriterInvalid)
	}
	return nil
}

func reconcileSnapshot(state stateRecord) (ReconcileSnapshot, error) {
	writerStart, err := strconv.ParseUint(state.WriterStart, 10, 64)
	if err != nil {
		return ReconcileSnapshot{}, errInvalidRecord
	}
	childStart, err := strconv.ParseUint(state.ChildStart, 10, 64)
	if err != nil {
		return ReconcileSnapshot{}, errInvalidRecord
	}
	recordedAt, err := time.Parse("2006-01-02T15:04:05.000000000Z", state.Timestamp)
	if err != nil {
		return ReconcileSnapshot{}, errInvalidRecord
	}
	return ReconcileSnapshot{
		Writer:     RecoveryProcess{PID: state.WriterPID, Start: writerStart},
		Child:      RecoveryProcess{PID: state.ChildPID, Start: childStart},
		RecordedAt: recordedAt,
	}, nil
}

func readStateEvidence(
	fd int,
	policy validatedRecordPolicy,
) (stateRecord, *RecordEvidence, error) {
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return stateRecord{}, nil, fmt.Errorf("stat pane state: %w", err)
	}
	evidence := &RecordEvidence{
		Size: stat.Size, MagicValid: EvidenceNo, ChecksumValid: EvidenceNo,
	}
	if stat.Size != recordSize {
		if stat.Size >= int64(len(stateMagic)) {
			prefix, err := readFixed(fd, len(stateMagic))
			if err != nil {
				evidence.MagicValid = EvidenceUnknown
				return stateRecord{}, evidence, errInvalidRecord
			}
			evidence.MagicValid = evidenceVerdict(string(prefix) == stateMagic)
		}
		return stateRecord{}, evidence, errInvalidRecord
	}
	raw, err := readFixed(fd, recordSize)
	if err != nil {
		evidence.MagicValid = EvidenceUnknown
		evidence.ChecksumValid = EvidenceUnknown
		return stateRecord{}, evidence, err
	}
	evidence.MagicValid = evidenceVerdict(string(raw[:len(stateMagic)]) == stateMagic)
	evidence.ChecksumValid = evidenceVerdict(validRecordChecksum(raw))
	if evidence.MagicValid == EvidenceYes && evidence.ChecksumValid == EvidenceYes {
		evidence.Version = recordVersion(raw)
	}
	record, err := decodeStateRecord(policy, raw)
	return record, evidence, err
}

func validRecordChecksum(raw []byte) bool {
	if len(raw) != recordSize ||
		!bytes.Equal(raw[1016:1023], bytes.Repeat([]byte{' '}, 7)) || raw[1023] != '\n' {
		return false
	}
	digest := sha256.Sum256(raw[:recordBodyEnd])
	want := make([]byte, hex.EncodedLen(len(digest)))
	hex.Encode(want, digest[:])
	return bytes.Equal(raw[recordBodyEnd:1016], want)
}

func recordVersion(raw []byte) *int {
	body, err := decodeRecord(stateMagic, raw)
	if err != nil {
		return nil
	}
	var header struct {
		Version int `json:"v"`
	}
	if err := json.Unmarshal(body, &header); err != nil {
		return nil
	}
	version := header.Version
	return &version
}

func cloneRecordEvidence(value *RecordEvidence) *RecordEvidence {
	if value == nil {
		return nil
	}
	clone := *value
	if value.Version != nil {
		version := *value.Version
		clone.Version = &version
	}
	return &clone
}

var (
	_ reconcileRecordStore = (*lockedRecoveryStore)(nil)
	_ repairRecordStore    = (*lockedRecoveryStore)(nil)
)
