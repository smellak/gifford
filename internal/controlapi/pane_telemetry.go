package controlapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"log/slog"

	"github.com/smellak/gifford/internal/protocol"
)

const paneOperationEvent = "pane_admin_operation"

var errPaneOperationTelemetry = errors.New("invalid pane operation telemetry")

// PaneAdminOperation is the closed administrative operation vocabulary used
// by node telemetry.
type PaneAdminOperation string

// Administrative pane operations.
const (
	PaneAdminProvision PaneAdminOperation = "provision"
	PaneAdminTake      PaneAdminOperation = "take"
	PaneAdminReturn    PaneAdminOperation = "return"
	PaneAdminReconcile PaneAdminOperation = "reconcile"
	PaneAdminRepair    PaneAdminOperation = "repair"
)

// PaneOperationLogResult is the closed content-free result vocabulary.
type PaneOperationLogResult string

// Pane operation telemetry results.
const (
	PaneLogDenied    PaneOperationLogResult = "denied"
	PaneLogChanged   PaneOperationLogResult = "changed"
	PaneLogUnchanged PaneOperationLogResult = "unchanged"
	PaneLogFailed    PaneOperationLogResult = "failed"
	PaneLogConfirmed PaneOperationLogResult = "confirmed"
	PaneLogNoEffect  PaneOperationLogResult = "no_effect"
	PaneLogRepaired  PaneOperationLogResult = "repaired"
)

// PaneEvidenceVerdict preserves yes, no, and unavailable as distinct values.
type PaneEvidenceVerdict string

// Pane evidence verdicts.
const (
	PaneEvidenceUnknown PaneEvidenceVerdict = "unknown"
	PaneEvidenceYes     PaneEvidenceVerdict = "yes"
	PaneEvidenceNo      PaneEvidenceVerdict = "no"
)

// PaneRecordEvidence is the only telemetry permitted for an unreadable or
// corrupt record. It deliberately cannot carry record or screen bytes.
type PaneRecordEvidence struct {
	Size          int64
	MagicValid    PaneEvidenceVerdict
	ChecksumValid PaneEvidenceVerdict
	Version       *int
}

// PaneOperationEvidence is content-free recovery evidence. Nil evidence is
// used by provision and ownership transfers.
type PaneOperationEvidence struct {
	WriterExited  PaneEvidenceVerdict
	ChildExited   PaneEvidenceVerdict
	BindingEqual  PaneEvidenceVerdict
	ComposerEmpty PaneEvidenceVerdict
	LineVisible   PaneEvidenceVerdict
	Record        *PaneRecordEvidence
}

// PaneOperationTelemetryEvent is one structured administrative audit event.
type PaneOperationTelemetryEvent struct {
	Operation PaneAdminOperation
	Actor     PaneOperationActor
	Target    protocol.PaneOperationTarget
	Attempt   protocol.PaneAttemptID
	Result    PaneOperationLogResult
	Evidence  *PaneOperationEvidence
}

// PaneOperationTelemetry writes structured events through the node logger.
// Nil Logger selects slog.Default, whose service stderr is collected by the
// node journal.
type PaneOperationTelemetry struct {
	logger *slog.Logger
}

// NewPaneOperationTelemetry constructs the common pane-operation logger.
func NewPaneOperationTelemetry(logger *slog.Logger) *PaneOperationTelemetry {
	if logger == nil {
		logger = slog.Default()
	}
	return &PaneOperationTelemetry{logger: logger}
}

// Record validates and writes one content-free structured event.
func (t *PaneOperationTelemetry) Record(
	ctx context.Context,
	event PaneOperationTelemetryEvent,
) error {
	if t == nil || t.logger == nil || ctx == nil || !event.Operation.valid() ||
		event.Actor.PID < 0 || event.Target.Validate() != nil || !event.Attempt.Valid() ||
		!event.Result.valid() || !event.Evidence.valid() {
		return errPaneOperationTelemetry
	}
	digest := sha256.Sum256([]byte(event.Target.Socket))
	attrs := []slog.Attr{
		slog.String("event", paneOperationEvent),
		slog.String("op", string(event.Operation)),
		slog.Group("actor",
			slog.Uint64("uid", uint64(event.Actor.UID)),
			slog.Int("pid", event.Actor.PID),
		),
		slog.Group("target",
			slog.Uint64("uid", uint64(event.Target.OwnerUID)),
			slog.String("socket_sha", hex.EncodeToString(digest[:])),
			slog.String("pane", string(event.Target.PaneID)),
		),
		slog.String("attempt", string(event.Attempt)),
		slog.String("result", string(event.Result)),
	}
	if event.Evidence != nil {
		attrs = append(attrs, slog.Attr{Key: "evidence", Value: event.Evidence.logValue()})
	}
	t.logger.LogAttrs(ctx, slog.LevelInfo, "Pane administrative operation", attrs...)
	return nil
}

func (operation PaneAdminOperation) valid() bool {
	switch operation {
	case PaneAdminProvision, PaneAdminTake, PaneAdminReturn, PaneAdminReconcile, PaneAdminRepair:
		return true
	default:
		return false
	}
}

func (result PaneOperationLogResult) valid() bool {
	switch result {
	case PaneLogDenied, PaneLogChanged, PaneLogUnchanged, PaneLogFailed,
		PaneLogConfirmed, PaneLogNoEffect, PaneLogRepaired:
		return true
	default:
		return false
	}
}

func (verdict PaneEvidenceVerdict) valid() bool {
	return verdict == PaneEvidenceUnknown || verdict == PaneEvidenceYes || verdict == PaneEvidenceNo
}

func (evidence *PaneOperationEvidence) valid() bool {
	if evidence == nil {
		return true
	}
	if !evidence.WriterExited.valid() || !evidence.ChildExited.valid() ||
		!evidence.BindingEqual.valid() || !evidence.ComposerEmpty.valid() ||
		!evidence.LineVisible.valid() {
		return false
	}
	if evidence.Record == nil {
		return true
	}
	return evidence.Record.Size >= 0 && evidence.Record.MagicValid.valid() &&
		evidence.Record.ChecksumValid.valid() &&
		(evidence.Record.Version == nil || *evidence.Record.Version >= 0)
}

func (evidence PaneOperationEvidence) logValue() slog.Value {
	attrs := []slog.Attr{
		slog.String("writer_exited", string(evidence.WriterExited)),
		slog.String("child_exited", string(evidence.ChildExited)),
		slog.String("binding_equal", string(evidence.BindingEqual)),
		slog.String("composer_empty", string(evidence.ComposerEmpty)),
		slog.String("line_visible", string(evidence.LineVisible)),
	}
	if evidence.Record != nil {
		recordAttrs := []slog.Attr{
			slog.Int64("st_size", evidence.Record.Size),
			slog.String("magic_valid", string(evidence.Record.MagicValid)),
			slog.String("checksum_valid", string(evidence.Record.ChecksumValid)),
		}
		if evidence.Record.Version != nil {
			recordAttrs = append(recordAttrs, slog.Int("version", *evidence.Record.Version))
		}
		attrs = append(attrs, slog.Attr{Key: "record", Value: slog.GroupValue(recordAttrs...)})
	}
	return slog.GroupValue(attrs...)
}
