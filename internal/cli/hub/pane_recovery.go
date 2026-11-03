package hub

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/charmbracelet/x/ansi"

	"github.com/smellak/gifford/internal/controlapi"
	"github.com/smellak/gifford/internal/detect"
	"github.com/smellak/gifford/internal/procguard"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy/tmuxattach"
	"github.com/smellak/gifford/internal/tmuxinstance"
	"github.com/smellak/gifford/internal/tmuxlock"
)

var errPaneRecoveryConfig = errors.New("invalid pane recovery service configuration")

type paneRecoveryWriter interface {
	Reconcile(
		context.Context,
		tmuxlock.LiveBinding,
		string,
		tmuxlock.ReconcileObserver,
	) (tmuxlock.RecoveryResult, error)
	Repair(context.Context, tmuxlock.LiveBinding) (tmuxlock.RecoveryResult, error)
}

type paneRecoveryRegistry interface {
	ListActive(context.Context) ([]tmuxinstance.Instance, error)
}

type paneRecoveryTransport interface {
	Capture(context.Context, tmuxattach.Target) ([]byte, error)
	ObservePane(context.Context, tmuxattach.Target) (tmuxattach.PaneObservation, error)
}

type paneRecoveryClassifier func(
	detect.Harness,
	detect.Table,
	[]byte,
	int,
	int,
	int,
	[]detect.Process,
) detect.State

type paneRecoveryServiceOptions struct {
	Writer        paneRecoveryWriter
	Registry      paneRecoveryRegistry
	Transport     paneRecoveryTransport
	Clock         paneOwnershipClock
	Drain         time.Duration
	Telemetry     *controlapi.PaneOperationTelemetry
	DetectorTable detect.Table
	Notices       []protocol.NoticeLine
	SampleTree    func(int) ([]procguard.Entry, error)
	SampleProcess func(int) (procguard.Entry, error)
	BootID        func() (string, error)
	Classify      paneRecoveryClassifier
}

type tmuxPaneRecoveryService struct {
	writer        paneRecoveryWriter
	registry      paneRecoveryRegistry
	transport     paneRecoveryTransport
	clock         paneOwnershipClock
	drain         time.Duration
	telemetry     *controlapi.PaneOperationTelemetry
	detectorTable detect.Table
	notices       []protocol.NoticeLine
	sampleTree    func(int) ([]procguard.Entry, error)
	sampleProcess func(int) (procguard.Entry, error)
	bootID        func() (string, error)
	classify      paneRecoveryClassifier
}

func newTmuxPaneRecoveryService(
	options paneRecoveryServiceOptions,
) (*tmuxPaneRecoveryService, error) {
	if options.SampleTree == nil {
		options.SampleTree = procguard.SampleTree
	}
	if options.SampleProcess == nil {
		options.SampleProcess = procguard.SampleProcess
	}
	if options.BootID == nil {
		options.BootID = readPaneRecoveryBootID
	}
	if options.Classify == nil {
		options.Classify = detect.Classify
	}
	if options.Writer == nil || options.Registry == nil || options.Transport == nil ||
		options.Clock == nil || options.Drain <= 0 || options.Telemetry == nil ||
		len(options.DetectorTable) == 0 || len(options.Notices) == 0 ||
		options.SampleTree == nil || options.SampleProcess == nil || options.BootID == nil ||
		options.Classify == nil {
		return nil, errPaneRecoveryConfig
	}
	for _, notice := range options.Notices {
		if !notice.Valid() {
			return nil, errPaneRecoveryConfig
		}
	}
	return &tmuxPaneRecoveryService{
		writer: options.Writer, registry: options.Registry, transport: options.Transport,
		clock: options.Clock, drain: options.Drain, telemetry: options.Telemetry,
		detectorTable: options.DetectorTable,
		notices:       append([]protocol.NoticeLine(nil), options.Notices...),
		sampleTree:    options.SampleTree, sampleProcess: options.SampleProcess,
		bootID:   options.BootID,
		classify: options.Classify,
	}, nil
}

func (s *tmuxPaneRecoveryService) Reconcile(
	ctx context.Context,
	params protocol.PaneAttemptParams,
	actor controlapi.PaneOperationActor,
) (protocol.PaneOperationResult, error) {
	// The lock accepts "-" only for a repaired record, so it is passed through.
	if s == nil || ctx == nil || actor.PID <= 0 || params.Validate() != nil {
		return protocol.PaneOperationResult{}, errPaneRecoveryConfig
	}
	result, err := s.writer.Reconcile(
		ctx,
		paneLiveBinding(params.Target),
		string(params.ExpectedAttempt),
		func(observeCtx context.Context, snapshot tmuxlock.ReconcileSnapshot) (
			tmuxlock.ReconcileObservation,
			error,
		) {
			return s.observeReconcile(observeCtx, params.Target, snapshot)
		},
	)
	logResult := controlapi.PaneLogFailed
	if err == nil {
		switch result.Outcome {
		case tmuxlock.RecoveryUnchanged:
			logResult = controlapi.PaneLogUnchanged
		case tmuxlock.RecoveryConfirmed:
			logResult = controlapi.PaneLogConfirmed
		case tmuxlock.RecoveryNoEffect:
			logResult = controlapi.PaneLogNoEffect
		default:
			err = errPaneRecoveryConfig
		}
	}
	logErr := s.telemetry.Record(context.WithoutCancel(ctx), controlapi.PaneOperationTelemetryEvent{
		Operation: controlapi.PaneAdminReconcile,
		Actor:     actor, Target: params.Target, Attempt: params.ExpectedAttempt,
		Result: logResult, Evidence: paneRecoveryTelemetryEvidence(result.Evidence),
	})
	if err != nil {
		return protocol.PaneOperationResult{}, errors.Join(err, logErr)
	}
	if logErr != nil {
		return protocol.PaneOperationResult{}, fmt.Errorf("record pane reconcile telemetry: %w", logErr)
	}
	return protocol.PaneOperationResult{Changed: result.Changed}, nil
}

func (s *tmuxPaneRecoveryService) Repair(
	ctx context.Context,
	params protocol.PaneOperationParams,
	actor controlapi.PaneOperationActor,
) (protocol.PaneOperationResult, error) {
	if s == nil || ctx == nil || actor.PID <= 0 || params.Validate() != nil {
		return protocol.PaneOperationResult{}, errPaneRecoveryConfig
	}
	result, err := s.writer.Repair(ctx, paneLiveBinding(params.Target))
	logResult := controlapi.PaneLogFailed
	if err == nil {
		switch result.Outcome {
		case tmuxlock.RecoveryUnchanged:
			logResult = controlapi.PaneLogUnchanged
		case tmuxlock.RecoveryRepaired:
			logResult = controlapi.PaneLogRepaired
		default:
			err = errPaneRecoveryConfig
		}
	}
	logErr := s.telemetry.Record(context.WithoutCancel(ctx), controlapi.PaneOperationTelemetryEvent{
		Operation: controlapi.PaneAdminRepair,
		Actor:     actor, Target: params.Target, Attempt: "-",
		Result: logResult, Evidence: paneRecoveryTelemetryEvidence(result.Evidence),
	})
	if err != nil {
		return protocol.PaneOperationResult{}, errors.Join(err, logErr)
	}
	if logErr != nil {
		return protocol.PaneOperationResult{}, fmt.Errorf("record pane repair telemetry: %w", logErr)
	}
	return protocol.PaneOperationResult{Changed: result.Changed}, nil
}

func (s *tmuxPaneRecoveryService) observeReconcile(
	ctx context.Context,
	target protocol.PaneOperationTarget,
	snapshot tmuxlock.ReconcileSnapshot,
) (tmuxlock.ReconcileObservation, error) {
	observation := unknownReconcileObservation()
	instance, err := s.lookupInstance(ctx, target)
	if err != nil {
		return observation, err
	}
	tmuxTarget := tmuxattach.Target{
		Server: tmuxattach.Server{
			OwnerUser: instance.OwnerUser, OwnerUID: instance.OwnerUID,
			Socket: instance.Socket, Bank: instance.Bank,
		},
		Pane: instance.PaneID,
		Authority: tmuxattach.PaneAuthority{
			Host: target.Host, BootID: target.BootID,
			PanePID: target.PanePID, PaneStart: target.PaneStart,
		},
	}
	before, err := s.transport.ObservePane(ctx, tmuxTarget)
	if err != nil {
		return observation, fmt.Errorf("observe pane before recovery capture: %w", err)
	}
	screen, err := s.transport.Capture(ctx, tmuxTarget)
	if err != nil {
		return observation, fmt.Errorf("capture pane for recovery: %w", err)
	}
	after, err := s.transport.ObservePane(ctx, tmuxTarget)
	if err != nil {
		return observation, fmt.Errorf("observe pane after recovery capture: %w", err)
	}
	tmuxEqual := before == after && before.PaneID == target.PaneID &&
		before.PanePID == target.PanePID
	observation.BindingEqual = recoveryVerdict(tmuxEqual)
	if !tmuxEqual {
		return observation, nil
	}
	bootID, bootAvailable := paneRecoveryBootID(s.bootID)
	if !bootAvailable {
		observation.BindingEqual = tmuxlock.EvidenceUnknown
		return observation, nil
	}
	if bootID != target.BootID {
		observation.BindingEqual = tmuxlock.EvidenceNo
		return observation, nil
	}
	entries, treeAvailable := paneRecoveryTree(s.sampleTree, int(target.PanePID))
	if !treeAvailable {
		observation.BindingEqual = tmuxlock.EvidenceUnknown
		return observation, nil
	}
	if !paneRecoveryRootMatches(entries, target) {
		observation.BindingEqual = tmuxlock.EvidenceNo
		return observation, nil
	}
	observation.BindingEqual = tmuxlock.EvidenceYes

	visible := s.lineVisible(screen)
	observation.LineVisible = recoveryVerdict(visible)
	observation.NoticeAbsent = recoveryVerdict(!visible)
	observation.WriterExited = s.writerDone(snapshot.Writer)
	observation.ChildExited = s.processExited(snapshot.Child)
	observation.DrainWaitElapsed = recoveryVerdict(
		!s.clock.Now().Before(snapshot.RecordedAt.Add(s.drain)),
	)

	if harness, ok := paneRecoveryHarness(instance.Harness); ok {
		state := s.classify(
			harness, s.detectorTable, screen, after.Cols, after.Rows,
			int(target.PanePID), paneRecoveryProcesses(entries),
		)
		empty := state.Certain && !state.BoxBusy && state.IsCLI &&
			paneRecoveryForeground(instance, entries)
		observation.ComposerEmpty = recoveryVerdict(empty)
	}
	return observation, nil
}

func (s *tmuxPaneRecoveryService) lookupInstance(
	ctx context.Context,
	target protocol.PaneOperationTarget,
) (tmuxinstance.Instance, error) {
	instances, err := s.registry.ListActive(ctx)
	if err != nil {
		return tmuxinstance.Instance{}, fmt.Errorf("list active panes for recovery: %w", err)
	}
	var match tmuxinstance.Instance
	found := false
	for _, instance := range instances {
		if instance.OwnerUID != target.OwnerUID || instance.Socket != target.Socket ||
			instance.PaneID != target.PaneID || instance.BootID != target.BootID ||
			instance.PanePID != target.PanePID || instance.PaneStart != target.PaneStart {
			continue
		}
		if found {
			return tmuxinstance.Instance{}, errPaneRecoveryConfig
		}
		match, found = instance, true
	}
	if !found {
		return tmuxinstance.Instance{}, tmuxinstance.ErrNotRegistered
	}
	return match, nil
}

func (s *tmuxPaneRecoveryService) lineVisible(screen []byte) bool {
	plain := ansi.Strip(string(screen))
	for _, notice := range s.notices {
		if strings.Contains(plain, string(notice)) {
			return true
		}
	}
	return false
}

// writerDone reports whether the record's writer has finished. A writer
// that is this very node process counts as finished too: every write it
// makes holds the pane flock for the whole effect, and reconcile observes
// under that same flock.
func (s *tmuxPaneRecoveryService) writerDone(writer tmuxlock.RecoveryProcess) tmuxlock.EvidenceVerdict {
	verdict := s.processExited(writer)
	if verdict != tmuxlock.EvidenceNo || int64(os.Getpid()) != writer.PID {
		return verdict
	}
	self, err := s.sampleProcess(os.Getpid())
	if err != nil {
		return tmuxlock.EvidenceUnknown
	}
	return recoveryVerdict(self.StartTime == writer.Start)
}

func (s *tmuxPaneRecoveryService) processExited(process tmuxlock.RecoveryProcess) tmuxlock.EvidenceVerdict {
	if process.PID == 0 && process.Start == 0 {
		return tmuxlock.EvidenceYes
	}
	if process.PID <= 0 || process.Start == 0 {
		return tmuxlock.EvidenceUnknown
	}
	entry, err := s.sampleProcess(int(process.PID))
	if errors.Is(err, procguard.ErrNoRoot) {
		return tmuxlock.EvidenceYes
	}
	if err != nil {
		return tmuxlock.EvidenceUnknown
	}
	return recoveryVerdict(entry.StartTime != process.Start)
}

func unknownReconcileObservation() tmuxlock.ReconcileObservation {
	return tmuxlock.ReconcileObservation{
		BindingEqual:     tmuxlock.EvidenceUnknown,
		LineVisible:      tmuxlock.EvidenceUnknown,
		ComposerEmpty:    tmuxlock.EvidenceUnknown,
		NoticeAbsent:     tmuxlock.EvidenceUnknown,
		WriterExited:     tmuxlock.EvidenceUnknown,
		ChildExited:      tmuxlock.EvidenceUnknown,
		DrainWaitElapsed: tmuxlock.EvidenceUnknown,
	}
}

func recoveryVerdict(value bool) tmuxlock.EvidenceVerdict {
	if value {
		return tmuxlock.EvidenceYes
	}
	return tmuxlock.EvidenceNo
}

func paneRecoveryHarness(harness protocol.Harness) (detect.Harness, bool) {
	switch harness {
	case protocol.HarnessCodex:
		return detect.HarnessCodex, true
	case protocol.HarnessClaudeCode:
		return detect.HarnessClaude, true
	case protocol.HarnessMuseCode:
		return detect.HarnessMuse, true
	case protocol.HarnessOpencode:
		return detect.HarnessOpenCode, true
	default:
		return "", false
	}
}

func paneRecoveryProcesses(entries []procguard.Entry) []detect.Process {
	processes := make([]detect.Process, 0, len(entries))
	for _, entry := range entries {
		var state byte
		if len(entry.State) == 1 {
			state = entry.State[0]
		}
		var argv []string
		if entry.Program != "" {
			argv = []string{entry.Program}
		}
		processes = append(processes, detect.Process{
			PID: entry.PID, PPID: entry.PPID, Pgrp: entry.Pgrp, State: state,
			TPGID: entry.TPGID, StartTime: entry.StartTime, Argv: argv,
		})
	}
	return processes
}

func paneRecoveryForeground(instance tmuxinstance.Instance, entries []procguard.Entry) bool {
	for _, entry := range entries {
		if int64(entry.PID) != instance.HarnessPID {
			continue
		}
		running := entry.State == "R" || entry.State == "S" || entry.State == "D"
		return instance.HarnessStart >= 0 && entry.StartTime == uint64(instance.HarnessStart) &&
			entry.Pgrp > 0 && entry.Pgrp == entry.TPGID && running
	}
	return false
}

func paneRecoveryRootMatches(
	entries []procguard.Entry,
	target protocol.PaneOperationTarget,
) bool {
	for _, entry := range entries {
		if int64(entry.PID) == target.PanePID {
			return target.PaneStart > 0 && entry.StartTime == uint64(target.PaneStart)
		}
	}
	return false
}

func paneRecoveryBootID(read func() (string, error)) (string, bool) {
	bootID, err := read()
	return bootID, err == nil
}

func paneRecoveryTree(
	sample func(int) ([]procguard.Entry, error),
	pid int,
) ([]procguard.Entry, bool) {
	entries, err := sample(pid)
	return entries, err == nil
}

func readPaneRecoveryBootID() (string, error) {
	raw, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return "", fmt.Errorf("read boot identity: %w", err)
	}
	bootID := strings.TrimSpace(string(raw))
	if len(bootID) != 36 {
		return "", errPaneRecoveryConfig
	}
	return bootID, nil
}

func paneRecoveryTelemetryEvidence(
	evidence tmuxlock.RecoveryEvidence,
) *controlapi.PaneOperationEvidence {
	out := &controlapi.PaneOperationEvidence{
		WriterExited:  paneTelemetryVerdict(evidence.WriterExited),
		ChildExited:   paneTelemetryVerdict(evidence.ChildExited),
		BindingEqual:  paneTelemetryVerdict(evidence.BindingEqual),
		ComposerEmpty: paneTelemetryVerdict(evidence.ComposerEmpty),
		LineVisible:   paneTelemetryVerdict(evidence.LineVisible),
	}
	if evidence.Record != nil {
		out.Record = &controlapi.PaneRecordEvidence{
			Size:          evidence.Record.Size,
			MagicValid:    paneTelemetryVerdict(evidence.Record.MagicValid),
			ChecksumValid: paneTelemetryVerdict(evidence.Record.ChecksumValid),
		}
		if evidence.Record.Version != nil {
			version := *evidence.Record.Version
			out.Record.Version = &version
		}
	}
	return out
}

func paneTelemetryVerdict(verdict tmuxlock.EvidenceVerdict) controlapi.PaneEvidenceVerdict {
	switch verdict {
	case tmuxlock.EvidenceYes:
		return controlapi.PaneEvidenceYes
	case tmuxlock.EvidenceNo:
		return controlapi.PaneEvidenceNo
	default:
		return controlapi.PaneEvidenceUnknown
	}
}

var _ controlapi.PaneRecoveryService = (*tmuxPaneRecoveryService)(nil)
