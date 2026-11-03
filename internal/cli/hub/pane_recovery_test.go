package hub

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/controlapi"
	"github.com/smellak/gifford/internal/detect"
	"github.com/smellak/gifford/internal/procguard"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy/tmuxattach"
	"github.com/smellak/gifford/internal/tmuxinstance"
	"github.com/smellak/gifford/internal/tmuxlock"
)

const (
	paneRecoveryTestShell = "bash"
	paneRecoveryTestCodex = "codex"
)

type paneRecoveryWriterStub struct {
	reconcileResult tmuxlock.RecoveryResult
	repairResult    tmuxlock.RecoveryResult
	err             error
	reconciles      []tmuxlock.LiveBinding
	repairs         []tmuxlock.LiveBinding
	attempts        []string
}

func (w *paneRecoveryWriterStub) Reconcile(
	_ context.Context,
	binding tmuxlock.LiveBinding,
	attempt string,
	_ tmuxlock.ReconcileObserver,
) (tmuxlock.RecoveryResult, error) {
	w.reconciles = append(w.reconciles, binding)
	w.attempts = append(w.attempts, attempt)
	return w.reconcileResult, w.err
}

func (w *paneRecoveryWriterStub) Repair(
	_ context.Context,
	binding tmuxlock.LiveBinding,
) (tmuxlock.RecoveryResult, error) {
	w.repairs = append(w.repairs, binding)
	return w.repairResult, w.err
}

type paneRecoveryRegistryStub struct {
	instances []tmuxinstance.Instance
	err       error
}

func (r paneRecoveryRegistryStub) ListActive(context.Context) ([]tmuxinstance.Instance, error) {
	return append([]tmuxinstance.Instance(nil), r.instances...), r.err
}

type paneRecoveryTransportStub struct {
	screen       []byte
	observations []tmuxattach.PaneObservation
	observeCalls int
}

func (t *paneRecoveryTransportStub) Capture(
	context.Context,
	tmuxattach.Target,
) ([]byte, error) {
	return append([]byte(nil), t.screen...), nil
}

func (t *paneRecoveryTransportStub) ObservePane(
	context.Context,
	tmuxattach.Target,
) (tmuxattach.PaneObservation, error) {
	if t.observeCalls >= len(t.observations) {
		return tmuxattach.PaneObservation{}, errors.New("unexpected observation")
	}
	observation := t.observations[t.observeCalls]
	t.observeCalls++
	return observation, nil
}

func TestPaneRecoveryServiceMapsResultsAndStructuredTelemetry(t *testing.T) {
	t.Parallel()
	target := paneOwnershipTarget()
	writer := &paneRecoveryWriterStub{
		reconcileResult: tmuxlock.RecoveryResult{
			Changed: true, Outcome: tmuxlock.RecoveryConfirmed,
			Evidence: recoveryTestEvidence(),
		},
		repairResult: tmuxlock.RecoveryResult{
			Changed: true, Outcome: tmuxlock.RecoveryRepaired,
			Evidence: tmuxlock.RecoveryEvidence{
				WriterExited:  tmuxlock.EvidenceUnknown,
				ChildExited:   tmuxlock.EvidenceUnknown,
				BindingEqual:  tmuxlock.EvidenceYes,
				ComposerEmpty: tmuxlock.EvidenceUnknown,
				LineVisible:   tmuxlock.EvidenceUnknown,
				Record: &tmuxlock.RecordEvidence{
					Size: 7, MagicValid: tmuxlock.EvidenceNo,
					ChecksumValid: tmuxlock.EvidenceNo,
				},
			},
		},
	}
	var logs bytes.Buffer
	service, err := newTmuxPaneRecoveryService(paneRecoveryServiceOptions{
		Writer: writer, Registry: paneRecoveryRegistryStub{},
		Transport: &paneRecoveryTransportStub{},
		Clock:     &paneOwnershipTestClock{now: time.Now()}, Drain: time.Second,
		Telemetry: controlapi.NewPaneOperationTelemetry(
			slog.New(slog.NewTextHandler(&logs, nil)),
		),
		DetectorTable: detect.DefaultTable(),
		Notices:       []protocol.NoticeLine{tmuxattach.FixedNoticeLine},
	})
	require.NoError(t, err)
	actor := controlapi.PaneOperationActor{UID: 42, PID: 700}
	attempt := protocol.PaneAttemptID("11111111111111111111111111111111")

	result, err := service.Reconcile(t.Context(), protocol.PaneAttemptParams{
		Target: target, ExpectedAttempt: attempt,
	}, actor)
	require.NoError(t, err)
	require.Equal(t, protocol.PaneOperationResult{Changed: true}, result)
	require.Equal(t, []tmuxlock.LiveBinding{paneLiveBinding(target)}, writer.reconciles)
	require.Equal(t, []string{string(attempt)}, writer.attempts)

	result, err = service.Repair(t.Context(), protocol.PaneOperationParams{Target: target}, actor)
	require.NoError(t, err)
	require.Equal(t, protocol.PaneOperationResult{Changed: true}, result)
	require.Equal(t, []tmuxlock.LiveBinding{paneLiveBinding(target)}, writer.repairs)
	text := logs.String()
	require.Contains(t, text, "op=reconcile")
	require.Contains(t, text, "result=confirmed")
	require.Contains(t, text, "evidence.line_visible=yes")
	require.Contains(t, text, "op=repair")
	require.Contains(t, text, "result=repaired")
	require.Contains(t, text, "evidence.record.st_size=7")
	require.NotContains(t, text, target.Socket)
}

func TestPaneRecoveryObserverUsesCaptureBracketedByLiveIdentity(t *testing.T) {
	t.Parallel()
	target := paneOwnershipTarget()
	instance := paneRecoveryInstance(target)
	transport := &paneRecoveryTransportStub{
		screen: []byte("\x1b[32m" + string(tmuxattach.FixedNoticeLine) + "\x1b[0m"),
		observations: []tmuxattach.PaneObservation{
			{PaneID: target.PaneID, PanePID: target.PanePID, Cols: 120, Rows: 40},
			{PaneID: target.PaneID, PanePID: target.PanePID, Cols: 120, Rows: 40},
		},
	}
	recordedAt := time.Date(2026, 11, 3, 9, 0, 0, 0, time.UTC)
	clock := &paneOwnershipTestClock{now: recordedAt.Add(30 * time.Second)}
	service, err := newTmuxPaneRecoveryService(paneRecoveryServiceOptions{
		Writer:    &paneRecoveryWriterStub{},
		Registry:  paneRecoveryRegistryStub{instances: []tmuxinstance.Instance{instance}},
		Transport: transport, Clock: clock, Drain: 30 * time.Second,
		Telemetry:     controlapi.NewPaneOperationTelemetry(nil),
		DetectorTable: detect.DefaultTable(),
		Notices:       []protocol.NoticeLine{tmuxattach.FixedNoticeLine},
		SampleTree: func(pid int) ([]procguard.Entry, error) {
			require.Equal(t, int(target.PanePID), pid)
			return []procguard.Entry{
				{PID: int(target.PanePID), StartTime: uint64(target.PaneStart), Program: paneRecoveryTestShell},
				{
					PID: int(instance.HarnessPID), StartTime: uint64(instance.HarnessStart),
					Program: paneRecoveryTestCodex, State: "S", Pgrp: int(instance.HarnessPID), TPGID: int(instance.HarnessPID),
				},
			}, nil
		},
		SampleProcess: func(pid int) (procguard.Entry, error) {
			switch pid {
			case 800:
				return procguard.Entry{PID: pid, StartTime: 11}, nil
			case 801:
				return procguard.Entry{}, procguard.ErrNoRoot
			default:
				return procguard.Entry{}, errors.New("unexpected pid")
			}
		},
		BootID: func() (string, error) { return target.BootID, nil },
		Classify: func(
			detect.Harness, detect.Table, []byte, int, int, int, []detect.Process,
		) detect.State {
			return detect.State{Certain: true, IsCLI: true}
		},
	})
	require.NoError(t, err)

	observation, err := service.observeReconcile(t.Context(), target, tmuxlock.ReconcileSnapshot{
		Writer:     tmuxlock.RecoveryProcess{PID: 800, Start: 10},
		Child:      tmuxlock.RecoveryProcess{PID: 801, Start: 12},
		RecordedAt: recordedAt,
	})
	require.NoError(t, err)
	require.Equal(t, 2, transport.observeCalls)
	require.Equal(t, tmuxlock.EvidenceYes, observation.BindingEqual)
	require.Equal(t, tmuxlock.EvidenceYes, observation.LineVisible)
	require.Equal(t, tmuxlock.EvidenceNo, observation.NoticeAbsent)
	require.Equal(t, tmuxlock.EvidenceYes, observation.ComposerEmpty)
	require.Equal(t, tmuxlock.EvidenceYes, observation.WriterExited)
	require.Equal(t, tmuxlock.EvidenceYes, observation.ChildExited)
	require.Equal(t, tmuxlock.EvidenceYes, observation.DrainWaitElapsed)
}

func TestPaneRecoveryObserverLeavesOtherProofsUnknownWhenBindingChanges(t *testing.T) {
	t.Parallel()
	target := paneOwnershipTarget()
	instance := paneRecoveryInstance(target)
	transport := &paneRecoveryTransportStub{
		screen: []byte(tmuxattach.FixedNoticeLine),
		observations: []tmuxattach.PaneObservation{
			{PaneID: target.PaneID, PanePID: target.PanePID, Cols: 120, Rows: 40},
			{PaneID: target.PaneID, PanePID: target.PanePID + 1, Cols: 120, Rows: 40},
		},
	}
	service, err := newTmuxPaneRecoveryService(paneRecoveryServiceOptions{
		Writer:    &paneRecoveryWriterStub{},
		Registry:  paneRecoveryRegistryStub{instances: []tmuxinstance.Instance{instance}},
		Transport: transport, Clock: &paneOwnershipTestClock{now: time.Now()},
		Drain: time.Second, Telemetry: controlapi.NewPaneOperationTelemetry(nil),
		DetectorTable: detect.DefaultTable(),
		Notices:       []protocol.NoticeLine{tmuxattach.FixedNoticeLine},
		SampleTree: func(int) ([]procguard.Entry, error) {
			t.Fatal("changed binding must stop before process classification")
			return nil, nil
		},
		BootID: func() (string, error) { return target.BootID, nil },
	})
	require.NoError(t, err)

	observation, err := service.observeReconcile(
		t.Context(), target, tmuxlock.ReconcileSnapshot{RecordedAt: time.Now()},
	)
	require.NoError(t, err)
	require.Equal(t, tmuxlock.EvidenceNo, observation.BindingEqual)
	require.Equal(t, tmuxlock.EvidenceUnknown, observation.LineVisible)
	require.Equal(t, tmuxlock.EvidenceUnknown, observation.ComposerEmpty)
}

func recoveryTestEvidence() tmuxlock.RecoveryEvidence {
	return tmuxlock.RecoveryEvidence{
		WriterExited: tmuxlock.EvidenceYes, ChildExited: tmuxlock.EvidenceYes,
		BindingEqual: tmuxlock.EvidenceYes, ComposerEmpty: tmuxlock.EvidenceYes,
		LineVisible: tmuxlock.EvidenceYes,
	}
}

func paneRecoveryInstance(target protocol.PaneOperationTarget) tmuxinstance.Instance {
	return tmuxinstance.Instance{
		BootID: target.BootID, OwnerUser: "operator", OwnerUID: target.OwnerUID,
		Socket: target.Socket, PaneID: target.PaneID, PanePID: target.PanePID,
		PaneStart: target.PaneStart, Harness: protocol.HarnessCodex,
		HarnessPID: 3000, HarnessStart: 4000,
	}
}

// A record written by this very node process is finished: its writes hold
// the pane flock for the whole effect and reconcile observes under that
// flock. Another live writer still counts as running.
func TestPaneRecoveryWriterIsThisNodeCountsAsFinished(t *testing.T) {
	t.Parallel()
	self := os.Getpid()
	other := self + 1
	service := &tmuxPaneRecoveryService{sampleProcess: func(pid int) (procguard.Entry, error) {
		switch pid {
		case self:
			return procguard.Entry{PID: pid, StartTime: 40}, nil
		case other:
			return procguard.Entry{PID: pid, StartTime: 50}, nil
		default:
			return procguard.Entry{}, errors.New("unexpected pid")
		}
	}}
	require.Equal(t, tmuxlock.EvidenceYes,
		service.writerDone(tmuxlock.RecoveryProcess{PID: int64(self), Start: 40}))
	require.Equal(t, tmuxlock.EvidenceNo,
		service.writerDone(tmuxlock.RecoveryProcess{PID: int64(other), Start: 50}))
	require.Equal(t, tmuxlock.EvidenceYes,
		service.writerDone(tmuxlock.RecoveryProcess{PID: int64(other), Start: 49}),
		"a reused pid is a finished writer")
}
