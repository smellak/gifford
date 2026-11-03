package orchestrator

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/daemon"
	"github.com/smellak/gifford/internal/daemon/agentlink"
	"github.com/smellak/gifford/internal/detect"
	"github.com/smellak/gifford/internal/procguard"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy/tmuxattach"
	"github.com/smellak/gifford/internal/tmuxinstance"
)

const (
	tmuxTestInstance  protocol.ULID       = "01JB0000000000000000000042"
	tmuxTestPane      protocol.TmuxPaneID = "%7"
	tmuxTestProgram                       = "codex"
	tmuxTestInputExpr                     = `[$#] ?(.*)$`
	tmuxTestLauncher                      = "runner"
)

func TestTmuxDetectorHarness(t *testing.T) {
	t.Parallel()

	tests := map[protocol.Harness]struct {
		want detect.Harness
		ok   bool
	}{
		protocol.HarnessClaudeCode:  {want: detect.HarnessClaude, ok: true},
		protocol.HarnessCodex:       {want: detect.HarnessCodex, ok: true},
		protocol.HarnessMuseCode:    {want: detect.HarnessMuse, ok: true},
		protocol.HarnessOpencode:    {want: detect.HarnessOpenCode, ok: true},
		protocol.HarnessAntigravity: {},
		protocol.HarnessGeneric:     {},
	}
	values := protocol.Harness("").EnumValues()
	require.Len(t, tests, len(values))
	for _, value := range values {
		harness := protocol.Harness(value)
		test, exists := tests[harness]
		require.Truef(t, exists, "protocol harness %q must declare mapping or rejection", harness)

		got, ok := tmuxDetectorHarness(harness)
		require.Equalf(t, test.ok, ok, "protocol harness %q", harness)
		require.Equalf(t, test.want, got, "protocol harness %q", harness)
	}

	got, ok := tmuxDetectorHarness(protocol.Harness("unknown"))
	require.False(t, ok)
	require.Empty(t, got)
}

type tmuxRegistryStub struct {
	instance  tmuxinstance.Instance
	verifyErr error
	verified  int
}

func (r *tmuxRegistryStub) Lookup(
	_ context.Context,
	agent protocol.AgentID,
) (tmuxinstance.Instance, error) {
	if agent != r.instance.AgentID {
		return tmuxinstance.Instance{}, tmuxinstance.ErrNotRegistered
	}
	return r.instance, nil
}

func (r *tmuxRegistryStub) Verify(
	_ context.Context,
	instance tmuxinstance.Instance,
	observation tmuxinstance.PaneObservation,
) error {
	r.verified++
	if instance.ID != r.instance.ID || observation.PaneID != r.instance.PaneID ||
		observation.PanePID != r.instance.PanePID {
		return tmuxinstance.ErrInvalidated
	}
	return r.verifyErr
}

type tmuxSessionStub struct{ invalidated int }

func (s *tmuxSessionStub) Invalidate(
	_ context.Context,
	_ protocol.AgentID,
	_ protocol.OpaqueID,
) error {
	s.invalidated++
	return nil
}

type tmuxTransportStub struct {
	db               *sql.DB
	initial          []byte
	plain            []byte
	clients          []byte
	postSubmit       []byte
	processes        []procguard.Entry
	locked           tmuxattach.LockedCapture
	lockErr          error
	writes           int
	stateBeforeWrite daemon.AttemptState
}

func (t *tmuxTransportStub) CaptureLocked(
	_ context.Context,
	_ tmuxattach.Target,
) (tmuxattach.LockedCapture, error) {
	if t.lockErr != nil {
		return tmuxattach.LockedCapture{}, t.lockErr
	}
	return t.locked, nil
}

func (t *tmuxTransportStub) Deliver(
	ctx context.Context,
	request tmuxattach.Request,
	guard tmuxattach.Guard,
) (tmuxattach.Result, error) {
	result := tmuxattach.Result{Outcome: tmuxattach.OutcomeNotWritten, Stage: tmuxattach.StageRevalidate}
	observation := tmuxattach.PaneObservation{
		PaneID: request.Target.Pane, PanePID: 100, Cols: 80, Rows: 24,
	}
	if err := guard.Revalidate(ctx, request.Target, observation, t.clients, t.initial, t.plain); err != nil {
		return result, errors.Join(err, guard.Record(ctx, result))
	}
	if err := t.db.QueryRowContext(ctx, `SELECT state FROM delivery_attempts
		ORDER BY created_at DESC, delivery_id DESC LIMIT 1`).Scan(&t.stateBeforeWrite); err != nil {
		return result, errors.Join(err, guard.Record(ctx, result))
	}

	result.Outcome, result.Stage = tmuxattach.OutcomeUncertain, tmuxattach.StageWrite
	t.writes++
	result.Stage = tmuxattach.StageVerify
	staged := []byte("$ " + string(request.Notice))
	if err := guard.VerifyNotice(ctx, request.Target, staged, request.Notice); err != nil {
		return result, errors.Join(err, guard.Record(ctx, result))
	}
	result.Stage = tmuxattach.StageConfirm
	if err := guard.VerifySubmitted(ctx, request.Target, t.postSubmit); err != nil {
		return result, errors.Join(err, guard.Record(ctx, result))
	}
	result.Outcome = tmuxattach.OutcomeCommitted
	if err := guard.Record(ctx, result); err != nil {
		result.Outcome = tmuxattach.OutcomeUncertain
		return result, err
	}
	return result, nil
}

func newTmuxExecutorFixture(
	t *testing.T,
) (*executorFixture, *tmuxTransportStub, *tmuxRegistryStub) {
	t.Helper()
	fixture := newExecutorFixture(t)
	manifest := protocol.CapabilityManifest{
		SchemaVersion: protocol.SchemaVersion, AgentID: executorTestAgent,
		Harness: protocol.HarnessCodex, HarnessVersion: executorTestHarnessVersion,
		AdapterType:        protocol.AdapterTypeGenericPTY,
		InputGuardProvider: protocol.InputGuardProviderTmuxAttach,
		CaptureMode:        protocol.CaptureModeTmuxAttach, Deliverable: true,
		Capabilities: protocol.Capabilities{PTYCapture: true},
	}
	require.NoError(t, manifest.Validate())
	manifestJSON, err := json.Marshal(manifest)
	require.NoError(t, err)
	_, err = fixture.store.DB().ExecContext(t.Context(), `UPDATE sessions
		SET capture_mode = ?, manifest_json = ? WHERE session_id = ?`,
		protocol.CaptureModeTmuxAttach, string(manifestJSON), executorTestSession)
	require.NoError(t, err)
	_, err = fixture.store.DB().ExecContext(t.Context(),
		"DELETE FROM input_guards WHERE session_id = ?", executorTestSession)
	require.NoError(t, err)

	registry := &tmuxRegistryStub{instance: tmuxinstance.Instance{
		ID: tmuxTestInstance, AgentID: executorTestAgent,
		OwnerUser: "agent_owner", OwnerUID: 1001, Socket: "/run/user/1001/tmux.sock",
		PaneID: tmuxTestPane, PanePID: 100, Harness: protocol.HarnessCodex,
		HarnessPID: 101, HarnessStart: 202, AgentUID: 303, AgentGID: 404,
		SessionID: executorTestSession, State: tmuxinstance.StateActive,
	}}
	transport := &tmuxTransportStub{
		db: fixture.store.DB(), initial: []byte("$ "), postSubmit: []byte("$ "),
		processes: []procguard.Entry{
			{PID: 100, StartTime: 201, Comm: tmuxTestLauncher, Program: tmuxTestLauncher, Pgrp: 100, State: "S", TPGID: 101},
			{PID: 101, PPID: 100, StartTime: 202, Comm: tmuxTestProgram, Program: tmuxTestProgram, Pgrp: 101, State: "S", TPGID: 101},
		},
	}
	sessions := &tmuxSessionStub{}
	inputGuards, err := agentlink.NewInputGuardStore(fixture.store.DB(), fixture.clock)
	require.NoError(t, err)
	processGuard := &procguard.Guard{}
	table := detect.Table{
		detect.Harness(protocol.HarnessCodex): {
			InputExpr: tmuxTestInputExpr, CLIBinaries: []string{tmuxTestProgram},
		},
	}
	executor, err := NewExecutor(ExecutorOptions{
		DB: fixture.store.DB(), Channel: fixture.executor.channel,
		Approvals: fixture.controller, Links: fixture.links, Clock: fixture.clock,
		Entropy: bytes.NewReader(make([]byte, 4096)),
		TmuxAttach: &TmuxAttachOptions{
			Registry: registry, Transport: transport, Sessions: sessions,
			InputGuards: inputGuards, ProcessGuard: processGuard,
			SampleTree: func(int) ([]procguard.Entry, error) {
				return append([]procguard.Entry(nil), transport.processes...), nil
			},
			DetectorTable: table, HumanActivityWindow: 15 * time.Second,
		},
	})
	require.NoError(t, err)
	fixture.executor = executor
	return fixture, transport, registry
}

func TestTmuxAttachWarmsBaselineThenCommitsAfterDurableCommitSent(t *testing.T) {
	t.Parallel()
	fixture, transport, registry := newTmuxExecutorFixture(t)
	candidate := fixture.seedDelivery(t, 90)

	require.NoError(t, fixture.executor.EvaluateTmuxAttach(t.Context(), candidate))
	require.Zero(t, transport.writes, "the baseline-warming pass must not write")
	var attempts, approvals int64
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(),
		"SELECT count(*) FROM delivery_attempts").Scan(&attempts))
	require.Zero(t, attempts)
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(),
		"SELECT count(*) FROM approval_requests WHERE state = 'PENDING'").Scan(&approvals))
	require.Zero(t, approvals, "Hold leaves the queue unchanged without an approval")

	require.NoError(t, fixture.executor.EvaluateTmuxAttach(t.Context(), candidate))
	require.Equal(t, 1, transport.writes)
	require.Equal(t, daemon.AttemptCommitSent, transport.stateBeforeWrite,
		"the no-retry point is durable before the first physical effect")
	require.Equal(t, 3, registry.verified,
		"one Revalidate per pass plus the pre-Enter check of the write pass")

	var queueState string
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT state
		FROM delivery_queue WHERE message_id = ? AND agent_id = ?`,
		candidate.MessageID, candidate.AgentID).Scan(&queueState))
	require.Equal(t, queueStateDelivered, queueState)
	var attemptState daemon.AttemptState
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT state
		FROM delivery_attempts WHERE message_id = ? AND agent_id = ?`,
		candidate.MessageID, candidate.AgentID).Scan(&attemptState))
	require.Equal(t, daemon.AttemptCommitted, attemptState)

	var source string
	var version int64
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT detection_source,
		version FROM input_guards WHERE session_id = ?`, executorTestSession).Scan(&source, &version))
	require.Equal(t, string(protocol.InputGuardProviderTmuxAttach), source)
	require.Equal(t, int64(2), version)
}

func TestTmuxAttachUncertainDetectorDefersWithoutAttemptOrApproval(t *testing.T) {
	t.Parallel()
	fixture, transport, _ := newTmuxExecutorFixture(t)
	warm := fixture.seedDelivery(t, 91)
	require.NoError(t, fixture.executor.EvaluateTmuxAttach(t.Context(), warm))

	transport.initial = []byte("screen without an input line")
	uncertain := fixture.seedDelivery(t, 92)
	require.NoError(t, fixture.executor.EvaluateTmuxAttach(t.Context(), uncertain))
	require.Zero(t, transport.writes)

	var attempts int64
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT count(*)
		FROM delivery_attempts WHERE message_id = ? AND agent_id = ?`,
		uncertain.MessageID, uncertain.AgentID).Scan(&attempts))
	require.Zero(t, attempts, "pre-effect uncertainty must not fabricate an attempt")
	var queueState string
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT state
		FROM delivery_queue WHERE message_id = ? AND agent_id = ?`,
		uncertain.MessageID, uncertain.AgentID).Scan(&queueState))
	require.Equal(t, queueStateQueued, queueState)
	var approvals int64
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT count(*)
		FROM approval_requests WHERE object_id = ? AND agent_id = ? AND state = 'PENDING'`,
		uncertain.MessageID, uncertain.AgentID).Scan(&approvals))
	require.Zero(t, approvals, "pre-effect uncertainty must not block the panel")
	var status protocol.ExecutionStatus
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT execution_status
		FROM orchestrator_decisions WHERE message_id = ? AND agent_id = ?
		ORDER BY created_at DESC, decision_id DESC LIMIT 1`,
		uncertain.MessageID, uncertain.AgentID).Scan(&status))
	require.Equal(t, protocol.ExecutionStatusExecuted, status)
}

func TestTmuxAttachRecentHumanActivityHoldsWithoutAnAttempt(t *testing.T) {
	t.Parallel()
	fixture, transport, _ := newTmuxExecutorFixture(t)
	transport.clients = []byte(fmt.Sprintf("%s\t%d\n", tmuxTestPane, fixture.clock.Now().Unix()))
	candidate := fixture.seedDelivery(t, 93)

	require.NoError(t, fixture.executor.EvaluateTmuxAttach(t.Context(), candidate))
	require.Zero(t, transport.writes)
	var attempts int64
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(),
		"SELECT count(*) FROM delivery_attempts").Scan(&attempts))
	require.Zero(t, attempts)
	require.Equal(t, protocol.GuardOutcomeFail,
		guardOutcome(latestGuards(t, fixture.store.DB(), candidate.MessageID),
			protocol.GuardNameHumanActivityNotActive))
}

func TestTmuxAttachPostEnterUsesOnlyBusyOrCertainEmptyBox(t *testing.T) {
	t.Parallel()
	target := tmuxattach.Target{Pane: tmuxTestPane}
	guard := &tmuxDeliveryGuard{
		runtime: &tmuxAttachRuntime{
			sampleTree: func(int) ([]procguard.Entry, error) {
				return []procguard.Entry{{
					PID: 100, Program: "shell", Pgrp: 100, State: "S", TPGID: 100,
				}}, nil
			},
			detectorTable: detect.Table{
				detect.Harness(protocol.HarnessCodex): {
					BusyMarks: []string{"working"}, InputExpr: tmuxTestInputExpr,
					CLIBinaries: []string{tmuxTestProgram},
				},
			},
		},
		instance:    tmuxinstance.Instance{PanePID: 100, Harness: protocol.HarnessCodex},
		target:      target,
		observation: tmuxattach.PaneObservation{Cols: 80, Rows: 24},
	}

	for name, screen := range map[string][]byte{
		"agent became busy without a foreground cli":      []byte("working\nno composer"),
		"box is certainly empty without a foreground cli": []byte("$ "),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			require.NoError(t, guard.VerifySubmitted(t.Context(), target, screen))
		})
	}
	for name, screen := range map[string][]byte{
		"typed text remains": []byte("$ draft"),
		"detector uncertain": []byte("no input line"),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			require.Error(t, guard.VerifySubmitted(t.Context(), target, screen))
		})
	}
}

func TestTmuxAttachPreEnterRequiresTheRegisteredCLIInTheForeground(t *testing.T) {
	t.Parallel()
	const notice protocol.NoticeLine = "[GIFFORD] 1 pending message"
	target := tmuxattach.Target{Pane: tmuxTestPane}
	instance := tmuxinstance.Instance{
		ID: tmuxTestInstance, AgentID: executorTestAgent, PaneID: tmuxTestPane,
		PanePID: 100, Harness: protocol.HarnessCodex, HarnessPID: 101, HarnessStart: 202,
	}
	// The measured shell-hosted form: a shell leads the pane and the CLI is
	// its direct child, in a process group of its own.
	shell := procguard.Entry{PID: 100, StartTime: 201, Program: "bash", Pgrp: 100, State: "S", TPGID: 101}
	cli := procguard.Entry{PID: 101, PPID: 100, StartTime: 202, Program: tmuxTestProgram, Pgrp: 101, State: "S", TPGID: 101}
	newGuard := func(verifyErr, sampleErr error, entries ...procguard.Entry) (*tmuxDeliveryGuard, *tmuxRegistryStub) {
		registry := &tmuxRegistryStub{instance: instance, verifyErr: verifyErr}
		return &tmuxDeliveryGuard{
			runtime: &tmuxAttachRuntime{
				registry: registry,
				sampleTree: func(int) ([]procguard.Entry, error) {
					return append([]procguard.Entry(nil), entries...), sampleErr
				},
				detectorTable: detect.Table{
					detect.Harness(protocol.HarnessCodex): {
						InputExpr: tmuxTestInputExpr, CLIBinaries: []string{tmuxTestProgram},
					},
				},
			},
			instance: instance, target: target, notice: notice,
			observation: tmuxattach.PaneObservation{PaneID: tmuxTestPane, PanePID: 100, Cols: 80, Rows: 24},
		}, registry
	}
	typed := []byte("$ " + string(notice))

	guard, registry := newGuard(nil, nil, shell, cli)
	require.NoError(t, guard.VerifyNotice(t.Context(), target, typed, notice))
	require.Equal(t, 1, registry.verified, "Enter waits for a fresh registry check")

	// The fleet form: «bash -c» without job control keeps the CLI in the
	// shell's own group, which owns the tty.
	groupShell, groupCLI := shell, cli
	groupShell.TPGID, groupCLI.Pgrp, groupCLI.TPGID = 100, 100, 100
	guard, _ = newGuard(nil, nil, groupShell, groupCLI)
	require.NoError(t, guard.VerifyNotice(t.Context(), target, typed, notice))
	groupStoppedShell, groupStoppedCLI := groupShell, groupCLI
	groupStoppedShell.State, groupStoppedCLI.State = "T", "T"
	// The measured launcher form: the registered node launcher is stopped on
	// its own while its native child still runs in front.
	launcher := cli
	launcher.Program, launcher.State = "node", "T"
	native := procguard.Entry{PID: 102, PPID: 101, StartTime: 203, Program: tmuxTestProgram, Pgrp: 101, State: "S", TPGID: 101}

	stopped, inBackground, shellInFront, reused := cli, cli, shell, cli
	stopped.State, stopped.TPGID, shellInFront.TPGID = "T", 100, 100
	inBackground.TPGID = 100
	reused.StartTime = 999
	// A second CLI in front of the stopped registered one: the generic
	// detector check alone would accept it.
	shellBehindOther, stoppedBehindOther := shell, stopped
	shellBehindOther.TPGID, stoppedBehindOther.TPGID = 105, 105
	other := procguard.Entry{PID: 105, PPID: 100, StartTime: 300, Program: tmuxTestProgram, Pgrp: 105, State: "S", TPGID: 105}
	sampleFailed := errors.New("sample failed")
	for name, test := range map[string]struct {
		verifyErr, sampleErr error
		entries              []procguard.Entry
		want                 error
	}{
		"cli stopped and the shell back in front": {entries: []procguard.Entry{shellInFront, stopped}},
		"cli in the background":                   {entries: []procguard.Entry{shellInFront, inBackground}},
		"cli exited and only the shell is left":   {entries: []procguard.Entry{shellInFront}},
		"another cli in front of the stopped registered one": {
			entries: []procguard.Entry{shellBehindOther, stoppedBehindOther, other},
		},
		"registered pid reused by another process": {entries: []procguard.Entry{shell, reused}},
		"whole group stopped with the foreground unchanged": {
			entries: []procguard.Entry{groupStoppedShell, groupStoppedCLI},
		},
		"registered launcher stopped under a running cli": {
			entries: []procguard.Entry{shell, launcher, native},
		},
		"registered instance invalidated": {
			verifyErr: tmuxinstance.ErrInvalidated, entries: []procguard.Entry{shell, cli},
			want: tmuxinstance.ErrInvalidated,
		},
		"process tree unreadable": {
			sampleErr: sampleFailed, entries: []procguard.Entry{shell, cli}, want: sampleFailed,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			guard, _ := newGuard(test.verifyErr, test.sampleErr, test.entries...)
			err := guard.VerifyNotice(t.Context(), target, typed, notice)
			require.Error(t, err)
			if test.want != nil {
				require.ErrorIs(t, err, test.want)
			}
		})
	}

	t.Run("notice not typed exactly", func(t *testing.T) {
		t.Parallel()
		guard, registry := newGuard(nil, nil, shell, cli)
		require.Error(t, guard.VerifyNotice(t.Context(), target, []byte("$ draft"), notice))
		require.Zero(t, registry.verified, "a wrong screen fails before the process checks")
	})
}

func TestTmuxAttachAnotherForegroundCLIDoesNotStandInForTheRegisteredOne(t *testing.T) {
	t.Parallel()
	fixture, transport, _ := newTmuxExecutorFixture(t)
	transport.processes = []procguard.Entry{
		{PID: 100, StartTime: 201, Comm: tmuxTestLauncher, Program: tmuxTestLauncher, Pgrp: 100, State: "S", TPGID: 105},
		{PID: 101, PPID: 100, StartTime: 202, Comm: tmuxTestProgram, Program: tmuxTestProgram, Pgrp: 101, State: "T", TPGID: 105},
		{PID: 105, PPID: 100, StartTime: 300, Comm: tmuxTestProgram, Program: tmuxTestProgram, Pgrp: 105, State: "S", TPGID: 105},
	}
	candidate := fixture.seedDelivery(t, 94)

	require.NoError(t, fixture.executor.EvaluateTmuxAttach(t.Context(), candidate))
	require.Zero(t, transport.writes)
	require.Equal(t, protocol.GuardOutcomeFail,
		guardOutcome(latestGuards(t, fixture.store.DB(), candidate.MessageID),
			protocol.GuardNameAdapterConnected),
		"the registered CLI is stopped; the CLI in front is not the registered one")
}
