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
	tmuxTestInstance protocol.ULID       = "01JB0000000000000000000042"
	tmuxTestPane     protocol.TmuxPaneID = "%7"
	tmuxTestProgram                      = "codex"
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
	clients          []byte
	postSubmit       []byte
	writes           int
	stateBeforeWrite daemon.AttemptState
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
	if err := guard.Revalidate(ctx, request.Target, observation, t.clients, t.initial); err != nil {
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
	}
	sessions := &tmuxSessionStub{}
	inputGuards, err := agentlink.NewInputGuardStore(fixture.store.DB(), fixture.clock)
	require.NoError(t, err)
	processes := []procguard.Entry{
		{PID: 100, StartTime: 201, Comm: "runner", Program: "runner", Pgrp: 100, State: "S", TPGID: 101},
		{PID: 101, PPID: 100, StartTime: 202, Comm: tmuxTestProgram, Program: tmuxTestProgram, Pgrp: 101, State: "S", TPGID: 101},
	}
	processGuard := &procguard.Guard{}
	table := detect.Table{
		detect.Harness(protocol.HarnessCodex): {
			InputExpr: `[$#] ?(.*)$`, CLIBinaries: []string{tmuxTestProgram},
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
				return append([]procguard.Entry(nil), processes...), nil
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
	require.Equal(t, 2, registry.verified)

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
					BusyMarks: []string{"working"}, InputExpr: `[$#] ?(.*)$`,
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
