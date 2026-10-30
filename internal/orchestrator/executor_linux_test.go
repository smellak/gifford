//go:build linux

package orchestrator

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/creack/pty"
	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/agentsock"
	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/daemon"
	"github.com/smellak/gifford/internal/daemon/agentlink"
	"github.com/smellak/gifford/internal/profiles"
	"github.com/smellak/gifford/internal/protocol"
	proxysession "github.com/smellak/gifford/internal/proxy/session"
	"github.com/smellak/gifford/internal/registry"
)

type integrationGuard struct {
	report  protocol.InputGuardReport
	changed chan struct{}
}

func newIntegrationGuard() *integrationGuard {
	return &integrationGuard{
		report: protocol.InputGuardReport{
			DraftState: protocol.DraftStateClosed, ActivityState: protocol.ActivityStateIdle,
			DraftGeneration: 2, InputGuardVersion: 3,
		},
		changed: make(chan struct{}, 1),
	}
}

func (g *integrationGuard) InputGuard() protocol.InputGuardReport { return g.report }
func (g *integrationGuard) GuardChanged() <-chan struct{}         { return g.changed }

type integrationDelivery struct {
	mu             sync.Mutex
	prepares       int
	commits        int
	cancels        int
	lines          []string
	prepareResult  protocol.DeliveryResultKind
	commitResult   protocol.DeliveryResultKind
	prepareHook    func()
	commitHook     func()
	lastGeneration uint64
	lastVersion    int64
	firstBlock     chan struct{}
}

func (*integrationDelivery) CheckLine(string) error { return nil }

func (d *integrationDelivery) Prepare(
	_ protocol.OpaqueID,
	generation uint64,
	version int64,
) (agentsock.DeliveryReport, error) {
	d.mu.Lock()
	d.prepares++
	d.lastGeneration = generation
	d.lastVersion = version
	result := d.prepareResult
	hook := d.prepareHook
	d.mu.Unlock()
	if hook != nil {
		hook()
	}
	if result == "" {
		result = protocol.DeliveryResultKindPrepared
	}
	return agentsock.DeliveryReport{
		Result: result, Generation: generation, Version: version,
	}, nil
}

func (d *integrationDelivery) Commit(
	_ protocol.OpaqueID,
	line string,
	_ protocol.InjectMode,
) (agentsock.DeliveryReport, error) {
	d.mu.Lock()
	d.commits++
	commit := d.commits
	result := d.commitResult
	hook := d.commitHook
	if result == "" {
		result = protocol.DeliveryResultKindCommitted
	}
	if result == protocol.DeliveryResultKindCommitted {
		d.lines = append(d.lines, line)
	}
	block := d.firstBlock
	d.mu.Unlock()
	if hook != nil {
		hook()
	}
	if commit == 1 && block != nil {
		<-block
	}
	return agentsock.DeliveryReport{
		Result: result, Generation: 2, Version: 3,
	}, nil
}

func (d *integrationDelivery) Cancel(
	protocol.OpaqueID,
	protocol.CancelReason,
) (agentsock.DeliveryReport, error) {
	d.mu.Lock()
	d.cancels++
	d.mu.Unlock()
	return agentsock.DeliveryReport{Result: protocol.DeliveryResultKindCancelled}, nil
}

func (*integrationDelivery) DefaultMode() protocol.InjectMode { return protocol.InjectModeTyped }
func (*integrationDelivery) InputHolds(string) bool           { return false }

func (d *integrationDelivery) commitCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.commits
}

func (d *integrationDelivery) prepareCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.prepares
}

func (d *integrationDelivery) cancelCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.cancels
}

func (d *integrationDelivery) preparedGuard() (uint64, int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.lastGeneration, d.lastVersion
}

type integrationLinks struct {
	client *agentlink.Client
}

type realSessionSink struct {
	mu     sync.Mutex
	events []proxysession.Event
}

func (s *realSessionSink) Emit(event proxysession.Event) {
	s.mu.Lock()
	s.events = append(s.events, event)
	s.mu.Unlock()
}

func (s *realSessionSink) count(kind protocol.EventType) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	count := 0
	for _, event := range s.events {
		if event.Type == kind {
			count++
		}
	}
	return count
}

// humanPrioritySessionDelivery injects a real human key after the Core has
// fixed the prepare's expected generation, then delegates every operation to
// internal/proxy/session. The ABORTED_HUMAN_PRIORITY result is produced by
// the real session state machine, not by this interposer.
type humanPrioritySessionDelivery struct {
	inner   agentsock.Delivery
	session *proxysession.Session
	outer   *os.File

	mu      sync.Mutex
	commits int
}

func (d *humanPrioritySessionDelivery) CheckLine(line string) error {
	return d.inner.CheckLine(line)
}

func (d *humanPrioritySessionDelivery) Prepare(
	id protocol.OpaqueID,
	generation uint64,
	version int64,
) (agentsock.DeliveryReport, error) {
	if _, err := d.outer.Write([]byte("h")); err != nil {
		return agentsock.DeliveryReport{}, err
	}
	deadline := time.Now().Add(2 * time.Second)
	for d.session.Generation() == generation && d.session.GuardVersion() == version && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	return d.inner.Prepare(id, generation, version)
}

func (d *humanPrioritySessionDelivery) Commit(
	id protocol.OpaqueID,
	line string,
	mode protocol.InjectMode,
) (agentsock.DeliveryReport, error) {
	d.mu.Lock()
	d.commits++
	d.mu.Unlock()
	return d.inner.Commit(id, line, mode)
}

func (d *humanPrioritySessionDelivery) Cancel(
	id protocol.OpaqueID,
	reason protocol.CancelReason,
) (agentsock.DeliveryReport, error) {
	return d.inner.Cancel(id, reason)
}

func (d *humanPrioritySessionDelivery) DefaultMode() protocol.InjectMode {
	return d.inner.DefaultMode()
}

func (d *humanPrioritySessionDelivery) InputHolds(line string) bool {
	return d.inner.InputHolds(line)
}

func (d *humanPrioritySessionDelivery) commitCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.commits
}

func (l integrationLinks) Snapshot(agent protocol.AgentID) (agentlink.Snapshot, bool) {
	if l.client.Snapshot().AgentID != agent {
		return agentlink.Snapshot{}, false
	}
	return l.client.Snapshot(), true
}

func (l integrationLinks) Client(agent protocol.AgentID) (*agentlink.Client, bool) {
	if l.client.Snapshot().AgentID != agent {
		return nil, false
	}
	return l.client, true
}

func attachIntegrationLink(
	t *testing.T,
	fixture *executorFixture,
	delivery *integrationDelivery,
	callTimeout time.Duration,
) *Executor {
	t.Helper()
	process, err := registry.CurrentProcess()
	require.NoError(t, err)
	processJSON, err := json.Marshal(process)
	require.NoError(t, err)
	fixture.processes.mu.Lock()
	fixture.processes.tree.Root = ProcessToken{
		PID: process.PID, StartTime: process.ProcessStartTime,
	}
	fixture.processes.mu.Unlock()
	var manifestJSON string
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(),
		"SELECT manifest_json FROM sessions WHERE session_id = ?", executorTestSession).Scan(&manifestJSON))
	var manifest protocol.CapabilityManifest
	require.NoError(t, json.Unmarshal([]byte(manifestJSON), &manifest))
	_, err = fixture.store.DB().ExecContext(t.Context(), `UPDATE sessions SET process_json = ?
		WHERE session_id = ?`, string(processJSON), executorTestSession)
	require.NoError(t, err)

	runtimeDir, err := os.MkdirTemp("", "gifford-executor-") //nolint:usetesting // Unix socket paths have a small fixed limit.
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(runtimeDir)) })
	server, err := agentsock.Listen(t.Context(), agentsock.Options{
		RuntimeDir: runtimeDir,
		Register:   protocol.RegisterAgent{Manifest: manifest, Process: &process},
		Guard:      newIntegrationGuard(), Delivery: delivery, Clock: agentsock.SystemClock{},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, server.Close()) })
	writer, err := agentlink.NewInputGuardStore(fixture.store.DB(), fixture.clock)
	require.NoError(t, err)
	client, err := agentlink.Start(t.Context(), agentlink.Options{
		RuntimeDir: runtimeDir, DB: fixture.store.DB(), AgentID: executorTestAgent,
		SessionID: executorTestSession, Writer: writer, Clock: agentlink.SystemClock{},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	require.Eventually(t, func() bool {
		snapshot := client.Snapshot()
		return snapshot.State == agentlink.StateUp && snapshot.HasReport
	}, 5*time.Second, time.Millisecond)

	executor, err := NewExecutor(ExecutorOptions{
		DB: fixture.store.DB(), Channel: fixture.executor.channel,
		Approvals: fixture.controller, Links: integrationLinks{client: client},
		Clock: fixture.clock, Processes: fixture.processes,
		Entropy: fixture.executor.entropy, CallTimeout: callTimeout,
	})
	require.NoError(t, err)
	return executor
}

type realSessionRun struct {
	executor *Executor
	delivery *humanPrioritySessionDelivery
	sink     *realSessionSink
	output   string
}

func attachRealSessionLink(t *testing.T, fixture *executorFixture) realSessionRun {
	t.Helper()
	outer, terminal, err := pty.Open()
	require.NoError(t, err)
	require.NoError(t, pty.Setsize(terminal, &pty.Winsize{Cols: 80, Rows: 24}))

	profileSet, err := profiles.Load()
	require.NoError(t, err)
	profile, err := profileSet.Profile("codex", "1.0.0")
	require.NoError(t, err)
	harnessDir := t.TempDir()
	output := filepath.Join(harnessDir, "harness.out")
	script := filepath.Join(harnessDir, "raw-harness")
	require.NoError(t, os.WriteFile(script, []byte("#!/bin/sh\nOUT=\"$1\"\n: > \"$OUT\"\nstty -icanon -echo min 1 time 0\nprintf 'real-ready\\n'\nexec cat >> \"$OUT\"\n"), 0o700))
	sink := &realSessionSink{}
	realSession, err := proxysession.NewSession(proxysession.Options{
		Command: []string{script, output}, In: terminal, Out: terminal,
		DataDir: t.TempDir(), SessionID: "real-session", Profile: profile,
		Sink: sink, RecordTo: io.Discard, Notice: io.Discard,
	})
	require.NoError(t, err)
	runCtx, cancelRun := context.WithCancel(t.Context())
	runDone := make(chan error, 1)
	go func() {
		_, runErr := realSession.Run(runCtx)
		runDone <- runErr
	}()
	require.Eventually(t, func() bool {
		_, statErr := os.Stat(output)
		return statErr == nil
	}, 5*time.Second, time.Millisecond, "real harness did not start")

	process, err := registry.CurrentProcess()
	require.NoError(t, err)
	processJSON, err := json.Marshal(process)
	require.NoError(t, err)
	fixture.processes.mu.Lock()
	fixture.processes.tree.Root = ProcessToken{
		PID: process.PID, StartTime: process.ProcessStartTime,
	}
	fixture.processes.mu.Unlock()
	var manifestJSON string
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(),
		"SELECT manifest_json FROM sessions WHERE session_id = ?", executorTestSession).Scan(&manifestJSON))
	var manifest protocol.CapabilityManifest
	require.NoError(t, json.Unmarshal([]byte(manifestJSON), &manifest))
	_, err = fixture.store.DB().ExecContext(t.Context(), `UPDATE sessions SET process_json = ?
		WHERE session_id = ?`, string(processJSON), executorTestSession)
	require.NoError(t, err)

	runtimeDir, err := os.MkdirTemp("", "gifford-real-session-") //nolint:usetesting // Unix socket paths have a small fixed limit.
	require.NoError(t, err)
	delivery := &humanPrioritySessionDelivery{
		inner: realSession.SocketDelivery(), session: realSession, outer: outer,
	}
	server, err := agentsock.Listen(t.Context(), agentsock.Options{
		RuntimeDir: runtimeDir,
		Register:   protocol.RegisterAgent{Manifest: manifest, Process: &process},
		Guard:      realSession, Delivery: delivery, Clock: agentsock.SystemClock{},
	})
	require.NoError(t, err)
	writer, err := agentlink.NewInputGuardStore(fixture.store.DB(), fixture.clock)
	require.NoError(t, err)
	client, err := agentlink.Start(t.Context(), agentlink.Options{
		RuntimeDir: runtimeDir, DB: fixture.store.DB(), AgentID: executorTestAgent,
		SessionID: executorTestSession, Writer: writer, Clock: agentlink.SystemClock{},
	})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		snapshot := client.Snapshot()
		return snapshot.State == agentlink.StateUp && snapshot.HasReport &&
			snapshot.LastReport.DraftState == protocol.DraftStateClosed
	}, 5*time.Second, time.Millisecond)
	executor, err := NewExecutor(ExecutorOptions{
		DB: fixture.store.DB(), Channel: fixture.executor.channel,
		Approvals: fixture.controller, Links: integrationLinks{client: client},
		Clock: fixture.clock, Processes: fixture.processes,
		Entropy: fixture.executor.entropy, CallTimeout: 2 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, client.Close())
		require.NoError(t, server.Close())
		cancelRun()
		_ = outer.Close()
		_ = terminal.Close()
		select {
		case runErr := <-runDone:
			if runErr != nil && !errors.Is(runErr, context.Canceled) {
				t.Errorf("real proxy session stopped: %v", runErr)
			}
		case <-time.After(10 * time.Second):
			t.Error("real proxy session did not stop")
		}
		require.NoError(t, os.RemoveAll(runtimeDir))
	})
	return realSessionRun{executor: executor, delivery: delivery, sink: sink, output: output}
}

func pendingApproval(t *testing.T, fixture *executorFixture) protocol.ApprovalRequest {
	t.Helper()
	list, err := fixture.controller.Approvals(t.Context(), protocol.ListApprovalsParams{
		State: protocol.ApprovalStatePending, AgentID: executorTestAgent,
	})
	require.NoError(t, err)
	require.Len(t, list.Approvals, 1)
	return list.Approvals[0]
}

func TestManualApprovalDeliversThroughRealAgentSocket(t *testing.T) {
	fixture := newExecutorFixture(t)
	delivery := &integrationDelivery{}
	executor := attachIntegrationLink(t, fixture, delivery, 2*time.Second)
	candidate := fixture.seedDelivery(t, 30)
	require.NoError(t, executor.EvaluateAutomatic(t.Context(), candidate))
	require.Zero(t, delivery.commitCount(), "automatic evaluation must never inject")
	commitState := make(chan [2]string, 1)
	delivery.commitHook = func() {
		var states [2]string
		if err := fixture.store.DB().QueryRowContext(t.Context(), `SELECT da.state, dq.state
			FROM delivery_attempts AS da JOIN delivery_queue AS dq
			ON dq.message_id = da.message_id AND dq.agent_id = da.agent_id
			AND dq.delivery_generation = da.delivery_generation
			WHERE da.message_id = ? AND da.agent_id = ?`, candidate.MessageID, candidate.AgentID).
			Scan(&states[0], &states[1]); err != nil {
			states[0] = "ERROR"
		}
		commitState <- states
	}

	approval := pendingApproval(t, fixture)
	resolved, err := executor.ResolveApproval(t.Context(), protocol.ResolveApprovalParams{
		ApprovalID: approval.ApprovalID, Option: executorTestDeliverNow,
	}, channel.Actor{Type: protocol.ActorTypeHuman, ID: executorTestOperator})
	require.NoError(t, err)
	require.Equal(t, protocol.ApprovalStateApproved, resolved.State)
	require.Equal(t, executorTestOperator, resolved.ResolvedBy)
	require.Equal(t, "human selected deliver_now", resolved.Resolution.Reason)
	require.Equal(t, 1, delivery.commitCount())
	require.Equal(t, [2]string{string(daemon.AttemptCommitSent), queueStateDelivering}, <-commitState,
		"COMMIT_SENT and DELIVERING must be visible before the commit frame reaches Proxy")
	generation, version := delivery.preparedGuard()
	require.Equal(t, uint64(2), generation)
	require.Equal(t, int64(3), version)
	replayed, err := executor.ResolveApproval(t.Context(), protocol.ResolveApprovalParams{
		ApprovalID: approval.ApprovalID, Option: executorTestDeliverNow,
	}, channel.Actor{Type: protocol.ActorTypeHuman, ID: executorTestOperator})
	require.NoError(t, err)
	require.Equal(t, resolved, replayed)
	require.Equal(t, 1, delivery.commitCount(), "approval replay must not create another delivery")

	var queueState, attemptState string
	var channelDecision string
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT state
		FROM delivery_queue WHERE message_id = ? AND agent_id = ?`, candidate.MessageID, candidate.AgentID).
		Scan(&queueState))
	require.Equal(t, "DELIVERED", queueState)
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT state,
		channel_decision_id FROM delivery_attempts WHERE message_id = ? AND agent_id = ?`,
		candidate.MessageID, candidate.AgentID).Scan(&attemptState, &channelDecision))
	require.Equal(t, string(daemon.AttemptCommitted), attemptState)
	require.NotEmpty(t, channelDecision)
}

func TestAutomaticDeliveryNeverCallsProxyAbortPaths(t *testing.T) {
	configs := []*integrationDelivery{
		{prepareResult: protocol.DeliveryResultKindAbortedHumanPriority},
		{commitResult: protocol.DeliveryResultKindAbortedHumanPriority},
	}
	for index := range configs {
		fixture := newExecutorFixture(t)
		delivery := configs[index]
		executor := attachIntegrationLink(t, fixture, delivery, 2*time.Second)
		candidate := fixture.seedDelivery(t, 70+index)
		require.NoError(t, executor.EvaluateAutomatic(t.Context(), candidate))
		require.Zero(t, delivery.prepareCount(), "automatic evaluation must stop before prepare")
		require.Zero(t, delivery.commitCount(), "automatic evaluation must stop before commit")
		var queueState string
		require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT state
			FROM delivery_queue WHERE message_id = ? AND agent_id = ?`, candidate.MessageID, candidate.AgentID).
			Scan(&queueState))
		require.Equal(t, "QUEUED", queueState)
	}
}

func TestProxyHumanPriorityAbortDefersWithoutUncertainty(t *testing.T) {
	tests := []struct {
		delivery   *integrationDelivery
		wantCommit int
	}{
		{delivery: &integrationDelivery{prepareResult: protocol.DeliveryResultKindAbortedHumanPriority}},
		{delivery: &integrationDelivery{commitResult: protocol.DeliveryResultKindAbortedHumanPriority}, wantCommit: 1},
	}
	for index := range tests {
		fixture := newExecutorFixture(t)
		delivery := tests[index].delivery
		executor := attachIntegrationLink(t, fixture, delivery, 2*time.Second)
		candidate := fixture.seedDelivery(t, 80+index)
		require.NoError(t, executor.EvaluateAutomatic(t.Context(), candidate))
		approval := pendingApproval(t, fixture)
		resolved, err := executor.ResolveApproval(t.Context(), protocol.ResolveApprovalParams{
			ApprovalID: approval.ApprovalID, Option: executorTestDeliverNow,
		}, channel.Actor{Type: protocol.ActorTypeHuman, ID: executorTestOperator})
		require.NoError(t, err)
		require.Equal(t, protocol.ApprovalStateApproved, resolved.State)
		require.Equal(t, 1, delivery.prepareCount())
		require.Equal(t, tests[index].wantCommit, delivery.commitCount())

		var queueState, attemptState string
		require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT state
			FROM delivery_queue WHERE message_id = ? AND agent_id = ?`, candidate.MessageID, candidate.AgentID).
			Scan(&queueState))
		require.Equal(t, "QUEUED", queueState)
		require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT state
			FROM delivery_attempts WHERE message_id = ? AND agent_id = ?`,
			candidate.MessageID, candidate.AgentID).Scan(&attemptState))
		require.Equal(t, string(daemon.AttemptAbortedHumanPriority), attemptState)
		var uncertain int64
		require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT count(*)
			FROM delivery_attempts WHERE message_id = ? AND state = 'UNCERTAIN'`, candidate.MessageID).
			Scan(&uncertain))
		require.Zero(t, uncertain)
	}
}

func TestRealProxySessionHumanPriorityAbortDefersWithZeroDeliveryBytes(t *testing.T) {
	fixture := newExecutorFixture(t)
	run := attachRealSessionLink(t, fixture)
	candidate := fixture.seedDelivery(t, 89)
	require.NoError(t, run.executor.EvaluateAutomatic(t.Context(), candidate))
	approval := pendingApproval(t, fixture)
	resolved, err := run.executor.ResolveApproval(t.Context(), protocol.ResolveApprovalParams{
		ApprovalID: approval.ApprovalID, Option: executorTestDeliverNow,
	}, channel.Actor{Type: protocol.ActorTypeHuman, ID: executorTestOperator})
	require.NoError(t, err)
	require.Equal(t, protocol.ApprovalStateApproved, resolved.State)
	require.Zero(t, run.delivery.commitCount())

	var attemptState, queueState string
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT state
		FROM delivery_attempts WHERE message_id = ? AND agent_id = ?`,
		candidate.MessageID, candidate.AgentID).Scan(&attemptState))
	require.Equal(t, string(daemon.AttemptAbortedHumanPriority), attemptState)
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT state
		FROM delivery_queue WHERE message_id = ? AND agent_id = ?`,
		candidate.MessageID, candidate.AgentID).Scan(&queueState))
	require.Equal(t, queueStateQueued, queueState)
	var uncertain int64
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT count(*)
		FROM delivery_attempts WHERE message_id = ? AND state = 'UNCERTAIN'`,
		candidate.MessageID).Scan(&uncertain))
	require.Zero(t, uncertain)
	require.Zero(t, run.sink.count(protocol.EventTypeInputDeliveryStarted))
	require.Eventually(t, func() bool {
		contents, readErr := os.ReadFile(run.output)
		return readErr == nil && string(contents) == "h"
	}, 5*time.Second, time.Millisecond, "the real harness did not receive the human key")
	contents, err := os.ReadFile(run.output)
	require.NoError(t, err)
	require.NotContains(t, strings.ToLower(string(contents)), "gifford",
		"no delivery notice byte may reach the real PTY")
}

func TestPostPrepareHardGuardsBlockCommit(t *testing.T) {
	tests := []struct {
		name      string
		wantQueue string
		prepare   func(*executorFixture)
	}{
		{
			name:      "new harness descendant",
			wantQueue: "QUEUED",
			prepare: func(fixture *executorFixture) {
				fixture.processes.mu.Lock()
				fixture.processes.tree.Descendants = append(
					fixture.processes.tree.Descendants,
					ProcessToken{PID: 500, StartTime: 600},
				)
				fixture.processes.mu.Unlock()
			},
		},
		{
			name:      "process tree unreadable",
			wantQueue: "QUEUED",
			prepare: func(fixture *executorFixture) {
				fixture.processes.mu.Lock()
				fixture.processes.err = os.ErrPermission
				fixture.processes.mu.Unlock()
			},
		},
		{
			name:      "delivery expired",
			wantQueue: "EXPIRED",
			prepare: func(fixture *executorFixture) {
				fixture.clock.mu.Lock()
				fixture.clock.now = fixture.clock.now.Add(2 * time.Hour)
				fixture.clock.mu.Unlock()
			},
		},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newExecutorFixture(t)
			delivery := &integrationDelivery{}
			delivery.prepareHook = func() { test.prepare(fixture) }
			executor := attachIntegrationLink(t, fixture, delivery, 2*time.Second)
			candidate := fixture.seedDelivery(t, 90+index)
			require.NoError(t, executor.EvaluateAutomatic(t.Context(), candidate))

			approval := pendingApproval(t, fixture)
			resolved, err := executor.ResolveApproval(t.Context(), protocol.ResolveApprovalParams{
				ApprovalID: approval.ApprovalID, Option: executorTestDeliverNow,
			}, channel.Actor{Type: protocol.ActorTypeHuman, ID: executorTestOperator})
			require.NoError(t, err)
			require.Equal(t, protocol.ApprovalStateApproved, resolved.State)
			require.Equal(t, 1, delivery.prepareCount())
			require.Zero(t, delivery.commitCount())
			require.Equal(t, 1, delivery.cancelCount())

			var attemptState, terminalReason, queueState string
			require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT state,
				terminal_reason FROM delivery_attempts WHERE message_id = ? AND agent_id = ?`,
				candidate.MessageID, candidate.AgentID).Scan(&attemptState, &terminalReason))
			require.Equal(t, string(daemon.AttemptCancelled), attemptState)
			require.Equal(t, string(protocol.CancelReasonStateChanged), terminalReason)
			require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT state
				FROM delivery_queue WHERE message_id = ? AND agent_id = ?`,
				candidate.MessageID, candidate.AgentID).Scan(&queueState))
			require.Equal(t, test.wantQueue, queueState)
			var uncertain int64
			require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT count(*)
				FROM delivery_attempts WHERE message_id = ? AND state = 'UNCERTAIN'`, candidate.MessageID).
				Scan(&uncertain))
			require.Zero(t, uncertain)
		})
	}
}

func TestControlOffBetweenPrepareAndCommitCancelsWithoutCommit(t *testing.T) {
	fixture := newExecutorFixture(t)
	delivery := &integrationDelivery{}
	executor := attachIntegrationLink(t, fixture, delivery, 2*time.Second)
	candidate := fixture.seedDelivery(t, 105)
	hookErr := make(chan error, 1)
	delivery.prepareHook = func() {
		_, err := fixture.controller.SetGlobal(t.Context(), protocol.SetGlobalParams{
			Mode: protocol.ChannelModeOff, ExpectedVersion: 0,
		}, channel.Actor{Type: protocol.ActorTypeHuman, ID: executorTestOperator})
		hookErr <- err
	}
	require.NoError(t, executor.EvaluateAutomatic(t.Context(), candidate))
	approval := pendingApproval(t, fixture)
	resolved, err := executor.ResolveApproval(t.Context(), protocol.ResolveApprovalParams{
		ApprovalID: approval.ApprovalID, Option: executorTestDeliverNow,
	}, channel.Actor{Type: protocol.ActorTypeHuman, ID: executorTestOperator})
	require.NoError(t, err)
	require.NoError(t, <-hookErr)
	require.Equal(t, protocol.ApprovalStateApproved, resolved.State)
	require.Equal(t, 1, delivery.prepareCount())
	require.Zero(t, delivery.commitCount())
	require.Equal(t, 1, delivery.cancelCount())

	var attemptState, reason, queueState string
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT state, terminal_reason
		FROM delivery_attempts WHERE message_id = ? AND agent_id = ?`,
		candidate.MessageID, candidate.AgentID).Scan(&attemptState, &reason))
	require.Equal(t, string(daemon.AttemptCancelled), attemptState)
	require.Equal(t, string(daemon.ReasonControlChanged), reason)
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT state
		FROM delivery_queue WHERE message_id = ? AND agent_id = ?`,
		candidate.MessageID, candidate.AgentID).Scan(&queueState))
	require.Equal(t, queueStateQueued, queueState)
}

func TestUnreadableProcessAfterPreparePermanentlyBlocksSession(t *testing.T) {
	fixture := newExecutorFixture(t)
	delivery := &integrationDelivery{}
	delivery.prepareHook = func() {
		fixture.processes.mu.Lock()
		fixture.processes.err = os.ErrPermission
		fixture.processes.mu.Unlock()
	}
	executor := attachIntegrationLink(t, fixture, delivery, 2*time.Second)
	candidate := fixture.seedDelivery(t, 107)
	require.NoError(t, executor.EvaluateAutomatic(t.Context(), candidate))

	approval := pendingApproval(t, fixture)
	resolved, err := executor.ResolveApproval(t.Context(), protocol.ResolveApprovalParams{
		ApprovalID: approval.ApprovalID, Option: executorTestDeliverNow,
	}, channel.Actor{Type: protocol.ActorTypeHuman, ID: executorTestOperator})
	require.NoError(t, err)
	require.Equal(t, protocol.ApprovalStateApproved, resolved.State)
	require.Equal(t, 1, delivery.prepareCount())
	require.Zero(t, delivery.commitCount())
	require.Equal(t, 1, delivery.cancelCount())
	state, reason := storedProcessBinding(t, fixture.store.DB())
	require.Equal(t, processBindingStateBlocked, state)
	require.Equal(t, "OBSERVATION_FAILED", reason)

	fixture.processes.mu.Lock()
	fixture.processes.err = nil
	fixture.processes.mu.Unlock()
	replacement := replacementExecutor(t, fixture)
	next := fixture.seedDelivery(t, 108)
	require.NoError(t, replacement.EvaluateAutomatic(t.Context(), next))
	nextApproval := pendingApproval(t, fixture)
	deferred, err := replacement.ResolveApproval(t.Context(), protocol.ResolveApprovalParams{
		ApprovalID: nextApproval.ApprovalID, Option: executorTestDeliverNow,
	}, channel.Actor{Type: protocol.ActorTypeHuman, ID: executorTestOperator})
	require.ErrorIs(t, err, ErrDeliveryDeferred)
	require.Equal(t, protocol.ApprovalStatePending, deferred.State)
	require.Equal(t, 1, delivery.prepareCount())
	require.Zero(t, delivery.commitCount())
	state, reason = storedProcessBinding(t, fixture.store.DB())
	require.Equal(t, processBindingStateBlocked, state)
	require.Equal(t, "OBSERVATION_FAILED", reason)
}

func TestTerminalQueueRaceDuringPreparePreservesFailedAndFinishesAttempt(t *testing.T) {
	fixture := newExecutorFixture(t)
	delivery := &integrationDelivery{}
	executor := attachIntegrationLink(t, fixture, delivery, 2*time.Second)
	candidate := fixture.seedDelivery(t, 106)
	hookErr := make(chan error, 1)
	delivery.prepareHook = func() {
		_, err := fixture.store.DB().ExecContext(t.Context(), `UPDATE delivery_queue SET state = 'FAILED'
			WHERE message_id = ? AND agent_id = ? AND delivery_generation = ?`,
			candidate.MessageID, candidate.AgentID, candidate.DeliveryGeneration)
		hookErr <- err
	}
	require.NoError(t, executor.EvaluateAutomatic(t.Context(), candidate))
	approval := pendingApproval(t, fixture)
	_, err := executor.ResolveApproval(t.Context(), protocol.ResolveApprovalParams{
		ApprovalID: approval.ApprovalID, Option: executorTestDeliverNow,
	}, channel.Actor{Type: protocol.ActorTypeHuman, ID: executorTestOperator})
	require.NoError(t, err)
	require.NoError(t, <-hookErr)
	require.Equal(t, 1, delivery.prepareCount())
	require.Zero(t, delivery.commitCount())
	require.Equal(t, 1, delivery.cancelCount())

	var attemptState, queueState string
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT state
		FROM delivery_attempts WHERE message_id = ? AND agent_id = ?`,
		candidate.MessageID, candidate.AgentID).Scan(&attemptState))
	require.Equal(t, string(daemon.AttemptCancelled), attemptState,
		"the terminal queue race must not roll back and strand PREPARED")
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT state
		FROM delivery_queue WHERE message_id = ? AND agent_id = ?`,
		candidate.MessageID, candidate.AgentID).Scan(&queueState))
	require.Equal(t, queueStateFailed, queueState)
}

func TestCommitTimeoutRequiresExplicitRecoveryApproval(t *testing.T) {
	fixture := newExecutorFixture(t)
	block := make(chan struct{})
	delivery := &integrationDelivery{firstBlock: block}
	executor := attachIntegrationLink(t, fixture, delivery, 50*time.Millisecond)
	t.Cleanup(func() {
		select {
		case <-block:
		default:
			close(block)
		}
	})
	candidate := fixture.seedDelivery(t, 31)
	require.NoError(t, executor.EvaluateAutomatic(t.Context(), candidate))
	first := pendingApproval(t, fixture)
	_, err := executor.ResolveApproval(t.Context(), protocol.ResolveApprovalParams{
		ApprovalID: first.ApprovalID, Option: executorTestDeliverNow,
	}, channel.Actor{Type: protocol.ActorTypeHuman, ID: executorTestOperator})
	require.NoError(t, err)

	var uncertainID protocol.OpaqueID
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT delivery_id
		FROM delivery_attempts WHERE message_id = ? AND state = 'UNCERTAIN'`, candidate.MessageID).
		Scan(&uncertainID))
	second := pendingApproval(t, fixture)
	require.NotEqual(t, first.ApprovalID, second.ApprovalID)
	require.Contains(t, second.RequestNote, string(uncertainID))
	require.Contains(t, second.RequestNote, "may already have executed")

	close(block)
	client := executor.links.(integrationLinks).client
	deadline := time.After(2 * time.Second)
	lateCommitted := false
	for !lateCommitted {
		select {
		case result := <-client.DeliveryResults():
			lateCommitted = result.Result.DeliveryID == uncertainID &&
				result.Result.Result == protocol.DeliveryResultKindCommitted
		case <-deadline:
			t.Fatal("timed-out commit did not finish at the Proxy")
		}
	}
	executor.timeout = 2 * time.Second
	recovered, err := executor.ResolveApproval(t.Context(), protocol.ResolveApprovalParams{
		ApprovalID: second.ApprovalID, Option: executorTestDeliverNow,
	}, channel.Actor{Type: protocol.ActorTypeHuman, ID: executorTestOperator})
	require.NoError(t, err)
	replayed, err := executor.ResolveApproval(t.Context(), protocol.ResolveApprovalParams{
		ApprovalID: second.ApprovalID, Option: executorTestDeliverNow,
	}, channel.Actor{Type: protocol.ActorTypeHuman, ID: executorTestOperator})
	require.NoError(t, err)
	require.Equal(t, recovered, replayed)
	require.Equal(t, 2, delivery.commitCount(), "recovery replay must not create a second successor")

	var uncertainState string
	var linkedApproval, resolutionID protocol.ApprovalID
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT state,
		uncertain_approval_id, resolution_approval_id FROM delivery_attempts WHERE delivery_id = ?`, uncertainID).
		Scan(&uncertainState, &linkedApproval, &resolutionID))
	require.Equal(t, string(daemon.AttemptUncertain), uncertainState)
	require.Equal(t, second.ApprovalID, linkedApproval)
	require.Equal(t, second.ApprovalID, resolutionID)
	var committed, recoveries int64
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT count(*)
		FROM delivery_attempts WHERE message_id = ? AND state = 'COMMITTED'`, candidate.MessageID).
		Scan(&committed))
	require.Equal(t, int64(1), committed)
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT count(*)
		FROM delivery_attempts WHERE recovery_of_delivery_id = ?`, uncertainID).Scan(&recoveries))
	require.Equal(t, int64(1), recoveries)

	tx, err := fixture.store.DB().BeginTx(t.Context(), nil)
	require.NoError(t, err)
	blocked, err := executor.attempts.HasUncertain(t.Context(), tx, executorTestAgent)
	require.NoError(t, err)
	require.False(t, blocked)
	require.NoError(t, tx.Rollback())
}

func TestLateCommittedEvidenceBlocksRecoveryResend(t *testing.T) {
	fixture := newExecutorFixture(t)
	block := make(chan struct{})
	delivery := &integrationDelivery{firstBlock: block}
	executor := attachIntegrationLink(t, fixture, delivery, 50*time.Millisecond)
	t.Cleanup(func() {
		select {
		case <-block:
		default:
			close(block)
		}
	})
	candidate := fixture.seedDelivery(t, 107)
	require.NoError(t, executor.EvaluateAutomatic(t.Context(), candidate))
	first := pendingApproval(t, fixture)
	_, err := executor.ResolveApproval(t.Context(), protocol.ResolveApprovalParams{
		ApprovalID: first.ApprovalID, Option: executorTestDeliverNow,
	}, channel.Actor{Type: protocol.ActorTypeHuman, ID: executorTestOperator})
	require.NoError(t, err)

	var uncertainID protocol.OpaqueID
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT delivery_id
		FROM delivery_attempts WHERE message_id = ? AND state = 'UNCERTAIN'`, candidate.MessageID).
		Scan(&uncertainID))
	recoveryApproval := pendingApproval(t, fixture)
	close(block)
	late := waitForDeliveryResult(t, executor.links.(integrationLinks).client,
		uncertainID, protocol.DeliveryResultKindCommitted)
	consumer, err := daemon.NewDeliveryResultConsumer(fixture.store.DB(), fixture.clock)
	require.NoError(t, err)
	inserted, err := consumer.Record(t.Context(), candidate.AgentID, executorTestSession, late)
	require.NoError(t, err)
	require.True(t, inserted)

	executor.timeout = 2 * time.Second
	request, err := executor.ResolveApproval(t.Context(), protocol.ResolveApprovalParams{
		ApprovalID: recoveryApproval.ApprovalID, Option: executorTestDeliverNow,
	}, channel.Actor{Type: protocol.ActorTypeHuman, ID: executorTestOperator})
	require.ErrorIs(t, err, ErrDeliveryDeferred)
	require.Equal(t, protocol.ApprovalStatePending, request.State)
	require.Equal(t, 1, delivery.commitCount(), "committed evidence must veto a recovery resend")
	var sourceState string
	var resolution sql.NullString
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT state,
		resolution_approval_id FROM delivery_attempts WHERE delivery_id = ?`, uncertainID).
		Scan(&sourceState, &resolution))
	require.Equal(t, string(daemon.AttemptUncertain), sourceState)
	require.False(t, resolution.Valid)
	require.Equal(t, protocol.GuardOutcomeFail,
		guardOutcome(latestGuards(t, fixture.store.DB(), candidate.MessageID),
			protocol.GuardNameNotDuplicate))
}

func TestReviewLateEvidenceRemainsAVetoAfterRecoveryCancellation(t *testing.T) {
	fixture := newExecutorFixture(t)
	block := make(chan struct{})
	delivery := &integrationDelivery{firstBlock: block}
	executor := attachIntegrationLink(t, fixture, delivery, 50*time.Millisecond)
	t.Cleanup(func() {
		select {
		case <-block:
		default:
			close(block)
		}
	})
	candidate := fixture.seedDelivery(t, 109)
	require.NoError(t, executor.EvaluateAutomatic(t.Context(), candidate))
	first := pendingApproval(t, fixture)
	_, err := executor.ResolveApproval(t.Context(), protocol.ResolveApprovalParams{
		ApprovalID: first.ApprovalID, Option: executorTestDeliverNow,
	}, channel.Actor{Type: protocol.ActorTypeHuman, ID: executorTestOperator})
	require.NoError(t, err)

	var uncertainID protocol.OpaqueID
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT delivery_id
		FROM delivery_attempts WHERE message_id = ? AND state = 'UNCERTAIN'`, candidate.MessageID).
		Scan(&uncertainID))
	recoveryApproval := pendingApproval(t, fixture)
	close(block)
	late := waitForDeliveryResult(t, executor.links.(integrationLinks).client,
		uncertainID, protocol.DeliveryResultKindCommitted)
	consumer, err := daemon.NewDeliveryResultConsumer(fixture.store.DB(), fixture.clock)
	require.NoError(t, err)
	recorded := make(chan error, 1)
	delivery.prepareHook = func() {
		_, recordErr := consumer.Record(t.Context(), candidate.AgentID, executorTestSession, late)
		recorded <- recordErr
	}

	executor.timeout = 2 * time.Second
	resolved, err := executor.ResolveApproval(t.Context(), protocol.ResolveApprovalParams{
		ApprovalID: recoveryApproval.ApprovalID, Option: executorTestDeliverNow,
	}, channel.Actor{Type: protocol.ActorTypeHuman, ID: executorTestOperator})
	require.NoError(t, err)
	require.Equal(t, protocol.ApprovalStateApproved, resolved.State)
	require.NoError(t, <-recorded)
	require.Equal(t, 1, delivery.commitCount(),
		"late evidence must cancel the prepared recovery before its commit")
	require.Equal(t, 1, delivery.cancelCount())

	var sourceState string
	var sourceResolution sql.NullString
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT state,
		resolution_approval_id FROM delivery_attempts WHERE delivery_id = ?`, uncertainID).
		Scan(&sourceState, &sourceResolution))
	require.Equal(t, string(daemon.AttemptUncertain), sourceState)
	require.True(t, sourceResolution.Valid)
	require.Equal(t, string(recoveryApproval.ApprovalID), sourceResolution.String)
	var recoveryState string
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT state
		FROM delivery_attempts WHERE recovery_of_delivery_id = ?`, uncertainID).Scan(&recoveryState))
	require.Equal(t, string(daemon.AttemptCancelled), recoveryState)

	require.NoError(t, executor.EvaluateAutomatic(t.Context(), candidate))
	ordinary := pendingApproval(t, fixture)
	require.NotEqual(t, recoveryApproval.ApprovalID, ordinary.ApprovalID)
	request, err := executor.ResolveApproval(t.Context(), protocol.ResolveApprovalParams{
		ApprovalID: ordinary.ApprovalID, Option: executorTestDeliverNow,
	}, channel.Actor{Type: protocol.ActorTypeHuman, ID: executorTestOperator})
	require.ErrorIs(t, err, ErrDeliveryDeferred)
	require.Equal(t, protocol.ApprovalStatePending, request.State)
	require.Equal(t, 1, delivery.commitCount(),
		"identity-wide COMMITTED evidence must veto every later resend")
	require.Equal(t, protocol.GuardOutcomeFail,
		guardOutcome(latestGuards(t, fixture.store.DB(), candidate.MessageID),
			protocol.GuardNameNotDuplicate))
}

func TestTimeoutSupersedesConcurrentOrdinaryPendingWithFreshRecoveryApproval(t *testing.T) {
	fixture := newExecutorFixture(t)
	block := make(chan struct{})
	delivery := &integrationDelivery{firstBlock: block}
	executor := attachIntegrationLink(t, fixture, delivery, 150*time.Millisecond)
	t.Cleanup(func() {
		select {
		case <-block:
		default:
			close(block)
		}
	})
	candidate := fixture.seedDelivery(t, 108)
	require.NoError(t, executor.EvaluateAutomatic(t.Context(), candidate))
	first := pendingApproval(t, fixture)
	resolved := make(chan error, 1)
	go func() {
		_, err := executor.ResolveApproval(t.Context(), protocol.ResolveApprovalParams{
			ApprovalID: first.ApprovalID, Option: executorTestDeliverNow,
		}, channel.Actor{Type: protocol.ActorTypeHuman, ID: executorTestOperator})
		resolved <- err
	}()
	require.Eventually(t, func() bool { return delivery.commitCount() == 1 },
		2*time.Second, time.Millisecond)
	tx, err := fixture.store.DB().BeginTx(t.Context(), nil)
	require.NoError(t, err)
	ordinary, created, err := fixture.controller.EnsureOrchestratorApproval(
		t.Context(), tx, candidate.MessageID, candidate.AgentID, "ordinary pending during commit")
	require.NoError(t, err)
	require.True(t, created)
	require.NoError(t, tx.Commit())
	require.NoError(t, <-resolved)

	storedOrdinary, err := fixture.controller.Approval(t.Context(), ordinary.ApprovalID)
	require.NoError(t, err)
	require.Equal(t, protocol.ApprovalStateSuperseded, storedOrdinary.State)
	fresh := pendingApproval(t, fixture)
	require.NotEqual(t, ordinary.ApprovalID, fresh.ApprovalID)
	var attemptState string
	var linked protocol.ApprovalID
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT state,
		uncertain_approval_id FROM delivery_attempts WHERE message_id = ? AND state = 'UNCERTAIN'`,
		candidate.MessageID).Scan(&attemptState, &linked))
	require.Equal(t, string(daemon.AttemptUncertain), attemptState)
	require.Equal(t, fresh.ApprovalID, linked)
}

func waitForDeliveryResult(
	t *testing.T,
	client *agentlink.Client,
	deliveryID protocol.OpaqueID,
	kind protocol.DeliveryResultKind,
) agentlink.DeliveryResult {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case result := <-client.DeliveryResults():
			if result.Result.DeliveryID == deliveryID && result.Result.Result == kind {
				return result
			}
		case <-deadline:
			t.Fatalf("delivery result %s/%s did not arrive", deliveryID, kind)
		}
	}
}
