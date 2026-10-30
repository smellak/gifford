//go:build linux

package orchestrator

import (
	"encoding/json"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/agentsock"
	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/daemon"
	"github.com/smellak/gifford/internal/daemon/agentlink"
	"github.com/smellak/gifford/internal/protocol"
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
	mu            sync.Mutex
	prepares      int
	commits       int
	cancels       int
	lines         []string
	prepareResult protocol.DeliveryResultKind
	commitResult  protocol.DeliveryResultKind
	prepareHook   func()
	firstBlock    chan struct{}
}

func (*integrationDelivery) CheckLine(string) error { return nil }

func (d *integrationDelivery) Prepare(
	_ protocol.OpaqueID,
	generation uint64,
	version int64,
) (agentsock.DeliveryReport, error) {
	d.mu.Lock()
	d.prepares++
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
	if result == "" {
		result = protocol.DeliveryResultKindCommitted
	}
	if result == protocol.DeliveryResultKindCommitted {
		d.lines = append(d.lines, line)
	}
	block := d.firstBlock
	d.mu.Unlock()
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

type integrationLinks struct {
	client *agentlink.Client
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

	approval := pendingApproval(t, fixture)
	resolved, err := executor.ResolveApproval(t.Context(), protocol.ResolveApprovalParams{
		ApprovalID: approval.ApprovalID, Option: executorTestDeliverNow,
	}, channel.Actor{Type: protocol.ActorTypeHuman, ID: executorTestOperator})
	require.NoError(t, err)
	require.Equal(t, protocol.ApprovalStateApproved, resolved.State)
	require.Equal(t, executorTestOperator, resolved.ResolvedBy)
	require.Equal(t, "human selected deliver_now", resolved.Resolution.Reason)
	require.Equal(t, 1, delivery.commitCount())
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
				fixture.processes.tokens = []ProcessToken{{PID: 500, StartTime: 600}}
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
