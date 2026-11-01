package orchestrator

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/daemon"
	"github.com/smellak/gifford/internal/daemon/agentlink"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

var executorTestTime = time.Date(2026, time.October, 30, 18, 30, 0, 0, time.UTC)

const (
	executorTestAgent               protocol.AgentID  = "agent-executor"
	executorTestSender              protocol.AgentID  = "agent-sender"
	executorTestSession             protocol.OpaqueID = "session-executor"
	executorTestNode                protocol.NodeID   = "node-executor"
	executorTestDeliverNow                            = "deliver_now"
	executorTestReject                                = "reject"
	executorTestOperator                              = "operator"
	executorTestBootID                                = "11111111-2222-3333-4444-555555555555"
	executorTestHarnessVersion                        = "1.0.0"
	executorTestHarnessExecutable                     = "/opt/gifford-test/codex"
	executorTestAuxiliaryExecutable                   = "/opt/gifford-test/codex-code-mode-host"
	executorTestToolExecutable                        = "/opt/gifford-test/tool"
)

type executorClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *executorClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *executorClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	now := c.now
	c.mu.Unlock()
	ready := make(chan time.Time, 1)
	ready <- now.Add(d)
	return ready
}

type executorLinks struct {
	mu       sync.Mutex
	snapshot agentlink.Snapshot
}

func (l *executorLinks) Snapshot(protocol.AgentID) (agentlink.Snapshot, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.snapshot, true
}

func (*executorLinks) Client(protocol.AgentID) (*agentlink.Client, bool) { return nil, false }

type executorProcesses struct {
	mu   sync.Mutex
	tree ProcessTree
	err  error
}

type executorEndpoint struct {
	mu      sync.Mutex
	agent   protocol.AgentID
	session protocol.OpaqueID
	pid     int64
	start   int64
	live    bool
}

func (e *executorEndpoint) RegisteredEndpointProcess(
	agent protocol.AgentID,
	session protocol.OpaqueID,
) (int64, int64, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.live || e.agent != agent || e.session != session {
		return 0, 0, false
	}
	return e.pid, e.start, true
}

func (p *executorProcesses) Inspect(protocol.ProcessIdentity) (ProcessTree, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	tree := p.tree
	tree.Descendants = append([]ProcessNode(nil), tree.Descendants...)
	return tree, p.err
}

func testProcessNode(token, parent ProcessToken, executable string) ProcessNode {
	return ProcessNode{Token: token, Parent: parent, Executable: executable}
}

type executorFixture struct {
	store      *eventstore.Store
	clock      *executorClock
	links      *executorLinks
	processes  *executorProcesses
	endpoint   *executorEndpoint
	controller *channel.Controller
	executor   *Executor
}

func newExecutorFixture(t *testing.T) *executorFixture {
	t.Helper()
	store, err := eventstore.Open(t.Context(), filepath.Join(t.TempDir(), "executor.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	clk := &executorClock{now: executorTestTime}
	engine, err := channel.New(channel.Options{Clock: clk})
	require.NoError(t, err)
	controller, err := channel.NewController(channel.ControllerOptions{
		DB: store.DB(), Engine: engine, Clock: clk, NodeID: executorTestNode,
		Entropy: bytes.NewReader(make([]byte, 4096)),
	})
	require.NoError(t, err)
	process := protocol.ProcessIdentity{PID: 101, ProcessStartTime: 202, UID: 303, GID: 404}
	manifest := protocol.CapabilityManifest{
		SchemaVersion: protocol.SchemaVersion, AgentID: executorTestAgent,
		Harness: protocol.HarnessCodex, HarnessVersion: executorTestHarnessVersion,
		AdapterType:        protocol.AdapterTypeGenericPTY,
		InputGuardProvider: protocol.InputGuardProviderPTYProxy,
		CaptureMode:        protocol.CaptureModePTYProxy, Deliverable: true,
		Capabilities: protocol.Capabilities{PTYCapture: true},
	}
	manifestJSON, err := json.Marshal(manifest)
	require.NoError(t, err)
	processJSON, err := json.Marshal(process)
	require.NoError(t, err)
	stamp := eventstore.FormatTime(clk.Now())
	_, err = store.DB().ExecContext(t.Context(), `INSERT INTO agents
		(agent_id, node_id, harness, created_at, updated_at) VALUES (?, ?, ?, ?, ?)`,
		executorTestAgent, executorTestNode, protocol.HarnessCodex, stamp, stamp)
	require.NoError(t, err)
	_, err = store.DB().ExecContext(t.Context(), `INSERT INTO sessions
		(session_id, agent_id, state, capture_mode, deliverable, manifest_json,
		 process_json, started_at, ended_at, updated_at)
		VALUES (?, ?, 'CONNECTED', 'pty_proxy', 1, ?, ?, ?, NULL, ?)`,
		executorTestSession, executorTestAgent, string(manifestJSON), string(processJSON), stamp, stamp)
	require.NoError(t, err)
	report := protocol.InputGuardReport{
		AgentID: executorTestAgent, SessionID: executorTestSession,
		DraftState: protocol.DraftStateClosed, ActivityState: protocol.ActivityStateIdle,
		DraftGeneration: 2, InputGuardVersion: 3,
		DetectionSource: protocol.InputGuardProviderPTYProxy, Confidence: 1,
	}
	guardStore, err := agentlink.NewInputGuardStore(store.DB(), clk)
	require.NoError(t, err)
	written, err := guardStore.WriteInputGuard(t.Context(), report)
	require.NoError(t, err)
	require.True(t, written)
	producer, err := NewStateProducer(StateProducerOptions{
		DB: store.DB(), Clock: clk, Channel: controller,
		Entropy: bytes.NewReader(make([]byte, 4096)),
	})
	require.NoError(t, err)
	require.NoError(t, producer.publishInitialStates(t.Context()))
	links := &executorLinks{snapshot: agentlink.Snapshot{
		AgentID: executorTestAgent, SessionID: executorTestSession,
		State: agentlink.StateUp, HasReport: true, LastReport: report,
	}}
	parent := ProcessToken{PID: process.PID, StartTime: process.ProcessStartTime}
	child := ProcessToken{PID: 102, StartTime: 203}
	processes := &executorProcesses{tree: ProcessTree{
		BootID: executorTestBootID, Root: parent,
		Descendants: []ProcessNode{testProcessNode(child, parent, executorTestHarnessExecutable)},
	}}
	endpoint := &executorEndpoint{}
	executor, err := NewExecutor(ExecutorOptions{
		DB: store.DB(), Channel: engine, Approvals: controller, Links: links,
		Clock: clk, Entropy: bytes.NewReader(make([]byte, 4096)), Processes: processes,
		Endpoints: endpoint,
	})
	require.NoError(t, err)
	return &executorFixture{
		store: store, clock: clk, links: links, processes: processes, endpoint: endpoint,
		controller: controller, executor: executor,
	}
}

func (f *executorFixture) seedDelivery(t *testing.T, sequence int) daemon.QueueDelivery {
	t.Helper()
	messageID := protocol.ULID(fmt.Sprintf("01ARZ3NDEKTSV4RRFFQ69G%04d", sequence))
	created := eventstore.FormatTime(f.clock.Now())
	expires := eventstore.FormatTime(f.clock.Now().Add(time.Hour))
	threadRef := fmt.Sprintf("thr_01ARZ3NDEKTSV4RRFFQ69G%04d", sequence)
	_, err := f.store.DB().ExecContext(t.Context(), `INSERT INTO messages
		(message_id, client_msg_id, origin_node, from_agent, destination_node,
		 thread_ref, thread_id, priority, state, created_at, expires_at, updated_at,
		 hop_count, route_digest)
		VALUES (?, ?, ?, ?, ?, ?, ?, 'NORMAL', 'PENDING', ?, ?, ?, 0, ?)`,
		messageID, fmt.Sprintf("client-%d", sequence), executorTestNode, executorTestSender,
		executorTestNode, threadRef, fmt.Sprintf("thread-%d", sequence), created, expires,
		created, fmt.Sprintf("sha256:route-%d", sequence))
	require.NoError(t, err)
	_, err = f.store.DB().ExecContext(t.Context(), `INSERT INTO delivery_queue
		(message_id, agent_id, delivery_generation, from_agent, thread_ref,
		 state, priority, created_at, expires_at, updated_at, hop_count, route_digest)
		VALUES (?, ?, 1, ?, ?, 'QUEUED', 2, ?, ?, ?, 0, ?)`,
		messageID, executorTestAgent, executorTestSender, threadRef, created, expires,
		created, fmt.Sprintf("sha256:route-%d", sequence))
	require.NoError(t, err)
	return daemon.QueueDelivery{MessageID: messageID, AgentID: executorTestAgent, DeliveryGeneration: 1}
}

func latestGuards(t *testing.T, db *sql.DB, message protocol.ULID) []protocol.GuardResult {
	t.Helper()
	var raw string
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT hard_guards_json
		FROM orchestrator_decisions WHERE message_id = ? ORDER BY created_at DESC, decision_id DESC LIMIT 1`,
		message).Scan(&raw))
	var guards []protocol.GuardResult
	require.NoError(t, json.Unmarshal([]byte(raw), &guards))
	return guards
}

func guardOutcome(guards []protocol.GuardResult, name protocol.GuardName) protocol.GuardOutcome {
	for _, result := range guards {
		if result.Guard == name {
			return result.Outcome
		}
	}
	return ""
}

func TestAutomaticEvaluationAlwaysDefersAndOpensOneApproval(t *testing.T) {
	t.Parallel()

	fixture := newExecutorFixture(t)
	wake := make(chan struct{}, 1)
	fixture.executor.notificationWake = wake
	candidate := fixture.seedDelivery(t, 1)
	require.NoError(t, fixture.executor.EvaluateAutomatic(t.Context(), candidate))
	require.NoError(t, fixture.executor.EvaluateAutomatic(t.Context(), candidate))
	require.Len(t, wake, 1, "approval commits use a coalescing nonblocking hint")

	var queueState string
	var attempts, durableAttempts, decisions, approvals int64
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT state, attempts
		FROM delivery_queue WHERE message_id = ? AND agent_id = ?`, candidate.MessageID, candidate.AgentID).
		Scan(&queueState, &attempts))
	require.Equal(t, "QUEUED", queueState)
	require.Zero(t, attempts)
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(),
		"SELECT count(*) FROM delivery_attempts").Scan(&durableAttempts))
	require.Zero(t, durableAttempts)
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(),
		"SELECT count(*) FROM orchestrator_decisions WHERE message_id = ?", candidate.MessageID).Scan(&decisions))
	require.Equal(t, int64(2), decisions)
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT count(*)
		FROM approval_requests WHERE object_id = ? AND agent_id = ? AND state = 'PENDING'`,
		candidate.MessageID, candidate.AgentID).Scan(&approvals))
	require.Equal(t, int64(1), approvals)
	guards := latestGuards(t, fixture.store.DB(), candidate.MessageID)
	require.Equal(t, protocol.GuardOutcomePass,
		guardOutcome(guards, protocol.GuardNameNoNewChildOrTool))
	require.Equal(t, protocol.GuardOutcomeFail,
		guardOutcome(guards, protocol.GuardNamePIdleThreshold))

	list, err := fixture.controller.Approvals(t.Context(), protocol.ListApprovalsParams{
		State: protocol.ApprovalStatePending, AgentID: candidate.AgentID,
	})
	require.NoError(t, err)
	require.Len(t, list.Approvals, 1)
	require.Equal(t, []protocol.ApprovalOption{executorTestDeliverNow, executorTestReject}, list.Approvals[0].Options)
}

func TestAutomaticEvaluationRollbackDoesNotWakeNotifications(t *testing.T) {
	t.Parallel()

	fixture := newExecutorFixture(t)
	wake := make(chan struct{}, 1)
	fixture.executor.notificationWake = wake
	err := fixture.executor.EvaluateAutomatic(t.Context(), daemon.QueueDelivery{
		MessageID: notificationMessageIDForTest(), AgentID: executorTestAgent,
		DeliveryGeneration: 1,
	})
	require.Error(t, err)
	select {
	case <-wake:
		t.Fatal("rolled-back automatic evaluation emitted a notification wake")
	default:
	}
}

func notificationMessageIDForTest() protocol.ULID {
	return "01JB0000000000000000000999"
}

func TestAutomaticEvaluationDoesNotInventStateForAnotherRecipient(t *testing.T) {
	t.Parallel()

	fixture := newExecutorFixture(t)
	first := fixture.seedDelivery(t, 2)
	second := daemon.QueueDelivery{
		MessageID: first.MessageID, AgentID: "agent-executor-second",
		DeliveryGeneration: first.DeliveryGeneration,
	}
	_, err := fixture.store.DB().ExecContext(t.Context(), `INSERT INTO delivery_queue
		(message_id, agent_id, delivery_generation, from_agent, thread_ref,
		 state, priority, attempts, created_at, expires_at, updated_at,
		 hop_count, route_digest)
		SELECT message_id, ?, delivery_generation, from_agent, thread_ref,
		 state, priority, attempts, created_at, expires_at, updated_at,
		 hop_count, route_digest FROM delivery_queue
		WHERE message_id = ? AND agent_id = ? AND delivery_generation = ?`,
		second.AgentID, first.MessageID, first.AgentID, first.DeliveryGeneration)
	require.NoError(t, err)

	require.NoError(t, fixture.executor.EvaluateAutomatic(t.Context(), first))
	require.NoError(t, fixture.executor.EvaluateAutomatic(t.Context(), second))
	var decisions, approvals int64
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT count(*)
		FROM orchestrator_decisions WHERE message_id = ?`, first.MessageID).Scan(&decisions))
	require.Equal(t, int64(1), decisions)
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT count(*)
		FROM approval_requests WHERE object_id = ? AND state = 'PENDING'`, first.MessageID).
		Scan(&approvals))
	require.Equal(t, int64(1), approvals)
}

func TestAutomaticEvaluationDefersOnNewChildAndProcFailure(t *testing.T) {
	t.Parallel()

	fixture := newExecutorFixture(t)
	fixture.processes.mu.Lock()
	fixture.processes.tree.Descendants = append(
		fixture.processes.tree.Descendants,
		testProcessNode(
			ProcessToken{PID: 500, StartTime: 600},
			fixture.processes.tree.Descendants[0].Token,
			executorTestToolExecutable,
		),
	)
	fixture.processes.mu.Unlock()
	lateChild := fixture.seedDelivery(t, 10)
	require.NoError(t, fixture.executor.EvaluateAutomatic(t.Context(), lateChild))
	require.Equal(t, protocol.GuardOutcomeFail,
		guardOutcome(latestGuards(t, fixture.store.DB(), lateChild.MessageID),
			protocol.GuardNameNoNewChildOrTool))

	fixture.processes.mu.Lock()
	fixture.processes.err = errors.New("proc unreadable")
	fixture.processes.mu.Unlock()
	unreadable := fixture.seedDelivery(t, 11)
	require.NoError(t, fixture.executor.EvaluateAutomatic(t.Context(), unreadable))
	require.Equal(t, protocol.GuardOutcomeFail,
		guardOutcome(latestGuards(t, fixture.store.DB(), unreadable.MessageID),
			protocol.GuardNameNoNewChildOrTool))
	var attempts int64
	var queueState string
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(),
		"SELECT count(*) FROM delivery_attempts").Scan(&attempts))
	require.Zero(t, attempts, "process uncertainty must defer before creating a delivery attempt")
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT state
		FROM delivery_queue WHERE message_id = ? AND agent_id = ?`, unreadable.MessageID, unreadable.AgentID).
		Scan(&queueState))
	require.Equal(t, "QUEUED", queueState)
}

func TestManualDeliverNowCannotOverrideHardHumanGuards(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		guard protocol.GuardName
		patch func(*executorFixture)
	}{
		{name: "open draft", guard: protocol.GuardNameHumanDraftClosed, patch: func(fixture *executorFixture) {
			fixture.links.mu.Lock()
			fixture.links.snapshot.LastReport.DraftState = protocol.DraftStateOpen
			fixture.links.mu.Unlock()
		}},
		{name: "active human", guard: protocol.GuardNameHumanActivityNotActive, patch: func(fixture *executorFixture) {
			fixture.links.mu.Lock()
			fixture.links.snapshot.LastReport.ActivityState = protocol.ActivityStateActive
			fixture.links.mu.Unlock()
		}},
		{name: "proc unreadable", guard: protocol.GuardNameNoNewChildOrTool, patch: func(fixture *executorFixture) {
			fixture.processes.mu.Lock()
			fixture.processes.err = errors.New("proc unreadable")
			fixture.processes.mu.Unlock()
		}},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			fixture := newExecutorFixture(t)
			candidate := fixture.seedDelivery(t, 20+index)
			test.patch(fixture)
			require.NoError(t, fixture.executor.EvaluateAutomatic(t.Context(), candidate))
			list, err := fixture.controller.Approvals(t.Context(), protocol.ListApprovalsParams{
				State: protocol.ApprovalStatePending, AgentID: candidate.AgentID,
			})
			require.NoError(t, err)
			require.Len(t, list.Approvals, 1)
			request, err := fixture.executor.ResolveApproval(t.Context(), protocol.ResolveApprovalParams{
				ApprovalID: list.Approvals[0].ApprovalID, Option: executorTestDeliverNow,
			}, channel.Actor{Type: protocol.ActorTypeHuman, ID: executorTestOperator})
			require.ErrorIs(t, err, ErrDeliveryDeferred)
			require.Equal(t, protocol.ApprovalStatePending, request.State)
			require.Equal(t, protocol.GuardOutcomeFail,
				guardOutcome(latestGuards(t, fixture.store.DB(), candidate.MessageID), test.guard))
			var queueState string
			require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT state
				FROM delivery_queue WHERE message_id = ? AND agent_id = ?`, candidate.MessageID, candidate.AgentID).
				Scan(&queueState))
			require.Equal(t, "QUEUED", queueState)
		})
	}
}

func TestManualDeliverNowWithoutLinkOrSessionDefersDurably(t *testing.T) {
	tests := []struct {
		name          string
		removeSession bool
	}{
		{name: "link unavailable"},
		{name: "session unavailable", removeSession: true},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newExecutorFixture(t)
			fixture.links.mu.Lock()
			fixture.links.snapshot = agentlink.Snapshot{}
			fixture.links.mu.Unlock()
			if test.removeSession {
				_, err := fixture.store.DB().ExecContext(t.Context(),
					"DELETE FROM sessions WHERE session_id = ?", executorTestSession)
				require.NoError(t, err)
			}
			candidate := fixture.seedDelivery(t, 30+index)
			require.NoError(t, fixture.executor.EvaluateAutomatic(t.Context(), candidate))
			if test.removeSession {
				var decisions, approvals int64
				require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(),
					"SELECT count(*) FROM orchestrator_decisions WHERE message_id = ?", candidate.MessageID).Scan(&decisions))
				require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(),
					"SELECT count(*) FROM approval_requests WHERE object_id = ?", candidate.MessageID).Scan(&approvals))
				require.Zero(t, decisions, "no session observation means no invented decision")
				require.Zero(t, approvals, "the bounded worker retry remains silent")
				return
			}
			approval := pendingApproval(t, fixture)
			request, err := fixture.executor.ResolveApproval(t.Context(), protocol.ResolveApprovalParams{
				ApprovalID: approval.ApprovalID, Option: executorTestDeliverNow,
			}, channel.Actor{Type: protocol.ActorTypeHuman, ID: executorTestOperator})
			require.ErrorIs(t, err, ErrDeliveryDeferred)
			require.Equal(t, protocol.ApprovalStatePending, request.State)

			stored, err := fixture.controller.Approval(t.Context(), approval.ApprovalID)
			require.NoError(t, err)
			require.Equal(t, protocol.ApprovalStatePending, stored.State)
			var action protocol.OrchestratorAction
			var status protocol.ExecutionStatus
			require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT selected_action,
				execution_status FROM orchestrator_decisions WHERE message_id = ?
				ORDER BY rowid DESC LIMIT 1`, candidate.MessageID).Scan(&action, &status))
			require.Equal(t, protocol.OrchestratorActionDefer, action)
			require.Equal(t, protocol.ExecutionStatusExecuted, status)
			require.Equal(t, protocol.GuardOutcomeFail,
				guardOutcome(latestGuards(t, fixture.store.DB(), candidate.MessageID),
					protocol.GuardNameAdapterConnected))
		})
	}
}

func TestClaimDeliveryUsesQueuedStateCAS(t *testing.T) {
	t.Parallel()
	fixture := newExecutorFixture(t)
	candidate := fixture.seedDelivery(t, 39)
	tx, err := fixture.store.DB().BeginTx(t.Context(), nil)
	require.NoError(t, err)
	record, err := fixture.executor.loadDelivery(t.Context(), tx, candidate, executorTestSession)
	require.NoError(t, err)
	_, err = tx.ExecContext(t.Context(), `UPDATE delivery_queue SET state = 'RETRY_WAIT'
		WHERE message_id = ? AND agent_id = ? AND delivery_generation = ?`,
		candidate.MessageID, candidate.AgentID, candidate.DeliveryGeneration)
	require.NoError(t, err)
	require.ErrorIs(t, fixture.executor.claimDelivery(t.Context(), tx, record), daemon.ErrInvalidAttempt)
	var state string
	var attempts int64
	require.NoError(t, tx.QueryRowContext(t.Context(), `SELECT state, attempts FROM delivery_queue
		WHERE message_id = ? AND agent_id = ? AND delivery_generation = ?`,
		candidate.MessageID, candidate.AgentID, candidate.DeliveryGeneration).Scan(&state, &attempts))
	require.Equal(t, "RETRY_WAIT", state)
	require.Zero(t, attempts)
	require.NoError(t, tx.Rollback())
}

func seedExecutorAttempt(
	t *testing.T,
	fixture *executorFixture,
	candidate daemon.QueueDelivery,
	deliveryID protocol.OpaqueID,
	state daemon.AttemptState,
) {
	t.Helper()
	tx, err := fixture.store.DB().BeginTx(t.Context(), nil)
	require.NoError(t, err)
	_, err = tx.ExecContext(t.Context(), `UPDATE delivery_queue SET state = 'CLAIMED',
		claimed_by = 'test-worker' WHERE message_id = ? AND agent_id = ? AND delivery_generation = ?`,
		candidate.MessageID, candidate.AgentID, candidate.DeliveryGeneration)
	require.NoError(t, err)
	spec := daemon.AttemptSpec{
		DeliveryID: deliveryID, MessageID: candidate.MessageID, AgentID: candidate.AgentID,
		DeliveryGeneration: candidate.DeliveryGeneration, AdapterSessionID: executorTestSession,
		OrchestratorDecisionID: candidate.MessageID,
		ChannelDecisionID:      protocol.ChannelDecisionID("chd_" + string(candidate.MessageID)),
		DraftGeneration:        2, InputGuardVersion: 3,
		ControlVersions: protocol.ControlVersions{"global": 0},
	}
	_, err = fixture.executor.attempts.Create(t.Context(), tx, spec)
	require.NoError(t, err)
	if state == daemon.AttemptPrepared || state == daemon.AttemptCommitSent {
		require.NoError(t, fixture.executor.attempts.MarkPrepared(
			t.Context(), tx, deliveryID, protocol.OpaqueID("lease-"+string(deliveryID))))
	}
	if state == daemon.AttemptCommitSent {
		require.NoError(t, fixture.executor.attempts.MarkCommitSent(t.Context(), tx, deliveryID))
		_, err = tx.ExecContext(t.Context(), `UPDATE delivery_queue SET state = 'DELIVERING'
			WHERE message_id = ? AND agent_id = ? AND delivery_generation = ?`,
			candidate.MessageID, candidate.AgentID, candidate.DeliveryGeneration)
		require.NoError(t, err)
	}
	require.NoError(t, tx.Commit())
}

func TestRestrictiveControlAndPauseCancelPreparedAttempts(t *testing.T) {
	t.Parallel()

	fixture := newExecutorFixture(t)
	controlCandidate := fixture.seedDelivery(t, 40)
	seedExecutorAttempt(t, fixture, controlCandidate, "delivery-control", daemon.AttemptPrepared)
	require.NoError(t, fixture.executor.ControlChanged(t.Context(), controlCandidate.AgentID))
	var state string
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT state
		FROM delivery_attempts WHERE delivery_id = 'delivery-control'`).Scan(&state))
	require.Equal(t, string(daemon.AttemptCancelled), state)

	pauseCandidate := fixture.seedDelivery(t, 41)
	seedExecutorAttempt(t, fixture, pauseCandidate, "delivery-pause", daemon.AttemptPrepared)
	require.NoError(t, fixture.executor.PauseAutomation(t.Context(), pauseCandidate.AgentID))
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT state
		FROM delivery_attempts WHERE delivery_id = 'delivery-pause'`).Scan(&state))
	require.Equal(t, string(daemon.AttemptCancelled), state)
	var queued int64
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT count(*)
		FROM delivery_queue WHERE agent_id = ? AND state = 'QUEUED'`, pauseCandidate.AgentID).Scan(&queued))
	require.Equal(t, int64(2), queued)
}

func TestPreparedCancellationPreservesEveryTerminalQueueAndOtherGeneration(t *testing.T) {
	for index, terminal := range []string{
		queueStateFailed, queueStateCancelled, queueStateExpired, queueStateDelivered,
	} {
		t.Run(terminal, func(t *testing.T) {
			fixture := newExecutorFixture(t)
			candidate := fixture.seedDelivery(t, 100+index)
			deliveryID := protocol.OpaqueID(fmt.Sprintf("delivery-terminal-%d", index))
			seedExecutorAttempt(t, fixture, candidate, deliveryID, daemon.AttemptPrepared)
			_, err := fixture.store.DB().ExecContext(t.Context(), `UPDATE delivery_queue SET state = ?
				WHERE message_id = ? AND agent_id = ? AND delivery_generation = 1`,
				terminal, candidate.MessageID, candidate.AgentID)
			require.NoError(t, err)
			_, err = fixture.store.DB().ExecContext(t.Context(), `INSERT INTO delivery_queue
				(message_id, agent_id, delivery_generation, from_agent, thread_ref,
				 state, priority, created_at, expires_at, updated_at, hop_count, route_digest)
				SELECT message_id, agent_id, 2, from_agent, thread_ref,
				 'QUEUED', priority, created_at, expires_at, updated_at, hop_count, route_digest
				FROM delivery_queue WHERE message_id = ? AND agent_id = ? AND delivery_generation = 1`,
				candidate.MessageID, candidate.AgentID)
			require.NoError(t, err)
			tx, err := fixture.store.DB().BeginTx(t.Context(), nil)
			require.NoError(t, err)
			require.ErrorIs(t, transitionAttemptQueue(t.Context(), tx,
				protocol.OpaqueID("delivery-wrong-identity"), deliveryRecord{QueueDelivery: candidate},
				queueStateClaimed, queueStateQueued, fixture.clock.Now(), false), daemon.ErrInvalidAttempt)
			require.NoError(t, tx.Rollback())

			require.NoError(t, fixture.executor.ControlChanged(t.Context(), candidate.AgentID))
			var attemptState, generationOne, generationTwo string
			require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(),
				"SELECT state FROM delivery_attempts WHERE delivery_id = ?", deliveryID).Scan(&attemptState))
			require.Equal(t, string(daemon.AttemptCancelled), attemptState)
			require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT state
				FROM delivery_queue WHERE message_id = ? AND agent_id = ? AND delivery_generation = 1`,
				candidate.MessageID, candidate.AgentID).Scan(&generationOne))
			require.Equal(t, terminal, generationOne)
			require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT state
				FROM delivery_queue WHERE message_id = ? AND agent_id = ? AND delivery_generation = 2`,
				candidate.MessageID, candidate.AgentID).Scan(&generationTwo))
			require.Equal(t, queueStateQueued, generationTwo)
		})
	}
}

func TestCommitResultPreservesEveryTerminalQueueWhileCommittingAttempt(t *testing.T) {
	for index, terminal := range []string{
		queueStateFailed, queueStateCancelled, queueStateExpired, queueStateDelivered,
	} {
		t.Run(terminal, func(t *testing.T) {
			fixture := newExecutorFixture(t)
			candidate := fixture.seedDelivery(t, 110+index)
			deliveryID := protocol.OpaqueID(fmt.Sprintf("delivery-commit-terminal-%d", index))
			seedExecutorAttempt(t, fixture, candidate, deliveryID, daemon.AttemptCommitSent)
			_, err := fixture.store.DB().ExecContext(t.Context(), `UPDATE delivery_queue SET state = ?
				WHERE message_id = ? AND agent_id = ? AND delivery_generation = ?`, terminal,
				candidate.MessageID, candidate.AgentID, candidate.DeliveryGeneration)
			require.NoError(t, err)
			tx, err := fixture.store.DB().BeginTx(t.Context(), nil)
			require.NoError(t, err)
			attempt, err := fixture.executor.attempts.Get(t.Context(), tx, deliveryID)
			require.NoError(t, err)
			require.NoError(t, tx.Rollback())
			plan := executionPlan{
				record: deliveryRecord{QueueDelivery: candidate}, attempt: attempt,
			}
			require.NoError(t, fixture.executor.finishCommitResult(t.Context(), plan,
				protocol.DeliveryResult{Result: protocol.DeliveryResultKindCommitted}))
			var attemptState, queueState string
			require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(),
				"SELECT state FROM delivery_attempts WHERE delivery_id = ?", deliveryID).
				Scan(&attemptState))
			require.Equal(t, string(daemon.AttemptCommitted), attemptState)
			require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT state
				FROM delivery_queue WHERE message_id = ? AND agent_id = ? AND delivery_generation = ?`,
				candidate.MessageID, candidate.AgentID, candidate.DeliveryGeneration).Scan(&queueState))
			require.Equal(t, terminal, queueState)
		})
	}
}

func TestPreparedHumanGuardChangesProduceDurableCancellation(t *testing.T) {
	tests := []struct {
		name  string
		patch func(*agentlink.Snapshot, time.Time)
	}{
		{name: "human key after prepare", patch: func(snapshot *agentlink.Snapshot, preparedAt time.Time) {
			snapshot.LastReport.LastHumanKeyAt = preparedAt.Add(time.Nanosecond)
		}},
		{name: "input guard version changed", patch: func(snapshot *agentlink.Snapshot, _ time.Time) {
			snapshot.LastReport.InputGuardVersion++
		}},
		{name: "draft no longer closed", patch: func(snapshot *agentlink.Snapshot, _ time.Time) {
			snapshot.LastReport.DraftState = protocol.DraftStateOpen
		}},
		{name: "human activity active", patch: func(snapshot *agentlink.Snapshot, _ time.Time) {
			snapshot.LastReport.ActivityState = protocol.ActivityStateActive
		}},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newExecutorFixture(t)
			candidate := fixture.seedDelivery(t, 120+index)
			tx, err := fixture.store.DB().BeginTx(t.Context(), nil)
			require.NoError(t, err)
			record, err := fixture.executor.loadDelivery(t.Context(), tx, candidate, executorTestSession)
			require.NoError(t, err)
			require.NoError(t, tx.Rollback())
			deliveryID := protocol.OpaqueID(fmt.Sprintf("delivery-human-guard-%d", index))
			seedExecutorAttempt(t, fixture, candidate, deliveryID, daemon.AttemptPrepared)
			tx, err = fixture.store.DB().BeginTx(t.Context(), nil)
			require.NoError(t, err)
			attempt, err := fixture.executor.attempts.Get(t.Context(), tx, deliveryID)
			require.NoError(t, err)
			require.NoError(t, tx.Rollback())

			fixture.links.mu.Lock()
			original := fixture.links.snapshot.LastReport
			preparedAt := fixture.clock.Now()
			test.patch(&fixture.links.snapshot, preparedAt)
			fixture.links.mu.Unlock()
			plan := executionPlan{record: record, attempt: attempt, report: original}
			tx, err = fixture.store.DB().BeginTx(t.Context(), nil)
			require.NoError(t, err)
			cancellation, err := fixture.executor.revalidatePreparedGuards(
				t.Context(), tx, plan, preparedAt)
			require.NoError(t, err)
			require.NotNil(t, cancellation)
			require.Equal(t, daemon.AttemptAbortedHumanPriority, cancellation.state)
			require.NoError(t, tx.Rollback())
			require.NoError(t, fixture.executor.cancelPrepared(t.Context(), plan,
				cancellation.state, cancellation.reason, cancellation.cancelReason,
				cancellation.queueState))

			var attemptState, queueState string
			require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(),
				"SELECT state FROM delivery_attempts WHERE delivery_id = ?", deliveryID).
				Scan(&attemptState))
			require.Equal(t, string(daemon.AttemptAbortedHumanPriority), attemptState,
				"cancel must durably leave PREPARED")
			require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT state
				FROM delivery_queue WHERE message_id = ? AND agent_id = ? AND delivery_generation = ?`,
				candidate.MessageID, candidate.AgentID, candidate.DeliveryGeneration).Scan(&queueState))
			require.Equal(t, queueStateQueued, queueState)
		})
	}
}

func TestExecutorRecoveryTurnsCommitSentIntoUncertainWithApproval(t *testing.T) {
	t.Parallel()

	fixture := newExecutorFixture(t)
	candidate := fixture.seedDelivery(t, 50)
	seedExecutorAttempt(t, fixture, candidate, "delivery-restart", daemon.AttemptCommitSent)
	count, err := fixture.executor.Recover(t.Context())
	require.NoError(t, err)
	require.Equal(t, int64(1), count)
	var attemptState, queueState string
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT state
		FROM delivery_attempts WHERE delivery_id = 'delivery-restart'`).Scan(&attemptState))
	require.Equal(t, string(daemon.AttemptUncertain), attemptState)
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT state
		FROM delivery_queue WHERE message_id = ? AND agent_id = ?`, candidate.MessageID, candidate.AgentID).
		Scan(&queueState))
	require.Equal(t, "QUEUED", queueState)
	approval := pendingApproval(t, fixture)
	require.Equal(t, string(candidate.MessageID), approval.ObjectID)
}

func TestExecutorRecoveryCreatesFreshApprovalForEachRecipient(t *testing.T) {
	t.Parallel()
	fixture := newExecutorFixture(t)
	first := fixture.seedDelivery(t, 55)
	secondAgent := protocol.AgentID("agent-executor-2")
	_, err := fixture.store.DB().ExecContext(t.Context(), `INSERT INTO delivery_queue
		(message_id, agent_id, delivery_generation, from_agent, thread_ref,
		 state, priority, attempts, created_at, expires_at, updated_at,
		 hop_count, route_digest)
		SELECT message_id, ?, delivery_generation, from_agent, thread_ref,
		 state, priority, attempts, created_at, expires_at, updated_at,
		 hop_count, route_digest FROM delivery_queue
		WHERE message_id = ? AND agent_id = ? AND delivery_generation = ?`,
		secondAgent, first.MessageID, first.AgentID, first.DeliveryGeneration)
	require.NoError(t, err)
	second := daemon.QueueDelivery{
		MessageID: first.MessageID, AgentID: secondAgent,
		DeliveryGeneration: first.DeliveryGeneration,
	}
	seedExecutorAttempt(t, fixture, first, "delivery-recover-first", daemon.AttemptCommitSent)
	seedExecutorAttempt(t, fixture, second, "delivery-recover-second", daemon.AttemptCommitSent)

	count, err := fixture.executor.Recover(t.Context())
	require.NoError(t, err)
	require.Equal(t, int64(2), count)
	list, err := fixture.controller.Approvals(t.Context(), protocol.ListApprovalsParams{
		State: protocol.ApprovalStatePending,
	})
	require.NoError(t, err)
	require.Len(t, list.Approvals, 2)
	seen := map[protocol.AgentID]protocol.ApprovalID{}
	for _, approval := range list.Approvals {
		seen[approval.AgentID] = approval.ApprovalID
		var linked protocol.ApprovalID
		require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT uncertain_approval_id
			FROM delivery_attempts WHERE message_id = ? AND agent_id = ? AND state = 'UNCERTAIN'`,
			first.MessageID, approval.AgentID).Scan(&linked))
		require.Equal(t, approval.ApprovalID, linked)
	}
	require.Contains(t, seen, first.AgentID)
	require.Contains(t, seen, secondAgent)
	require.NotEqual(t, seen[first.AgentID], seen[secondAgent])
}

func TestExecutorRecoveryPreservesTerminalQueueWhilePersistingUncertain(t *testing.T) {
	for index, terminal := range []string{
		queueStateFailed, queueStateCancelled, queueStateExpired, queueStateDelivered,
	} {
		t.Run(terminal, func(t *testing.T) {
			fixture := newExecutorFixture(t)
			candidate := fixture.seedDelivery(t, 65+index)
			deliveryID := protocol.OpaqueID(fmt.Sprintf("delivery-recover-terminal-%d", index))
			seedExecutorAttempt(t, fixture, candidate, deliveryID, daemon.AttemptCommitSent)
			_, err := fixture.store.DB().ExecContext(t.Context(), `UPDATE delivery_queue SET state = ?
				WHERE message_id = ? AND agent_id = ? AND delivery_generation = ?`, terminal,
				candidate.MessageID, candidate.AgentID, candidate.DeliveryGeneration)
			require.NoError(t, err)
			count, err := fixture.executor.Recover(t.Context())
			require.NoError(t, err)
			require.Equal(t, int64(1), count)
			var attemptState, queueState string
			require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(),
				"SELECT state FROM delivery_attempts WHERE delivery_id = ?", deliveryID).
				Scan(&attemptState))
			require.Equal(t, string(daemon.AttemptUncertain), attemptState)
			require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT state
				FROM delivery_queue WHERE message_id = ? AND agent_id = ? AND delivery_generation = ?`,
				candidate.MessageID, candidate.AgentID, candidate.DeliveryGeneration).Scan(&queueState))
			require.Equal(t, terminal, queueState)
		})
	}
}

func TestUnresolvedUncertainAttemptVetoesAnotherManualDelivery(t *testing.T) {
	t.Parallel()
	fixture := newExecutorFixture(t)
	uncertainCandidate := fixture.seedDelivery(t, 75)
	seedExecutorAttempt(t, fixture, uncertainCandidate, "delivery-veto-source", daemon.AttemptCommitSent)
	_, err := fixture.executor.Recover(t.Context())
	require.NoError(t, err)

	blockedCandidate := fixture.seedDelivery(t, 76)
	require.NoError(t, fixture.executor.EvaluateAutomatic(t.Context(), blockedCandidate))
	list, err := fixture.controller.Approvals(t.Context(), protocol.ListApprovalsParams{
		State: protocol.ApprovalStatePending, AgentID: blockedCandidate.AgentID,
	})
	require.NoError(t, err)
	var blockedApproval protocol.ApprovalRequest
	for _, approval := range list.Approvals {
		if approval.ObjectID == string(blockedCandidate.MessageID) {
			blockedApproval = approval
		}
	}
	require.True(t, blockedApproval.ApprovalID.Valid())
	request, err := fixture.executor.ResolveApproval(t.Context(), protocol.ResolveApprovalParams{
		ApprovalID: blockedApproval.ApprovalID, Option: executorTestDeliverNow,
	}, channel.Actor{Type: protocol.ActorTypeHuman, ID: executorTestOperator})
	require.ErrorIs(t, err, ErrDeliveryDeferred)
	require.Equal(t, protocol.ApprovalStatePending, request.State)
	require.Equal(t, protocol.GuardOutcomeFail,
		guardOutcome(latestGuards(t, fixture.store.DB(), blockedCandidate.MessageID),
			protocol.GuardNameNotDuplicate))
	var attempts int64
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT count(*)
		FROM delivery_attempts WHERE message_id = ?`, blockedCandidate.MessageID).Scan(&attempts))
	require.Zero(t, attempts)
}

func TestRejectUncertainApprovalCancelsOnlyItsRecipientAndLeavesOtherEpisodeBlocking(t *testing.T) {
	t.Parallel()

	fixture := newExecutorFixture(t)
	candidate := fixture.seedDelivery(t, 60)
	other := fixture.seedDelivery(t, 61)
	seedExecutorAttempt(t, fixture, candidate, "delivery-reject", daemon.AttemptCommitSent)
	tx, err := fixture.store.DB().BeginTx(t.Context(), nil)
	require.NoError(t, err)
	require.NoError(t, fixture.executor.attempts.MarkUncertain(
		t.Context(), tx, "delivery-reject", daemon.ReasonTimedOut))
	approval, err := fixture.controller.CreateOrchestratorApproval(
		t.Context(), tx, candidate.MessageID, candidate.AgentID, "commit outcome uncertain",
	)
	require.NoError(t, err)
	require.NoError(t, fixture.executor.attempts.BindUncertainApproval(
		t.Context(), tx, "delivery-reject", approval.ApprovalID))
	require.NoError(t, tx.Commit())

	seedExecutorAttempt(t, fixture, other, "delivery-other", daemon.AttemptCommitSent)
	tx, err = fixture.store.DB().BeginTx(t.Context(), nil)
	require.NoError(t, err)
	require.NoError(t, fixture.executor.attempts.MarkUncertain(
		t.Context(), tx, "delivery-other", daemon.ReasonTimedOut))
	otherApproval, err := fixture.controller.CreateOrchestratorApproval(
		t.Context(), tx, other.MessageID, other.AgentID, "another commit outcome uncertain",
	)
	require.NoError(t, err)
	require.NoError(t, fixture.executor.attempts.BindUncertainApproval(
		t.Context(), tx, "delivery-other", otherApproval.ApprovalID))
	require.NoError(t, tx.Commit())

	resolved, err := fixture.executor.ResolveApproval(t.Context(), protocol.ResolveApprovalParams{
		ApprovalID: approval.ApprovalID, Option: executorTestReject, Reason: "do not retry",
	}, channel.Actor{Type: protocol.ActorTypeHuman, ID: executorTestOperator})
	require.NoError(t, err)
	require.Equal(t, protocol.ApprovalStateRejected, resolved.State)
	var queueState, uncertainState string
	var resolutionID protocol.ApprovalID
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT state
		FROM delivery_queue WHERE message_id = ? AND agent_id = ?`, candidate.MessageID, candidate.AgentID).
		Scan(&queueState))
	require.Equal(t, "CANCELLED", queueState)
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT state, resolution_approval_id
		FROM delivery_attempts WHERE delivery_id = 'delivery-reject'`).Scan(&uncertainState, &resolutionID))
	require.Equal(t, string(daemon.AttemptUncertain), uncertainState)
	require.Equal(t, approval.ApprovalID, resolutionID)
	var otherResolution sql.NullString
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT resolution_approval_id
		FROM delivery_attempts WHERE delivery_id = 'delivery-other'`).Scan(&otherResolution))
	require.False(t, otherResolution.Valid)
	tx, err = fixture.store.DB().BeginTx(t.Context(), nil)
	require.NoError(t, err)
	blocked, err := fixture.executor.attempts.HasUncertain(t.Context(), tx, candidate.AgentID)
	require.NoError(t, err)
	require.True(t, blocked)
	require.NoError(t, tx.Rollback())
}

func TestRejectUncertainApprovalPreservesTerminalQueue(t *testing.T) {
	t.Parallel()

	fixture := newExecutorFixture(t)
	candidate := fixture.seedDelivery(t, 62)
	const deliveryID = protocol.OpaqueID("delivery-reject-terminal")
	seedExecutorAttempt(t, fixture, candidate, deliveryID, daemon.AttemptCommitSent)
	tx, err := fixture.store.DB().BeginTx(t.Context(), nil)
	require.NoError(t, err)
	require.NoError(t, fixture.executor.attempts.MarkUncertain(
		t.Context(), tx, deliveryID, daemon.ReasonTimedOut))
	approval, err := fixture.controller.CreateOrchestratorApproval(
		t.Context(), tx, candidate.MessageID, candidate.AgentID, "commit outcome uncertain",
	)
	require.NoError(t, err)
	require.NoError(t, fixture.executor.attempts.BindUncertainApproval(
		t.Context(), tx, deliveryID, approval.ApprovalID))
	require.NoError(t, tx.Commit())
	_, err = fixture.store.DB().ExecContext(t.Context(), `UPDATE delivery_queue SET state = 'DELIVERED'
		WHERE message_id = ? AND agent_id = ? AND delivery_generation = ?`,
		candidate.MessageID, candidate.AgentID, candidate.DeliveryGeneration)
	require.NoError(t, err)

	resolved, err := fixture.executor.ResolveApproval(t.Context(), protocol.ResolveApprovalParams{
		ApprovalID: approval.ApprovalID, Option: executorTestReject, Reason: "do not retry",
	}, channel.Actor{Type: protocol.ActorTypeHuman, ID: executorTestOperator})
	require.NoError(t, err)
	require.Equal(t, protocol.ApprovalStateRejected, resolved.State)
	var queueState, attemptState string
	var resolutionID protocol.ApprovalID
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT state
		FROM delivery_queue WHERE message_id = ? AND agent_id = ? AND delivery_generation = ?`,
		candidate.MessageID, candidate.AgentID, candidate.DeliveryGeneration).Scan(&queueState))
	require.Equal(t, queueStateDelivered, queueState)
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT state, resolution_approval_id
		FROM delivery_attempts WHERE delivery_id = ?`, deliveryID).Scan(&attemptState, &resolutionID))
	require.Equal(t, string(daemon.AttemptUncertain), attemptState)
	require.Equal(t, approval.ApprovalID, resolutionID)
}
