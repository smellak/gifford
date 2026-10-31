//go:build linux

package orchestrator

import (
	"encoding/json"
	"os"
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

// restartableLink is the test agent's real socket, with a Proxy side the
// test can drop and listen again, as when the Proxy restarts.
type restartableLink struct {
	executor *Executor
	client   *agentlink.Client
	restart  func(*testing.T)
}

func attachRestartableLink(t *testing.T, fixture *executorFixture, delivery *integrationDelivery) restartableLink {
	t.Helper()
	process, err := registry.CurrentProcess()
	require.NoError(t, err)
	processJSON, err := json.Marshal(process)
	require.NoError(t, err)
	fixture.processes.mu.Lock()
	fixture.processes.tree.Root = ProcessToken{PID: process.PID, StartTime: process.ProcessStartTime}
	fixture.processes.mu.Unlock()
	var manifestJSON string
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(),
		"SELECT manifest_json FROM sessions WHERE session_id = ?", executorTestSession).Scan(&manifestJSON))
	var manifest protocol.CapabilityManifest
	require.NoError(t, json.Unmarshal([]byte(manifestJSON), &manifest))
	_, err = fixture.store.DB().ExecContext(t.Context(), `UPDATE sessions SET process_json = ?
		WHERE session_id = ?`, string(processJSON), executorTestSession)
	require.NoError(t, err)

	runtimeDir, err := os.MkdirTemp("", "gifford-automation-") //nolint:usetesting // Unix socket paths have a small fixed limit.
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(runtimeDir)) })
	listen := func() *agentsock.Server {
		server, err := agentsock.Listen(t.Context(), agentsock.Options{
			RuntimeDir: runtimeDir,
			Register:   protocol.RegisterAgent{Manifest: manifest, Process: &process},
			Guard:      newIntegrationGuard(), Delivery: delivery, Clock: agentsock.SystemClock{},
		})
		require.NoError(t, err)
		return server
	}
	server := listen()
	t.Cleanup(func() { _ = server.Close() })
	writer, err := agentlink.NewInputGuardStore(fixture.store.DB(), fixture.clock)
	require.NoError(t, err)
	client, err := agentlink.Start(t.Context(), agentlink.Options{
		RuntimeDir: runtimeDir, DB: fixture.store.DB(), AgentID: executorTestAgent,
		SessionID: executorTestSession, Writer: writer, Clock: agentlink.SystemClock{},
		ReconnectMin: 10 * time.Millisecond, ReconnectMax: 10 * time.Millisecond,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	up := func() bool {
		snapshot := client.Snapshot()
		return snapshot.State == agentlink.StateUp && snapshot.HasReport
	}
	require.Eventually(t, up, 5*time.Second, time.Millisecond)

	executor, err := NewExecutor(ExecutorOptions{
		DB: fixture.store.DB(), Channel: fixture.executor.channel,
		Approvals: fixture.controller, Links: integrationLinks{client: client},
		Clock: fixture.clock, Processes: fixture.processes,
		Entropy: fixture.executor.entropy, CallTimeout: 2 * time.Second,
	})
	require.NoError(t, err)
	return restartableLink{executor: executor, client: client, restart: func(t *testing.T) {
		t.Helper()
		require.NoError(t, server.Close())
		require.Eventually(t, func() bool { return client.Snapshot().State != agentlink.StateUp },
			5*time.Second, time.Millisecond)
		server = listen()
		require.Eventually(t, up, 5*time.Second, time.Millisecond)
	}}
}

// setAgentMode sets the test agent's mode as an operator does, at the
// control's current version.
func setAgentMode(t *testing.T, fixture *executorFixture, mode protocol.AgentMode) {
	t.Helper()
	current, err := fixture.controller.Agent(t.Context(), executorTestAgent)
	require.NoError(t, err)
	_, err = fixture.controller.SetAgentMode(t.Context(), protocol.SetAgentModeParams{
		AgentID: executorTestAgent, Mode: mode, ExpectedVersion: current.Version,
	}, channel.Actor{Type: protocol.ActorTypeHuman, ID: executorTestOperator})
	require.NoError(t, err)
}

// deliverNow defers candidate to the human and approves it with
// deliver_now, then reads the attempt it made, the last one inserted.
func deliverNow(t *testing.T, fixture *executorFixture, executor *Executor, candidate daemon.QueueDelivery) string {
	t.Helper()
	require.NoError(t, executor.EvaluateAutomatic(t.Context(), candidate))
	_, _ = executor.ResolveApproval(t.Context(), protocol.ResolveApprovalParams{
		ApprovalID: pendingApproval(t, fixture).ApprovalID, Option: executorTestDeliverNow,
	}, channel.Actor{Type: protocol.ActorTypeHuman, ID: executorTestOperator})
	var state string
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT state
		FROM delivery_attempts WHERE message_id = ? ORDER BY rowid DESC LIMIT 1`,
		candidate.MessageID).Scan(&state))
	return state
}

// A pause outlives the control that asked for it until the agent's
// confirmed control allows automatic delivery and the Proxy is resumed. A
// resume while the agent is still MANUAL lifts nothing, so after FULL a
// delivery is still cancelled at the paused Proxy; once resumed, the next
// delivery commits, exactly once.
func TestResumeLiftsThePauseOnlyOnceTheControlAllows(t *testing.T) {
	fixture := newExecutorFixture(t)
	delivery := &integrationDelivery{}
	link := attachRestartableLink(t, fixture, delivery)
	candidate := fixture.seedDelivery(t, 400)
	setAgentMode(t, fixture, protocol.AgentModeManual)
	require.NoError(t, link.executor.PauseAutomation(t.Context(), executorTestAgent))
	require.NoError(t, link.executor.ResumeAutomation(t.Context(), executorTestAgent))
	setAgentMode(t, fixture, protocol.AgentModeFull)

	require.Equal(t, string(daemon.AttemptCancelled), deliverNow(t, fixture, link.executor, candidate),
		"the Proxy is still paused")
	require.Zero(t, delivery.prepareCount(), "the session never saw the prepare")
	require.Equal(t, queueStateQueued, queueStateOf(t, fixture.store.DB(), candidate))

	require.NoError(t, link.executor.ResumeAutomation(t.Context(), executorTestAgent))
	require.Equal(t, string(daemon.AttemptCommitted), deliverNow(t, fixture, link.executor, candidate))
	require.Equal(t, 1, delivery.prepareCount())
	require.Equal(t, 1, delivery.commitCount())
	require.Equal(t, queueStateDelivered, queueStateOf(t, fixture.store.DB(), candidate))
}

// A pause that arrives after the agent's control allows automatic delivery
// again, as when the sink of MANUAL runs after FULL's has resumed the
// Proxy, does not pause it, not even once the link reconnects. It may still
// cancel the agent's prepared work, which only costs a retry: the delivery
// then commits, exactly once.
func TestALatePauseUnderFullLeavesTheProxyRunning(t *testing.T) {
	fixture := newExecutorFixture(t)
	delivery := &integrationDelivery{}
	link := attachRestartableLink(t, fixture, delivery)
	candidate := fixture.seedDelivery(t, 401)
	setAgentMode(t, fixture, protocol.AgentModeOff)
	require.NoError(t, link.executor.PauseAutomation(t.Context(), executorTestAgent))
	setAgentMode(t, fixture, protocol.AgentModeManual)
	setAgentMode(t, fixture, protocol.AgentModeFull)
	require.NoError(t, link.executor.ResumeAutomation(t.Context(), executorTestAgent))
	seedExecutorAttempt(t, fixture, candidate, "dlv-prepared", daemon.AttemptPrepared)
	require.NoError(t, link.executor.PauseAutomation(t.Context(), executorTestAgent))
	link.restart(t)

	require.Equal(t, string(daemon.AttemptCommitted), deliverNow(t, fixture, link.executor, candidate))
	require.Equal(t, 1, delivery.prepareCount(), "the Proxy is not paused")
	require.Equal(t, 1, delivery.commitCount())
	db := fixture.store.DB()
	require.Equal(t, 1, countRows(t, db, `SELECT count(*) FROM delivery_attempts
		WHERE message_id = ? AND state = 'COMMITTED'`, candidate.MessageID))
	require.Equal(t, queueStateDelivered, queueStateOf(t, db, candidate))
}

// Resuming twice is resuming once, and the link keeps the resume: after
// the Proxy side drops and listens again, the reconnected link does not
// pause it, and one delivery commits.
func TestResumeIsIdempotentAndOutlivesAReconnection(t *testing.T) {
	fixture := newExecutorFixture(t)
	delivery := &integrationDelivery{}
	link := attachRestartableLink(t, fixture, delivery)
	candidate := fixture.seedDelivery(t, 402)
	setAgentMode(t, fixture, protocol.AgentModeOff)
	require.NoError(t, link.executor.PauseAutomation(t.Context(), executorTestAgent))
	setAgentMode(t, fixture, protocol.AgentModeFull)
	require.NoError(t, link.executor.ResumeAutomation(t.Context(), executorTestAgent))
	require.NoError(t, link.executor.ResumeAutomation(t.Context(), executorTestAgent))
	link.restart(t)

	require.Equal(t, string(daemon.AttemptCommitted), deliverNow(t, fixture, link.executor, candidate))
	require.Equal(t, 1, delivery.prepareCount())
	require.Equal(t, 1, delivery.commitCount())
	require.Equal(t, 1, attemptCount(t, fixture.store.DB(), candidate.MessageID))
}
