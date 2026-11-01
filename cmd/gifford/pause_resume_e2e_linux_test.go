package main

import (
	"context"
	"database/sql"
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

const pauseResumeRecipient protocol.AgentID = "recipient"

// pauseResumeNode is one hub with a quiet recipient behind a real Proxy and
// a sender, started as the other end-to-end tests start them. The hub never
// restarts, and every delivery goes through the recipient's first session.
type pauseResumeNode struct {
	ctx         context.Context
	environment e2eEnvironment
	hub         *e2eProcess
	hubStderr   *lockedBuffer
	recipient   *e2eRecipient
	sender      *e2eAgent
	db          *sql.DB
	session     string
}

func startPauseResumeNode(ctx context.Context, t *testing.T) *pauseResumeNode {
	t.Helper()
	environment := newE2EEnvironment(t)
	hub, hubStderr := startE2EHubBinary(ctx, t, environment, e2eMeasuredGiffordBinary)
	t.Cleanup(func() { _ = hub.terminateAndWait() })
	recipient := startE2ERecipient(ctx, t, environment, pauseResumeRecipient, "quiet")
	t.Cleanup(func() {
		_ = recipient.process.terminateAndWait()
		_ = recipient.outer.Close()
	})
	sender := startE2EAgentBinary(ctx, t, environment.env, "sender", e2eMeasuredGiffordBinary)
	t.Cleanup(func() {
		select {
		case <-sender.process.done:
		default:
			_ = sender.process.terminateAndWait()
		}
		_ = sender.outer.Close()
	})
	db := openE2EReadDB(t, environment.database)
	var session string
	require.Eventually(t, func() bool {
		err := db.QueryRowContext(ctx, `SELECT session_id FROM sessions
			WHERE agent_id = ? AND state = 'CONNECTED'
			AND capture_mode = 'pty_proxy' AND deliverable = 1 AND ended_at IS NULL`,
			pauseResumeRecipient).Scan(&session)
		return err == nil
	}, e2eRPCTimeout, 20*time.Millisecond, "recipient session did not connect")
	waitForPauseResumeQuiet(ctx, t, db, session)
	return &pauseResumeNode{
		ctx: ctx, environment: environment, hub: hub, hubStderr: hubStderr,
		recipient: recipient, sender: sender, db: db, session: session,
	}
}

// waitForPauseResumeQuiet observes a closed and idle guard for the recipient's
// exact session.
func waitForPauseResumeQuiet(
	ctx context.Context,
	t *testing.T,
	db *sql.DB,
	session string,
) {
	t.Helper()
	var draft, activity string
	require.Eventually(t, func() bool {
		err := db.QueryRowContext(ctx, `SELECT draft_state, activity_state
			FROM input_guards WHERE session_id = ? AND agent_id = ?`,
			session, pauseResumeRecipient).Scan(&draft, &activity)
		return err == nil && draft == string(protocol.DraftStateClosed) &&
			activity == string(protocol.ActivityStateIdle)
	}, 12*time.Second, 20*time.Millisecond,
		"recipient session %s did not become quiet", session)
}

func (n *pauseResumeNode) stop(t *testing.T) {
	t.Helper()
	n.sender.stop(t)
	require.NoError(t, n.hub.terminateAndWait(), "hub stderr: %s", n.hubStderr.String())
}

// ctl runs one ctl command against the hub and decodes the agent channel
// it prints.
func (n *pauseResumeNode) ctl(t *testing.T, args ...string) protocol.AgentChannel {
	t.Helper()
	var agent protocol.AgentChannel
	runE2ECLIBinary(n.ctx, t, n.environment.env, e2eMeasuredGiffordBinary, &agent,
		append([]string{"ctl"}, args...)...)
	return agent
}

func pauseResumeMode(mode string, flags ...string) []string {
	return append([]string{"agent", string(pauseResumeRecipient), "channel", "mode", mode}, flags...)
}

// waitAutoDelivery waits for the hub's expiry sweep to turn the recipient's
// automatic delivery back on.
func (n *pauseResumeNode) waitAutoDelivery(t *testing.T) protocol.AgentChannel {
	t.Helper()
	var agent protocol.AgentChannel
	require.Eventually(t, func() bool {
		agent = protocol.AgentChannel{}
		err := readE2ECLIJSON(n.ctx, n.environment.env, e2eMeasuredGiffordBinary, &agent,
			"ctl", "agent", string(pauseResumeRecipient), "channel", "status")
		return err == nil && agent.Effective.AutoDelivery
	}, 15*time.Second, 100*time.Millisecond, "the override did not expire")
	return agent
}

// send admits one message to the recipient.
func (n *pauseResumeNode) send(t *testing.T, clientID string) protocol.ULID {
	t.Helper()
	var admitted protocol.AdmissionResult
	n.sender.callTool(t, "send_message", map[string]any{
		e2eToField: pauseResumeRecipient, e2eSubjectField: "pause and resume",
		e2eBodyField: "one delivery after a resume", e2eClientIDField: clientID,
	}, &admitted)
	require.Equal(t, protocol.AdmissionStatusAccepted, admitted.Result)
	require.True(t, admitted.MessageID.Valid())
	return admitted.MessageID
}

// pendingFor returns the message's pending approval once the worker has
// opened it.
func (n *pauseResumeNode) pendingFor(message protocol.ULID) (protocol.ApprovalRequest, bool) {
	var pending protocol.ApprovalList
	if err := readE2ECLIJSON(n.ctx, n.environment.env, e2eMeasuredGiffordBinary,
		&pending, "ctl", "approvals", "--pending"); err != nil {
		return protocol.ApprovalRequest{}, false
	}
	for _, approval := range pending.Approvals {
		if approval.ObjectID == string(message) && approval.AgentID == pauseResumeRecipient {
			return approval, true
		}
	}
	return protocol.ApprovalRequest{}, false
}

type pauseResumeDeliveryState struct {
	attempt        string
	terminalReason string
	queue          string
	session        string
}

func pauseResumeAttemptActive(state string) bool {
	switch state {
	case "PREPARING", "PREPARED", "COMMIT_SENT":
		return true
	default:
		return false
	}
}

func pauseResumeQueueTerminal(state string) bool {
	switch state {
	case "DELIVERED", "FAILED", "EXPIRED", "CANCELLED":
		return true
	default:
		return false
	}
}

// waitForPauseResumeDelivery waits for the attempt to become durable and
// terminal before the test looks at the harness. A terminal non-commit is a
// delivery failure, not a missing-line timeout, so report its durable reason
// immediately.
func (n *pauseResumeNode) waitForPauseResumeDelivery(
	t *testing.T,
	message protocol.ULID,
) pauseResumeDeliveryState {
	t.Helper()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	timer := time.NewTimer(e2eRPCTimeout)
	defer timer.Stop()

	var observed pauseResumeDeliveryState
	for {
		err := n.db.QueryRowContext(n.ctx, `SELECT attempt.state,
			coalesce(attempt.terminal_reason, ''), queue.state, attempt.adapter_session_id
			FROM delivery_attempts AS attempt JOIN delivery_queue AS queue
			ON queue.message_id = attempt.message_id AND queue.agent_id = attempt.agent_id
				AND queue.delivery_generation = attempt.delivery_generation
			WHERE attempt.message_id = ? AND attempt.agent_id = ?
			ORDER BY attempt.rowid DESC LIMIT 1`, message, pauseResumeRecipient).Scan(
			&observed.attempt, &observed.terminalReason, &observed.queue, &observed.session)
		if err == nil && (!pauseResumeAttemptActive(observed.attempt) ||
			pauseResumeQueueTerminal(observed.queue)) {
			require.Equal(t, "COMMITTED", observed.attempt,
				"durable delivery failed: queue=%s terminal_reason=%s",
				observed.queue, observed.terminalReason)
			require.Equal(t, "DELIVERED", observed.queue,
				"durable delivery failed: attempt=%s terminal_reason=%s",
				observed.attempt, observed.terminalReason)
			return observed
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			require.NoError(t, err, "read durable delivery state")
		}

		select {
		case <-ticker.C:
		case <-timer.C:
			require.FailNow(t, "delivery did not reach a durable terminal state",
				"attempt=%q queue=%q terminal_reason=%q",
				observed.attempt, observed.queue, observed.terminalReason)
			return observed
		case <-n.ctx.Done():
			require.FailNow(t, "delivery context ended before a durable terminal state",
				"attempt=%q queue=%q terminal_reason=%q: %v",
				observed.attempt, observed.queue, observed.terminalReason, n.ctx.Err())
			return observed
		}
	}
}

// deliver approves the message with deliver_now and checks the recipient got
// it exactly once: one more typed line, and a single attempt, COMMITTED, in
// the first session. A deferred deliver_now opens no attempt and is asked
// again; a Proxy left paused cancels the one attempt and types nothing.
func (n *pauseResumeNode) deliver(t *testing.T, message protocol.ULID, wait time.Duration) {
	t.Helper()
	var approval protocol.ApprovalRequest
	require.Eventually(t, func() bool {
		var ok bool
		approval, ok = n.pendingFor(message)
		return ok
	}, wait, 50*time.Millisecond, "the worker did not open the approval")
	before := len(n.recipient.deliveredLines(t))
	var resolved protocol.ApprovalRequest
	require.Eventually(t, func() bool {
		resolved = protocol.ApprovalRequest{}
		return readE2ECLIJSON(n.ctx, n.environment.env, e2eMeasuredGiffordBinary, &resolved,
			"ctl", "approve", string(approval.ApprovalID), "--deliver-now") == nil
	}, 20*time.Second, 250*time.Millisecond, "deliver_now never went through")
	require.Equal(t, protocol.ApprovalStateApproved, resolved.State)
	delivery := n.waitForPauseResumeDelivery(t, message)
	require.Equal(t, n.session, delivery.session)

	var attempts int
	require.NoError(t, n.db.QueryRowContext(n.ctx, `SELECT count(*)
		FROM delivery_attempts WHERE message_id = ? AND agent_id = ?`,
		message, pauseResumeRecipient).Scan(&attempts))
	require.Equal(t, 1, attempts, "one attempt: no PREPARE cancelled or repeated")

	require.Eventually(t, func() bool {
		return len(n.recipient.deliveredLines(t)) == before+1
	}, e2eRPCTimeout, 20*time.Millisecond, "recipient stderr: %s", n.recipient.stderr.String())
	waitForPauseResumeQuiet(n.ctx, t, n.db, n.session)
}

// Every way to turn the recipient's automatic delivery off and on again
// through the CLI, against a running hub and a real Proxy, ends in exactly
// one committed delivery: once the agent's confirmed control allows
// automatic delivery, the hub lifts the pause it put on the Proxy. That
// holds for FULL after a pause, MANUAL, OFF and SEND_ONLY, and when an
// override's TTL expires, whether the override is deleted or restored.
func TestEveryResumeThroughTheCLIDeliversOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	node := startPauseResumeNode(ctx, t)

	t.Run("an expired override with nothing to restore", func(t *testing.T) {
		require.False(t, node.ctl(t, pauseResumeMode("manual", "--ttl", "1s")...).Effective.AutoDelivery)
		require.Nil(t, node.waitAutoDelivery(t).Control, "the override was deleted")
		node.deliver(t, node.send(t, "e2e-ttl-deleted"), 12*time.Second)
	})
	pause := []string{"pause", string(pauseResumeRecipient)}
	for _, c := range []struct {
		name  string
		id    string
		steps [][]string
		off   bool
	}{
		{name: "ctl pause", id: "ctl-pause", steps: [][]string{pause}, off: true},
		{name: "manual", id: "manual", steps: [][]string{pauseResumeMode("manual")}, off: true},
		{name: "off", id: "off", steps: [][]string{pauseResumeMode("off")}, off: true},
		// SEND_ONLY keeps the agent's automatic delivery as it is: alone it
		// leaves it on, over a pause it keeps it off.
		{name: "send-only", id: "send-only", steps: [][]string{pauseResumeMode("send-only")}},
		{
			name: "send-only over a pause", id: "send-only-paused",
			steps: [][]string{pause, pauseResumeMode("send-only")}, off: true,
		},
	} {
		t.Run(c.name+" then FULL", func(t *testing.T) {
			var agent protocol.AgentChannel
			for _, step := range c.steps {
				agent = node.ctl(t, step...)
			}
			require.Equal(t, !c.off, agent.Effective.AutoDelivery)
			require.True(t, node.ctl(t, pauseResumeMode("full")...).Effective.AutoDelivery)
			node.deliver(t, node.send(t, "e2e-"+c.id), 12*time.Second)
		})
	}
	t.Run("an expired override restored", func(t *testing.T) {
		require.NotNil(t, node.ctl(t, pauseResumeMode("full")...).Control)
		require.False(t, node.ctl(t, pauseResumeMode("manual", "--ttl", "1s")...).Effective.AutoDelivery)
		restored := node.waitAutoDelivery(t)
		require.NotNil(t, restored.Control, "the FULL control came back")
		require.Equal(t, protocol.AgentModeFull, restored.Mode)
		node.deliver(t, node.send(t, "e2e-ttl-restored"), 12*time.Second)
	})
	node.stop(t)
}

// Resuming twice is resuming once, and the agent link keeps what FULL
// asked for: after the recipient's Proxy stops for longer than the link's
// heartbeat timeout and then continues, the link reconnects to the same
// session without pausing the Proxy again. Each ends in exactly one
// committed delivery.
func TestResumeThroughTheCLIIsIdempotentAndOutlivesAReconnection(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	node := startPauseResumeNode(ctx, t)

	t.Run("two resumes", func(t *testing.T) {
		require.False(t, node.ctl(t, "pause", string(pauseResumeRecipient)).Effective.AutoDelivery)
		require.True(t, node.ctl(t, "resume", string(pauseResumeRecipient)).Effective.AutoDelivery)
		require.True(t, node.ctl(t, "resume", string(pauseResumeRecipient)).Effective.AutoDelivery)
		node.deliver(t, node.send(t, "e2e-resumed-twice"), 12*time.Second)
	})
	t.Run("a reconnection after FULL", func(t *testing.T) {
		require.False(t, node.ctl(t, pauseResumeMode("off")...).Effective.AutoDelivery)
		require.True(t, node.ctl(t, pauseResumeMode("full")...).Effective.AutoDelivery)
		pid := node.recipient.process.cmd.Process.Pid
		require.NoError(t, syscall.Kill(pid, syscall.SIGSTOP))
		stopped := true
		t.Cleanup(func() {
			if stopped {
				_ = syscall.Kill(pid, syscall.SIGCONT)
			}
		})
		// Past the seven-second heartbeat timeout the hub has dropped the
		// link, and its scan skips the recipient: no approval opens.
		time.Sleep(9 * time.Second)
		message := node.send(t, "e2e-reconnected")
		require.Never(t, func() bool {
			_, ok := node.pendingFor(message)
			return ok
		}, 2*time.Second, 100*time.Millisecond, "the link was still up")
		require.NoError(t, syscall.Kill(pid, syscall.SIGCONT))
		stopped = false
		node.deliver(t, message, 30*time.Second)
	})
	node.stop(t)
}
