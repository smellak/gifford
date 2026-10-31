//go:build linux

package main

import (
	"context"
	"database/sql"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/hub"
	"github.com/smellak/gifford/internal/protocol"
)

const (
	acknowledgedActionApprove  = "approve"
	acknowledgedActionReject   = "reject"
	acknowledgedQueueCancelled = "CANCELLED"
	acknowledgedStatus         = "ACKNOWLEDGED"
	acknowledgedCTL            = "ctl"
)

type acknowledgedApprovalCase struct {
	name         string
	acknowledge  bool
	action       string
	wantQueue    string
	wantApproval protocol.ApprovalState
	wantAttempts int
	wantNotices  int
}

// TestAcknowledgedInboxClosesPendingNotice exercises the whole local route:
// real Hub, Endpoint tools, measured Proxy and ctl. The Endpoint session exits
// before the quiet Proxy reconnects for the same agent, so there is only one
// live recipient session at a time.
func TestAcknowledgedInboxClosesPendingNotice(t *testing.T) {
	tests := []acknowledgedApprovalCase{
		{
			name: "ack then approve", acknowledge: true, action: acknowledgedActionApprove,
			wantQueue: acknowledgedQueueCancelled, wantApproval: protocol.ApprovalStateSuperseded,
		},
		{
			name: "ack then reject", acknowledge: true, action: acknowledgedActionReject,
			wantQueue: acknowledgedQueueCancelled, wantApproval: protocol.ApprovalStateSuperseded,
		},
		{
			name: "read then approve", action: acknowledgedActionApprove,
			wantQueue: "DELIVERED", wantApproval: protocol.ApprovalStateApproved,
			wantAttempts: 1, wantNotices: 1,
		},
		{
			name: "read then reject", action: acknowledgedActionReject,
			wantQueue: acknowledgedQueueCancelled, wantApproval: protocol.ApprovalStateRejected,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runAcknowledgedApprovalCase(t, test)
		})
	}
}

func runAcknowledgedApprovalCase(t *testing.T, test acknowledgedApprovalCase) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Second)
	defer cancel()
	environment := newE2EEnvironment(t)
	hubProcess, hubStderr := startE2EHubBinary(ctx, t, environment, e2eMeasuredGiffordBinary)
	t.Cleanup(func() { _ = hubProcess.terminateAndWait() })

	sender := startE2EAgentBinary(ctx, t, environment.env, "sender", e2eMeasuredGiffordBinary)
	t.Cleanup(func() { stopE2EAgentProcess(sender) })
	recipientEndpoint := startE2EAgentBinary(ctx, t, environment.env, "recipient", e2eMeasuredGiffordBinary)
	t.Cleanup(func() { stopE2EAgentProcess(recipientEndpoint) })

	admitted, approval := sendE2EMessageAndWaitApproval(
		ctx, t, environment, sender, "recipient", "e2e-acked-"+strings.ReplaceAll(test.name, " ", "-"),
	)
	var pending hub.ListPendingMessagesResult
	recipientEndpoint.callTool(t, "list_pending_messages", map[string]any{}, &pending)
	require.Len(t, pending.Messages, 1)
	require.Equal(t, admitted.MessageID, pending.Messages[0].MessageID)
	generation := pending.Messages[0].DeliveryGeneration
	var message protocol.LogicalMessage
	recipientEndpoint.callTool(t, "read_message", map[string]any{
		"id": admitted.MessageID, e2eGenerationField: generation,
	}, &message)
	require.Equal(t, admitted.MessageID, message.MessageID)
	if test.acknowledge {
		var acked map[string]bool
		recipientEndpoint.callTool(t, "ack_message", map[string]any{
			"id": admitted.MessageID, e2eGenerationField: generation,
			e2eStatusField: acknowledgedStatus,
		}, &acked)
		require.True(t, acked["acked"])
	}
	recipientEndpoint.callTool(t, "list_pending_messages", map[string]any{}, &pending)
	if test.acknowledge {
		require.Empty(t, pending.Messages)
	} else {
		require.Len(t, pending.Messages, 1, "read_message alone is not an acknowledgement")
	}
	recipientEndpoint.stop(t)

	recipientProxy := startE2ERecipient(ctx, t, environment, "recipient", "quiet")
	t.Cleanup(func() {
		_ = recipientProxy.process.terminateAndWait()
		_ = recipientProxy.outer.Close()
	})
	db := openE2EReadDB(t, environment.database)
	waitForQuietE2ERecipient(ctx, t, db)

	if test.acknowledge {
		args := acknowledgedApprovalAction(test.action, approval.ApprovalID)
		_, actionErr := runE2ECLIError(ctx, environment.env, e2eMeasuredGiffordBinary, args...)
		var exitErr *exec.ExitError
		require.ErrorAs(t, actionErr, &exitErr)
		require.Equal(t, 1, exitErr.ExitCode())
	} else {
		var resolved protocol.ApprovalRequest
		runE2ECLIBinary(ctx, t, environment.env, e2eMeasuredGiffordBinary, &resolved,
			acknowledgedApprovalAction(test.action, approval.ApprovalID)...)
		require.Equal(t, test.wantApproval, resolved.State)
	}

	if test.wantNotices == 0 {
		require.Never(t, func() bool {
			return len(recipientProxy.deliveredLines(t)) != 0
		}, time.Second, 20*time.Millisecond)
	} else {
		require.Eventually(t, func() bool {
			return len(recipientProxy.deliveredLines(t)) == test.wantNotices
		}, e2eRPCTimeout, 20*time.Millisecond, "recipient stderr: %s", recipientProxy.stderr.String())
	}
	wantAcknowledged := 0
	if test.acknowledge {
		wantAcknowledged = 1
	}
	assertAcknowledgedApprovalState(ctx, t, db, admitted.MessageID, approval.ApprovalID, test,
		wantAcknowledged)

	if !test.acknowledge && test.action == acknowledgedActionApprove {
		// This is the other deterministic race outcome: approval already
		// authorized its one 2PC attempt before the acknowledgement arrives.
		_ = recipientProxy.process.terminateAndWait()
		require.NoError(t, recipientProxy.outer.Close())
		lateEndpoint := startE2EAgentBinary(ctx, t, environment.env, "recipient", e2eMeasuredGiffordBinary)
		t.Cleanup(func() { stopE2EAgentProcess(lateEndpoint) })
		var acked map[string]bool
		lateEndpoint.callTool(t, "ack_message", map[string]any{
			"id": admitted.MessageID, e2eGenerationField: generation,
			e2eStatusField: acknowledgedStatus,
		}, &acked)
		require.True(t, acked["acked"])
		assertAcknowledgedApprovalState(ctx, t, db, admitted.MessageID, approval.ApprovalID, test, 1)
		lateEndpoint.stop(t)
	}

	sender.stop(t)
	require.NoError(t, hubProcess.terminateAndWait(), "hub stderr: %s", hubStderr.String())
}

func acknowledgedApprovalAction(action string, approval protocol.ApprovalID) []string {
	if action == acknowledgedActionReject {
		return []string{acknowledgedCTL, acknowledgedActionReject, string(approval), "--reason", "end-to-end rejection"}
	}
	return []string{acknowledgedCTL, acknowledgedActionApprove, string(approval), "--deliver-now"}
}

func waitForQuietE2ERecipient(ctx context.Context, t *testing.T, db *sql.DB) {
	t.Helper()
	require.Eventually(t, func() bool {
		var draft, activity string
		err := db.QueryRowContext(ctx, `SELECT guard.draft_state, guard.activity_state
			FROM input_guards AS guard JOIN sessions AS session
			ON session.session_id = guard.session_id AND session.agent_id = guard.agent_id
			WHERE guard.agent_id = 'recipient' AND session.state = 'CONNECTED'
				AND session.capture_mode = 'pty_proxy' AND session.deliverable = 1
			ORDER BY session.started_at DESC LIMIT 1`).Scan(&draft, &activity)
		return err == nil && draft == "CLOSED" && activity == "IDLE"
	}, 12*time.Second, 20*time.Millisecond, "recipient did not become quiet")
	// The daemon reconciles Proxy links every two seconds.
	time.Sleep(2300 * time.Millisecond)
}

func assertAcknowledgedApprovalState(
	ctx context.Context,
	t *testing.T,
	db *sql.DB,
	messageID protocol.ULID,
	approvalID protocol.ApprovalID,
	test acknowledgedApprovalCase,
	wantAcknowledged int,
) {
	t.Helper()
	var messageState, queueState, approvalState string
	var attempts, committed, acknowledged int
	require.NoError(t, db.QueryRowContext(ctx, `SELECT state FROM messages WHERE message_id = ?`,
		messageID).Scan(&messageState))
	require.NoError(t, db.QueryRowContext(ctx, `SELECT state FROM delivery_queue
		WHERE message_id = ? AND agent_id = 'recipient' AND delivery_generation = 1`,
		messageID).Scan(&queueState))
	require.NoError(t, db.QueryRowContext(ctx, `SELECT state FROM approval_requests
		WHERE approval_id = ?`, approvalID).Scan(&approvalState))
	require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*),
		coalesce(sum(state = 'COMMITTED'), 0) FROM delivery_attempts
		WHERE message_id = ? AND agent_id = 'recipient'`, messageID).Scan(&attempts, &committed))
	require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM acks
		WHERE message_id = ? AND agent_id = 'recipient' AND delivery_generation = 1
			AND status = 'ACKNOWLEDGED'`, messageID).Scan(&acknowledged))
	require.Equal(t, "PENDING", messageState)
	require.Equal(t, test.wantQueue, queueState)
	require.Equal(t, string(test.wantApproval), approvalState)
	require.Equal(t, test.wantAttempts, attempts)
	require.Equal(t, test.wantAttempts, committed)
	require.Equal(t, wantAcknowledged, acknowledged)
}

func stopE2EAgentProcess(agent *e2eAgent) {
	select {
	case <-agent.process.done:
	default:
		_ = agent.process.terminateAndWait()
	}
	_ = agent.outer.Close()
}
