//go:build linux

package orchestrator

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/daemon"
	"github.com/smellak/gifford/internal/protocol"
)

// A commit that timed out leaves its attempt UNCERTAIN, its row QUEUED and
// a recovery approval pending, and the scan skips the agent. Once the
// Proxy's late COMMITTED is recorded, the sweep closes the row as delivered
// and leaves the episode to the human: deliver_now then reaches no Proxy
// and keeps the approval pending, and reject resolves it with the row
// still delivered.
func TestSweptLateCommitLeavesTheRecoveryToTheHuman(t *testing.T) {
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
	links := executor.links.(integrationLinks)
	worker := newSweepWorker(t, fixture, executor, links)
	human := channel.Actor{Type: protocol.ActorTypeHuman, ID: executorTestOperator}
	db := fixture.store.DB()
	candidate := fixture.seedDelivery(t, 230)
	processed, err := worker.RunOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, processed, "the scan defers the fresh row")
	_, err = executor.ResolveApproval(t.Context(), protocol.ResolveApprovalParams{
		ApprovalID: pendingApproval(t, fixture).ApprovalID, Option: executorTestDeliverNow,
	}, human)
	require.NoError(t, err)
	var uncertainID protocol.OpaqueID
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT delivery_id
		FROM delivery_attempts WHERE message_id = ? AND state = 'UNCERTAIN'`, candidate.MessageID).
		Scan(&uncertainID))
	recovery := pendingApproval(t, fixture)
	close(block)
	late := waitForDeliveryResult(t, links.client, uncertainID, protocol.DeliveryResultKindCommitted)
	consumer, err := daemon.NewDeliveryResultConsumer(db, fixture.clock)
	require.NoError(t, err)
	inserted, err := consumer.Record(t.Context(), candidate.AgentID, executorTestSession, late)
	require.NoError(t, err)
	require.True(t, inserted)
	require.Equal(t, queueStateQueued, queueStateOf(t, db, candidate))

	processed, err = worker.RunOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, processed)
	require.Equal(t, queueStateDelivered, queueStateOf(t, db, candidate))
	require.Equal(t, recovery, pendingApproval(t, fixture), "the recovery approval stays pending")
	state, resolution := attemptResolution(t, db, uncertainID)
	require.Equal(t, string(daemon.AttemptUncertain), state)
	require.False(t, resolution.Valid, "the sweep resolves no UNCERTAIN attempt")

	executor.timeout = 2 * time.Second
	_, err = executor.ResolveApproval(t.Context(), protocol.ResolveApprovalParams{
		ApprovalID: recovery.ApprovalID, Option: executorTestDeliverNow,
	}, human)
	require.Error(t, err)
	require.Equal(t, 1, delivery.prepareCount(), "deliver_now on a delivered row prepares nothing")
	require.Equal(t, 1, delivery.commitCount())
	require.Equal(t, 1, attemptCount(t, db, candidate.MessageID))
	require.Equal(t, recovery, pendingApproval(t, fixture))
	_, resolution = attemptResolution(t, db, uncertainID)
	require.False(t, resolution.Valid)

	rejected, err := executor.ResolveApproval(t.Context(), protocol.ResolveApprovalParams{
		ApprovalID: recovery.ApprovalID, Option: executorTestReject,
	}, human)
	require.NoError(t, err)
	require.Equal(t, protocol.ApprovalStateRejected, rejected.State)
	require.Equal(t, queueStateDelivered, queueStateOf(t, db, candidate))
	state, resolution = attemptResolution(t, db, uncertainID)
	require.Equal(t, string(daemon.AttemptUncertain), state)
	require.Equal(t, string(recovery.ApprovalID), resolution.String)
}

// A paused channel holds every delivery, but expiring a row or closing a
// committed one delivers nothing: with the agent's link up, the sweep
// closes both and the Proxy sees no PREPARE.
func TestSweepClosesWithoutPrepareWhileTheChannelHolds(t *testing.T) {
	fixture := newExecutorFixture(t)
	delivery := &integrationDelivery{}
	executor := attachIntegrationLink(t, fixture, delivery, 2*time.Second)
	worker := newSweepWorker(t, fixture, executor, executor.links.(integrationLinks))
	paused, err := fixture.controller.SetGlobal(t.Context(), protocol.SetGlobalParams{
		Mode: protocol.ChannelModePaused, ExpectedVersion: 0,
	}, channel.Actor{Type: protocol.ActorTypeHuman, ID: executorTestOperator})
	require.NoError(t, err)
	require.Equal(t, protocol.ChannelModePaused, paused.EffectiveMode)
	expired := fixture.seedDelivery(t, 231)
	expireRow(t, fixture, expired)
	committed := fixture.seedDelivery(t, 232)
	seedUncertainCommit(t, fixture, committed, "dlv-held")

	processed, err := worker.RunOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, 2, processed)
	db := fixture.store.DB()
	require.Equal(t, queueStateExpired, queueStateOf(t, db, expired))
	require.Equal(t, queueStateDelivered, queueStateOf(t, db, committed))
	require.Equal(t, []string{"PENDING:"}, approvalStates(t, db, committed.MessageID),
		"the recovery approval stays pending")
	require.Zero(t, delivery.prepareCount(), "closing a row is no delivery")
	require.Zero(t, delivery.commitCount())
	require.Zero(t, attemptCount(t, db, expired.MessageID))
	require.Equal(t, 1, attemptCount(t, db, committed.MessageID))
}
