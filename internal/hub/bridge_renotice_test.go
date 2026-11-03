package hub

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/daemon"
	"github.com/smellak/gifford/internal/orchestrator"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/renotify"
)

// TestBridgeMarkReadCancelsTheQueuedRenotice files a renotice the way the
// backup loop does, behind a bridge notice already delivered to the pane,
// and then reads the message at the source. The read closes the queued
// renotice as well: the worker's sweep and scan find nothing left to
// deliver, so no further notice reaches the pane.
func TestBridgeMarkReadCancelsTheQueuedRenotice(t *testing.T) {
	f := newBridgeFixture(t)
	_, err := f.store.DB().ExecContext(t.Context(), `UPDATE sessions SET capture_mode = 'tmux_attach'
		WHERE agent_id = ?`, testHubRecipient)
	require.NoError(t, err)
	mirrorParams := testBridgeMirrorParams()
	mirrored, err := callBridgeMirror(t, f, mirrorParams)
	require.NoError(t, err)
	messageID := mirrored.GiffordMessageID
	_, err = f.store.DB().ExecContext(t.Context(), `UPDATE delivery_queue SET state = 'DELIVERED'
		WHERE message_id = ? AND agent_id = ? AND delivery_generation = 1`, messageID, testHubRecipient)
	require.NoError(t, err)
	tx, err := f.store.DB().BeginTx(t.Context(), nil)
	require.NoError(t, err)
	require.NoError(t, renotify.Requeue(t.Context(), tx, f.clock, f.service.engine, messageID, testHubRecipient))
	require.NoError(t, tx.Commit())
	require.Equal(t, "QUEUED", bridgeNoticeState(t, f, messageID, 2))

	_, err = callBridgeRead(t, f, BridgeMarkReadParams{
		SourceNodeID: testBridgeSource, SourceMessageID: mirrorParams.SourceMessageID,
		RecipientAgent: testHubRecipient, ReadAt: testBridgeRead,
	})
	require.NoError(t, err)
	picked := runBridgeQueueWorker(t, f)

	require.Equal(t, "DELIVERED", bridgeNoticeState(t, f, messageID, 1))
	require.Equal(t, "CANCELLED", bridgeNoticeState(t, f, messageID, 2),
		"the source read closes the queued renotice; the worker took %v", picked)
	require.Empty(t, picked, "nothing is left to notify after the source read")
}

// pickedDeliveries stands in for the delivery processor: it leaves the
// queue sweep to the real executor and records each delivery the worker
// hands over instead of sending it, so a test sees what would reach a pane.
type pickedDeliveries struct {
	sweep  daemon.QueueReconciler
	mu     sync.Mutex
	picked []daemon.QueueDelivery
}

func (p *pickedDeliveries) ReconcileQueue(ctx context.Context) (int, error) {
	return p.sweep.ReconcileQueue(ctx)
}

func (p *pickedDeliveries) EvaluateAutomatic(_ context.Context, delivery daemon.QueueDelivery) error {
	p.pick(delivery)
	return nil
}

func (p *pickedDeliveries) EvaluateTmuxAttach(_ context.Context, delivery daemon.QueueDelivery) error {
	p.pick(delivery)
	return nil
}

func (p *pickedDeliveries) pick(delivery daemon.QueueDelivery) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.picked = append(p.picked, delivery)
}

// runBridgeQueueWorker runs one worker pass, sweep and scan, and returns
// the deliveries it handed over.
func runBridgeQueueWorker(t *testing.T, f *hubFixture) []daemon.QueueDelivery {
	t.Helper()
	executor, err := orchestrator.NewExecutor(orchestrator.ExecutorOptions{
		DB: f.store.DB(), Channel: f.service.engine, Approvals: f.controller,
		Links: federatedOrchestratorLinks{}, Clock: f.clock,
	})
	require.NoError(t, err)
	processor := &pickedDeliveries{sweep: executor}
	worker, err := daemon.NewWorker(daemon.WorkerOptions{
		DB: f.store.DB(), Processor: processor, Approvals: executor,
		Prepared: executor, Proxies: federatedOrchestratorLinks{}, Clock: f.clock,
	})
	require.NoError(t, err)
	_, err = worker.RunOnce(t.Context())
	require.NoError(t, err)
	processor.mu.Lock()
	defer processor.mu.Unlock()
	return processor.picked
}

func bridgeNoticeState(t *testing.T, f *hubFixture, messageID protocol.ULID, generation int64) string {
	t.Helper()
	var state string
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(), `SELECT state FROM delivery_queue
		WHERE message_id = ? AND agent_id = ? AND delivery_generation = ?`,
		messageID, testHubRecipient, generation).Scan(&state))
	return state
}
