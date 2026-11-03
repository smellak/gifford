package hub

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/daemon"
	"github.com/smellak/gifford/internal/orchestrator"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/renotify"
)

// TestAckOfAnEarlierNoticeCancelsTheQueuedRenotice files a renotice the
// way the backup loop does, behind a notice already delivered to the
// pane, and then has the agent acknowledge that delivered notice. The ACK
// closes the queued renotice as well: the worker's sweep and scan find
// nothing left to deliver, so no further notice reaches the pane.
func TestAckOfAnEarlierNoticeCancelsTheQueuedRenotice(t *testing.T) {
	f := newHubFixture(t)
	params := testAdmitParams(f)
	answer, err := callAdmit(t, f, params)
	require.NoError(t, err)
	admitted := answer.(AdmitMessageResult)
	rawEnvelope, err := json.Marshal(signedEnvelope(t, f, admitted))
	require.NoError(t, err)
	_, err = f.service.SubmitMessageEnvelope(t.Context(), testHubSender, rawEnvelope)
	require.NoError(t, err)
	messageID := admitted.MessageID
	_, err = f.store.DB().ExecContext(t.Context(), `UPDATE delivery_queue SET state = 'DELIVERED'
		WHERE message_id = ? AND agent_id = ? AND delivery_generation = 1`, messageID, testHubRecipient)
	require.NoError(t, err)
	tx, err := f.store.DB().BeginTx(t.Context(), nil)
	require.NoError(t, err)
	require.NoError(t, renotify.Requeue(t.Context(), tx, f.clock, f.service.engine, messageID, testHubRecipient))
	require.NoError(t, tx.Commit())
	require.Equal(t, "QUEUED", queueGenerationState(t, f, messageID, 2))

	ack, err := json.Marshal(AckMessageParams{
		MessageID: messageID, DeliveryGeneration: 1, Status: AckMessageAcknowledged,
	})
	require.NoError(t, err)
	_, err = f.service.AckMessage(t.Context(), testHubRecipient, ack)
	require.NoError(t, err)
	handed := runOneWorkerPass(t, f)

	require.Equal(t, "DELIVERED", queueGenerationState(t, f, messageID, 1))
	require.Equal(t, "CANCELLED", queueGenerationState(t, f, messageID, 2),
		"the ACK closes the queued renotice; the worker took %v", handed)
	require.Empty(t, handed, "nothing is left to notify after the ACK")
}

// recordingProcessor stands in for the delivery processor: it leaves the
// queue sweep to the real executor and records each delivery the worker
// hands over instead of sending it, so a test sees what would reach a pane.
type recordingProcessor struct {
	sweep  daemon.QueueReconciler
	mu     sync.Mutex
	handed []daemon.QueueDelivery
}

func (p *recordingProcessor) ReconcileQueue(ctx context.Context) (int, error) {
	return p.sweep.ReconcileQueue(ctx)
}

func (p *recordingProcessor) EvaluateAutomatic(_ context.Context, delivery daemon.QueueDelivery) error {
	p.record(delivery)
	return nil
}

func (p *recordingProcessor) EvaluateTmuxAttach(_ context.Context, delivery daemon.QueueDelivery) error {
	p.record(delivery)
	return nil
}

func (p *recordingProcessor) record(delivery daemon.QueueDelivery) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.handed = append(p.handed, delivery)
}

// runOneWorkerPass runs one worker pass, sweep and scan, and returns the
// deliveries it handed over.
func runOneWorkerPass(t *testing.T, f *hubFixture) []daemon.QueueDelivery {
	t.Helper()
	executor, err := orchestrator.NewExecutor(orchestrator.ExecutorOptions{
		DB: f.store.DB(), Channel: f.service.engine, Approvals: f.controller,
		Links: federatedOrchestratorLinks{}, Clock: f.clock,
	})
	require.NoError(t, err)
	processor := &recordingProcessor{sweep: executor}
	worker, err := daemon.NewWorker(daemon.WorkerOptions{
		DB: f.store.DB(), Processor: processor, Approvals: executor,
		Prepared: executor, Proxies: federatedOrchestratorLinks{}, Clock: f.clock,
	})
	require.NoError(t, err)
	_, err = worker.RunOnce(t.Context())
	require.NoError(t, err)
	processor.mu.Lock()
	defer processor.mu.Unlock()
	return processor.handed
}

func queueGenerationState(t *testing.T, f *hubFixture, messageID protocol.ULID, generation int64) string {
	t.Helper()
	var state string
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(), `SELECT state FROM delivery_queue
		WHERE message_id = ? AND agent_id = ? AND delivery_generation = ?`,
		messageID, testHubRecipient, generation).Scan(&state))
	return state
}
