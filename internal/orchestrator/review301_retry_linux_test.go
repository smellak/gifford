//go:build linux

package orchestrator

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/gateway"
	"github.com/smellak/gifford/internal/protocol"
)

// A valid retry may turn STALE at tx C. It remains part of the failed-call
// series, consumes that attempt, and resumes from fresh state after restart.
func TestReview301StaleRetryCanResumeOnFreshState(t *testing.T) {
	delivery := &integrationDelivery{}
	f := newLinkedChoice(t, delivery)
	f.model.set(func(m *choiceModel) {
		m.answer = func(req protocol.InferRequest) (protocol.InferResult, error) {
			if req.Contract == choiceTriage {
				return triageAnswer(protocol.TriageLabelNormal), nil
			}
			return protocol.InferResult{}, gateway.ErrCallFailed
		}
	})
	worker := newSweepWorker(t, f.executorFixture, f.executor, f.executor.links)
	candidate := seedReadableDelivery(t, f.executorFixture, 312, "synthetic review message")
	_, err := worker.RunOnce(t.Context())
	require.NoError(t, err)
	f.model.set(func(m *choiceModel) {
		m.answer = nil
		m.action = choiceInject
		m.during = func(req protocol.InferRequest) {
			if req.Contract == choiceChoose {
				f.states.set(func(s *choiceStates) {
					s.onRead = func(state *StateRead) { state.Snapshot.StateVersion++ }
				})
			}
		}
	})
	f.advance(5 * time.Second)
	_, err = worker.RunOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, protocol.ExecutionStatusStale, lastDecision(t, f.store.DB(), candidate.MessageID).status)
	require.Zero(t, delivery.prepareCount(), "STALE never delivers against superseded state")
	require.Zero(t, delivery.commitCount())
	f.model.set(func(m *choiceModel) { m.during = nil })
	worker = newSweepWorker(t, f.executorFixture, f.executor, f.executor.links)
	f.advance(10*time.Second - time.Nanosecond)
	processed, err := worker.RunOnce(t.Context())
	require.NoError(t, err)
	require.Zero(t, processed, "STALE consumes the second attempt and its backoff")
	f.advance(time.Nanosecond)
	processed, err = worker.RunOnce(t.Context())
	require.NoError(t, err)
	t.Logf("after state settles: processed=%d choose_calls=%d prepares=%d commits=%d queue=%s pending=%v", processed, len(f.model.calls(choiceChoose)), delivery.prepareCount(), delivery.commitCount(), queueStateOf(t, f.store.DB(), candidate), pendingNotes(t, f.store.DB(), candidate.MessageID))
	require.Equal(t, 1, processed, "STALE must return to the gate rather than strand a failed-call approval")
	require.Len(t, f.model.calls(choiceChoose), 3)
	require.Equal(t, "DELIVERED", queueStateOf(t, f.store.DB(), candidate))
	require.Equal(t, 1, delivery.commitCount())
	decisions := decisionsFor(t, f.store.DB(), candidate.MessageID)
	require.Len(t, decisions, 3)
	require.Equal(t, protocol.ExecutionStatusFailed, decisions[0].status)
	require.Equal(t, protocol.ExecutionStatusStale, decisions[1].status)
	require.Equal(t, protocol.ExecutionStatusExecuted, decisions[2].status)
	require.Empty(t, pendingNotes(t, f.store.DB(), candidate.MessageID))
}
