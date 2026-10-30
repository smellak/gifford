package daemon

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/daemon/agentlink"
	"github.com/smellak/gifford/internal/protocol"
)

type resultSource struct {
	results chan agentlink.DeliveryResult
}

func (s *resultSource) DeliveryResults() <-chan agentlink.DeliveryResult { return s.results }

func committedEvidence(spec AttemptSpec, frameID int64) agentlink.DeliveryResult {
	guardVersion := spec.InputGuardVersion
	return agentlink.DeliveryResult{
		FrameID: frameID,
		ReplyTo: frameID - 1,
		Result: protocol.DeliveryResult{
			DeliveryID:                spec.DeliveryID,
			AgentID:                   spec.AgentID,
			Result:                    protocol.DeliveryResultKindCommitted,
			ObservedAt:                attemptTestTime.Add(time.Second),
			ObservedDraftGeneration:   spec.DraftGeneration,
			ObservedInputGuardVersion: &guardVersion,
			ControlVersions:           spec.ControlVersions,
			WriterLeaseID:             attemptTestWriterLease,
		},
	}
}

func TestDeliveryResultConsumerPersistsDeduplicatedEvidenceWithoutRewritingUncertain(t *testing.T) {
	path := filepath.Join(t.TempDir(), "results.db")
	store, attempts, clk := openAttemptTestStore(t, path)
	spec := attemptTestSpec(80)
	seedAttemptQueue(t, store.DB(), spec)
	tx := beginAttemptTx(t, store.DB())
	_, err := attempts.Create(t.Context(), tx, spec)
	require.NoError(t, err)
	require.NoError(t, attempts.MarkPrepared(t.Context(), tx, spec.DeliveryID, attemptTestWriterLease))
	require.NoError(t, attempts.MarkCommitSent(t.Context(), tx, spec.DeliveryID))
	require.NoError(t, attempts.MarkUncertain(t.Context(), tx, spec.DeliveryID, ReasonTimedOut))
	require.NoError(t, tx.Commit())

	consumer, err := NewDeliveryResultConsumer(store.DB(), clk)
	require.NoError(t, err)
	report := committedEvidence(spec, 9)
	inserted, err := consumer.Record(t.Context(), spec.AgentID, spec.AdapterSessionID, report)
	require.NoError(t, err)
	require.True(t, inserted)
	inserted, err = consumer.Record(t.Context(), spec.AgentID, spec.AdapterSessionID, report)
	require.NoError(t, err)
	require.False(t, inserted)

	var count int
	var result, receivedAt string
	require.NoError(t, store.DB().QueryRowContext(t.Context(), `SELECT COUNT(*), result, received_at
		FROM delivery_result_evidence WHERE delivery_id = ?`, spec.DeliveryID).
		Scan(&count, &result, &receivedAt))
	require.Equal(t, 1, count)
	require.Equal(t, string(protocol.DeliveryResultKindCommitted), result)
	require.Equal(t, attemptTestTime, clk.now)
	require.NotEmpty(t, receivedAt)

	tx = beginAttemptTx(t, store.DB())
	got, err := attempts.Get(t.Context(), tx, spec.DeliveryID)
	require.NoError(t, err)
	require.Equal(t, AttemptUncertain, got.State)
	committed, err := attempts.HasCommittedEvidence(t.Context(), tx, spec.DeliveryID)
	require.NoError(t, err)
	require.True(t, committed)
	committed, err = attempts.HasCommittedEvidenceForIdentity(t.Context(), tx,
		spec.MessageID, spec.AgentID, spec.DeliveryGeneration)
	require.NoError(t, err)
	require.True(t, committed)
	committed, err = attempts.HasCommittedEvidenceForIdentity(t.Context(), tx,
		spec.MessageID, "agent-other", spec.DeliveryGeneration)
	require.NoError(t, err)
	require.False(t, committed, "evidence for one recipient must not block another")
	committed, err = attempts.HasCommittedEvidenceForIdentity(t.Context(), tx,
		spec.MessageID, spec.AgentID, spec.DeliveryGeneration+1)
	require.NoError(t, err)
	require.False(t, committed, "evidence for one generation must not block another")
	committed, err = attempts.HasCommittedEvidenceForIdentity(t.Context(), tx,
		attemptTestMessage(82), spec.AgentID, spec.DeliveryGeneration)
	require.NoError(t, err)
	require.False(t, committed, "evidence for one message must not block another")
	require.NoError(t, tx.Commit())

	_, err = consumer.Record(t.Context(), spec.AgentID, "other-session", report)
	require.ErrorIs(t, err, ErrAttemptNotFound)
	wrongControls := report
	wrongControls.Result.ControlVersions = protocol.ControlVersions{attemptTestControlKey: 99}
	_, err = consumer.Record(t.Context(), spec.AgentID, spec.AdapterSessionID, wrongControls)
	require.ErrorIs(t, err, ErrInvalidAttempt)
	wrongAgent := report
	wrongAgent.Result.AgentID = "agent-other"
	_, err = consumer.Record(t.Context(), spec.AgentID, spec.AdapterSessionID, wrongAgent)
	require.ErrorIs(t, err, ErrInvalidAttempt)
	wrongDelivery := report
	wrongDelivery.Result.DeliveryID = "delivery-other"
	_, err = consumer.Record(t.Context(), spec.AgentID, spec.AdapterSessionID, wrongDelivery)
	require.ErrorIs(t, err, ErrAttemptNotFound)
	wrongLease := report
	wrongLease.Result.WriterLeaseID = "writer-lease-other"
	_, err = consumer.Record(t.Context(), spec.AgentID, spec.AdapterSessionID, wrongLease)
	require.ErrorIs(t, err, ErrInvalidAttempt)
}

func TestDeliveryResultConsumerRunStopsWhenLifecycleStreamCloses(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "results.db")
	store, attempts, clk := openAttemptTestStore(t, path)
	spec := attemptTestSpec(81)
	seedAttemptQueue(t, store.DB(), spec)
	tx := beginAttemptTx(t, store.DB())
	_, err := attempts.Create(t.Context(), tx, spec)
	require.NoError(t, err)
	require.NoError(t, attempts.MarkPrepared(t.Context(), tx, spec.DeliveryID, attemptTestWriterLease))
	require.NoError(t, attempts.MarkCommitSent(t.Context(), tx, spec.DeliveryID))
	require.NoError(t, tx.Commit())

	consumer, err := NewDeliveryResultConsumer(store.DB(), clk)
	require.NoError(t, err)
	source := &resultSource{results: make(chan agentlink.DeliveryResult, 1)}
	source.results <- committedEvidence(spec, 11)
	close(source.results)
	require.NoError(t, consumer.Run(t.Context(), spec.AgentID, spec.AdapterSessionID, source))

	var count int
	require.NoError(t, store.DB().QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM delivery_result_evidence WHERE delivery_id = ?", spec.DeliveryID).Scan(&count))
	require.Equal(t, 1, count)
}

func TestDeliveryResultConsumerRunHonorsContext(t *testing.T) {
	t.Parallel()
	consumer := &DeliveryResultConsumer{}
	source := &resultSource{results: make(chan agentlink.DeliveryResult)}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	err := consumer.Run(ctx, "agent-1", "session-1", source)
	require.ErrorIs(t, err, context.Canceled)
}
