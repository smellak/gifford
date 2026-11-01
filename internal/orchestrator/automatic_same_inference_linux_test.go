//go:build linux

package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/analyst"
	"github.com/smellak/gifford/internal/daemon"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

func TestAutomaticChoiceUsesFiveSecondInferenceTTL(t *testing.T) {
	for name, tc := range map[string]struct {
		id      string
		elapsed time.Duration
		status  protocol.ExecutionStatus
		commits int
	}{
		"choice returns at 3.5 seconds": {
			id:      "within",
			elapsed: 3500 * time.Millisecond,
			status:  protocol.ExecutionStatusExecuted,
			commits: 1,
		},
		"choice returns at 5.1 seconds": {
			id:      "expired",
			elapsed: 5100 * time.Millisecond,
			status:  protocol.ExecutionStatusStale,
		},
	} {
		t.Run(name, func(t *testing.T) {
			delivery := &integrationDelivery{}
			f := newLinkedChoice(t, delivery)
			profile, analyzer := reviewRefreshAnalysis(
				t, f.executorFixture, protocol.OpaqueID("call-choice-ttl-"+tc.id),
			)
			producer := newTestStateProducer(t, f.executorFixture, analyzer, profile)
			observedAt := executorTestTime.Add(time.Second)
			f.clock.set(observedAt)
			report := reviewRefreshObservation(
				protocol.OpaqueID("observation-choice-ttl-"+tc.id), observedAt,
			)
			require.NoError(t, producer.Submit(report))
			require.NoError(t, producer.Demand(report.AgentID))
			require.NoError(t, producer.produceOne(t.Context(), report.AgentID))
			f.executor.states = producer
			f.executor.stateProfile = &profile
			f.model.set(func(model *choiceModel) {
				model.during = func(request protocol.InferRequest) {
					if request.Contract == choiceChoose {
						f.clock.set(observedAt.Add(tc.elapsed))
					}
				}
			})

			candidate := seedReadableDelivery(
				t, f.executorFixture, 74, "please review the parser change",
			)
			require.NoError(t, f.executor.EvaluateAutomatic(t.Context(), candidate))
			decision := lastDecision(t, f.store.DB(), candidate.MessageID)
			require.Equal(t, tc.status, decision.status)
			require.Equal(t, tc.commits, delivery.commitCount())
			if tc.status == protocol.ExecutionStatusStale {
				require.Contains(t,
					guardDetail(decision.guards, protocol.GuardNamePIdleThreshold),
					StateEligibilityExpired,
				)
				require.Zero(t, attemptCount(t, f.store.DB(), candidate.MessageID))
			}
		})
	}
}

type bankLatencyAnalyzer struct {
	inner   StateAnalyzer
	cadence *bankObservationCadence
}

func (a *bankLatencyAnalyzer) Analyze(
	ctx context.Context,
	observation analyst.Observation,
) (analyst.Analysis, error) {
	a.cadence.advance(ctx, 1300*time.Millisecond, false)
	return a.inner.Analyze(ctx, observation)
}

// bankObservationCadence advances a fake clock while preserving the
// production ordering: observations arrive each second, and the producer may
// commit only after its one-second interval. Unrelated approvals make the
// refreshed snapshots observably distinct without changing the delivery's
// control vector.
type bankObservationCadence struct {
	t                *testing.T
	fixture          *executorFixture
	producer         *StateProducer
	first            protocol.StateObservationReport
	next             time.Time
	generation       int64
	approvalSequence int
}

func (c *bankObservationCadence) advance(ctx context.Context, elapsed time.Duration, flush bool) {
	c.t.Helper()
	target := c.fixture.clock.Now().Add(elapsed)
	for !c.next.After(target) {
		c.fixture.clock.set(c.next)
		c.addPendingApproval(ctx)
		report := refreshReviewObservation(
			c.first,
			protocol.OpaqueID(fmt.Sprintf("observation-bank-refresh-%d", c.generation)),
			true,
		)
		report.ObservationGeneration = c.generation
		report.ObservedAt = c.next
		report.Activity.SecondsSinceLastOutput =
			c.first.Activity.SecondsSinceLastOutput + c.next.Sub(c.first.ObservedAt).Seconds()
		require.NoError(c.t, c.producer.Submit(report))
		if flush {
			c.flush(ctx)
		}
		c.generation++
		c.next = c.next.Add(time.Second)
	}
	c.fixture.clock.set(target)
	if flush {
		c.flush(ctx)
	}
}

func (c *bankObservationCadence) addPendingApproval(ctx context.Context) {
	c.t.Helper()
	c.approvalSequence++
	tx, err := c.fixture.store.DB().BeginTx(ctx, nil)
	require.NoError(c.t, err)
	defer rollback(tx)
	messageID := protocol.ULID(fmt.Sprintf(
		"01ARZ3NDEKTSV4RRFFQ69G%04d", 5000+c.approvalSequence,
	))
	_, err = c.fixture.controller.CreateOrchestratorApproval(
		ctx, tx, messageID, executorTestAgent, "unrelated cadence fixture",
	)
	require.NoError(c.t, err)
	require.NoError(c.t, tx.Commit())
}

func (c *bankObservationCadence) flush(ctx context.Context) {
	c.t.Helper()
	if !c.producer.hasQueuedObservation(executorTestAgent) {
		return
	}
	err := c.producer.produceOne(ctx, executorTestAgent)
	if errors.Is(err, errStateCommitRateLimited) {
		return
	}
	require.NoError(c.t, err)
}

// This is the observed bank path end to end: ANALYZE takes 1.3s, TRIAGE
// 1.4s, CHOOSE 1.45s, observations continue each second and refreshed state
// rows reuse the original inference. The first safe tx-A decision must cross
// tx C and the post-PREPARE check into exactly one committed attempt.
func TestAutomaticBankLatenciesCommitFirstSafeInference(t *testing.T) {
	delivery := &integrationDelivery{}
	f := newLinkedChoice(t, delivery)
	profile, inner := reviewRefreshAnalysis(t, f.executorFixture, "call-bank-latencies")
	analyzer := &bankLatencyAnalyzer{inner: inner}
	producer := newTestStateProducer(t, f.executorFixture, analyzer, profile)
	observedAt := executorTestTime.Add(time.Second)
	first := reviewRefreshObservation("observation-bank-first", observedAt)
	cadence := &bankObservationCadence{
		t: t, fixture: f.executorFixture, producer: producer, first: first,
		next: observedAt.Add(time.Second), generation: first.ObservationGeneration + 1,
	}
	analyzer.cadence = cadence
	f.clock.set(observedAt)
	require.NoError(t, producer.Submit(first))
	require.NoError(t, producer.Demand(first.AgentID))
	require.NoError(t, producer.produceOne(t.Context(), first.AgentID))
	f.executor.states = producer
	f.executor.stateProfile = &profile
	f.model.set(func(model *choiceModel) {
		model.during = func(request protocol.InferRequest) {
			switch request.Contract {
			case choiceTriage:
				cadence.advance(t.Context(), 1400*time.Millisecond, true)
			case choiceChoose:
				cadence.advance(t.Context(), 1450*time.Millisecond, true)
			}
		}
	})

	candidate := seedReadableDelivery(
		t, f.executorFixture, 75, "please review the parser change",
	)
	worker, err := daemon.NewWorker(daemon.WorkerOptions{
		DB: f.store.DB(), Processor: f.executor, Approvals: f.executor,
		Prepared: f.executor, Proxies: f.executor.links, Clock: f.clock,
	})
	require.NoError(t, err)
	processed, err := worker.RunOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, processed)

	db := f.store.DB()
	decisions := decisionsFor(t, db, candidate.MessageID)
	require.Len(t, decisions, 1, "the first safe decision must not need a STALE retry")
	require.Equal(t, protocol.ExecutionStatusExecuted, decisions[0].status)
	require.Equal(t, choiceInject, decisions[0].selected)
	require.Equal(t, 1, attemptCount(t, db, candidate.MessageID))
	require.Equal(t, 1, countRows(t, db, `SELECT count(*) FROM delivery_attempts
		WHERE message_id = ? AND state = 'COMMITTED'`, candidate.MessageID))
	require.Equal(t, 1, delivery.prepareCount())
	require.Equal(t, 1, delivery.commitCount())
	require.Zero(t, delivery.cancelCount())
	require.Equal(t, "DELIVERED", queueStateOf(t, db, candidate))

	latest, err := ReadLatestState(
		t.Context(), db, executorTestAgent, executorTestSession, f.clock.Now(), &profile,
	)
	require.NoError(t, err)
	require.True(t, latest.AutomaticEligible)
	require.Equal(t, protocol.OpaqueID("call-bank-latencies"), latest.GlimmerCallID)
	require.NotEqual(t, decisions[0].stateID, string(latest.Snapshot.StateID),
		"CHOOSE must cross a committed refresh of the same inference")

	rows, err := db.QueryContext(t.Context(), `SELECT committed_at FROM state_snapshots
		WHERE glimmer_call_id = ? ORDER BY committed_at`, "call-bank-latencies")
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()
	var committed []time.Time
	for rows.Next() {
		var raw string
		require.NoError(t, rows.Scan(&raw))
		at, parseErr := eventstore.ParseTime(raw)
		require.NoError(t, parseErr)
		committed = append(committed, at)
	}
	require.NoError(t, rows.Err())
	require.Len(t, committed, 3, "ANALYZE plus two cadence refreshes reuse one call")
	for i := 1; i < len(committed); i++ {
		require.GreaterOrEqual(t, committed[i].Sub(committed[i-1]), stateCommitInterval)
	}
	require.Equal(t, observedAt.Add(4150*time.Millisecond), f.clock.Now())
}
