package orchestrator

import (
	"bytes"
	"context"
	"database/sql"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

func TestPersistenceGateRequiresDegradedThenLaterHealthyInSameEpoch(t *testing.T) {
	t.Parallel()
	gate := NewPersistenceGate()
	require.False(t, gate.Blocked())

	epoch1 := gate.Fail()
	first := gate.snapshotPlan()
	require.True(t, first.forceDegraded)
	gate.snapshotCommitted(first, protocol.HealthStateHealthy)
	require.True(t, gate.Blocked(), "an early healthy commit cannot reopen the gate")

	gate.snapshotCommitted(first, protocol.HealthStateDegraded)
	recovery := gate.snapshotPlan()
	require.True(t, recovery.active)
	require.False(t, recovery.forceDegraded)

	epoch2 := gate.Fail()
	require.Greater(t, epoch2, epoch1)
	gate.snapshotCommitted(recovery, protocol.HealthStateHealthy)
	require.True(t, gate.Blocked(), "a prior epoch cannot reopen a new failure")

	second := gate.snapshotPlan()
	require.Equal(t, epoch2, second.epoch)
	require.True(t, second.forceDegraded)
	gate.snapshotCommitted(second, protocol.HealthStateDegraded)
	later := gate.snapshotPlan()
	require.False(t, later.forceDegraded)
	gate.snapshotCommitted(later, protocol.HealthStateHealthy)
	require.True(t, gate.Blocked(), "state snapshots do not prove the events writer recovered")
	gate.WriterRecovered()
	require.False(t, gate.Blocked())
}

func TestPersistenceGateFailureRacingRecoveryAlwaysStaysClosed(t *testing.T) {
	t.Parallel()
	for range 100 {
		gate := NewPersistenceGate()
		gate.Fail()
		degraded := gate.snapshotPlan()
		gate.snapshotCommitted(degraded, protocol.HealthStateDegraded)
		gate.WriterRecovered()
		recovery := gate.snapshotPlan()

		start := make(chan struct{})
		var workers sync.WaitGroup
		workers.Add(2)
		go func() {
			defer workers.Done()
			<-start
			gate.Fail()
		}()
		go func() {
			defer workers.Done()
			<-start
			gate.snapshotCommitted(recovery, protocol.HealthStateHealthy)
		}()
		close(start)
		workers.Wait()
		require.True(t, gate.Blocked())
	}
}

func TestStateProducerPersistenceFailureCommitAndRecovery(t *testing.T) {
	fixture := newExecutorFixture(t)
	gate := NewPersistenceGate()
	producer, err := NewStateProducer(StateProducerOptions{
		DB: fixture.store.DB(), Clock: fixture.clock, Channel: fixture.controller,
		Persistence: gate, Entropy: bytes.NewReader(bytes.Repeat([]byte{2}, 4096)),
	})
	require.NoError(t, err)

	gate.Fail()
	tx, err := fixture.store.DB().BeginTx(t.Context(), &sql.TxOptions{ReadOnly: true})
	require.NoError(t, err)
	read, err := producer.ReadLatestForAutomatic(
		t.Context(), tx, executorTestAgent, executorTestSession, fixture.clock.Now(), nil,
	)
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())
	require.False(t, read.AutomaticEligible)
	require.Equal(t, StateEligibilityPersistence, read.EligibilityReason)

	_, err = fixture.store.DB().ExecContext(t.Context(), `
		CREATE TABLE state_persistence_fail (active INTEGER NOT NULL);
		INSERT INTO state_persistence_fail VALUES (1);
		CREATE TRIGGER state_persistence_fail_insert BEFORE INSERT ON state_snapshots
		WHEN EXISTS (SELECT 1 FROM state_persistence_fail WHERE active = 1)
		BEGIN SELECT RAISE(FAIL, 'state persistence failed'); END;`)
	require.NoError(t, err)
	fixture.clock.set(executorTestTime.Add(2 * time.Second))
	require.Error(t, producer.ensurePersistenceDegraded(t.Context()))
	require.True(t, gate.Blocked())
	require.True(t, gate.snapshotPlan().forceDegraded,
		"a failed DEGRADED commit cannot advance recovery")

	_, err = fixture.store.DB().ExecContext(
		t.Context(), "UPDATE state_persistence_fail SET active = 0",
	)
	require.NoError(t, err)
	require.NoError(t, producer.ensurePersistenceDegraded(t.Context()))
	require.True(t, gate.Blocked())
	require.False(t, gate.snapshotPlan().forceDegraded)

	degraded, err := ReadLatestState(
		t.Context(), fixture.store.DB(), executorTestAgent, executorTestSession,
		fixture.clock.Now(), nil,
	)
	require.NoError(t, err)
	require.Equal(t, protocol.HealthStateDegraded, degraded.Snapshot.HealthState)
	require.True(t, strings.HasPrefix(string(degraded.ObservationID), "state-persistence-"))

	healthyAt := executorTestTime.Add(4 * time.Second)
	fixture.clock.set(healthyAt)
	report := stateObservation("observation-after-persistence-recovery", 2, healthyAt)
	require.NoError(t, producer.Submit(report))
	require.NoError(t, producer.produceOne(t.Context(), executorTestAgent))
	require.True(t, gate.Blocked(), "a HEALTHY state commit cannot stand in for events writer recovery")
	gate.WriterRecovered()
	require.False(t, gate.Blocked(), "all three recovery facts reopen the gate")

	healthy, err := ReadLatestState(
		t.Context(), fixture.store.DB(), executorTestAgent, executorTestSession,
		healthyAt, nil,
	)
	require.NoError(t, err)
	require.Equal(t, protocol.HealthStateHealthy, healthy.Snapshot.HealthState)
	tx, err = fixture.store.DB().BeginTx(t.Context(), &sql.TxOptions{ReadOnly: true})
	require.NoError(t, err)
	read, err = producer.ReadLatestForAutomatic(
		t.Context(), tx, executorTestAgent, executorTestSession, healthyAt, nil,
	)
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())
	require.NotEqual(t, StateEligibilityPersistence, read.EligibilityReason)
}

func TestPersistenceGateStaysClosedWhileEventWriterStillFails(t *testing.T) {
	fixture := newExecutorFixture(t)
	gate := NewPersistenceGate()
	producer, err := NewStateProducer(StateProducerOptions{
		DB: fixture.store.DB(), Clock: fixture.clock, Channel: fixture.controller,
		Persistence: gate, Entropy: bytes.NewReader(bytes.Repeat([]byte{3}, 4096)),
	})
	require.NoError(t, err)

	_, err = fixture.store.DB().ExecContext(t.Context(), `
		CREATE TABLE events_persistence_fail (active INTEGER NOT NULL);
		INSERT INTO events_persistence_fail VALUES (1);
		CREATE TRIGGER events_persistence_fail_insert BEFORE INSERT ON events
		WHEN EXISTS (SELECT 1 FROM events_persistence_fail WHERE active = 1)
		BEGIN SELECT RAISE(FAIL, 'events persistence failed'); END;`)
	require.NoError(t, err)
	failed := make(chan struct{})
	recovered := make(chan struct{})
	var failedOnce, recoveredOnce sync.Once
	buffer, err := eventstore.NewBuffer(fixture.store, eventstore.BufferOptions{
		RetryInterval: 50 * time.Millisecond,
		OnRejected:    func(context.Context, eventstore.Rejection) {},
		OnWriteFailure: func(error) {
			gate.Fail()
			failedOnce.Do(func() { close(failed) })
		},
		OnWriteRecovery: func() {
			gate.WriterRecovered()
			recoveredOnce.Do(func() { close(recovered) })
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = fixture.store.DB().ExecContext(
			context.Background(), "UPDATE events_persistence_fail SET active = 0",
		)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		require.NoError(t, buffer.Close(ctx))
	})

	event := protocol.EventEnvelope{
		EventID: "01JB0000000000000000000060", SchemaVersion: protocol.SchemaVersion,
		NodeID: executorTestNode, SessionID: executorTestSession, AgentID: executorTestAgent,
		Harness: protocol.HarnessCodex, HarnessVersion: executorTestHarnessVersion,
		AdapterType: protocol.AdapterTypeGenericPTY, SourceQuality: protocol.SourceQualitySynthetic,
		EventType: protocol.EventTypeIOChunk, Direction: protocol.DirectionOut,
		Stream: protocol.StreamPTY, Origin: protocol.OriginHarness,
		WallTime: executorTestTime, MonotonicNS: 1, Sequence: 1,
		Payload: protocol.Payload(`{"count":1}`), Sensitivity: protocol.SensitivityNormal,
	}
	receipt, err := buffer.EnqueueWithReceipt(t.Context(), []protocol.EventEnvelope{event})
	require.NoError(t, err)
	requireSignal(t, failed)
	require.True(t, gate.Blocked())

	fixture.clock.set(executorTestTime.Add(2 * time.Second))
	require.NoError(t, producer.ensurePersistenceDegraded(t.Context()))
	require.False(t, gate.snapshotPlan().forceDegraded)
	healthyAt := executorTestTime.Add(4 * time.Second)
	fixture.clock.set(healthyAt)
	report := stateObservation("observation-while-events-fail", 2, healthyAt)
	require.NoError(t, producer.Submit(report))
	require.NoError(t, producer.produceOne(t.Context(), executorTestAgent))
	require.True(t, gate.Blocked())
	stats := buffer.Stats()
	require.True(t, stats.Degraded)
	require.Zero(t, stats.Stored)
	require.Equal(t, int64(1), stats.Events)
	select {
	case result := <-receipt:
		require.Fail(t, "receipt resolved while events writer still fails", "%+v", result)
	default:
	}

	tx, err := fixture.store.DB().BeginTx(t.Context(), &sql.TxOptions{ReadOnly: true})
	require.NoError(t, err)
	read, err := producer.ReadLatestForAutomatic(
		t.Context(), tx, executorTestAgent, executorTestSession, healthyAt, nil,
	)
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())
	require.Equal(t, StateEligibilityPersistence, read.EligibilityReason)

	_, err = fixture.store.DB().ExecContext(
		t.Context(), "UPDATE events_persistence_fail SET active = 0",
	)
	require.NoError(t, err)
	requireSignal(t, recovered)
	result := receivePersistenceReceipt(t, receipt)
	require.True(t, result.Stored)
	require.NoError(t, result.Err)
	require.False(t, gate.Blocked())
}

func requireSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for persistence signal")
	}
}

func receivePersistenceReceipt(
	t *testing.T,
	receipt <-chan eventstore.BufferReceipt,
) eventstore.BufferReceipt {
	t.Helper()
	select {
	case result := <-receipt:
		return result
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for event receipt")
	}
	return eventstore.BufferReceipt{}
}
