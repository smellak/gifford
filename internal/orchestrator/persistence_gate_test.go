package orchestrator

import (
	"bytes"
	"database/sql"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

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
	require.False(t, gate.Blocked())
}

func TestPersistenceGateFailureRacingRecoveryAlwaysStaysClosed(t *testing.T) {
	t.Parallel()
	for range 100 {
		gate := NewPersistenceGate()
		gate.Fail()
		degraded := gate.snapshotPlan()
		gate.snapshotCommitted(degraded, protocol.HealthStateDegraded)
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
	require.False(t, gate.Blocked(), "only the later durable HEALTHY snapshot reopens the gate")

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
