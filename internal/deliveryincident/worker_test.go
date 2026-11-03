package deliveryincident

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

var incidentT0 = time.Date(2026, time.November, 3, 12, 0, 0, 0, time.UTC)

const (
	incidentRecipient = protocol.AgentID("agent-recipient")
	incidentOperator  = protocol.AgentID("agent-operator")
	incidentMessage   = protocol.ULID("01ARZ3NDEKTSV4RRFFQ69G00B1")
)

type incidentClock struct{ now time.Time }

func (c *incidentClock) Now() time.Time { return c.now }

type recordingNotifier struct {
	calls          []Notice
	unique         map[protocol.OpaqueID]Notice
	failFirstAfter bool
}

func (n *recordingNotifier) FileDeliveryIncidentNotice(
	_ context.Context,
	operator protocol.AgentID,
	notice Notice,
) error {
	if operator != incidentOperator {
		return errors.New("unexpected operator")
	}
	n.calls = append(n.calls, notice)
	if n.unique == nil {
		n.unique = make(map[protocol.OpaqueID]Notice)
	}
	n.unique[notice.ClientMsgID()] = notice
	if n.failFirstAfter {
		n.failFirstAfter = false
		return errors.New("lost acknowledgement after filing")
	}
	return nil
}

type workerFixture struct {
	db       *sql.DB
	clock    *incidentClock
	notifier *recordingNotifier
	worker   *Worker
	nextID   int
}

func newWorkerFixture(t *testing.T) *workerFixture {
	t.Helper()
	store, err := eventstore.Open(t.Context(), filepath.Join(t.TempDir(), "gifford.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	clock := &incidentClock{now: incidentT0.Add(5 * time.Minute)}
	notifier := &recordingNotifier{}
	worker, err := NewWorker(Options{
		DB: store.DB(), Clock: clock, Operator: incidentOperator, Notifier: notifier,
	})
	require.NoError(t, err)
	fixture := &workerFixture{
		db: store.DB(), clock: clock, notifier: notifier, worker: worker,
	}
	fixture.seedMessage(t)
	return fixture
}

func (f *workerFixture) seedMessage(t *testing.T) {
	t.Helper()
	stamp := eventstore.FormatTime(incidentT0)
	_, err := f.db.ExecContext(t.Context(), `INSERT INTO messages
		(message_id, client_msg_id, origin_node, from_agent, destination_node,
		 thread_ref, thread_id, priority, state, created_at, expires_at, updated_at,
		 hop_count, route_digest)
		VALUES (?, ?, 'node-local', 'agent-sender', 'node-local',
		 'thr_01ARZ3NDEKTSV4RRFFQ69G00B2', 'incident-source', 'NORMAL', 'PENDING',
		 ?, ?, ?, 0, 'sha256:incident-route')`, incidentMessage, "client-incident-source",
		stamp, eventstore.FormatTime(incidentT0.Add(24*time.Hour)), stamp)
	require.NoError(t, err)
	_, err = f.db.ExecContext(t.Context(), `INSERT INTO delivery_queue
		(message_id, agent_id, delivery_generation, from_agent, thread_ref,
		 state, priority, created_at, expires_at, updated_at, hop_count, route_digest)
		VALUES (?, ?, 1, 'agent-sender', 'thr_01ARZ3NDEKTSV4RRFFQ69G00B2',
		 'DELIVERED', 2, ?, ?, ?, 0, 'sha256:incident-route')`, incidentMessage,
		incidentRecipient, stamp, eventstore.FormatTime(incidentT0.Add(24*time.Hour)), stamp)
	require.NoError(t, err)
}

func (f *workerFixture) attempt(
	t *testing.T,
	state, reason string,
	finishedAt time.Time,
) {
	t.Helper()
	f.nextID++
	deliveryID := "incident-delivery-" + strconv.Itoa(f.nextID)
	var terminalReason any
	if reason != "" {
		terminalReason = reason
	}
	stamp := eventstore.FormatTime(finishedAt)
	_, err := f.db.ExecContext(t.Context(), `INSERT INTO delivery_attempts
		(delivery_id, message_id, agent_id, delivery_generation, adapter_session_id,
		 orchestrator_decision_id, channel_decision_id, draft_generation,
		 input_guard_version, control_versions_json, state, terminal_reason,
		 finished_at, created_at, updated_at)
		VALUES (?, ?, ?, 1, 'adapter-session', '01ARZ3NDEKTSV4RRFFQ69G00B3',
		 'chd_01ARZ3NDEKTSV4RRFFQ69G00B3', 0, 0, '{}', ?, ?, ?, ?, ?)`,
		deliveryID, incidentMessage, incidentRecipient, state, terminalReason, stamp, stamp, stamp)
	require.NoError(t, err)
}

func (f *workerFixture) threeFailures(t *testing.T) {
	t.Helper()
	f.attempt(t, "FAILED", "DISCONNECTED", incidentT0)
	f.attempt(t, "FAILED", "DISCONNECTED", incidentT0.Add(time.Minute))
	f.attempt(t, "FAILED", "DISCONNECTED", incidentT0.Add(2*time.Minute))
}

func TestWorkingDoesNotAlert(t *testing.T) {
	t.Parallel()
	f := newWorkerFixture(t)
	f.attempt(t, "FAILED", "AGENT_BUSY", incidentT0)
	f.attempt(t, "FAILED", "AGENT_BUSY", incidentT0.Add(time.Minute))
	f.attempt(t, "FAILED", "AGENT_BUSY", incidentT0.Add(2*time.Minute))
	f.clock.now = incidentT0.Add(10 * time.Minute)

	filed, err := f.worker.RunOnce(t.Context())
	require.NoError(t, err)
	require.Zero(t, filed)
	require.Empty(t, f.notifier.calls)
	var incidents int
	require.NoError(t, f.db.QueryRowContext(t.Context(),
		"SELECT count(*) FROM delivery_incidents").Scan(&incidents))
	require.Zero(t, incidents, "busy observations never enter the alert register")
}

func TestThreeObservationsAtFiveMinutesAlertExactlyOnce(t *testing.T) {
	t.Parallel()
	f := newWorkerFixture(t)
	f.threeFailures(t)

	filed, err := f.worker.RunOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, filed)
	filed, err = f.worker.RunOnce(t.Context())
	require.NoError(t, err)
	require.Zero(t, filed)
	require.Len(t, f.notifier.calls, 1)
	require.Equal(t, NoticeAlert, f.notifier.calls[0].Kind)
	require.Equal(t, int64(3), f.notifier.calls[0].Attempts)
	require.Equal(t, int64(5), f.notifier.calls[0].Minutes)

	var attempts, links int64
	var firstSeen, alertedAt string
	require.NoError(t, f.db.QueryRowContext(t.Context(), `SELECT attempt_count, first_seen, alerted_at
		FROM delivery_incidents WHERE recipient_agent = ? AND normalized_reason = 'DISCONNECTED'`,
		incidentRecipient).Scan(&attempts, &firstSeen, &alertedAt))
	require.Equal(t, int64(3), attempts)
	require.Equal(t, eventstore.FormatTime(incidentT0), firstSeen)
	require.Equal(t, eventstore.FormatTime(f.clock.now), alertedAt)
	require.NoError(t, f.db.QueryRowContext(t.Context(),
		"SELECT count(*) FROM delivery_incident_messages WHERE recipient_agent = ?",
		incidentRecipient).Scan(&links))
	require.Equal(t, int64(3), links)
}

func TestAlertCooldown(t *testing.T) {
	t.Parallel()
	f := newWorkerFixture(t)
	f.threeFailures(t)
	requireRunFiled(t, f, 1)

	f.clock.now = incidentT0.Add(5*time.Minute + alertCooldown - time.Second)
	requireRunFiled(t, f, 0)
	require.Len(t, f.notifier.calls, 1)

	f.clock.now = incidentT0.Add(5*time.Minute + alertCooldown)
	requireRunFiled(t, f, 1)
	require.Len(t, f.notifier.calls, 2)
	require.Equal(t, NoticeAlert, f.notifier.calls[1].Kind)
	require.Equal(t, int64(245), f.notifier.calls[1].Minutes)
}

func TestRecoveryObservationFilesOnce(t *testing.T) {
	t.Parallel()
	f := newWorkerFixture(t)
	f.threeFailures(t)
	requireRunFiled(t, f, 1)

	f.clock.now = incidentT0.Add(6 * time.Minute)
	f.attempt(t, "COMMITTED", "", f.clock.now)
	requireRunFiled(t, f, 1)
	requireRunFiled(t, f, 0)
	require.Len(t, f.notifier.calls, 2)
	require.Equal(t, NoticeRecovery, f.notifier.calls[1].Kind)
	require.Equal(t, int64(3), f.notifier.calls[1].Attempts)
	require.Equal(t, int64(6), f.notifier.calls[1].Minutes)

	var recoveredAt, notifiedAt string
	require.NoError(t, f.db.QueryRowContext(t.Context(), `SELECT recovered_at, recovery_notified_at
		FROM delivery_incidents WHERE recipient_agent = ? AND normalized_reason = 'DISCONNECTED'`,
		incidentRecipient).Scan(&recoveredAt, &notifiedAt))
	require.Equal(t, eventstore.FormatTime(f.clock.now), recoveredAt)
	require.Equal(t, eventstore.FormatTime(f.clock.now), notifiedAt)
}

func TestRestartDoesNotDuplicatePendingNotice(t *testing.T) {
	t.Parallel()
	f := newWorkerFixture(t)
	f.threeFailures(t)
	f.notifier.failFirstAfter = true

	filed, err := f.worker.RunOnce(t.Context())
	require.Error(t, err)
	require.Zero(t, filed)
	require.Len(t, f.notifier.unique, 1, "the first filing reached the normal path")

	restarted, err := NewWorker(Options{
		DB: f.db, Clock: f.clock, Operator: incidentOperator, Notifier: f.notifier,
	})
	require.NoError(t, err)
	filed, err = restarted.RunOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, filed)
	require.Len(t, f.notifier.calls, 2, "the pending outbox item is retried")
	require.Equal(t, f.notifier.calls[0].ClientMsgID(), f.notifier.calls[1].ClientMsgID())
	require.Len(t, f.notifier.unique, 1, "the stable key converges to one filed notice")

	secondRestart, err := NewWorker(Options{
		DB: f.db, Clock: f.clock, Operator: incidentOperator, Notifier: f.notifier,
	})
	require.NoError(t, err)
	filed, err = secondRestart.RunOnce(t.Context())
	require.NoError(t, err)
	require.Zero(t, filed)
	require.Len(t, f.notifier.calls, 2, "the durable mark suppresses another retry")
}

func requireRunFiled(t *testing.T, f *workerFixture, want int) {
	t.Helper()
	filed, err := f.worker.RunOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, want, filed)
}
