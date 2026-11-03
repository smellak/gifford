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
	incidentSender    = protocol.AgentID("agent-sender")
	incidentMessage   = protocol.ULID("01ARZ3NDEKTSV4RRFFQ69G00B1")
	secondMessage     = protocol.ULID("01ARZ3NDEKTSV4RRFFQ69G00B4")
)

type incidentClock struct{ now time.Time }

func (c *incidentClock) Now() time.Time { return c.now }

type noticeCall struct {
	destination protocol.AgentID
	notice      Notice
}

type recordingNotifier struct {
	calls          []noticeCall
	unique         map[protocol.OpaqueID]noticeCall
	failFirstAfter bool
}

func (n *recordingNotifier) FileDeliveryIncidentNotice(
	_ context.Context,
	destination protocol.AgentID,
	notice Notice,
) error {
	if !destination.Valid() {
		return errors.New("unexpected destination")
	}
	call := noticeCall{destination: destination, notice: notice}
	n.calls = append(n.calls, call)
	if n.unique == nil {
		n.unique = make(map[protocol.OpaqueID]noticeCall)
	}
	n.unique[notice.ClientMsgID(destination)] = call
	if n.failFirstAfter {
		n.failFirstAfter = false
		return errors.New("lost acknowledgement after filing")
	}
	return nil
}

func (n *recordingNotifier) noticesFor(destination protocol.AgentID) []Notice {
	var notices []Notice
	for _, call := range n.calls {
		if call.destination == destination {
			notices = append(notices, call.notice)
		}
	}
	return notices
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
	f.seedMessageFrom(t, incidentMessage, incidentSender)
}

func (f *workerFixture) seedMessageFrom(
	t *testing.T,
	messageID protocol.ULID,
	sender protocol.AgentID,
) {
	t.Helper()
	stamp := eventstore.FormatTime(incidentT0)
	_, err := f.db.ExecContext(t.Context(), `INSERT INTO messages
		(message_id, client_msg_id, origin_node, from_agent, destination_node,
		 thread_ref, thread_id, priority, state, created_at, expires_at, updated_at,
		 hop_count, route_digest)
		VALUES (?, ?, 'node-local', ?, 'node-local',
		 'thr_01ARZ3NDEKTSV4RRFFQ69G00B2', 'incident-source', 'NORMAL', 'PENDING',
		 ?, ?, ?, 0, 'sha256:incident-route')`, messageID, "client-"+string(messageID), sender,
		stamp, eventstore.FormatTime(incidentT0.Add(24*time.Hour)), stamp)
	require.NoError(t, err)
	_, err = f.db.ExecContext(t.Context(), `INSERT INTO delivery_queue
		(message_id, agent_id, delivery_generation, from_agent, thread_ref,
		 state, priority, created_at, expires_at, updated_at, hop_count, route_digest)
		VALUES (?, ?, 1, ?, 'thr_01ARZ3NDEKTSV4RRFFQ69G00B2',
		 'DELIVERED', 2, ?, ?, ?, 0, 'sha256:incident-route')`, messageID,
		incidentRecipient, sender,
		stamp, eventstore.FormatTime(incidentT0.Add(24*time.Hour)), stamp)
	require.NoError(t, err)
}

func (f *workerFixture) attempt(
	t *testing.T,
	state, reason string,
	finishedAt time.Time,
) {
	f.attemptFor(t, incidentMessage, state, reason, finishedAt)
}

func (f *workerFixture) attemptFor(
	t *testing.T,
	messageID protocol.ULID,
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
		deliveryID, messageID, incidentRecipient, state, terminalReason, stamp, stamp, stamp)
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
	requireRunFiled(t, f, 1)
	requireRunFiled(t, f, 0)
	operatorNotices := f.notifier.noticesFor(incidentOperator)
	require.Len(t, operatorNotices, 1)
	require.Equal(t, NoticeAlert, operatorNotices[0].Kind)
	require.Equal(t, int64(3), operatorNotices[0].Attempts)
	require.Equal(t, int64(5), operatorNotices[0].Minutes)
	require.Len(t, f.notifier.noticesFor(incidentSender), 1)

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

func TestSenderWithTwoMessagesReceivesOneAlert(t *testing.T) {
	t.Parallel()
	f := newWorkerFixture(t)
	f.seedMessageFrom(t, secondMessage, incidentSender)
	f.attempt(t, "FAILED", "DISCONNECTED", incidentT0)
	f.attemptFor(t, secondMessage, "FAILED", "DISCONNECTED", incidentT0.Add(time.Minute))
	f.attempt(t, "FAILED", "DISCONNECTED", incidentT0.Add(2*time.Minute))

	requireRunFiled(t, f, 1)
	requireRunFiled(t, f, 1)
	requireRunFiled(t, f, 0)

	senderNotices := f.notifier.noticesFor(incidentSender)
	require.Len(t, senderNotices, 1)
	require.Equal(t, NoticeAlert, senderNotices[0].Kind)
	require.Equal(t, int64(3), senderNotices[0].Attempts)

	var queued int
	require.NoError(t, f.db.QueryRowContext(t.Context(), `SELECT count(*)
		FROM delivery_incident_sender_notices
		WHERE recipient_agent = ? AND sender_agent = ? AND kind = ?`,
		incidentRecipient, incidentSender, NoticeAlert).Scan(&queued))
	require.Equal(t, 1, queued, "the sender is aggregated rather than notified per message")
}

func TestSenderRecipientCooldown(t *testing.T) {
	t.Parallel()
	f := newWorkerFixture(t)
	f.threeFailures(t)
	requireRunFiled(t, f, 1)
	requireRunFiled(t, f, 1)

	queueAt := func(at time.Time) {
		t.Helper()
		tx, err := f.db.BeginTx(t.Context(), nil)
		require.NoError(t, err)
		require.NoError(t, queueSenderNotices(t.Context(), tx, Notice{
			Kind: NoticeAlert, Recipient: incidentRecipient,
			NormalizedReason: "DISCONNECTED", Minutes: elapsedMinutes(incidentT0, at),
			Attempts: 3, QueuedAt: at,
		}))
		require.NoError(t, tx.Commit())
	}
	countAlerts := func() int {
		t.Helper()
		var count int
		require.NoError(t, f.db.QueryRowContext(t.Context(), `SELECT count(*)
			FROM delivery_incident_sender_notices
			WHERE recipient_agent = ? AND sender_agent = ? AND kind = ?`,
			incidentRecipient, incidentSender, NoticeAlert).Scan(&count))
		return count
	}

	queueAt(incidentT0.Add(5*time.Minute + alertCooldown - time.Second))
	require.Equal(t, 1, countAlerts(), "the pair remains quiet inside four hours")
	queueAt(incidentT0.Add(5*time.Minute + alertCooldown))
	require.Equal(t, 2, countAlerts(), "the pair may reserve its next alert at four hours")
}

func TestAlertCooldown(t *testing.T) {
	t.Parallel()
	f := newWorkerFixture(t)
	f.threeFailures(t)
	requireRunFiled(t, f, 1)
	requireRunFiled(t, f, 1)

	f.clock.now = incidentT0.Add(5*time.Minute + alertCooldown - time.Second)
	requireRunFiled(t, f, 0)
	require.Len(t, f.notifier.noticesFor(incidentOperator), 1)
	require.Len(t, f.notifier.noticesFor(incidentSender), 1)

	f.clock.now = incidentT0.Add(5*time.Minute + alertCooldown)
	requireRunFiled(t, f, 1)
	requireRunFiled(t, f, 1)
	operatorNotices := f.notifier.noticesFor(incidentOperator)
	require.Len(t, operatorNotices, 2)
	require.Equal(t, NoticeAlert, operatorNotices[1].Kind)
	require.Equal(t, int64(245), operatorNotices[1].Minutes)
	require.Len(t, f.notifier.noticesFor(incidentSender), 2)
}

func TestRecoveryObservationFilesOnce(t *testing.T) {
	t.Parallel()
	f := newWorkerFixture(t)
	f.threeFailures(t)
	requireRunFiled(t, f, 1)
	requireRunFiled(t, f, 1)

	f.clock.now = incidentT0.Add(6 * time.Minute)
	f.attempt(t, "COMMITTED", "", f.clock.now)
	requireRunFiled(t, f, 1)
	requireRunFiled(t, f, 1)
	requireRunFiled(t, f, 0)
	operatorNotices := f.notifier.noticesFor(incidentOperator)
	require.Len(t, operatorNotices, 2)
	require.Equal(t, NoticeRecovery, operatorNotices[1].Kind)
	require.Equal(t, int64(3), operatorNotices[1].Attempts)
	require.Equal(t, int64(6), operatorNotices[1].Minutes)
	senderNotices := f.notifier.noticesFor(incidentSender)
	require.Len(t, senderNotices, 2)
	require.Equal(t, NoticeRecovery, senderNotices[1].Kind)
	require.Equal(t, int64(3), senderNotices[1].Attempts)
	require.Equal(t, int64(6), senderNotices[1].Minutes)

	var recoveredAt, notifiedAt string
	require.NoError(t, f.db.QueryRowContext(t.Context(), `SELECT recovered_at, recovery_notified_at
		FROM delivery_incidents WHERE recipient_agent = ? AND normalized_reason = 'DISCONNECTED'`,
		incidentRecipient).Scan(&recoveredAt, &notifiedAt))
	require.Equal(t, eventstore.FormatTime(f.clock.now), recoveredAt)
	require.Equal(t, eventstore.FormatTime(f.clock.now), notifiedAt)
}

func TestRunOnceRecipientCooldownSurvivesNewEpisode(t *testing.T) {
	t.Parallel()
	f := newWorkerFixture(t)
	f.threeFailures(t)
	requireRunFiled(t, f, 1)
	requireRunFiled(t, f, 1)

	f.clock.now = incidentT0.Add(6 * time.Minute)
	f.attempt(t, "COMMITTED", "", f.clock.now)
	requireRunFiled(t, f, 1)
	requireRunFiled(t, f, 1)

	f.attempt(t, "FAILED", "DISCONNECTED", incidentT0.Add(7*time.Minute))
	f.attempt(t, "FAILED", "DISCONNECTED", incidentT0.Add(8*time.Minute))
	f.attempt(t, "FAILED", "DISCONNECTED", incidentT0.Add(9*time.Minute))
	f.clock.now = incidentT0.Add(12 * time.Minute)
	requireRunFiled(t, f, 0)

	operatorNotices := f.notifier.noticesFor(incidentOperator)
	require.Len(t, operatorNotices, 2)
	require.Equal(t, NoticeAlert, operatorNotices[0].Kind)
	require.Equal(t, NoticeRecovery, operatorNotices[1].Kind)
	require.Len(t, f.notifier.noticesFor(incidentSender), 2)

	var firstSeen, lastAlertedAt string
	var alertedAt sql.NullString
	require.NoError(t, f.db.QueryRowContext(t.Context(), `SELECT first_seen, alerted_at,
		last_alerted_at FROM delivery_incidents
		WHERE recipient_agent = ? AND normalized_reason = 'DISCONNECTED'`,
		incidentRecipient).Scan(&firstSeen, &alertedAt, &lastAlertedAt))
	require.Equal(t, eventstore.FormatTime(incidentT0.Add(7*time.Minute)), firstSeen)
	require.False(t, alertedAt.Valid, "the new episode has not alerted")
	require.Equal(t, eventstore.FormatTime(incidentT0.Add(5*time.Minute)), lastAlertedAt,
		"the recipient cooldown survives the episode reset")
}

func TestRunOnceRecoverySurvivesSameKeyRefailure(t *testing.T) {
	t.Parallel()
	f := newWorkerFixture(t)
	f.threeFailures(t)
	requireRunFiled(t, f, 1)
	requireRunFiled(t, f, 1)

	f.attempt(t, "COMMITTED", "", incidentT0.Add(6*time.Minute))
	f.attempt(t, "FAILED", "DISCONNECTED", incidentT0.Add(7*time.Minute))
	f.clock.now = incidentT0.Add(10 * time.Minute)
	requireRunFiled(t, f, 1)

	operatorNotices := f.notifier.noticesFor(incidentOperator)
	require.Len(t, operatorNotices, 2)
	require.Equal(t, NoticeRecovery, operatorNotices[1].Kind)
	require.Equal(t, int64(3), operatorNotices[1].Attempts)
	require.Equal(t, int64(6), operatorNotices[1].Minutes)

	var firstSeen, recoveredAt, recoveryNotifiedAt string
	require.NoError(t, f.db.QueryRowContext(t.Context(), `SELECT first_seen, recovered_at,
		recovery_notified_at FROM delivery_incidents
		WHERE recipient_agent = ? AND normalized_reason = 'DISCONNECTED'`,
		incidentRecipient).Scan(&firstSeen, &recoveredAt, &recoveryNotifiedAt))
	require.Equal(t, eventstore.FormatTime(incidentT0), firstSeen,
		"the recovered episode stays projected until its notice is durable")
	require.Equal(t, eventstore.FormatTime(incidentT0.Add(6*time.Minute)), recoveredAt)
	require.Equal(t, eventstore.FormatTime(f.clock.now), recoveryNotifiedAt)

	requireRunFiled(t, f, 1)
	requireRunFiled(t, f, 0)
	senderNotices := f.notifier.noticesFor(incidentSender)
	require.Len(t, senderNotices, 2)
	require.Equal(t, NoticeRecovery, senderNotices[1].Kind)

	var currentFirst string
	var currentRecovered sql.NullString
	require.NoError(t, f.db.QueryRowContext(t.Context(), `SELECT first_seen, recovered_at
		FROM delivery_incidents
		WHERE recipient_agent = ? AND normalized_reason = 'DISCONNECTED'`,
		incidentRecipient).Scan(&currentFirst, &currentRecovered))
	require.Equal(t, eventstore.FormatTime(incidentT0.Add(7*time.Minute)), currentFirst)
	require.False(t, currentRecovered.Valid, "the relapse becomes current after recovery filing")
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
	require.Equal(t,
		f.notifier.calls[0].notice.ClientMsgID(f.notifier.calls[0].destination),
		f.notifier.calls[1].notice.ClientMsgID(f.notifier.calls[1].destination))
	require.Len(t, f.notifier.unique, 1, "the stable key converges to one filed notice")

	secondRestart, err := NewWorker(Options{
		DB: f.db, Clock: f.clock, Operator: incidentOperator, Notifier: f.notifier,
	})
	require.NoError(t, err)
	filed, err = secondRestart.RunOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, filed)
	require.Len(t, f.notifier.calls, 3, "the sender observation follows the durable operator mark")
	filed, err = secondRestart.RunOnce(t.Context())
	require.NoError(t, err)
	require.Zero(t, filed)
	require.Len(t, f.notifier.calls, 3, "both durable marks suppress another retry")
}

func requireRunFiled(t *testing.T, f *workerFixture, want int) {
	t.Helper()
	filed, err := f.worker.RunOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, want, filed)
}
