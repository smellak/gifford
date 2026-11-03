package hub

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/escalate"
	"github.com/smellak/gifford/internal/fallback"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/renotify"
)

// escalationOperator receives the escalation notices in these tests. It
// is an ordinary agent: insertHubAgent gives it the connected session
// the worker needs to hand a delivery over.
const escalationOperator = protocol.AgentID("agent-operator")

func seedEscalationOperator(t *testing.T, f *hubFixture) {
	t.Helper()
	insertHubAgent(t, f.store, escalationOperator)
	setHubDirectionalLink(t, f, testHubRecipient, escalationOperator, protocol.LinkModeAllow, 0)
}

func noticeMessageID(t *testing.T, db *sql.DB, original protocol.ULID) string {
	t.Helper()
	var id string
	require.NoError(t, db.QueryRowContext(context.Background(), `SELECT message_id FROM messages
		WHERE client_msg_id = ?`, string(escalate.ClientMsgID(original))).Scan(&id))
	return id
}

func noticeQueueState(t *testing.T, db *sql.DB, noticeID string, agent protocol.AgentID) (int64, string) {
	t.Helper()
	var generation int64
	var state string
	err := db.QueryRowContext(context.Background(), `SELECT delivery_generation, state FROM delivery_queue
		WHERE message_id = ? AND agent_id = ?`, noticeID, string(agent)).Scan(&generation, &state)
	if err == sql.ErrNoRows {
		return 0, "absent"
	}
	require.NoError(t, err)
	return generation, state
}

func countOperatorNotices(t *testing.T, db *sql.DB, original protocol.ULID) int {
	t.Helper()
	var n int
	require.NoError(t, db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM messages
		WHERE client_msg_id = ?`, string(escalate.ClientMsgID(original))).Scan(&n))
	return n
}

func overdueUnescalatedItem(at time.Time, id protocol.ULID) renotify.Item {
	return renotify.Item{
		MessageID: id,
		AgentID:   testHubRecipient,
		Facts: fallback.Facts{
			PendingSince: at,
			Notices:      []time.Time{at.Add(15 * time.Minute)},
			GuardsPassed: true,
			Pane:         fallback.PaneIdle,
		},
	}
}

func newEscalationRunner(t *testing.T, f *hubFixture) *renotify.Runner {
	t.Helper()
	escalator, err := escalate.New(escalationOperator, f.clock)
	require.NoError(t, err)
	return &renotify.Runner{
		DB:       f.store.DB(),
		Operator: escalationOperator,
		Notify: func(ctx context.Context, tx *sql.Tx, messageID protocol.ULID, agentID protocol.AgentID) error {
			return renotify.Requeue(ctx, tx, f.clock, f.service.engine, messageID, agentID)
		},
		AdmitOperator: func(ctx context.Context, messageID protocol.ULID, agentID protocol.AgentID, operator protocol.AgentID) error {
			return renotify.AdmitOperatorNotice(ctx, f.service, f.store.DB(), f.clock, agentID, operator, messageID)
		},
		EnqueueOperator: func(ctx context.Context, messageID protocol.ULID, agentID protocol.AgentID, operator protocol.AgentID) error {
			return renotify.EnqueueOperatorNotice(ctx, f.store.DB(), f.service.engine, f.clock, agentID, operator, messageID)
		},
		MarkEscalated: escalator.Escalate,
	}
}

// seedEscalatableMessage admits and submits an ordinary message and
// leaves its first generation delivered, the way a notified-then-read
// message looks before the backup loop turns.
func seedEscalatableMessage(t *testing.T, f *hubFixture) protocol.ULID {
	t.Helper()
	params := testAdmitParams(f)
	answer, err := callAdmit(t, f, params)
	require.NoError(t, err)
	admitted := answer.(AdmitMessageResult)
	rawEnvelope, err := json.Marshal(signedEnvelope(t, f, admitted))
	require.NoError(t, err)
	_, err = f.service.SubmitMessageEnvelope(t.Context(), testHubSender, rawEnvelope)
	require.NoError(t, err)
	_, err = f.store.DB().ExecContext(t.Context(), `UPDATE delivery_queue SET state = 'DELIVERED'
		WHERE message_id = ? AND agent_id = ? AND delivery_generation = 1`,
		admitted.MessageID, testHubRecipient)
	require.NoError(t, err)
	return admitted.MessageID
}

// A real turn files the operator notice exactly once and the worker
// hands it to the operator: the notice leaves a queued first
// generation, the escalation is marked, and a worker pass carries the
// delivery toward the operator. This fails if the admission is never
// enqueued.
func TestEscalationNoticeEnqueuedAndHandedToOperator(t *testing.T) {
	t.Parallel()

	f := newHubFixture(t)
	seedEscalationOperator(t, f)
	messageID := seedEscalatableMessage(t, f)
	runner := newEscalationRunner(t, f)
	now := f.clock.now

	_, err := runner.RunOnce(t.Context(), now.Add(31*time.Minute),
		[]renotify.Item{overdueUnescalatedItem(now, messageID)})
	require.NoError(t, err)
	require.Equal(t, 1, countOperatorNotices(t, f.store.DB(), messageID), "exactly one operator notice")

	noticeID := noticeMessageID(t, f.store.DB(), messageID)
	generation, state := noticeQueueState(t, f.store.DB(), noticeID, escalationOperator)
	require.Equal(t, int64(1), generation, "the notice is queued as its first generation")
	require.Equal(t, "QUEUED", state, "the notice waits for the worker")
	escalated, err := escalate.Escalated(t.Context(), f.store.DB(), messageID)
	require.NoError(t, err)
	require.True(t, escalated, "escalation marked durable")

	handed := runOneWorkerPass(t, f)
	found := false
	for _, delivery := range handed {
		if string(delivery.MessageID) == noticeID && delivery.AgentID == escalationOperator {
			found = true
		}
	}
	require.True(t, found, "the worker hands the notice to the operator, got %v", handed)
}

// A marked escalation whose notice was admitted but never queued
// recovers on the next turn through the same path: the admission
// deduplicates, the missing queue row is filed, the mark stays, and
// no second notice goes out.
func TestMarkedEscalationWithoutNoticeQueueRecovers(t *testing.T) {
	t.Parallel()

	f := newHubFixture(t)
	seedEscalationOperator(t, f)
	messageID := seedEscalatableMessage(t, f)
	now := f.clock.now

	require.NoError(t, renotify.AdmitOperatorNotice(t.Context(), f.service, f.store.DB(),
		f.clock, testHubRecipient, escalationOperator, messageID))
	escalator, err := escalate.New(escalationOperator, f.clock)
	require.NoError(t, err)
	tx, err := f.store.DB().BeginTx(t.Context(), nil)
	require.NoError(t, err)
	require.NoError(t, escalator.Escalate(t.Context(), tx, messageID, testHubRecipient))
	require.NoError(t, tx.Commit())
	_, state := noticeQueueState(t, f.store.DB(), noticeMessageID(t, f.store.DB(), messageID), escalationOperator)
	require.Equal(t, "absent", state, "the legacy mark left no delivery row")

	runner := newEscalationRunner(t, f)
	_, err = runner.RunOnce(t.Context(), now.Add(31*time.Minute),
		[]renotify.Item{overdueUnescalatedItem(now, messageID)})
	require.NoError(t, err)
	require.Equal(t, 1, countOperatorNotices(t, f.store.DB(), messageID), "the retry deduplicates the notice")

	noticeID := noticeMessageID(t, f.store.DB(), messageID)
	generation, state := noticeQueueState(t, f.store.DB(), noticeID, escalationOperator)
	require.Equal(t, int64(1), generation, "the recovery files the missing queue row")
	require.Equal(t, "QUEUED", state)
	escalated, err := escalate.Escalated(t.Context(), f.store.DB(), messageID)
	require.NoError(t, err)
	require.True(t, escalated, "the mark stays")

	handed := runOneWorkerPass(t, f)
	found := false
	for _, delivery := range handed {
		if string(delivery.MessageID) == noticeID && delivery.AgentID == escalationOperator {
			found = true
		}
	}
	require.True(t, found, "the worker hands the recovered notice to the operator, got %v", handed)
}
