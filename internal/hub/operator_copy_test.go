package hub

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/deliveryincident"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

// countNotices counts the messages filed under one client key.
func countNotices(t *testing.T, f *hubFixture, key protocol.OpaqueID) int {
	t.Helper()
	var count int
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
		"SELECT count(*) FROM messages WHERE client_msg_id = ?", string(key)).Scan(&count))
	return count
}

// insertUnreachableAgent registers an agent with a local route and no
// session that takes deliveries.
func insertUnreachableAgent(t *testing.T, f *hubFixture, agent protocol.AgentID) {
	t.Helper()
	stamp := eventstore.FormatTime(f.clock.now)
	_, err := f.store.DB().ExecContext(t.Context(), `INSERT INTO agents
		(agent_id, node_id, harness, created_at, updated_at) VALUES (?, ?, 'test', ?, ?)`,
		agent, testHubNode, stamp, stamp)
	require.NoError(t, err)
}

// Each operator after the first gets the same escalation under its own
// key, and every retry converges on one notice per operator.
func TestOperatorNoticeCopyFilesOneNoticePerOperator(t *testing.T) {
	t.Parallel()
	f := noticeFixture(t)
	second := protocol.AgentID("agent-second")
	insertHubAgent(t, f.store, second)
	original := protocol.ULID("01ARZ3NDEKTSV4RRFFQ69G00C1")
	seedNoticeOriginal(t, f, original)
	since := f.clock.now.Add(-time.Hour)
	for range 2 {
		require.NoError(t, f.service.FileOperatorNotice(t.Context(),
			testHubRecipient, protocol.AgentID("agent-operator"), original, since))
		require.NoError(t, f.service.FileOperatorNoticeCopy(t.Context(), testHubRecipient, second, original, since))
	}
	require.Len(t, readNotice(t, f, original), 1, "the first operator keeps its key")
	require.Equal(t, 1, countNotices(t, f, operatorCopyClientKey(original, second)))

	var recipient string
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(), `SELECT mr.agent_id
		FROM messages m JOIN message_recipients mr ON mr.message_id = m.message_id
		WHERE m.client_msg_id = ?`, string(operatorCopyClientKey(original, second))).Scan(&recipient))
	require.Equal(t, string(second), recipient)
}

// An operator with no session that takes deliveries, or no route at all,
// gets nothing, and the caller learns the operator cannot take it now.
func TestOperatorNoticeReportsOperatorThatCannotTakeIt(t *testing.T) {
	t.Parallel()
	f := noticeFixture(t)
	offline := protocol.AgentID("agent-offline")
	insertUnreachableAgent(t, f, offline)
	original := protocol.ULID("01ARZ3NDEKTSV4RRFFQ69G00C2")
	seedNoticeOriginal(t, f, original)
	since := f.clock.now.Add(-time.Hour)

	err := f.service.FileOperatorNoticeCopy(t.Context(), testHubRecipient, offline, original, since)
	require.ErrorIs(t, err, ErrOperatorNotDeliverable)
	require.Zero(t, countNotices(t, f, operatorCopyClientKey(original, offline)))

	err = f.service.FileOperatorNotice(t.Context(), testHubRecipient, protocol.AgentID("agent-unknown"), original, since)
	require.ErrorIs(t, err, ErrOperatorNotDeliverable)
	require.Empty(t, readNotice(t, f, original))

	notice := deliveryincident.Notice{
		Kind: deliveryincident.NoticeAlert, Recipient: testHubRecipient,
		NormalizedReason: "DISCONNECTED", Minutes: 7, Attempts: 3, QueuedAt: f.clock.now,
	}
	require.ErrorIs(t, f.service.FileDeliveryIncidentNotice(t.Context(), offline, notice), ErrOperatorNotDeliverable)
	require.Zero(t, countNotices(t, f, notice.ClientMsgID(offline)))
}
