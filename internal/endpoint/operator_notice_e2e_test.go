package endpoint

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/hub"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
)

// The operator escalation notice reaches the operator's tools like any
// other message: listed, read through the same envelope checks, and
// acknowledged. Replying to it fails closed: the system sender has no
// route, and no message row appears.
func TestEndpointOperatorNoticeServesOperatorTools(t *testing.T) {
	fixture := newEndpointE2E(t)
	ctx := t.Context()
	sent, err := fixture.sender.sendMessage(ctx, e2eSendArgs(t, "client-unread", "original body"))
	require.NoError(t, err)
	original := sent.(protocol.AdmissionResult).MessageID
	// The recipient never reads the original; the sender's agent is the
	// configured operator here.
	operator, stalled := fixture.sender, e2eRecipient
	require.NoError(t, fixture.service.FileOperatorNotice(ctx, stalled, e2eSender, original,
		fixture.clock.now.Add(-31*time.Minute)))

	listed, err := operator.listPendingMessages(ctx, json.RawMessage(`{}`))
	require.NoError(t, err)
	var notice protocol.ULID
	var generation int64
	for _, pending := range listed.(hub.ListPendingMessagesResult).Messages {
		if pending.FromAgent == registry.EmitterAgentID {
			notice, generation = pending.MessageID, pending.DeliveryGeneration
		}
	}
	require.True(t, notice.Valid(), "the operator lists the notice")

	readArgs, err := json.Marshal(readParams{ID: notice, DeliveryGeneration: generation})
	require.NoError(t, err)
	read, err := operator.readMessage(ctx, readArgs)
	require.NoError(t, err, "read_message decodes the notice like any message")
	message := read.(protocol.LogicalMessage)
	require.Equal(t, registry.EmitterAgentID, message.FromAgent)
	require.Equal(t, []protocol.AgentID{e2eSender}, message.ToAgents)
	require.Contains(t, message.Subject, string(original))
	require.Contains(t, message.Body, string(stalled))
	require.Contains(t, message.Body, "has waited 31 minutes")
	require.Contains(t, message.Body, "No response is needed.")
	require.NotContains(t, message.Body, "original body", "nothing of the original's content crosses")

	rowsBefore := countE2ERows(t, fixture)
	submitsBefore := fixture.senderHub.callCount(hub.MethodSubmitMessageEnvelope)
	replyArgs, err := json.Marshal(map[string]any{"id": notice, argBody: "seen"})
	require.NoError(t, err)
	_, err = operator.replyToMessage(ctx, replyArgs)
	require.ErrorIs(t, err, ErrHubUnavailable, "admission refuses the reply")
	require.ErrorContains(t, fixture.senderHub.refusal(hub.MethodAdmitMessage),
		"recipient "+string(registry.EmitterAgentID)+" has no route", "the system sender is not routable")
	require.Equal(t, submitsBefore, fixture.senderHub.callCount(hub.MethodSubmitMessageEnvelope))
	require.Equal(t, rowsBefore, countE2ERows(t, fixture), "a refused reply leaves no row")

	ackArgs, err := json.Marshal(ackParams{
		ID: notice, DeliveryGeneration: generation, Status: protocol.AckStatusAcknowledged,
	})
	require.NoError(t, err)
	_, err = operator.ackMessage(ctx, ackArgs)
	require.NoError(t, err)
	var acked string
	require.NoError(t, fixture.store.DB().QueryRowContext(ctx, `SELECT status FROM acks
		WHERE message_id = ? AND agent_id = ?`, notice, e2eSender).Scan(&acked))
	require.Equal(t, string(protocol.AckStatusAcknowledged), acked)

	// Control: the original still reads normally for its own recipient.
	readOriginal, err := json.Marshal(readParams{ID: original, DeliveryGeneration: 1})
	require.NoError(t, err)
	_, err = fixture.recipient.readMessage(ctx, readOriginal)
	require.NoError(t, err)
}

// countE2ERows counts the messages and envelopes the Hub has stored.
func countE2ERows(t *testing.T, fixture *endpointE2E) [2]int {
	t.Helper()
	var rows [2]int
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT
		(SELECT count(*) FROM messages), (SELECT count(*) FROM message_envelopes)`).Scan(&rows[0], &rows[1]))
	return rows
}
