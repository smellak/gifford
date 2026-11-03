package hub

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"testing"
	"time"

	giffordcrypto "github.com/smellak/gifford/internal/crypto"
	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
)

// noticeFixture wires a real node-local system sender into the hub
// fixture, so FileOperatorNotice signs like production. The default
// operator is registered with a route; tests needing more agents add
// them.
func noticeFixture(t *testing.T) *hubFixture {
	t.Helper()
	f := newHubFixture(t)
	insertHubAgent(t, f.store, protocol.AgentID("agent-operator"))
	return f
}

// seedNoticeOriginal files one ordinary PENDING message the loop would
// escalate: sender stalls, recipient never reads.
func seedNoticeOriginal(t *testing.T, f *hubFixture, id protocol.ULID) {
	t.Helper()
	_, err := f.store.DB().ExecContext(t.Context(), `INSERT INTO messages(message_id, client_msg_id,
		origin_node, from_agent, destination_node, thread_ref, thread_id, priority, state,
		created_at, expires_at, updated_at, hop_count, route_digest)
		VALUES(?, ?, ?, ?, ?, 'thr_00000000000000000000000001', 'thread-1',
		'NORMAL', 'PENDING', ?, ?, ?, 0, 'sha256:00')`,
		string(id), "client-"+string(id), string(testHubNode), string(testHubSender), string(testHubNode),
		eventstore.FormatTime(f.clock.now), eventstore.FormatTime(f.clock.now.Add(48*time.Hour)),
		eventstore.FormatTime(f.clock.now))
	require.NoError(t, err)
}

// noticeRow reads one notice filing back: message state, envelope
// presence and its queue rows.
type noticeRow struct {
	id       protocol.ULID
	state    string
	envelope bool
	queue    []string
}

func readNotice(t *testing.T, f *hubFixture, original protocol.ULID) []noticeRow {
	t.Helper()
	var out []noticeRow
	rows, err := f.store.DB().QueryContext(t.Context(), `SELECT message_id, state,
		EXISTS(SELECT 1 FROM message_envelopes e WHERE e.message_id = m.message_id)
		FROM messages m WHERE client_msg_id = ? ORDER BY created_at, message_id`,
		"operator-notice:"+string(original))
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var n noticeRow
		require.NoError(t, rows.Scan(&n.id, &n.state, &n.envelope))
		out = append(out, n)
	}
	require.NoError(t, rows.Err())
	for i := range out {
		qrows, err := f.store.DB().QueryContext(t.Context(), `SELECT agent_id, delivery_generation, state
			FROM delivery_queue WHERE message_id = ? ORDER BY delivery_generation`,
			string(out[i].id))
		require.NoError(t, err)
		for qrows.Next() {
			var agent string
			var generation int64
			var state string
			require.NoError(t, qrows.Scan(&agent, &generation, &state))
			out[i].queue = append(out[i].queue, state)
		}
		require.NoError(t, qrows.Err())
		_ = qrows.Close()
	}
	return out
}

// The notice files as a normal message: PENDING with its envelope and
// one queued delivery, carrying only ids and the frozen minutes.
func TestOperatorNoticeFilesNormalMessage(t *testing.T) {
	t.Parallel()

	f := noticeFixture(t)
	original := protocol.ULID("01ARZ3NDEKTSV4RRFFQ69G00A1")
	seedNoticeOriginal(t, f, original)
	pendingSince := f.clock.now.Add(-time.Hour)
	require.NoError(t, f.service.FileOperatorNotice(t.Context(),
		testHubRecipient, protocol.AgentID("agent-operator"), original, pendingSince))

	notices := readNotice(t, f, original)
	require.Len(t, notices, 1, "exactly one notice")
	require.Equal(t, "PENDING", notices[0].state)
	require.True(t, notices[0].envelope, "the notice carries its envelope")
	require.Equal(t, []string{"QUEUED"}, notices[0].queue)

	var from, mode string
	var ack int64
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(), `SELECT m.from_agent, mr.delivery_mode, mr.requires_ack
		FROM messages m JOIN message_recipients mr ON mr.message_id = m.message_id
		WHERE m.message_id = ?`, string(notices[0].id)).Scan(&from, &mode, &ack))
	require.Equal(t, string(registry.EmitterAgentID), from, "the system sender owns the notice")
	require.Equal(t, "WHEN_SAFE", mode)
	require.Equal(t, int64(1), ack)

	var rawEnvelope []byte
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
		`SELECT envelope FROM message_envelopes WHERE message_id = ?`,
		string(notices[0].id)).Scan(&rawEnvelope))
	var stored protocol.WireEnvelope
	require.NoError(t, json.Unmarshal(rawEnvelope, &stored))
	// The ciphertext is a normal logical message bound to the envelope:
	// readable, naming the original, the agent and the frozen minutes.
	var message protocol.LogicalMessage
	require.NoError(t, json.Unmarshal(stored.Ciphertext, &message))
	require.NoError(t, message.Validate())
	require.Equal(t, stored.MessageID, message.MessageID)
	require.Equal(t, stored.ClientMsgID, message.ClientMsgID)
	require.Equal(t, registry.EmitterAgentID, message.FromAgent)
	require.Equal(t, []protocol.AgentID{"agent-operator"}, message.ToAgents)
	require.True(t, message.CreatedAt.Equal(stored.CreatedAt))
	require.True(t, message.ExpiresAt.Equal(stored.ExpiresAt))
	require.Equal(t, stored.RouteDigest, message.RouteDigest)
	require.Equal(t, stored.ControlVersion, message.ControlVersion)
	require.Contains(t, message.Subject, string(original))
	require.Contains(t, message.Body, string(original))
	require.Contains(t, message.Body, string(testHubRecipient))
	require.Contains(t, message.Body, "has waited 60 minutes", "minutes freeze from the first filing")
	require.Contains(t, message.Body, "No response is needed.")
}

// The operator lists, claims and acknowledges the notice like any
// other message, and the sender's status tells the truth at each
// stage.
func TestOperatorNoticeListsClaimsAcks(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	f := noticeFixture(t)
	operator := protocol.AgentID("agent-operator")
	setHubDirectionalLink(t, f, registry.EmitterAgentID, operator, protocol.LinkModeAllow, 0)
	original := protocol.ULID("01ARZ3NDEKTSV4RRFFQ69G00A2")
	seedNoticeOriginal(t, f, original)
	require.NoError(t, f.service.FileOperatorNotice(ctx,
		testHubRecipient, operator, original, f.clock.now.Add(-time.Hour)))

	notice := readNotice(t, f, original)[0].id

	pending, err := f.service.ListPendingMessages(ctx, operator, json.RawMessage(`{}`))
	require.NoError(t, err)
	listed, ok := pending.(ListPendingMessagesResult)
	require.True(t, ok)
	found := false
	for _, m := range listed.Messages {
		if m.MessageID == notice {
			found = true
		}
	}
	require.True(t, found, "the notice lists for the operator")

	claimRaw, err := json.Marshal(ClaimMessageParams{MessageID: notice, DeliveryGeneration: 1})
	require.NoError(t, err)
	claimed, err := f.service.ClaimMessage(ctx, operator, claimRaw)
	require.NoError(t, err)
	result, ok := claimed.(ClaimMessageResult)
	require.True(t, ok)
	require.NotEmpty(t, result.Envelope, "the claim returns the envelope")

	statusRaw, _ := json.Marshal(GetDeliveryStatusParams{MessageID: notice})
	status, err := f.service.GetDeliveryStatus(ctx, registry.EmitterAgentID, statusRaw)
	require.NoError(t, err)
	answer, ok := status.(GetDeliveryStatusResult)
	require.True(t, ok)
	require.Equal(t, "PENDING", answer.State)
	require.Len(t, answer.Recipients, 1)
	require.Equal(t, "QUEUED", answer.Recipients[0].State)

	ackRaw, err := json.Marshal(AckMessageParams{MessageID: notice, DeliveryGeneration: 1, Status: AckMessageAcknowledged})
	require.NoError(t, err)
	_, err = f.service.AckMessage(ctx, operator, ackRaw)
	require.NoError(t, err, "the operator acknowledges the notice")

	status, err = f.service.GetDeliveryStatus(ctx, registry.EmitterAgentID, statusRaw)
	require.NoError(t, err)
	answer, ok = status.(GetDeliveryStatusResult)
	require.True(t, ok)
	require.Equal(t, AckMessageAcknowledged, answer.Recipients[0].AckStatus)
}

// The admitted expirer only touches ADMITTED rows: a delivered notice
// stays PENDING past its expiry, and the expirer counts zero.
func TestOperatorNoticeDeliveredSurvivesExpiry(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	f := noticeFixture(t)
	operator := protocol.AgentID("agent-operator")
	original := protocol.ULID("01ARZ3NDEKTSV4RRFFQ69G00A3")
	seedNoticeOriginal(t, f, original)
	require.NoError(t, f.service.FileOperatorNotice(ctx,
		testHubRecipient, operator, original, f.clock.now.Add(-time.Hour)))
	notice := readNotice(t, f, original)[0].id
	_, err := f.store.DB().ExecContext(ctx, `UPDATE delivery_queue SET state = 'DELIVERED'
		WHERE message_id = ?`, string(notice))
	require.NoError(t, err)

	f.clock.now = f.clock.now.Add(25 * time.Hour)
	expired, err := f.service.ExpireAdmitted(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(0), expired, "the delivered notice is not an admission")

	var state string
	require.NoError(t, f.store.DB().QueryRowContext(ctx,
		`SELECT state FROM messages WHERE message_id = ?`, string(notice)).Scan(&state))
	require.Equal(t, "PENDING", state, "the delivered notice still reads PENDING")
}

// A retry after the notice expired reports the expiry once and files
// nothing: the caller still records its single mark.
func TestOperatorNoticeRetryAfterExpiry(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	f := noticeFixture(t)
	operator := protocol.AgentID("agent-operator")
	original := protocol.ULID("01ARZ3NDEKTSV4RRFFQ69G00A4")
	seedNoticeOriginal(t, f, original)
	require.NoError(t, f.service.FileOperatorNotice(ctx,
		testHubRecipient, operator, original, f.clock.now.Add(-25*time.Hour)))

	f.clock.now = f.clock.now.Add(25 * time.Hour)
	err := f.service.FileOperatorNotice(ctx,
		testHubRecipient, operator, original, f.clock.now.Add(-50*time.Hour))
	require.ErrorIs(t, err, ErrOperatorNoticeExpired)
	require.Len(t, readNotice(t, f, original), 1, "no second notice after expiry")
}

// Eight concurrent filings converge: no error, one notice, one row.
func TestOperatorNoticeConcurrentFilings(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	f := noticeFixture(t)
	operator := protocol.AgentID("agent-operator")
	original := protocol.ULID("01ARZ3NDEKTSV4RRFFQ69G00A5")
	seedNoticeOriginal(t, f, original)

	const racers = 8
	errs := make([]error, racers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			errs[i] = f.service.FileOperatorNotice(ctx,
				testHubRecipient, operator, original, f.clock.now.Add(-time.Hour))
		}(i)
	}
	close(start)
	wg.Wait()
	for i, err := range errs {
		require.NoError(t, err, "racer %d", i)
	}
	notices := readNotice(t, f, original)
	require.Len(t, notices, 1, "concurrent filings file one notice")
	require.Equal(t, []string{"QUEUED"}, notices[0].queue, "one queue row")
}

// A notice admitted to a previous operator suppresses a fresh one to
// the new operator: the turn stays quiet on the old row.
func TestOperatorNoticeChangedOperatorCloses(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	f := noticeFixture(t)
	previous := protocol.AgentID("agent-operator")
	current := protocol.AgentID("agent-operator-2")
	insertHubAgent(t, f.store, current)
	original := protocol.ULID("01ARZ3NDEKTSV4RRFFQ69G00A6")
	seedNoticeOriginal(t, f, original)
	require.NoError(t, f.service.FileOperatorNotice(ctx,
		testHubRecipient, previous, original, f.clock.now.Add(-time.Hour)))
	require.Len(t, readNotice(t, f, original), 1)

	require.NoError(t, f.service.FileOperatorNotice(ctx,
		testHubRecipient, current, original, f.clock.now.Add(-time.Hour)))
	require.Len(t, readNotice(t, f, original), 1, "no fresh notice to the new operator")
}

// The operator's own unread message never notifies it: the filing
// skips quietly and the caller still closes the escalation.
func TestOperatorNoticeSelfNoticeSkips(t *testing.T) {
	t.Parallel()

	f := noticeFixture(t)
	original := protocol.ULID("01ARZ3NDEKTSV4RRFFQ69G00A7")
	seedNoticeOriginal(t, f, original)
	require.NoError(t, f.service.FileOperatorNotice(t.Context(),
		protocol.AgentID("agent-operator"), protocol.AgentID("agent-operator"), original, f.clock.now.Add(-time.Hour)))
	require.Empty(t, readNotice(t, f, original), "no notice to self")
}

// A renamed node id changes nothing: the prior lookup keys on the
// service's own id, so the retry still deduplicates under it with an
// older local row present.
func TestOperatorNoticeRetryIgnoresRenamedNode(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	f := noticeFixture(t)
	_, err := f.store.DB().ExecContext(ctx, `INSERT INTO nodes(node_id, organization_id, local, status,
		policy_version, created_at) VALUES('node-0', 'org-1', 1, 'ENROLLED', 'v1', ?)`,
		eventstore.FormatTime(f.clock.now))
	require.NoError(t, err)
	operator := protocol.AgentID("agent-operator")
	original := protocol.ULID("01ARZ3NDEKTSV4RRFFQ69G00A8")
	seedNoticeOriginal(t, f, original)
	require.NoError(t, f.service.FileOperatorNotice(ctx,
		testHubRecipient, operator, original, f.clock.now.Add(-time.Hour)))
	require.NoError(t, f.service.FileOperatorNotice(ctx,
		testHubRecipient, operator, original, f.clock.now.Add(-time.Hour)))
	require.Len(t, readNotice(t, f, original), 1, "the retry deduplicates under the node id")
}

// Notices signed before a rotation still claim after it: the claim
// resolves the retired key, while new filings sign with the new one.
func TestOperatorNoticeRotationKeepsOldClaims(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	f := noticeFixture(t)
	operator := protocol.AgentID("agent-operator")
	first := protocol.ULID("01ARZ3NDEKTSV4RRFFQ69G00A9")
	seedNoticeOriginal(t, f, first)
	require.NoError(t, f.service.FileOperatorNotice(ctx,
		testHubRecipient, operator, first, f.clock.now.Add(-time.Hour)))

	oldID, newID, err := f.keys.emitter.RotateEmitterKey()
	require.NoError(t, err)
	require.NotEqual(t, oldID, newID)

	second := protocol.ULID("01ARZ3NDEKTSV4RRFFQ69G00B1")
	seedNoticeOriginal(t, f, second)
	require.NoError(t, f.service.FileOperatorNotice(ctx,
		testHubRecipient, operator, second, f.clock.now.Add(-time.Hour)))

	for _, id := range []protocol.ULID{readNotice(t, f, first)[0].id, readNotice(t, f, second)[0].id} {
		claimRaw, err := json.Marshal(ClaimMessageParams{MessageID: id, DeliveryGeneration: 1})
		require.NoError(t, err)
		claimed, err := f.service.ClaimMessage(ctx, operator, claimRaw)
		require.NoError(t, err, "notice %s claims across the rotation", id)
		result, ok := claimed.(ClaimMessageResult)
		require.True(t, ok)
		require.NotEmpty(t, result.Envelope)
	}
}

// No agent ever registers under the reserved sender: the deny fires
// on the id itself, before any process check, so a conforming id
// stays registrable for everyone else.
func TestRegisterAgentRejectsReservedSender(t *testing.T) {
	t.Parallel()

	f := noticeFixture(t)
	raw, err := json.Marshal(map[string]any{
		"manifest": registrationManifest(registry.EmitterAgentID),
	})
	require.NoError(t, err)
	_, err = f.service.RegisterAgent(t.Context(), peerIdentity{}, raw)
	require.Error(t, err)
	require.Contains(t, err.Error(), "reserved")
}

// A federated message carrying the reserved sender never lands: the
// hub boundary refuses it with its reason before any row exists, even
// with a valid emitter signature from the originating node. The same
// holds for a reserved recipient, which holds no session anywhere.
func TestFederatedDeliveryRejectsReservedSender(t *testing.T) {
	t.Parallel()

	f := newFederatedHubFixture(t, true)
	emitterKey, err := f.origin.EmitterSigningKey()
	require.NoError(t, err)

	envelope, _, err := f.request.DecodeEnvelope()
	require.NoError(t, err)
	envelope.OriginAgent = registry.EmitterAgentID
	envelope.SenderSigningKeyID = emitterKey.KeyID
	routeRecipients := make([]MessageRecipient, len(envelope.RecipientAgents))
	for i, recipient := range envelope.RecipientAgents {
		routeRecipients[i].Agent = recipient
	}
	envelope.RouteDigest = localRouteDigest(
		envelope.MessageID, envelope.OriginNode, envelope.OriginAgent, routeRecipients,
	)
	require.NoError(t, giffordcrypto.SignWireEnvelope(emitterKey.Priv, &envelope))
	require.NoError(t, envelope.Validate())
	forged := f.request
	forged.Envelope, err = json.Marshal(envelope)
	require.NoError(t, err)

	status, body := f.post(t, forged)
	require.Equal(t, http.StatusForbidden, status)
	requireFederationErrorCode(t, body, protocol.ErrorCodeNotRecipient)
	requireNoFederatedMessageRows(t, f)
}
