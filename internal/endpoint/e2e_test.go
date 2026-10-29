package endpoint

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/hub"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
)

const (
	e2eNode      = protocol.NodeID("node-local")
	e2eSender    = protocol.AgentID("agent-sender")
	e2eRecipient = protocol.AgentID("agent-recipient")
	e2eIntruder  = protocol.AgentID("agent-intruder")
)

type e2eClock struct{ now time.Time }

func (c *e2eClock) Now() time.Time { return c.now }

type e2eRegistry struct {
	keys map[protocol.AgentID]registry.KeyInfo
}

func (r *e2eRegistry) ActiveSigningKey(
	_ context.Context,
	_ registry.Querier,
	agent protocol.AgentID,
) (registry.KeyInfo, error) {
	key, ok := r.keys[agent]
	if !ok {
		return registry.KeyInfo{}, registry.ErrNoUsableKey
	}
	return key, nil
}

func (*e2eRegistry) Blocked(context.Context, registry.Querier, protocol.AgentID) (bool, error) {
	return false, nil
}

type boundServiceHub struct {
	agent   protocol.AgentID
	service *hub.Service
	mutate  func(string, any) any

	mu    sync.Mutex
	calls map[string]int
}

func (h *boundServiceHub) WhoAmI(context.Context) (protocol.AgentID, error) { return h.agent, nil }

func (h *boundServiceHub) Call(ctx context.Context, method string, params any) (json.RawMessage, error) {
	h.mu.Lock()
	h.calls[method]++
	h.mu.Unlock()
	raw, err := json.Marshal(params)
	if err != nil {
		return nil, err
	}
	var result any
	switch method {
	case hub.MethodAdmitMessage:
		result, err = h.service.AdmitMessage(ctx, h.agent, raw)
	case hub.MethodSubmitMessageEnvelope:
		result, err = h.service.SubmitMessageEnvelope(ctx, h.agent, raw)
	case hub.MethodListPendingMessages:
		result, err = h.service.ListPendingMessages(ctx, h.agent, raw)
	case hub.MethodClaimMessage:
		result, err = h.service.ClaimMessage(ctx, h.agent, raw)
	case hub.MethodAckMessage:
		result, err = h.service.AckMessage(ctx, h.agent, raw)
	case hub.MethodGetDeliveryStatus:
		result, err = h.service.GetDeliveryStatus(ctx, h.agent, raw)
	default:
		return nil, fmt.Errorf("unsupported test hub method")
	}
	if err != nil {
		return nil, err
	}
	if h.mutate != nil {
		result = h.mutate(method, result)
	}
	return json.Marshal(result)
}

func (h *boundServiceHub) callCount(method string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.calls[method]
}

type endpointE2E struct {
	dataDir    string
	store      *eventstore.Store
	clock      *e2eClock
	service    *hub.Service
	sender     *Server
	recipient  *Server
	senderHub  *boundServiceHub
	receiveHub *boundServiceHub
}

func newEndpointE2E(t *testing.T) *endpointE2E {
	t.Helper()
	dataDir := t.TempDir()
	require.NoError(t, os.Chmod(dataDir, 0o700))
	senderPublic, err := EnsureAgentKey(dataDir, e2eSender)
	require.NoError(t, err)
	recipientPublic, err := EnsureAgentKey(dataDir, e2eRecipient)
	require.NoError(t, err)
	store, err := eventstore.Open(t.Context(), filepath.Join(t.TempDir(), "gifford.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	clk := &e2eClock{now: time.Date(2026, time.October, 29, 19, 0, 0, 0, time.UTC)}
	engine, err := channel.New(channel.Options{Clock: clk})
	require.NoError(t, err)
	reg := &e2eRegistry{keys: map[protocol.AgentID]registry.KeyInfo{
		e2eSender: {
			KeyID: "ed25519:agent-sender:v1", Agent: e2eSender, PublicKey: senderPublic,
		},
		e2eRecipient: {
			KeyID: "ed25519:agent-recipient:v1", Agent: e2eRecipient, PublicKey: recipientPublic,
		},
	}}
	service, err := hub.NewService(hub.ServiceOptions{
		DB: store.DB(), NodeID: e2eNode, Engine: engine, Registry: reg, Clock: clk,
	})
	require.NoError(t, err)
	insertE2EAgent(t, store, e2eSender, clk.now)
	insertE2EAgent(t, store, e2eRecipient, clk.now)
	senderHub := &boundServiceHub{agent: e2eSender, service: service, calls: make(map[string]int)}
	receiveHub := &boundServiceHub{agent: e2eRecipient, service: service, calls: make(map[string]int)}
	sender, err := New(t.Context(), dataDir, bytes.NewReader(nil), io.Discard, senderHub)
	require.NoError(t, err)
	recipient, err := New(t.Context(), dataDir, bytes.NewReader(nil), io.Discard, receiveHub)
	require.NoError(t, err)
	sender.now = clk.Now
	recipient.now = clk.Now
	return &endpointE2E{
		dataDir: dataDir, store: store, clock: clk, service: service,
		sender: sender, recipient: recipient,
		senderHub: senderHub, receiveHub: receiveHub,
	}
}

func insertE2EAgent(t *testing.T, store *eventstore.Store, agent protocol.AgentID, now time.Time) {
	t.Helper()
	stamp := eventstore.FormatTime(now)
	_, err := store.DB().ExecContext(t.Context(), `INSERT INTO agents
		(agent_id, node_id, harness, created_at, updated_at) VALUES (?, ?, 'test', ?, ?)`,
		agent, e2eNode, stamp, stamp)
	require.NoError(t, err)
	_, err = store.DB().ExecContext(t.Context(), `INSERT INTO sessions
		(session_id, agent_id, state, capture_mode, deliverable, manifest_json, started_at, updated_at)
		VALUES (?, ?, 'CONNECTED', 'pty_proxy', 1, '{}', ?, ?)`, "session-"+string(agent), agent, stamp, stamp)
	require.NoError(t, err)
}

func e2eSendArgs(t *testing.T, clientID, body string) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"to": e2eRecipient, "subject": "Review", "body": body, "client_msg_id": clientID,
	})
	require.NoError(t, err)
	return raw
}

func materializeE2EDelivery(t *testing.T, fixture *endpointE2E, id protocol.ULID) {
	t.Helper()
	var from, threadRef, createdAt, expiresAt, routeDigest string
	var hopCount int64
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT from_agent,
		thread_ref, created_at, expires_at, hop_count, route_digest FROM messages WHERE message_id = ?`, id).
		Scan(&from, &threadRef, &createdAt, &expiresAt, &hopCount, &routeDigest))
	_, err := fixture.store.DB().ExecContext(t.Context(), `INSERT INTO delivery_queue
		(message_id, agent_id, delivery_generation, from_agent, thread_ref, state, priority,
		 created_at, expires_at, updated_at, hop_count, route_digest)
		VALUES (?, ?, 1, ?, ?, 'DELIVERING', 2, ?, ?, ?, ?, ?)`, id, e2eRecipient, from,
		threadRef, createdAt, expiresAt, eventstore.FormatTime(fixture.clock.now), hopCount, routeDigest)
	require.NoError(t, err)
}

func TestEndpointSignedRoundTrip(t *testing.T) {
	fixture := newEndpointE2E(t)
	sent, err := fixture.sender.sendMessage(t.Context(), e2eSendArgs(t, "client-roundtrip", "signed body"))
	require.NoError(t, err)
	admission := sent.(protocol.AdmissionResult)
	require.Equal(t, protocol.AdmissionStatusAccepted, admission.Result)
	materializeE2EDelivery(t, fixture, admission.MessageID)

	pendingRaw, err := fixture.recipient.listPendingMessages(t.Context(), json.RawMessage(`{}`))
	require.NoError(t, err)
	pending := pendingRaw.(hub.ListPendingMessagesResult)
	require.Len(t, pending.Messages, 1)
	require.Equal(t, e2eSender, pending.Messages[0].FromAgent)

	readArgs, err := json.Marshal(readParams{
		ID: admission.MessageID, DeliveryGeneration: pending.Messages[0].DeliveryGeneration,
	})
	require.NoError(t, err)
	read, err := fixture.recipient.readMessage(t.Context(), readArgs)
	require.NoError(t, err)
	message := read.(protocol.LogicalMessage)
	require.Equal(t, "signed body", message.Body)
	require.Equal(t, e2eSender, message.FromAgent)

	ackArgs, err := json.Marshal(ackParams{
		ID: admission.MessageID, DeliveryGeneration: 1, Status: protocol.AckStatusAcknowledged,
	})
	require.NoError(t, err)
	_, err = fixture.recipient.ackMessage(t.Context(), ackArgs)
	require.NoError(t, err)
	statusArgs, err := json.Marshal(deliveryStatusParams{MessageID: admission.MessageID})
	require.NoError(t, err)
	statusRaw, err := fixture.sender.getDeliveryStatus(t.Context(), statusArgs)
	require.NoError(t, err)
	status := statusRaw.(hub.GetDeliveryStatusResult)
	require.Len(t, status.Recipients, 1)
	require.Equal(t, hub.AckMessageAcknowledged, status.Recipients[0].AckStatus)
}

func TestEndpointRejectsBadClaimSignature(t *testing.T) {
	fixture := newEndpointE2E(t)
	sent, err := fixture.sender.sendMessage(t.Context(), e2eSendArgs(t, "client-bad-signature", "hidden body"))
	require.NoError(t, err)
	id := sent.(protocol.AdmissionResult).MessageID
	materializeE2EDelivery(t, fixture, id)
	fixture.receiveHub.mutate = func(method string, result any) any {
		if method != hub.MethodClaimMessage {
			return result
		}
		claim := result.(hub.ClaimMessageResult)
		var envelope protocol.WireEnvelope
		require.NoError(t, json.Unmarshal(claim.Envelope, &envelope))
		envelope.Signature[0] ^= 1
		claim.Envelope, err = json.Marshal(envelope)
		require.NoError(t, err)
		return claim
	}
	args, err := json.Marshal(readParams{ID: id, DeliveryGeneration: 1})
	require.NoError(t, err)
	_, err = fixture.recipient.readMessage(t.Context(), args)
	require.ErrorIs(t, err, ErrIntegrity)
}

func TestEndpointRetryDoesNotDuplicateEnvelope(t *testing.T) {
	fixture := newEndpointE2E(t)
	args := e2eSendArgs(t, "client-retry", "once")
	first, err := fixture.sender.sendMessage(t.Context(), args)
	require.NoError(t, err)
	second, err := fixture.sender.sendMessage(t.Context(), args)
	require.NoError(t, err)
	require.Equal(t, first, second)
	var messages, envelopes int
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), "SELECT count(*) FROM messages").Scan(&messages))
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), "SELECT count(*) FROM message_envelopes").Scan(&envelopes))
	require.Equal(t, 1, messages)
	require.Equal(t, 1, envelopes)
	require.Equal(t, 1, fixture.senderHub.callCount(hub.MethodSubmitMessageEnvelope))
}

func TestEndpointRetrySurvivesRestartAndRejectsCollision(t *testing.T) {
	fixture := newEndpointE2E(t)
	args := e2eSendArgs(t, "client-durable-retry", "original body")
	first, err := fixture.sender.sendMessage(t.Context(), args)
	require.NoError(t, err)

	fixture.clock.now = fixture.clock.now.Add(10 * time.Minute)
	restarted, err := New(t.Context(), fixture.dataDir, bytes.NewReader(nil), io.Discard, fixture.senderHub)
	require.NoError(t, err)
	restarted.now = fixture.clock.Now
	second, err := restarted.sendMessage(t.Context(), args)
	require.NoError(t, err)
	require.Equal(t, first, second)
	require.Equal(t, 1, fixture.senderHub.callCount(hub.MethodAdmitMessage))
	require.Equal(t, 1, fixture.senderHub.callCount(hub.MethodSubmitMessageEnvelope))

	_, err = restarted.sendMessage(t.Context(), e2eSendArgs(t, "client-durable-retry", "different body"))
	require.ErrorIs(t, err, ErrBadArguments)
	require.Equal(t, 1, fixture.senderHub.callCount(hub.MethodAdmitMessage))
	require.Equal(t, 1, fixture.senderHub.callCount(hub.MethodSubmitMessageEnvelope))
	require.Equal(t, os.FileMode(0o700), dirMode(t,
		filepath.Join(fixture.dataDir, outboundStateDir, string(e2eSender))))
	require.Equal(t, os.FileMode(0o600), dirMode(t,
		filepath.Join(fixture.dataDir, outboundStateDir, string(e2eSender), outboundStateFile)))
	stored, err := os.ReadFile(filepath.Join(
		fixture.dataDir, outboundStateDir, string(e2eSender), outboundStateFile))
	require.NoError(t, err)
	var state persistedOutboundState
	require.NoError(t, json.Unmarshal(stored, &state))
	require.Len(t, state.Attempts, 1)
	require.Equal(t, "original body", state.Attempts[0].Request.Body)
	require.NotNil(t, state.Attempts[0].Envelope)
	require.NotNil(t, state.Attempts[0].Result)
}

func TestEndpointMetadataMismatchFailsClosed(t *testing.T) {
	fixture := newEndpointE2E(t)
	sent, err := fixture.sender.sendMessage(t.Context(), e2eSendArgs(t, "client-mismatch", "never returned"))
	require.NoError(t, err)
	id := sent.(protocol.AdmissionResult).MessageID
	materializeE2EDelivery(t, fixture, id)
	fixture.receiveHub.mutate = func(method string, result any) any {
		if method != hub.MethodClaimMessage {
			return result
		}
		claim := result.(hub.ClaimMessageResult)
		claim.DeliveryMode = protocol.DeliveryModeDisplayOnly
		return claim
	}
	args, err := json.Marshal(readParams{ID: id, DeliveryGeneration: 1})
	require.NoError(t, err)
	_, err = fixture.recipient.readMessage(t.Context(), args)
	require.ErrorIs(t, err, ErrMetadataMismatch)
	var queueState string
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT state FROM delivery_queue
		WHERE message_id = ? AND agent_id = ? AND delivery_generation = 1`, id, e2eRecipient).Scan(&queueState))
	require.Equal(t, "FAILED", queueState)
}

func TestEndpointDenialNeverSubmits(t *testing.T) {
	fixture := newEndpointE2E(t)
	raw, err := os.ReadFile(filepath.Join("..", "channel", "testdata", "hub", "recipient_rx_off.sql"))
	require.NoError(t, err)
	_, err = fixture.store.DB().ExecContext(t.Context(), string(raw))
	require.NoError(t, err)
	_, err = fixture.sender.sendMessage(t.Context(), e2eSendArgs(t, "client-denied", "not sent"))
	require.ErrorIs(t, err, ErrHubUnavailable)
	require.Zero(t, fixture.senderHub.callCount(hub.MethodSubmitMessageEnvelope))
	var envelopes int
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(),
		"SELECT count(*) FROM message_envelopes").Scan(&envelopes))
	require.Zero(t, envelopes)
}

func TestEndpointReplyIsUnavailableBeforeAdmission(t *testing.T) {
	fixture := newEndpointE2E(t)
	sent, err := fixture.sender.sendMessage(t.Context(), e2eSendArgs(t, "client-reply-source", "question"))
	require.NoError(t, err)
	id := sent.(protocol.AdmissionResult).MessageID
	materializeE2EDelivery(t, fixture, id)
	readArgs, err := json.Marshal(readParams{ID: id, DeliveryGeneration: 1})
	require.NoError(t, err)
	_, err = fixture.recipient.readMessage(t.Context(), readArgs)
	require.NoError(t, err)
	replyClientID := protocol.OpaqueID("client-reply")
	replyArgs, err := json.Marshal(replyParams{
		ID: id, Body: testReplyBody, ClientMsgID: &replyClientID,
	})
	require.NoError(t, err)
	_, err = fixture.recipient.replyToMessage(t.Context(), replyArgs)
	require.ErrorIs(t, err, ErrUnavailable)
	require.Zero(t, fixture.receiveHub.callCount(hub.MethodAdmitMessage))
	require.Zero(t, fixture.receiveHub.callCount(hub.MethodSubmitMessageEnvelope))
}

type fixedClaimHub struct {
	agent protocol.AgentID
	claim hub.ClaimMessageResult
	calls int
}

func (h *fixedClaimHub) WhoAmI(context.Context) (protocol.AgentID, error) { return h.agent, nil }

func (h *fixedClaimHub) Call(_ context.Context, method string, _ any) (json.RawMessage, error) {
	if method != hub.MethodClaimMessage {
		return nil, fmt.Errorf("unexpected test method")
	}
	h.calls++
	return json.Marshal(h.claim)
}

func TestEndpointRejectsValidClaimForAnotherRecipient(t *testing.T) {
	fixture := newEndpointE2E(t)
	sent, err := fixture.sender.sendMessage(t.Context(),
		e2eSendArgs(t, "client-wrong-recipient", "must stay hidden"))
	require.NoError(t, err)
	id := sent.(protocol.AdmissionResult).MessageID
	materializeE2EDelivery(t, fixture, id)
	params, err := json.Marshal(hub.ClaimMessageParams{MessageID: id, DeliveryGeneration: 1})
	require.NoError(t, err)
	claimed, err := fixture.service.ClaimMessage(t.Context(), e2eRecipient, params)
	require.NoError(t, err)

	_, err = EnsureAgentKey(fixture.dataDir, e2eIntruder)
	require.NoError(t, err)
	leakingHub := &fixedClaimHub{agent: e2eIntruder, claim: claimed.(hub.ClaimMessageResult)}
	intruder, err := New(t.Context(), fixture.dataDir, bytes.NewReader(nil), io.Discard, leakingHub)
	require.NoError(t, err)
	readArgs, err := json.Marshal(readParams{ID: id, DeliveryGeneration: 1})
	require.NoError(t, err)
	_, err = intruder.readMessage(t.Context(), readArgs)
	require.ErrorIs(t, err, ErrIntegrity)
	require.Equal(t, 1, leakingHub.calls)
	require.Empty(t, intruder.received)
}
