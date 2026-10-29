package hub

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/channel"
	giffordcrypto "github.com/smellak/gifford/internal/crypto"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
)

const (
	testHubNode      = protocol.NodeID("node-local")
	testHubSender    = protocol.AgentID("agent-sender")
	testHubRecipient = protocol.AgentID("agent-recipient")
	testHubMessage   = protocol.ULID("01ARZ3NDEKTSV4RRFFQ69G5FAV")
	testStatusKey    = "status"
)

type hubClock struct{ now time.Time }

func (c *hubClock) Now() time.Time { return c.now }

type hubRegistry struct {
	keys    map[protocol.AgentID]registry.KeyInfo
	blocked map[protocol.AgentID]bool
}

func (r *hubRegistry) ActiveSigningKey(
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

func (r *hubRegistry) Blocked(
	_ context.Context,
	_ registry.Querier,
	agent protocol.AgentID,
) (bool, error) {
	return r.blocked[agent], nil
}

type hubFixture struct {
	store      *eventstore.Store
	service    *Service
	controller *channel.Controller
	clock      *hubClock
	keys       *hubRegistry
	private    ed25519.PrivateKey
}

func newHubFixture(t *testing.T) *hubFixture {
	t.Helper()
	store, err := eventstore.Open(t.Context(), filepath.Join(t.TempDir(), "gifford.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	now := time.Date(2026, time.October, 29, 12, 0, 0, 0, time.UTC)
	clk := &hubClock{now: now}
	engine, err := channel.New(channel.Options{Clock: clk})
	require.NoError(t, err)
	controller, err := channel.NewController(channel.ControllerOptions{
		DB: store.DB(), Engine: engine, Clock: clk, NodeID: testHubNode,
	})
	require.NoError(t, err)
	require.NoError(t, controller.Reload(t.Context()))
	public, private, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	keys := &hubRegistry{
		keys: map[protocol.AgentID]registry.KeyInfo{
			testHubSender: {
				KeyID: "ed25519:agent-sender:v1", Agent: testHubSender, PublicKey: public,
			},
		},
		blocked: make(map[protocol.AgentID]bool),
	}
	service, err := NewService(ServiceOptions{
		DB: store.DB(), NodeID: testHubNode, Engine: engine, Registry: keys, Clock: clk,
	})
	require.NoError(t, err)
	insertHubAgent(t, store, testHubSender, true)
	insertHubAgent(t, store, testHubRecipient, true)
	return &hubFixture{
		store: store, service: service, controller: controller, clock: clk, keys: keys, private: private,
	}
}

func setHubLink(t *testing.T, f *hubFixture, mode protocol.LinkMode, expected int64) {
	t.Helper()
	_, err := f.controller.SetLink(t.Context(), protocol.SetLinkParams{
		FromAgent: testHubSender, ToAgent: testHubRecipient, Mode: mode, Plane: protocol.PlaneMessage,
		ExpectedVersions: protocol.ControlVersions{
			"link:agent-sender→agent-recipient:message": expected,
		},
	}, channel.Actor{Type: protocol.ActorTypeHuman, ID: "operator-test"})
	require.NoError(t, err)
}

func insertHubAgent(t *testing.T, store *eventstore.Store, agent protocol.AgentID, deliverable bool) {
	t.Helper()
	stamp := eventstore.FormatTime(time.Date(2026, time.October, 29, 11, 0, 0, 0, time.UTC))
	_, err := store.DB().ExecContext(t.Context(), `INSERT INTO agents
		(agent_id, node_id, harness, created_at, updated_at) VALUES (?, ?, 'test', ?, ?)`,
		agent, testHubNode, stamp, stamp)
	require.NoError(t, err)
	_, err = store.DB().ExecContext(t.Context(), `INSERT INTO sessions
		(session_id, agent_id, state, capture_mode, deliverable, manifest_json,
		 started_at, updated_at) VALUES (?, ?, 'CONNECTED', 'pty_proxy', ?, '{}', ?, ?)`,
		"session-"+string(agent), agent, deliverable, stamp, stamp)
	require.NoError(t, err)
}

func testAdmitParams(f *hubFixture) AdmitMessageParams {
	return AdmitMessageParams{
		ClientMsgID: "client-message-1",
		OriginNode:  testHubNode, DestinationNode: testHubNode,
		Recipients: []MessageRecipient{{
			Agent: testHubRecipient, DeliveryMode: protocol.DeliveryModeWhenSafe, RequiresAck: true,
		}},
		ThreadID: "thread-local",
		Priority: protocol.PriorityNormal, CreatedAt: f.clock.now, ExpiresAt: f.clock.now.Add(time.Hour),
		HopCount: 0,
	}
}

func callAdmit(t *testing.T, f *hubFixture, params AdmitMessageParams) (any, error) {
	t.Helper()
	raw, err := json.Marshal(params)
	require.NoError(t, err)
	return f.service.AdmitMessage(t.Context(), testHubSender, raw)
}

func signedEnvelope(t *testing.T, f *hubFixture, admitted AdmitMessageResult) protocol.WireEnvelope {
	t.Helper()
	return signedEnvelopeFor(t, f, admitted, testAdmitParams(f))
}

func signedEnvelopeFor(
	t *testing.T,
	f *hubFixture,
	admitted AdmitMessageResult,
	params AdmitMessageParams,
) protocol.WireEnvelope {
	t.Helper()
	params = resolvedAdmissionParams(params, admitted)
	ciphertext := []byte("opaque payload bytes")
	digest := sha256.Sum256(ciphertext)
	envelope := protocol.WireEnvelope{
		Protocol: protocol.WireProtocol, Version: protocol.WireVersion,
		MessageID: params.MessageID, ClientMsgID: params.ClientMsgID,
		OriginNode: params.OriginNode, OriginAgent: testHubSender,
		DestinationNode: params.DestinationNode, RecipientAgents: []protocol.AgentID{testHubRecipient},
		CreatedAt: params.CreatedAt, ExpiresAt: params.ExpiresAt, Priority: params.Priority,
		ThreadRef: params.ThreadRef, HopCount: params.HopCount, RouteDigest: params.RouteDigest,
		ControlVersion: admitted.AdmissionResult.ControlVersion, ContentType: protocol.WireContentType,
		EncryptionProfile:     protocol.EncryptionProfileNone,
		SenderSigningKeyID:    "ed25519:agent-sender:v1",
		RecipientKeyEnvelopes: []protocol.RecipientKeyEnvelope{}, Ciphertext: ciphertext,
		CiphertextSHA256: protocol.SHA256Hex(hex.EncodeToString(digest[:])),
	}
	require.NoError(t, giffordcrypto.SignWireEnvelope(f.private, &envelope))
	require.NoError(t, envelope.Validate())
	return envelope
}

func resolvedAdmissionParams(params AdmitMessageParams, admitted AdmitMessageResult) AdmitMessageParams {
	params.MessageID = admitted.MessageID
	params.ThreadRef = admitted.ThreadRef
	if params.OriginNode == "" {
		params.OriginNode = testHubNode
	}
	if params.DestinationNode == "" {
		params.DestinationNode = testHubNode
	}
	params.RouteDigest = localRouteDigest(admitted.MessageID, testHubNode, testHubSender, params.Recipients)
	return params
}

func insertHubDelivery(t *testing.T, f *hubFixture, params AdmitMessageParams) {
	t.Helper()
	stamp := eventstore.FormatTime(f.clock.now)
	_, err := f.store.DB().ExecContext(t.Context(), `INSERT INTO delivery_queue
		(message_id, agent_id, delivery_generation, from_agent, thread_ref, state,
		 priority, created_at, expires_at, updated_at, hop_count, route_digest)
		VALUES (?, ?, ?, ?, ?, 'DELIVERING', 2, ?, ?, ?, 0, ?)`,
		params.MessageID, testHubRecipient, int64(1), testHubSender, params.ThreadRef, stamp,
		eventstore.FormatTime(params.ExpiresAt), stamp, params.RouteDigest)
	require.NoError(t, err)
}

func requireCallCode(t *testing.T, err error, code int) *callError {
	t.Helper()
	var got *callError
	require.ErrorAs(t, err, &got)
	require.Equal(t, code, got.code)
	return got
}

func TestHubAdmissionSubmitClaimAndAckMatrix(t *testing.T) {
	f := newHubFixture(t)
	request := testAdmitParams(f)
	request.CreatedAt = f.clock.now.Add(-time.Minute)
	answer, err := callAdmit(t, f, request)
	require.NoError(t, err)
	admitted := answer.(AdmitMessageResult)
	params := resolvedAdmissionParams(request, admitted)
	require.False(t, admitted.Submitted)
	require.Equal(t, protocol.AdmissionStatusAccepted, admitted.AdmissionResult.Result)

	var state, updatedAt string
	var envelopes, decisions int
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
		"SELECT state, updated_at FROM messages WHERE message_id = ?", params.MessageID).Scan(&state, &updatedAt))
	require.Equal(t, messageStateAdmitted, state)
	require.Equal(t, eventstore.FormatTime(f.clock.now), updatedAt)
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
		"SELECT count(*) FROM message_envelopes WHERE message_id = ?", params.MessageID).Scan(&envelopes))
	require.Zero(t, envelopes)
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
		"SELECT count(*) FROM channel_control_decisions WHERE object_id = ?", params.MessageID).Scan(&decisions))
	require.Equal(t, 1, decisions)

	retry, err := callAdmit(t, f, request)
	require.NoError(t, err)
	require.Equal(t, admitted, retry)
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
		"SELECT count(*) FROM channel_control_decisions WHERE object_id = ?", params.MessageID).Scan(&decisions))
	require.Equal(t, 1, decisions)

	changed := request
	changed.Priority = protocol.PriorityHigh
	_, err = callAdmit(t, f, changed)
	_ = requireCallCode(t, err, codeInvalidRequest)

	envelope := signedEnvelopeFor(t, f, admitted, request)
	rawEnvelope, err := json.Marshal(envelope)
	require.NoError(t, err)
	submittedRaw, err := f.service.SubmitMessageEnvelope(t.Context(), testHubSender, rawEnvelope)
	require.NoError(t, err)
	submitted := submittedRaw.(SubmitMessageEnvelopeResult)
	require.Equal(t, messageStatePending, submitted.State)

	replayed, err := f.service.SubmitMessageEnvelope(t.Context(), testHubSender, rawEnvelope)
	require.NoError(t, err)
	require.Equal(t, submitted, replayed)
	otherBytes, err := json.MarshalIndent(envelope, "", "  ")
	require.NoError(t, err)
	_, err = f.service.SubmitMessageEnvelope(t.Context(), testHubSender, otherBytes)
	_ = requireCallCode(t, err, codeInvalidRequest)

	retry, err = callAdmit(t, f, request)
	require.NoError(t, err)
	require.True(t, retry.(AdmitMessageResult).Submitted)

	claimParams, err := json.Marshal(ClaimMessageParams{MessageID: params.MessageID, DeliveryGeneration: 1})
	require.NoError(t, err)
	claimedRaw, err := f.service.ClaimMessage(t.Context(), testHubRecipient, claimParams)
	require.NoError(t, err)
	claimed := claimedRaw.(ClaimMessageResult)
	require.JSONEq(t, string(rawEnvelope), string(claimed.Envelope))
	require.Equal(t, protocol.DeliveryModeWhenSafe, claimed.DeliveryMode)
	require.True(t, claimed.RequiresAck)

	ack := AckMessageParams{
		MessageID: params.MessageID, DeliveryGeneration: 1, Status: AckMessageAcknowledged,
	}
	rawAck, err := json.Marshal(ack)
	require.NoError(t, err)
	for range 2 {
		_, err = f.service.AckMessage(t.Context(), testHubRecipient, rawAck)
		require.NoError(t, err)
	}
	var acks int
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
		"SELECT count(*) FROM acks WHERE message_id = ?", params.MessageID).Scan(&acks))
	require.Equal(t, 1, acks)
}

func TestHubDeliveryReevaluatesHoldAndRecordsRelease(t *testing.T) {
	f := newHubFixture(t)
	setHubLink(t, f, protocol.LinkModeHold, 0)
	params := testAdmitParams(f)
	answer, err := callAdmit(t, f, params)
	require.NoError(t, err)
	admitted := answer.(AdmitMessageResult)
	require.Equal(t, protocol.AdmissionStatusAcceptedHeld, admitted.AdmissionResult.Result)
	require.Equal(t, protocol.DecisionReasonLinkHold, admitted.AdmissionResult.ControlReason)
	envelope := signedEnvelopeFor(t, f, admitted, params)
	rawEnvelope, err := json.Marshal(envelope)
	require.NoError(t, err)
	_, err = f.service.SubmitMessageEnvelope(t.Context(), testHubSender, rawEnvelope)
	require.NoError(t, err)

	var queueState string
	var controlState protocol.ControlState
	var controlReason protocol.DecisionReason
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(), `SELECT state, control_state,
		control_reason FROM delivery_queue WHERE message_id = ? AND agent_id = ?`,
		admitted.MessageID, testHubRecipient).Scan(&queueState, &controlState, &controlReason))
	require.Equal(t, "QUEUED", queueState)
	require.Equal(t, protocol.ControlStateHeld, controlState)
	require.Equal(t, protocol.DecisionReasonLinkHold, controlReason)

	listedRaw, err := f.service.ListPendingMessages(t.Context(), testHubRecipient, json.RawMessage(`{}`))
	require.NoError(t, err)
	require.Empty(t, listedRaw.(ListPendingMessagesResult).Messages)
	claim, err := json.Marshal(ClaimMessageParams{MessageID: admitted.MessageID, DeliveryGeneration: 1})
	require.NoError(t, err)
	_, err = f.service.ClaimMessage(t.Context(), testHubRecipient, claim)
	got := requireCallCode(t, err, codeDomainError)
	require.Equal(t, protocol.ErrorCodePolicyDenied, got.domain)
	require.ErrorContains(t, got, string(protocol.DecisionReasonLinkHold))

	setHubLink(t, f, protocol.LinkModeAllow, 1)
	listedRaw, err = f.service.ListPendingMessages(t.Context(), testHubRecipient, json.RawMessage(`{}`))
	require.NoError(t, err)
	require.Len(t, listedRaw.(ListPendingMessagesResult).Messages, 1)
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(), `SELECT control_state,
		control_reason FROM delivery_queue WHERE message_id = ? AND agent_id = ?`,
		admitted.MessageID, testHubRecipient).Scan(&controlState, &controlReason))
	require.Equal(t, protocol.ControlStateAllowed, controlState)
	require.Equal(t, protocol.DecisionReasonHoldReleased, controlReason)
	explanation, err := f.controller.Explain(t.Context(), protocol.ExplainParams{MessageID: admitted.MessageID})
	require.NoError(t, err)
	require.Equal(t, protocol.DecisionReasonHoldReleased, explanation.Reason)
	_, err = f.service.ClaimMessage(t.Context(), testHubRecipient, claim)
	require.NoError(t, err)
}

func TestHubDeliveryPauseAfterEnqueueFailsClosedUntilRelease(t *testing.T) {
	f := newHubFixture(t)
	params := testAdmitParams(f)
	answer, err := callAdmit(t, f, params)
	require.NoError(t, err)
	admitted := answer.(AdmitMessageResult)
	envelope := signedEnvelopeFor(t, f, admitted, params)
	rawEnvelope, err := json.Marshal(envelope)
	require.NoError(t, err)
	_, err = f.service.SubmitMessageEnvelope(t.Context(), testHubSender, rawEnvelope)
	require.NoError(t, err)

	actor := channel.Actor{Type: protocol.ActorTypeHuman, ID: "operator-test"}
	_, err = f.controller.SetGlobal(t.Context(), protocol.SetGlobalParams{
		Mode: protocol.ChannelModePaused, ExpectedVersion: 0,
	}, actor)
	require.NoError(t, err)
	listedRaw, err := f.service.ListPendingMessages(t.Context(), testHubRecipient, json.RawMessage(`{}`))
	require.NoError(t, err)
	require.Empty(t, listedRaw.(ListPendingMessagesResult).Messages)
	claim, err := json.Marshal(ClaimMessageParams{MessageID: admitted.MessageID, DeliveryGeneration: 1})
	require.NoError(t, err)
	_, err = f.service.ClaimMessage(t.Context(), testHubRecipient, claim)
	got := requireCallCode(t, err, codeDomainError)
	require.Equal(t, protocol.ErrorCodePolicyDenied, got.domain)
	require.ErrorContains(t, got, string(protocol.DecisionReasonChannelPaused))

	_, err = f.controller.SetGlobal(t.Context(), protocol.SetGlobalParams{
		Mode: protocol.ChannelModeActive, ExpectedVersion: 1,
	}, actor)
	require.NoError(t, err)
	listedRaw, err = f.service.ListPendingMessages(t.Context(), testHubRecipient, json.RawMessage(`{}`))
	require.NoError(t, err)
	require.Len(t, listedRaw.(ListPendingMessagesResult).Messages, 1)
}

func TestHubDeliveryApprovalAndDenyNeverExposeEnvelope(t *testing.T) {
	f := newHubFixture(t)
	params := testAdmitParams(f)
	answer, err := callAdmit(t, f, params)
	require.NoError(t, err)
	admitted := answer.(AdmitMessageResult)
	envelope := signedEnvelopeFor(t, f, admitted, params)
	rawEnvelope, err := json.Marshal(envelope)
	require.NoError(t, err)
	_, err = f.service.SubmitMessageEnvelope(t.Context(), testHubSender, rawEnvelope)
	require.NoError(t, err)
	claim, err := json.Marshal(ClaimMessageParams{MessageID: admitted.MessageID, DeliveryGeneration: 1})
	require.NoError(t, err)

	setHubLink(t, f, protocol.LinkModeRequireApproval, 0)
	listedRaw, err := f.service.ListPendingMessages(t.Context(), testHubRecipient, json.RawMessage(`{}`))
	require.NoError(t, err)
	require.Empty(t, listedRaw.(ListPendingMessagesResult).Messages)
	_, err = f.service.ClaimMessage(t.Context(), testHubRecipient, claim)
	got := requireCallCode(t, err, codeDomainError)
	require.Equal(t, protocol.ErrorCodePolicyDenied, got.domain)
	require.ErrorContains(t, got, string(protocol.DecisionReasonLinkApproval))

	setHubLink(t, f, protocol.LinkModeDeny, 1)
	listedRaw, err = f.service.ListPendingMessages(t.Context(), testHubRecipient, json.RawMessage(`{}`))
	require.NoError(t, err)
	require.Empty(t, listedRaw.(ListPendingMessagesResult).Messages)
	_, err = f.service.ClaimMessage(t.Context(), testHubRecipient, claim)
	got = requireCallCode(t, err, codeDomainError)
	require.Equal(t, protocol.ErrorCodeLinkDenied, got.domain)
	require.ErrorContains(t, got, string(protocol.DecisionReasonLinkDenied))
}

func TestHubRejectsTamperedEnvelopeBeforeAcceptance(t *testing.T) {
	tests := map[string]func(*protocol.WireEnvelope){
		"signature": func(envelope *protocol.WireEnvelope) { envelope.Signature[0] ^= 1 },
		"header":    func(envelope *protocol.WireEnvelope) { envelope.Priority = protocol.PriorityHigh },
		"ciphertext": func(envelope *protocol.WireEnvelope) {
			envelope.Ciphertext[0] ^= 1
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			f := newHubFixture(t)
			params := testAdmitParams(f)
			answer, err := callAdmit(t, f, params)
			require.NoError(t, err)
			admitted := answer.(AdmitMessageResult)
			envelope := signedEnvelopeFor(t, f, admitted, params)
			mutate(&envelope)
			raw, err := json.Marshal(envelope)
			require.NoError(t, err)
			_, err = f.service.SubmitMessageEnvelope(t.Context(), testHubSender, raw)
			_ = requireCallCode(t, err, codeInvalidRequest)

			var admittedRows, envelopes, decisions int
			require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
				"SELECT count(*) FROM messages WHERE message_id = ? AND state = 'ADMITTED'",
				admitted.MessageID).Scan(&admittedRows))
			require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
				"SELECT count(*) FROM message_envelopes WHERE message_id = ?", admitted.MessageID).
				Scan(&envelopes))
			require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
				"SELECT count(*) FROM channel_control_decisions WHERE object_id = ?", admitted.MessageID).
				Scan(&decisions))
			require.Equal(t, 1, admittedRows)
			require.Zero(t, envelopes)
			require.Equal(t, 1, decisions)
		})
	}
}

func TestHubAdmissionOwnsHeadersAndBindsEveryRetryParameter(t *testing.T) {
	t.Run("hub owned headers", func(t *testing.T) {
		tests := map[string]func(*AdmitMessageParams){
			"message id": func(params *AdmitMessageParams) { params.MessageID = testHubMessage },
			"thread ref": func(params *AdmitMessageParams) { params.ThreadRef = "thr_01ARZ3NDEKTSV4RRFFQ69G5FAV" },
			"hop count":  func(params *AdmitMessageParams) { params.HopCount = 1 },
			"route digest": func(params *AdmitMessageParams) {
				params.RouteDigest = protocol.Digest("sha256:" + strings.Repeat("a", 64))
			},
		}
		for name, mutate := range tests {
			t.Run(name, func(t *testing.T) {
				f := newHubFixture(t)
				params := testAdmitParams(f)
				mutate(&params)
				_, err := callAdmit(t, f, params)
				_ = requireCallCode(t, err, codeInvalidRequest)
				requireNoHubDecision(t, f)
			})
		}
	})

	t.Run("retry bindings", func(t *testing.T) {
		f := newHubFixture(t)
		maxTurns := int64(3)
		params := testAdmitParams(f)
		params.RequestedAutonomy = protocol.AutonomyAuto
		params.RequestedBudget = &protocol.RequestedBudget{MaxTurns: &maxTurns}
		answer, err := callAdmit(t, f, params)
		require.NoError(t, err)
		admitted := answer.(AdmitMessageResult)

		mutations := map[string]func(*AdmitMessageParams){
			"created at": func(retry *AdmitMessageParams) { retry.CreatedAt = retry.CreatedAt.Add(time.Nanosecond) },
			"thread id":  func(retry *AdmitMessageParams) { retry.ThreadID = "thread-other" },
			"budget": func(retry *AdmitMessageParams) {
				other := int64(4)
				retry.RequestedBudget = &protocol.RequestedBudget{MaxTurns: &other}
			},
			"autonomy": func(retry *AdmitMessageParams) { retry.RequestedAutonomy = protocol.AutonomyStep },
			"recipient metadata": func(retry *AdmitMessageParams) {
				retry.Recipients[0].RequiresAck = false
			},
		}
		for name, mutate := range mutations {
			t.Run(name, func(t *testing.T) {
				retry := params
				retry.Recipients = append([]MessageRecipient(nil), params.Recipients...)
				mutate(&retry)
				_, err := callAdmit(t, f, retry)
				_ = requireCallCode(t, err, codeInvalidRequest)
			})
		}
		params.OriginNode = ""
		params.DestinationNode = ""
		retry, err := callAdmit(t, f, params)
		require.NoError(t, err)
		require.Equal(t, admitted, retry)
	})
}

func TestHubDeliveryGenerationMustBeMaterialized(t *testing.T) {
	t.Run("claim and ack generations", func(t *testing.T) {
		f := newHubFixture(t)
		params := testAdmitParams(f)
		answer, err := callAdmit(t, f, params)
		require.NoError(t, err)
		admitted := answer.(AdmitMessageResult)
		envelope := signedEnvelopeFor(t, f, admitted, params)
		rawEnvelope, err := json.Marshal(envelope)
		require.NoError(t, err)
		_, err = f.service.SubmitMessageEnvelope(t.Context(), testHubSender, rawEnvelope)
		require.NoError(t, err)
		for _, generation := range []int64{0, 2} {
			claim, marshalErr := json.Marshal(ClaimMessageParams{
				MessageID: admitted.MessageID, DeliveryGeneration: generation,
			})
			require.NoError(t, marshalErr)
			_, claimErr := f.service.ClaimMessage(t.Context(), testHubRecipient, claim)
			if generation == 0 {
				_ = requireCallCode(t, claimErr, codeInvalidRequest)
			} else {
				got := requireCallCode(t, claimErr, codeDomainError)
				require.Equal(t, protocol.ErrorCodeNotFound, got.domain)
			}

			ack, marshalErr := json.Marshal(AckMessageParams{
				MessageID: admitted.MessageID, DeliveryGeneration: generation,
				Status: AckMessageAcknowledged,
			})
			require.NoError(t, marshalErr)
			_, ackErr := f.service.AckMessage(t.Context(), testHubRecipient, ack)
			if generation == 0 {
				_ = requireCallCode(t, ackErr, codeInvalidRequest)
			} else {
				got := requireCallCode(t, ackErr, codeDomainError)
				require.Equal(t, protocol.ErrorCodeNotFound, got.domain)
			}
		}
		var acks int
		require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
			"SELECT count(*) FROM acks WHERE message_id = ?", admitted.MessageID).Scan(&acks))
		require.Zero(t, acks)
	})

	t.Run("ack admitted message", func(t *testing.T) {
		f := newHubFixture(t)
		params := testAdmitParams(f)
		answer, err := callAdmit(t, f, params)
		require.NoError(t, err)
		admitted := answer.(AdmitMessageResult)
		resolved := resolvedAdmissionParams(params, admitted)
		insertHubDelivery(t, f, resolved)
		ack, err := json.Marshal(AckMessageParams{
			MessageID: admitted.MessageID, DeliveryGeneration: 1, Status: AckMessageAcknowledged,
		})
		require.NoError(t, err)
		_, err = f.service.AckMessage(t.Context(), testHubRecipient, ack)
		got := requireCallCode(t, err, codeDomainError)
		require.Equal(t, protocol.ErrorCodeNotFound, got.domain)
		var acks int
		require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
			"SELECT count(*) FROM acks WHERE message_id = ?", admitted.MessageID).Scan(&acks))
		require.Zero(t, acks)
	})
}

func TestHubGeneratesHeadersAndAcceptsInternalRPCShapes(t *testing.T) {
	f := newHubFixture(t)
	params := testAdmitParams(f)
	params.OriginNode = ""
	params.DestinationNode = ""
	answer, err := callAdmit(t, f, params)
	require.NoError(t, err)
	admitted := answer.(AdmitMessageResult)
	require.True(t, admitted.MessageID.Valid())
	require.True(t, admitted.ThreadRef.Valid())
	require.Equal(t, admitted.MessageID, admitted.AdmissionResult.MessageID)
	require.Equal(t, eventstore.FormatTime(params.CreatedAt), admitted.CreatedAt)
	require.Equal(t, eventstore.FormatTime(params.ExpiresAt), admitted.ExpiresAt)

	envelope := signedEnvelopeFor(t, f, admitted, params)
	rawEnvelope, err := json.Marshal(envelope)
	require.NoError(t, err)
	wrapper, err := json.Marshal(map[string]json.RawMessage{"envelope": rawEnvelope})
	require.NoError(t, err)
	_, err = f.service.SubmitMessageEnvelope(t.Context(), testHubSender, wrapper)
	require.NoError(t, err)

	claim, err := json.Marshal(map[string]any{"message_id": admitted.MessageID})
	require.NoError(t, err)
	_, err = f.service.ClaimMessage(t.Context(), testHubRecipient, claim)
	_ = requireCallCode(t, err, codeInvalidRequest)

	ack, err := json.Marshal(map[string]any{
		"message_id": admitted.MessageID, testStatusKey: AckMessageAcknowledged,
	})
	require.NoError(t, err)
	_, err = f.service.AckMessage(t.Context(), testHubRecipient, ack)
	_ = requireCallCode(t, err, codeInvalidRequest)
}

func TestHubExpiryAndMetadataMismatch(t *testing.T) {
	f := newHubFixture(t)
	params := testAdmitParams(f)
	answer, err := callAdmit(t, f, params)
	require.NoError(t, err)
	admitted := answer.(AdmitMessageResult)
	params = resolvedAdmissionParams(params, admitted)
	envelope := signedEnvelope(t, f, admitted)
	rawEnvelope, err := json.Marshal(envelope)
	require.NoError(t, err)
	_, err = f.service.SubmitMessageEnvelope(t.Context(), testHubSender, rawEnvelope)
	require.NoError(t, err)

	mismatch, err := json.Marshal(AckMessageParams{
		MessageID: params.MessageID, DeliveryGeneration: 1, Status: AckMessageMismatch,
	})
	require.NoError(t, err)
	_, err = f.service.AckMessage(t.Context(), testHubRecipient, mismatch)
	require.NoError(t, err)
	var queueState string
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(), `SELECT state FROM delivery_queue
		WHERE message_id = ? AND agent_id = ?`, params.MessageID, testHubRecipient).Scan(&queueState))
	require.Equal(t, messageStateFailed, queueState)
	var mismatchAcks int
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(), `SELECT count(*) FROM acks
		WHERE message_id = ? AND agent_id = ? AND status = 'MISMATCH'`,
		params.MessageID, testHubRecipient).Scan(&mismatchAcks))
	require.Equal(t, 1, mismatchAcks)

	second := testAdmitParams(f)
	second.ClientMsgID = "client-message-2"
	_, err = callAdmit(t, f, second)
	require.NoError(t, err)
	f.clock.now = second.ExpiresAt
	count, err := f.service.ExpireAdmitted(t.Context())
	require.NoError(t, err)
	require.Equal(t, int64(1), count)
	_, err = callAdmit(t, f, second)
	got := requireCallCode(t, err, codeDomainError)
	require.Equal(t, protocol.ErrorCodeExpired, got.domain)
}

func TestHubRefusalsPrecedeTheGate(t *testing.T) {
	t.Run("reply", func(t *testing.T) {
		f := newHubFixture(t)
		params := testAdmitParams(f)
		params.ParentMessageID = "01ARZ3NDEKTSV4RRFFQ69G5FAZ"
		_, err := callAdmit(t, f, params)
		_ = requireCallCode(t, err, codeInvalidRequest)
		requireNoHubDecision(t, f)
	})
	t.Run("unknown recipient", func(t *testing.T) {
		f := newHubFixture(t)
		params := testAdmitParams(f)
		params.Recipients[0].Agent = "agent-unknown"
		_, err := callAdmit(t, f, params)
		got := requireCallCode(t, err, codeDomainError)
		require.Equal(t, protocol.ErrorCodeRecipientNotDeliverable, got.domain)
		requireNoHubDecision(t, f)
	})
	t.Run("blocked recipient", func(t *testing.T) {
		f := newHubFixture(t)
		f.keys.blocked[testHubRecipient] = true
		_, err := callAdmit(t, f, testAdmitParams(f))
		got := requireCallCode(t, err, codeDomainError)
		require.Equal(t, protocol.ErrorCodeKeyUnavailable, got.domain)
		requireNoHubDecision(t, f)
	})
	t.Run("missing delivery mode", func(t *testing.T) {
		f := newHubFixture(t)
		params := testAdmitParams(f)
		params.Recipients[0].DeliveryMode = ""
		_, err := callAdmit(t, f, params)
		_ = requireCallCode(t, err, codeInvalidRequest)
		requireNoHubDecision(t, f)
	})
	t.Run("recipient not deliverable", func(t *testing.T) {
		f := newHubFixture(t)
		_, err := f.store.DB().ExecContext(t.Context(),
			"UPDATE sessions SET deliverable = 0 WHERE agent_id = ?", testHubRecipient)
		require.NoError(t, err)
		_, err = callAdmit(t, f, testAdmitParams(f))
		got := requireCallCode(t, err, codeDomainError)
		require.Equal(t, protocol.ErrorCodeRecipientNotDeliverable, got.domain)
		requireNoHubDecision(t, f)
	})
	t.Run("inactive envelope key", func(t *testing.T) {
		f := newHubFixture(t)
		params := testAdmitParams(f)
		answer, err := callAdmit(t, f, params)
		require.NoError(t, err)
		envelope := signedEnvelope(t, f, answer.(AdmitMessageResult))
		envelope.SenderSigningKeyID = "ed25519:agent-sender:v2"
		raw, err := json.Marshal(envelope)
		require.NoError(t, err)
		_, err = f.service.SubmitMessageEnvelope(t.Context(), testHubSender, raw)
		got := requireCallCode(t, err, codeDomainError)
		require.Equal(t, protocol.ErrorCodeKeyUnavailable, got.domain)
	})
}

func TestHubDenialAndRetryMatrix(t *testing.T) {
	t.Run("admission denial retries as a fresh message", func(t *testing.T) {
		f := newHubFixture(t)
		applyHubControlFixture(t, f, "recipient_rx_off.sql")
		params := testAdmitParams(f)
		_, err := callAdmit(t, f, params)
		got := requireCallCode(t, err, codeDomainError)
		require.Equal(t, protocol.ErrorCodeAgentRXDisabled, got.domain)
		var messages, decisions int
		require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
			"SELECT count(*) FROM messages").Scan(&messages))
		require.Zero(t, messages)
		require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
			"SELECT count(*) FROM channel_control_decisions").Scan(&decisions))
		require.Equal(t, 1, decisions)

		applyHubControlFixture(t, f, "recipient_rx_clear.sql")
		answer, err := callAdmit(t, f, params)
		require.NoError(t, err)
		require.Equal(t, protocol.AdmissionStatusAccepted, answer.(AdmitMessageResult).AdmissionResult.Result)
	})

	t.Run("acceptance denial is failed and durable", func(t *testing.T) {
		f := newHubFixture(t)
		params := testAdmitParams(f)
		answer, err := callAdmit(t, f, params)
		require.NoError(t, err)
		resolved := resolvedAdmissionParams(params, answer.(AdmitMessageResult))
		envelope := signedEnvelope(t, f, answer.(AdmitMessageResult))
		raw, err := json.Marshal(envelope)
		require.NoError(t, err)
		applyHubControlFixture(t, f, "recipient_rx_off.sql")
		_, err = f.service.SubmitMessageEnvelope(t.Context(), testHubSender, raw)
		got := requireCallCode(t, err, codeDomainError)
		require.Equal(t, protocol.ErrorCodeAgentRXDisabled, got.domain)
		var state, control string
		var envelopes int
		require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
			"SELECT state, control_state FROM messages WHERE message_id = ?", resolved.MessageID).
			Scan(&state, &control))
		require.Equal(t, messageStateFailed, state)
		require.Equal(t, string(protocol.ControlStateDenied), control)
		require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
			"SELECT count(*) FROM message_envelopes WHERE message_id = ?", resolved.MessageID).Scan(&envelopes))
		require.Zero(t, envelopes)

		_, err = f.service.SubmitMessageEnvelope(t.Context(), testHubSender, raw)
		got = requireCallCode(t, err, codeDomainError)
		require.Equal(t, protocol.ErrorCodeAgentRXDisabled, got.domain)
		_, err = callAdmit(t, f, params)
		got = requireCallCode(t, err, codeDomainError)
		require.Equal(t, protocol.ErrorCodeAgentRXDisabled, got.domain)
	})
}

func TestHubLateAndUnadmittedSubmit(t *testing.T) {
	f := newHubFixture(t)
	params := testAdmitParams(f)
	answer, err := callAdmit(t, f, params)
	require.NoError(t, err)
	envelope := signedEnvelope(t, f, answer.(AdmitMessageResult))
	raw, err := json.Marshal(envelope)
	require.NoError(t, err)
	f.clock.now = params.ExpiresAt
	_, err = f.service.SubmitMessageEnvelope(t.Context(), testHubSender, raw)
	got := requireCallCode(t, err, codeDomainError)
	require.Equal(t, protocol.ErrorCodeExpired, got.domain)
	_, err = f.service.SubmitMessageEnvelope(t.Context(), testHubSender, raw)
	got = requireCallCode(t, err, codeDomainError)
	require.Equal(t, protocol.ErrorCodeExpired, got.domain)
	_, err = callAdmit(t, f, params)
	got = requireCallCode(t, err, codeDomainError)
	require.Equal(t, protocol.ErrorCodeExpired, got.domain)

	fresh := envelope
	fresh.MessageID = "01ARZ3NDEKTSV4RRFFQ69G5FAY"
	fresh.ClientMsgID = "client-never-admitted"
	fresh.ExpiresAt = f.clock.now.Add(time.Hour)
	require.NoError(t, giffordcrypto.SignWireEnvelope(f.private, &fresh))
	raw, err = json.Marshal(fresh)
	require.NoError(t, err)
	_, err = f.service.SubmitMessageEnvelope(t.Context(), testHubSender, raw)
	_ = requireCallCode(t, err, codeInvalidRequest)
}

func TestHubTransactionsRollbackOnStoreFailure(t *testing.T) {
	t.Run("admission", func(t *testing.T) {
		f := newHubFixture(t)
		_, err := f.store.DB().ExecContext(t.Context(), `CREATE TRIGGER fail_hub_admit
			BEFORE INSERT ON messages BEGIN SELECT RAISE(ABORT, 'injected admission failure'); END`)
		require.NoError(t, err)
		params := testAdmitParams(f)
		_, err = callAdmit(t, f, params)
		require.Error(t, err)
		requireNoHubDecision(t, f)
	})

	t.Run("acceptance", func(t *testing.T) {
		f := newHubFixture(t)
		params := testAdmitParams(f)
		answer, err := callAdmit(t, f, params)
		require.NoError(t, err)
		resolved := resolvedAdmissionParams(params, answer.(AdmitMessageResult))
		envelope := signedEnvelope(t, f, answer.(AdmitMessageResult))
		raw, err := json.Marshal(envelope)
		require.NoError(t, err)
		_, err = f.store.DB().ExecContext(t.Context(), `CREATE TRIGGER fail_hub_submit
			BEFORE INSERT ON message_envelopes BEGIN SELECT RAISE(ABORT, 'injected submit failure'); END`)
		require.NoError(t, err)
		_, err = f.service.SubmitMessageEnvelope(t.Context(), testHubSender, raw)
		require.Error(t, err)
		var state, envelopes, decisions int
		require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
			"SELECT count(*) FROM messages WHERE message_id = ? AND state = 'ADMITTED'", resolved.MessageID).Scan(&state))
		require.Equal(t, 1, state)
		require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
			"SELECT count(*) FROM message_envelopes WHERE message_id = ?", resolved.MessageID).Scan(&envelopes))
		require.Zero(t, envelopes)
		require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
			"SELECT count(*) FROM channel_control_decisions WHERE object_id = ?", resolved.MessageID).Scan(&decisions))
		require.Equal(t, 1, decisions)
	})
}

func applyHubControlFixture(t *testing.T, f *hubFixture, name string) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "channel", "testdata", "hub", name))
	require.NoError(t, err)
	_, err = f.store.DB().ExecContext(t.Context(), string(raw))
	require.NoError(t, err)
}

func requireNoHubDecision(t *testing.T, f *hubFixture) {
	t.Helper()
	var messages, decisions int
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
		"SELECT count(*) FROM messages").Scan(&messages))
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
		"SELECT count(*) FROM channel_control_decisions").Scan(&decisions))
	require.Zero(t, messages)
	require.Zero(t, decisions)
}

func TestHubDomainErrorShape(t *testing.T) {
	handler := errorAdmitter{err: domainError(protocol.ErrorCodeExpired, fmt.Errorf("expired"))}
	response := dispatch(t.Context(), testHubSender, Handlers{Admit: handler}, rpcRequest{
		JSONRPC: jsonRPCVersion, Method: MethodAdmitMessage, Params: json.RawMessage(`{}`), ID: json.RawMessage(`1`),
	})
	require.NotNil(t, response.Error)
	require.Equal(t, codeDomainError, response.Error.Code)
	require.Equal(t, protocol.ErrorCodeExpired, response.Error.Data.ErrorCode)
}

type errorAdmitter struct{ err error }

func (e errorAdmitter) AdmitMessage(context.Context, protocol.AgentID, json.RawMessage) (any, error) {
	return nil, e.err
}
