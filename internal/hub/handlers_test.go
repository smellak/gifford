package hub

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/cli/ctl"
	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/controlapi"
	giffordcrypto "github.com/smellak/gifford/internal/crypto"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/federation"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
)

const (
	testHubNode                     = protocol.NodeID("node-local")
	testHubSender                   = protocol.AgentID("agent-sender")
	testHubRecipient                = protocol.AgentID("agent-recipient")
	testHubMessage                  = protocol.ULID("01ARZ3NDEKTSV4RRFFQ69G5FAV")
	testHubThread                   = protocol.ThreadID("thread-local")
	testHubSenderKeyID              = "ed25519:agent-sender:v1"
	testHubRecipientEncryptionKeyID = "x25519:agent-recipient:v1"
	testThirdKeyID                  = "ed25519:agent-third:v1"
	testStatusKey                   = "status"
	testOperatorID                  = "operator-test"
	testLoopback                    = "127.0.0.1:0"
	testCtlLoop                     = "loop"
	testCtlReason                   = "--reason"
)

type hubClock struct{ now time.Time }

func (c *hubClock) Now() time.Time { return c.now }

func (*hubClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

type hubRegistry struct {
	mu         sync.Mutex
	keys       map[protocol.AgentID]registry.KeyInfo
	encryption map[protocol.AgentID]registry.KeyInfo
	blocked    map[protocol.AgentID]bool
	routes     map[protocol.AgentID]registry.RouteResult
	routeCalls []protocol.AgentID
}

func (r *hubRegistry) ActiveEncryptionKey(
	_ context.Context,
	_ registry.Querier,
	agent protocol.AgentID,
) (registry.KeyInfo, error) {
	key, ok := r.encryption[agent]
	if !ok {
		return registry.KeyInfo{}, registry.ErrNoUsableKey
	}
	return key, nil
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

func (r *hubRegistry) Route(
	ctx context.Context,
	q registry.Querier,
	agent protocol.AgentID,
) (registry.RouteResult, error) {
	r.mu.Lock()
	r.routeCalls = append(r.routeCalls, agent)
	r.mu.Unlock()
	if route, ok := r.routes[agent]; ok {
		return route, nil
	}
	var node protocol.NodeID
	if err := q.QueryRowContext(ctx, "SELECT node_id FROM agents WHERE agent_id = ?", agent).Scan(&node); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return registry.RouteResult{Kind: registry.RouteNotFound}, nil
		}
		return registry.RouteResult{}, err
	}
	return registry.RouteResult{Kind: registry.RouteLocal, Node: node}, nil
}

func (r *hubRegistry) Register(
	_ context.Context,
	_ registry.Execer,
	req registry.RegisterRequest,
) (registry.RegisterResult, error) {
	keys := r.keys
	if req.Purpose == registry.KeyPurposeEncryption {
		keys = r.encryption
	}
	current, ok := keys[req.Agent]
	if ok && (current.KeyID != req.KeyID || !bytes.Equal(current.PublicKey, req.PublicKey)) {
		delete(keys, req.Agent)
		r.blocked[req.Agent] = true
		return registry.RegisterResult{
			Key:     registry.KeyInfo{KeyID: req.KeyID, Agent: req.Agent, PublicKey: req.PublicKey},
			Outcome: registry.OutcomeSuspended,
		}, nil
	}
	key := registry.KeyInfo{KeyID: req.KeyID, Agent: req.Agent, PublicKey: req.PublicKey}
	keys[req.Agent] = key
	return registry.RegisterResult{Key: key, Outcome: registry.OutcomeRegistered}, nil
}

type hubFixture struct {
	store      *eventstore.Store
	service    *Service
	controller *channel.Controller
	clock      *hubClock
	keys       *hubRegistry
	private    ed25519.PrivateKey
	replyKey   ed25519.PrivateKey
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
	replyPublic, replyPrivate, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	keys := &hubRegistry{
		keys: map[protocol.AgentID]registry.KeyInfo{
			testHubSender: {
				KeyID: testHubSenderKeyID, Agent: testHubSender, PublicKey: public,
			},
			testHubRecipient: {
				KeyID: "ed25519:agent-recipient:v1", Agent: testHubRecipient, PublicKey: replyPublic,
			},
		},
		encryption: make(map[protocol.AgentID]registry.KeyInfo),
		blocked:    make(map[protocol.AgentID]bool),
		routes:     make(map[protocol.AgentID]registry.RouteResult),
	}
	service, err := NewService(ServiceOptions{
		DB: store.DB(), NodeID: testHubNode, Engine: engine, Approvals: controller,
		Registry: keys, Clock: clk, EventStore: store,
	})
	require.NoError(t, err)
	insertHubAgent(t, store, testHubSender)
	insertHubAgent(t, store, testHubRecipient)
	return &hubFixture{
		store: store, service: service, controller: controller, clock: clk, keys: keys,
		private: private, replyKey: replyPrivate,
	}
}

func setHubLink(t *testing.T, f *hubFixture, mode protocol.LinkMode, expected int64) {
	setHubDirectionalLink(t, f, testHubSender, testHubRecipient, mode, expected)
}

func setHubDirectionalLink(
	t *testing.T,
	f *hubFixture,
	from protocol.AgentID,
	to protocol.AgentID,
	mode protocol.LinkMode,
	expected int64,
) {
	t.Helper()
	_, err := f.controller.SetLink(t.Context(), protocol.SetLinkParams{
		FromAgent: from, ToAgent: to, Mode: mode, Plane: protocol.PlaneMessage,
		ExpectedVersions: protocol.ControlVersions{
			"link:" + string(from) + "→" + string(to) + ":message": expected,
		},
	}, channel.Actor{Type: protocol.ActorTypeHuman, ID: testOperatorID})
	require.NoError(t, err)
}

func insertHubAgent(t *testing.T, store *eventstore.Store, agent protocol.AgentID) {
	t.Helper()
	stamp := eventstore.FormatTime(time.Date(2026, time.October, 29, 11, 0, 0, 0, time.UTC))
	_, err := store.DB().ExecContext(t.Context(), `INSERT INTO agents
		(agent_id, node_id, harness, created_at, updated_at) VALUES (?, ?, 'test', ?, ?)`,
		agent, testHubNode, stamp, stamp)
	require.NoError(t, err)
	_, err = store.DB().ExecContext(t.Context(), `INSERT INTO sessions
		(session_id, agent_id, state, capture_mode, deliverable, manifest_json,
		 started_at, updated_at) VALUES (?, ?, 'CONNECTED', 'pty_proxy', ?, '{}', ?, ?)`,
		"session-"+string(agent), agent, true, stamp, stamp)
	require.NoError(t, err)
}

func testAdmitParams(f *hubFixture) AdmitMessageParams {
	return AdmitMessageParams{
		ClientMsgID: "client-message-1",
		OriginNode:  testHubNode, DestinationNode: testHubNode,
		Recipients: []MessageRecipient{{
			Agent: testHubRecipient, DeliveryMode: protocol.DeliveryModeWhenSafe, RequiresAck: true,
		}},
		ThreadID: testHubThread,
		Priority: protocol.PriorityNormal, CreatedAt: f.clock.now, ExpiresAt: f.clock.now.Add(time.Hour),
		HopCount: 0,
	}
}

func testReplyAdmitParams(
	f *hubFixture,
	parent protocol.ULID,
	clientID protocol.OpaqueID,
) AdmitMessageParams {
	return AdmitMessageParams{
		ClientMsgID: clientID, OriginNode: testHubNode, DestinationNode: testHubNode,
		Recipients: []MessageRecipient{{
			Agent: testHubSender, DeliveryMode: protocol.DeliveryModeWhenSafe, RequiresAck: true,
		}},
		ThreadID: testHubThread, Priority: protocol.PriorityNormal,
		CreatedAt: f.clock.now, ExpiresAt: f.clock.now.Add(time.Hour), ParentMessageID: parent,
	}
}

func callAdmit(t *testing.T, f *hubFixture, params AdmitMessageParams) (any, error) {
	return callAdmitAs(t, f, testHubSender, params)
}

func callAdmitAs(
	t *testing.T,
	f *hubFixture,
	sender protocol.AgentID,
	params AdmitMessageParams,
) (any, error) {
	t.Helper()
	raw, err := json.Marshal(params)
	require.NoError(t, err)
	return f.service.AdmitMessage(t.Context(), sender, raw)
}

type concurrentAdmission struct {
	answer any
	err    error
}

func admitConcurrently(
	t *testing.T,
	f *hubFixture,
	senders []protocol.AgentID,
	params []AdmitMessageParams,
) []concurrentAdmission {
	t.Helper()
	require.Len(t, params, len(senders))
	raw := make([][]byte, len(params))
	for i := range params {
		var err error
		raw[i], err = json.Marshal(params[i])
		require.NoError(t, err)
	}
	start := make(chan struct{})
	results := make([]concurrentAdmission, len(params))
	var wg sync.WaitGroup
	for i := range params {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			<-start
			results[index].answer, results[index].err = f.service.AdmitMessage(
				t.Context(), senders[index], raw[index],
			)
		}(i)
	}
	close(start)
	wg.Wait()
	return results
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
		SenderSigningKeyID:    testHubSenderKeyID,
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
	params.RouteDigest = admitted.RouteDigest
	return params
}

func TestNotificationWakeFollowsCommittedSubmitAndCoalesces(t *testing.T) {
	f := newHubFixture(t)
	wake := make(chan struct{}, 1)
	f.service.notificationWake = wake

	for index := range 10 {
		params := testAdmitParams(f)
		params.ClientMsgID = protocol.OpaqueID(fmt.Sprintf("wake-client-%d", index))
		params.ThreadID = protocol.ThreadID(fmt.Sprintf("wake-thread-%d", index))
		answer, err := callAdmit(t, f, params)
		require.NoError(t, err)
		admitted := answer.(AdmitMessageResult)
		envelope := signedEnvelopeFor(t, f, admitted, params)
		raw, err := json.Marshal(envelope)
		require.NoError(t, err)
		_, err = f.service.SubmitMessageEnvelope(t.Context(), testHubSender, raw)
		require.NoError(t, err)
	}
	require.Len(t, wake, 1, "ten commits merge into one nonblocking hint")
	<-wake

	params := testAdmitParams(f)
	params.ClientMsgID = "wake-rollback-client"
	params.ThreadID = "wake-rollback-thread"
	answer, err := callAdmit(t, f, params)
	require.NoError(t, err)
	admitted := answer.(AdmitMessageResult)
	envelope := signedEnvelopeFor(t, f, admitted, params)
	envelope.Signature[0] ^= 0xff
	raw, err := json.Marshal(envelope)
	require.NoError(t, err)
	_, err = f.service.SubmitMessageEnvelope(t.Context(), testHubSender, raw)
	require.Error(t, err)
	select {
	case <-wake:
		t.Fatal("rolled-back submit emitted a notification wake")
	default:
	}
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

func TestLocalRouteDigestV1ExactVectorAndOrder(t *testing.T) {
	recipients := []MessageRecipient{
		{Agent: testHubRecipient},
		{Agent: "agent-third"},
	}
	digest, err := localRouteDigestV1("", testHubNode, testHubSender, testHubNode, recipients)
	require.NoError(t, err)
	require.Equal(t,
		protocol.Digest("sha256:35a9014872b249d98d87bf4de809e773196875353ec6209c76aa82b335dc6404"),
		digest)
	recipients[0], recipients[1] = recipients[1], recipients[0]
	digest, err = localRouteDigestV1("", testHubNode, testHubSender, testHubNode, recipients)
	require.NoError(t, err)
	require.NotEqual(t,
		protocol.Digest("sha256:35a9014872b249d98d87bf4de809e773196875353ec6209c76aa82b335dc6404"),
		digest)
}

func TestHubRemoteRootKeepsLegacyRouteDigest(t *testing.T) {
	f := newHubFixture(t)
	remoteNode := protocol.NodeID("node-remote")
	f.keys.routes[testHubRecipient] = registry.RouteResult{
		Kind: registry.RouteFederated,
		Node: remoteNode,
	}
	params := testAdmitParams(f)
	params.DestinationNode = remoteNode

	answer, err := callAdmit(t, f, params)
	require.NoError(t, err)
	admitted := answer.(AdmitMessageResult)
	require.Equal(t,
		localRouteDigest(admitted.MessageID, testHubNode, testHubSender, params.Recipients),
		admitted.RouteDigest)
	v1, err := localRouteDigestV1("", testHubNode, testHubSender, remoteNode, params.Recipients)
	require.NoError(t, err)
	require.NotEqual(t, v1, admitted.RouteDigest)
	envelope := signedEnvelopeFor(t, f, admitted, params)
	rawEnvelope, err := json.Marshal(envelope)
	require.NoError(t, err)
	_, err = f.service.bindReceivedDelivery(testHubNode, federation.DeliveryRequest{
		Envelope: rawEnvelope, ThreadID: params.ThreadID,
		Nonce: bytes.Repeat([]byte{0x42}, 16), Sequence: 1,
	})
	require.NoError(t, err, "the frozen federation route check accepts the admitted root")
}

func TestHubLegacyNullThreadSequenceProjectsOneWithoutBackfill(t *testing.T) {
	f := newHubFixture(t)
	params := testAdmitParams(f)
	answer, err := callAdmit(t, f, params)
	require.NoError(t, err)
	admitted := answer.(AdmitMessageResult)
	_, err = f.store.DB().ExecContext(t.Context(),
		"UPDATE messages SET thread_sequence = NULL WHERE message_id = ?", admitted.MessageID)
	require.NoError(t, err)

	retriedRaw, err := callAdmit(t, f, params)
	require.NoError(t, err)
	retried := retriedRaw.(AdmitMessageResult)
	require.Equal(t, int64(1), retried.ThreadSequence)
	envelope := signedEnvelopeFor(t, f, retried, params)
	raw, err := json.Marshal(envelope)
	require.NoError(t, err)
	_, err = f.service.SubmitMessageEnvelope(t.Context(), testHubSender, raw)
	require.NoError(t, err)

	var messageSequence sql.NullInt64
	var envelopeSequence, deliverySequence int64
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
		"SELECT thread_sequence FROM messages WHERE message_id = ?", admitted.MessageID).
		Scan(&messageSequence))
	require.False(t, messageSequence.Valid, "the legacy message row remains untouched")
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
		"SELECT thread_sequence FROM message_envelopes WHERE message_id = ?", admitted.MessageID).
		Scan(&envelopeSequence))
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
		"SELECT thread_sequence FROM delivery_queue WHERE message_id = ?", admitted.MessageID).
		Scan(&deliverySequence))
	require.Equal(t, int64(1), envelopeSequence)
	require.Equal(t, int64(1), deliverySequence)
}

func TestHubReplyAdmissionReservesOrdinalLoopAndRetry(t *testing.T) {
	f := newHubFixture(t)
	rootParams := testAdmitParams(f)
	rootRaw, err := callAdmit(t, f, rootParams)
	require.NoError(t, err)
	root := rootRaw.(AdmitMessageResult)
	require.Equal(t, int64(1), root.ThreadSequence)
	require.Equal(t,
		protocol.Digest("sha256:b85f4fb7a1f8c5e2e68b2ca6271bcd30a50bfb47e343a9a33b29456262ff7ff8"),
		root.RouteDigest)

	// A rejected attempt calculates the candidate ordinal in its transaction,
	// but stores neither the message nor a loop and therefore consumes nothing.
	setHubDirectionalLink(t, f, testHubRecipient, testHubSender, protocol.LinkModeDeny, 0)
	replyParams := testReplyAdmitParams(f, root.MessageID, "client-reply-1")
	_, err = callAdmitAs(t, f, testHubRecipient, replyParams)
	got := requireCallCode(t, err, codeDomainError)
	require.Equal(t, protocol.ErrorCodeLinkDenied, got.domain)
	var messages, loops int64
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(), "SELECT count(*) FROM messages").Scan(&messages))
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(), "SELECT count(*) FROM communication_loops").Scan(&loops))
	require.Equal(t, int64(1), messages)
	require.Zero(t, loops)

	setHubDirectionalLink(t, f, testHubRecipient, testHubSender, protocol.LinkModeAllow, 1)
	replyRaw, err := callAdmitAs(t, f, testHubRecipient, replyParams)
	require.NoError(t, err)
	reply := replyRaw.(AdmitMessageResult)
	require.Equal(t, int64(2), reply.ThreadSequence)
	require.Equal(t, protocol.AdmissionStatusAccepted, reply.AdmissionResult.Result)
	require.True(t, reply.AdmissionResult.LoopID.Valid())
	require.NotNil(t, reply.AdmissionResult.LoopGeneration)
	require.NotNil(t, reply.AdmissionResult.TurnIndex)
	require.Equal(t, int64(1), *reply.AdmissionResult.LoopGeneration)
	require.Equal(t, int64(1), *reply.AdmissionResult.TurnIndex)
	require.Equal(t,
		protocol.Digest("sha256:4e73139ce4aa8856e8f7d3739d9384dd498c41984d2bb44ce7b4454465b0aba1"),
		reply.RouteDigest)

	retry, err := callAdmitAs(t, f, testHubRecipient, replyParams)
	require.NoError(t, err)
	require.Equal(t, reply, retry)
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(), "SELECT count(*) FROM messages").Scan(&messages))
	require.Equal(t, int64(2), messages)
	var generation, turn, messageCount, turnCount int64
	var loopID protocol.LoopID
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(), `SELECT loop_id, loop_generation,
		turn_count, message_count FROM communication_loops`).
		Scan(&loopID, &generation, &turnCount, &messageCount))
	require.Equal(t, reply.AdmissionResult.LoopID, loopID)
	require.Equal(t, int64(1), generation)
	require.Equal(t, int64(1), turnCount)
	require.Equal(t, int64(1), messageCount)
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
		"SELECT thread_sequence FROM messages WHERE message_id = ?", reply.MessageID).Scan(&turn))
	require.Equal(t, int64(2), turn)
	var createdEvents int64
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
		"SELECT count(*) FROM events WHERE event_type = 'loop.created'").Scan(&createdEvents))
	require.Equal(t, int64(1), createdEvents)
}

func TestHubRootRecipientsBecomeImplicitLoopParticipants(t *testing.T) {
	f := newHubFixture(t)
	third := protocol.AgentID("agent-third")
	thirdPublic, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	f.keys.keys[third] = registry.KeyInfo{
		KeyID: testThirdKeyID, Agent: third, PublicKey: thirdPublic,
	}
	insertHubAgent(t, f.store, third)
	rootParams := testAdmitParams(f)
	rootParams.Recipients = append(rootParams.Recipients, MessageRecipient{
		Agent: third, DeliveryMode: protocol.DeliveryModeWhenSafe, RequiresAck: true,
	})
	rootRaw, err := callAdmit(t, f, rootParams)
	require.NoError(t, err)
	root := rootRaw.(AdmitMessageResult)
	firstRaw, err := callAdmitAs(t, f, testHubRecipient,
		testReplyAdmitParams(f, root.MessageID, "client-participant-first"))
	require.NoError(t, err)
	first := firstRaw.(AdmitMessageResult)
	secondRaw, err := callAdmitAs(t, f, third,
		testReplyAdmitParams(f, root.MessageID, "client-participant-second"))
	require.NoError(t, err)
	second := secondRaw.(AdmitMessageResult)
	require.Equal(t, first.AdmissionResult.LoopID, second.AdmissionResult.LoopID)

	var rawParticipants string
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
		"SELECT participants_json FROM communication_loops WHERE loop_id = ?",
		first.AdmissionResult.LoopID).Scan(&rawParticipants))
	var participants []protocol.AgentID
	require.NoError(t, json.Unmarshal([]byte(rawParticipants), &participants))
	require.Equal(t, []protocol.AgentID{testHubSender, testHubRecipient, third}, participants)
}

func TestHubConcurrentFirstRepliesCreateOneLoop(t *testing.T) {
	f := newHubFixture(t)
	rootRaw, err := callAdmit(t, f, testAdmitParams(f))
	require.NoError(t, err)
	root := rootRaw.(AdmitMessageResult)
	first := testReplyAdmitParams(f, root.MessageID, "client-concurrent-first-a")
	second := testReplyAdmitParams(f, root.MessageID, "client-concurrent-first-b")
	results := admitConcurrently(t, f,
		[]protocol.AgentID{testHubRecipient, testHubRecipient},
		[]AdmitMessageParams{first, second},
	)
	admitted := make([]AdmitMessageResult, len(results))
	bySequence := make(map[int64]AdmitMessageResult, len(results))
	for i, result := range results {
		require.NoError(t, result.err)
		admitted[i] = result.answer.(AdmitMessageResult)
		require.Equal(t, protocol.AdmissionStatusAccepted, admitted[i].AdmissionResult.Result)
		bySequence[admitted[i].ThreadSequence] = admitted[i]
	}
	require.Equal(t, admitted[0].AdmissionResult.LoopID, admitted[1].AdmissionResult.LoopID)
	sequences := map[int64]bool{
		admitted[0].ThreadSequence: true,
		admitted[1].ThreadSequence: true,
	}
	require.Equal(t, map[int64]bool{2: true, 3: true}, sequences)
	require.Equal(t,
		protocol.Digest("sha256:4e73139ce4aa8856e8f7d3739d9384dd498c41984d2bb44ce7b4454465b0aba1"),
		bySequence[2].RouteDigest)
	require.Equal(t,
		protocol.Digest("sha256:517b825602ab18ffae2f127b53060cddb91b1a6ff861a8d60c753de80f19e182"),
		bySequence[3].RouteDigest)
	tx, err := f.store.DB().BeginTx(t.Context(), &sql.TxOptions{ReadOnly: true})
	require.NoError(t, err)
	predecessor, err := replyRoutePredecessor(
		t.Context(), tx, root.MessageID, bySequence[3].AdmissionResult,
	)
	require.NoError(t, err)
	require.Equal(t, bySequence[3].RouteDigest, predecessor,
		"the next reply reconstructs the latest admitted route by ordinal")
	require.NoError(t, tx.Rollback())
	var loops, messages, loopMessages, turns int64
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
		"SELECT count(*) FROM communication_loops").Scan(&loops))
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
		"SELECT count(*) FROM messages").Scan(&messages))
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
		"SELECT message_count, turn_count FROM communication_loops").Scan(&loopMessages, &turns))
	require.Equal(t, int64(1), loops)
	require.Equal(t, int64(3), messages)
	require.Equal(t, int64(2), loopMessages)
	require.Equal(t, int64(1), turns, "consecutive replies from one sender share a turn")
}

func TestHubConcurrentRepliesCompeteForLastTurn(t *testing.T) {
	f := newHubFixture(t)
	third := protocol.AgentID("agent-third")
	thirdPublic, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	f.keys.keys[third] = registry.KeyInfo{
		KeyID: testThirdKeyID, Agent: third, PublicKey: thirdPublic,
	}
	insertHubAgent(t, f.store, third)
	requestedMaxTurns := int64(2)
	rootParams := testAdmitParams(f)
	rootParams.RequestedBudget = &protocol.RequestedBudget{MaxTurns: &requestedMaxTurns}
	rootParams.Recipients = append(rootParams.Recipients, MessageRecipient{
		Agent: third, DeliveryMode: protocol.DeliveryModeWhenSafe, RequiresAck: true,
	})
	rootRaw, err := callAdmit(t, f, rootParams)
	require.NoError(t, err)
	root := rootRaw.(AdmitMessageResult)
	firstRaw, err := callAdmitAs(t, f, testHubRecipient,
		testReplyAdmitParams(f, root.MessageID, "client-last-turn-first"))
	require.NoError(t, err)
	first := firstRaw.(AdmitMessageResult)

	fromRoot := testReplyAdmitParams(f, root.MessageID, "client-last-turn-from-third")
	fromReply := AdmitMessageParams{
		ClientMsgID: "client-last-turn-from-sender", OriginNode: testHubNode,
		DestinationNode: testHubNode,
		Recipients: []MessageRecipient{{
			Agent: testHubRecipient, DeliveryMode: protocol.DeliveryModeWhenSafe, RequiresAck: true,
		}},
		ThreadID: testHubThread, Priority: protocol.PriorityNormal,
		CreatedAt: f.clock.now, ExpiresAt: f.clock.now.Add(time.Hour), ParentMessageID: first.MessageID,
	}
	results := admitConcurrently(t, f,
		[]protocol.AgentID{third, testHubSender},
		[]AdmitMessageParams{fromRoot, fromReply},
	)
	statuses := map[protocol.AdmissionStatus]int{}
	for _, result := range results {
		require.NoError(t, result.err)
		admitted := result.answer.(AdmitMessageResult)
		statuses[admitted.AdmissionResult.Result]++
	}
	require.Equal(t, 1, statuses[protocol.AdmissionStatusAccepted])
	require.Equal(t, 1, statuses[protocol.AdmissionStatusAcceptedHeld])
	var turns, maxTurns, loopMessages, approvals, events int64
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(), `SELECT turn_count, max_turns,
		message_count FROM communication_loops WHERE loop_id = ?`, first.AdmissionResult.LoopID).
		Scan(&turns, &maxTurns, &loopMessages))
	require.Equal(t, maxTurns+1, turns)
	require.Equal(t, int64(3), loopMessages)
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(), `SELECT count(*) FROM approval_requests
		WHERE source = 'loop_budget' AND object_id = ? AND state = 'PENDING'`,
		first.AdmissionResult.LoopID).Scan(&approvals))
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(), `SELECT count(*) FROM events
		WHERE event_type = 'loop.budget_exhausted'`).Scan(&events))
	require.Equal(t, int64(1), approvals)
	require.Equal(t, int64(1), events)
}

func TestHubReplyOpeningLoopIsDeniedWhileDraining(t *testing.T) {
	f := newHubFixture(t)
	rootRaw, err := callAdmit(t, f, testAdmitParams(f))
	require.NoError(t, err)
	root := rootRaw.(AdmitMessageResult)
	_, err = f.controller.SetGlobal(t.Context(), protocol.SetGlobalParams{
		Mode: protocol.ChannelModeDraining, ExpectedVersion: 0,
	}, channel.Actor{Type: protocol.ActorTypeHuman, ID: testOperatorID})
	require.NoError(t, err)

	_, err = callAdmitAs(t, f, testHubRecipient,
		testReplyAdmitParams(f, root.MessageID, "client-draining-reply"))
	got := requireCallCode(t, err, codeDomainError)
	require.Equal(t, protocol.ErrorCodeChannelDraining, got.domain)
	var messages, loops int64
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
		"SELECT count(*) FROM messages").Scan(&messages))
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
		"SELECT count(*) FROM communication_loops").Scan(&loops))
	require.Equal(t, int64(1), messages)
	require.Zero(t, loops)
}

func TestHubReplyOpeningModeCreatesStepApproval(t *testing.T) {
	for _, test := range []struct {
		name   string
		reason protocol.DecisionReason
		setup  func(*testing.T, *hubFixture, *AdmitMessageParams)
	}{
		{
			name: "root requests step", reason: protocol.DecisionReasonLoopStepTokenRequired,
			setup: func(_ *testing.T, _ *hubFixture, root *AdmitMessageParams) {
				root.RequestedAutonomy = protocol.AutonomyStep
			},
		},
		{
			name: "first sender disables auto continue", reason: protocol.DecisionReasonAgentAutoContinueOff,
			setup: func(t *testing.T, f *hubFixture, _ *AdmitMessageParams) {
				off := false
				_, err := f.controller.SetAgent(t.Context(), protocol.SetAgentChannelParams{
					AgentID: testHubRecipient, AutoContinue: &off, ExpectedVersion: 0,
				}, channel.Actor{Type: protocol.ActorTypeHuman, ID: testOperatorID})
				require.NoError(t, err)
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newHubFixture(t)
			rootParams := testAdmitParams(f)
			test.setup(t, f, &rootParams)
			rootRaw, err := callAdmit(t, f, rootParams)
			require.NoError(t, err)
			root := rootRaw.(AdmitMessageResult)
			replyParams := testReplyAdmitParams(f, root.MessageID, "client-step-reply")
			replyRaw, err := callAdmitAs(t, f, testHubRecipient, replyParams)
			require.NoError(t, err)
			reply := replyRaw.(AdmitMessageResult)
			require.Equal(t, protocol.AdmissionStatusApprovalRequired, reply.AdmissionResult.Result)
			require.Equal(t, test.reason, reply.AdmissionResult.ControlReason)
			require.True(t, reply.AdmissionResult.ApprovalRequestID.Valid())

			var mode protocol.LoopMode
			require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
				"SELECT mode FROM communication_loops WHERE loop_id = ?", reply.AdmissionResult.LoopID).
				Scan(&mode))
			require.Equal(t, protocol.LoopModeStep, mode)
			var source protocol.ApprovalSource
			var objectType protocol.ApprovalObjectType
			var objectID, options string
			var state protocol.ApprovalState
			require.NoError(t, f.store.DB().QueryRowContext(t.Context(), `SELECT source, object_type,
				object_id, options_json, state FROM approval_requests WHERE approval_id = ?`,
				reply.AdmissionResult.ApprovalRequestID).
				Scan(&source, &objectType, &objectID, &options, &state))
			require.Equal(t, protocol.ApprovalSourceLoopStep, source)
			require.Equal(t, protocol.ApprovalObjectTypeMessage, objectType)
			require.Equal(t, string(reply.MessageID), objectID)
			require.JSONEq(t, `["steps:1","stop"]`, options)
			require.Equal(t, protocol.ApprovalStatePending, state)
		})
	}
}

func TestHubReplyOpeningStepToAutoReleasesDelivery(t *testing.T) {
	f := newHubFixture(t)
	rootParams := testAdmitParams(f)
	rootParams.RequestedAutonomy = protocol.AutonomyStep
	rootRaw, err := callAdmit(t, f, rootParams)
	require.NoError(t, err)
	root := rootRaw.(AdmitMessageResult)

	replyParams := testReplyAdmitParams(f, root.MessageID, "client-opening-step-reply")
	replyRaw, err := callAdmitAs(t, f, testHubRecipient, replyParams)
	require.NoError(t, err)
	reply := replyRaw.(AdmitMessageResult)
	require.Equal(t, protocol.AdmissionStatusApprovalRequired, reply.AdmissionResult.Result)
	require.Equal(t, protocol.DecisionReasonLoopStepTokenRequired, reply.AdmissionResult.ControlReason)

	var stepStage protocol.DecisionStage
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(), `SELECT stage
		FROM channel_control_decisions WHERE object_id = ? AND reason = ? ORDER BY rowid LIMIT 1`,
		reply.MessageID, protocol.DecisionReasonLoopStepTokenRequired).Scan(&stepStage))
	require.Equal(t, protocol.DecisionStageAdmission, stepStage)

	envelope := signedEnvelopeFor(t, f, reply, replyParams)
	envelope.OriginAgent = testHubRecipient
	envelope.RecipientAgents = []protocol.AgentID{testHubSender}
	envelope.ParentMessageID = replyParams.ParentMessageID
	envelope.LoopID = reply.AdmissionResult.LoopID
	envelope.LoopGeneration = reply.AdmissionResult.LoopGeneration
	envelope.TurnIndex = reply.AdmissionResult.TurnIndex
	envelope.SenderSigningKeyID = "ed25519:agent-recipient:v1"
	require.NoError(t, giffordcrypto.SignWireEnvelope(f.replyKey, &envelope))
	require.NoError(t, envelope.Validate())
	rawEnvelope, err := json.Marshal(envelope)
	require.NoError(t, err)
	_, err = f.service.SubmitMessageEnvelope(t.Context(), testHubRecipient, rawEnvelope)
	require.NoError(t, err)

	loop, err := f.controller.Loop(t.Context(), reply.AdmissionResult.LoopID)
	require.NoError(t, err)
	require.Equal(t, protocol.LoopModeStep, loop.Mode)
	loop, err = f.controller.SetLoopMode(t.Context(), protocol.SetLoopModeParams{
		LoopID: loop.LoopID, Mode: protocol.LoopModeAuto, ExpectedVersion: loop.ControlVersion,
	}, channel.Actor{Type: protocol.ActorTypeHuman, ID: testOperatorID})
	require.NoError(t, err)
	require.Equal(t, protocol.LoopModeAuto, loop.Mode)

	var approvalState protocol.ApprovalState
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
		"SELECT state FROM approval_requests WHERE approval_id = ?",
		reply.AdmissionResult.ApprovalRequestID).Scan(&approvalState))
	require.Equal(t, protocol.ApprovalStateSuperseded, approvalState)

	listedRaw, err := f.service.ListPendingMessages(t.Context(), testHubSender, json.RawMessage(`{}`))
	require.NoError(t, err)
	listed := listedRaw.(ListPendingMessagesResult)
	require.Len(t, listed.Messages, 1)
	require.Equal(t, reply.MessageID, listed.Messages[0].MessageID)
	var deliveryOutcome protocol.DecisionOutcome
	var deliveryReason protocol.DecisionReason
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(), `SELECT outcome, reason
		FROM channel_control_decisions WHERE object_id = ? AND stage = 'delivery'
		ORDER BY rowid DESC LIMIT 1`, reply.MessageID).Scan(&deliveryOutcome, &deliveryReason))
	require.Equal(t, protocol.DecisionOutcomeAllow, deliveryOutcome)
	require.Equal(t, protocol.DecisionReasonAllowed, deliveryReason)
}

func TestHubReplyHardBudgetDenialPausesWithoutReservation(t *testing.T) {
	f := newHubFixture(t)
	requestedMaxMessages := int64(1)
	rootParams := testAdmitParams(f)
	rootParams.RequestedBudget = &protocol.RequestedBudget{MaxMessages: &requestedMaxMessages}
	rootRaw, err := callAdmit(t, f, rootParams)
	require.NoError(t, err)
	root := rootRaw.(AdmitMessageResult)
	firstParams := testReplyAdmitParams(f, root.MessageID, "client-budget-first")
	firstRaw, err := callAdmitAs(t, f, testHubRecipient, firstParams)
	require.NoError(t, err)
	first := firstRaw.(AdmitMessageResult)

	second := AdmitMessageParams{
		ClientMsgID: "client-budget-denied", OriginNode: testHubNode, DestinationNode: testHubNode,
		Recipients: []MessageRecipient{{
			Agent: testHubRecipient, DeliveryMode: protocol.DeliveryModeWhenSafe, RequiresAck: true,
		}},
		ThreadID: testHubThread, Priority: protocol.PriorityNormal,
		CreatedAt: f.clock.now, ExpiresAt: f.clock.now.Add(time.Hour), ParentMessageID: first.MessageID,
	}
	for range 2 {
		_, err = callAdmitAs(t, f, testHubSender, second)
		got := requireCallCode(t, err, codeDomainError)
		require.Equal(t, protocol.ErrorCodeLoopBudgetExhausted, got.domain)
	}
	var mode protocol.LoopMode
	var stateReason protocol.LoopStateReason
	var messageCount, maxMessages int64
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(), `SELECT mode, state_reason,
		message_count, max_messages FROM communication_loops WHERE loop_id = ?`,
		first.AdmissionResult.LoopID).Scan(&mode, &stateReason, &messageCount, &maxMessages))
	require.Equal(t, protocol.LoopModePaused, mode)
	require.Equal(t, protocol.LoopStateReasonBudgetExhausted, stateReason)
	require.Equal(t, maxMessages, messageCount)
	var deniedMessages, approvals, exhaustedEvents int64
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
		"SELECT count(*) FROM messages WHERE client_msg_id = ?", second.ClientMsgID).Scan(&deniedMessages))
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(), `SELECT count(*) FROM approval_requests
		WHERE source = 'loop_budget' AND object_type = 'loop' AND object_id = ? AND state = 'PENDING'`,
		first.AdmissionResult.LoopID).Scan(&approvals))
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(), `SELECT count(*) FROM events
		WHERE event_type = 'loop.budget_exhausted'`).Scan(&exhaustedEvents))
	require.Zero(t, deniedMessages)
	require.Equal(t, int64(1), approvals)
	require.Equal(t, int64(1), exhaustedEvents)
	var greatestSequence int64
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
		"SELECT MAX(thread_sequence) FROM messages").Scan(&greatestSequence))
	require.Equal(t, int64(2), greatestSequence)

	runtimeBase := t.TempDir()
	t.Setenv("XDG_RUNTIME_DIR", runtimeBase)
	server, err := controlapi.New(controlapi.Options{
		RuntimeDir: filepath.Join(runtimeBase, "gifford"), DB: f.store.DB(), Controller: f.controller,
	})
	require.NoError(t, err)
	serverCtx, cancelServer := context.WithCancel(t.Context())
	serverDone := make(chan error, 1)
	go func() { serverDone <- server.Serve(serverCtx) }()
	t.Cleanup(func() {
		cancelServer()
		require.NoError(t, <-serverDone)
	})
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, statErr := os.Stat(server.Path()); statErr == nil {
			break
		}
		require.True(t, time.Now().Before(deadline), "control socket was not created")
		time.Sleep(5 * time.Millisecond)
	}
	runCtl := func(args []string, into any) {
		t.Helper()
		command := ctl.NewCmd(&deps.Deps{})
		var output bytes.Buffer
		command.SetOut(&output)
		command.SetErr(io.Discard)
		command.SetArgs(args)
		require.NoError(t, command.ExecuteContext(t.Context()))
		require.NoError(t, json.Unmarshal(output.Bytes(), into))
	}
	var pending protocol.ApprovalList
	runCtl([]string{"approvals", "--pending"}, &pending)
	require.Len(t, pending.Approvals, 1)
	require.Equal(t, protocol.ApprovalSourceLoopBudget, pending.Approvals[0].Source)
	require.Equal(t, first.AdmissionResult.LoopID, pending.Approvals[0].LoopID)

	var active protocol.LoopList
	runCtl([]string{testCtlLoop, "list", "--active"}, &active)
	require.Len(t, active.Loops, 1)
	require.Equal(t, first.AdmissionResult.LoopID, active.Loops[0].LoopID)
	var status protocol.Loop
	runCtl([]string{testCtlLoop, "status", string(first.AdmissionResult.LoopID)}, &status)
	require.Equal(t, protocol.LoopModePaused, status.Mode)
	require.Equal(t, status.Budget.MaxMessages, status.Counters.MessageCount)

	var extended protocol.Loop
	runCtl([]string{testCtlLoop, "budget", string(status.LoopID), "--add-messages", "1", testCtlReason, "continue once"}, &extended)
	require.Equal(t, protocol.LoopModeAuto, extended.Mode)
	require.Equal(t, status.Budget.MaxMessages+1, extended.Budget.MaxMessages)
	runCtl([]string{"approvals", "--pending"}, &pending)
	require.Empty(t, pending.Approvals)

	var stepped protocol.Loop
	runCtl([]string{testCtlLoop, "set-mode", string(status.LoopID), "step", testCtlReason, "operator step"}, &stepped)
	require.Equal(t, protocol.LoopModeStep, stepped.Mode)
	var stopped protocol.Loop
	runCtl([]string{testCtlLoop, "stop", string(status.LoopID), testCtlReason, "operator stop"}, &stopped)
	require.Equal(t, protocol.LoopModeStopped, stopped.Mode)
	runCtl([]string{testCtlLoop, "list", "--active"}, &active)
	require.Empty(t, active.Loops)
}

func TestHubOutsiderCannotPauseAnotherRootsLoop(t *testing.T) {
	f := newHubFixture(t)
	third := protocol.AgentID("agent-third")
	thirdPublic, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	f.keys.keys[third] = registry.KeyInfo{
		KeyID: testThirdKeyID, Agent: third, PublicKey: thirdPublic,
	}
	insertHubAgent(t, f.store, third)
	requestedMaxMessages := int64(1)
	firstRootParams := testAdmitParams(f)
	firstRootParams.RequestedBudget = &protocol.RequestedBudget{MaxMessages: &requestedMaxMessages}
	firstRootRaw, err := callAdmit(t, f, firstRootParams)
	require.NoError(t, err)
	firstRoot := firstRootRaw.(AdmitMessageResult)
	firstReplyRaw, err := callAdmitAs(t, f, testHubRecipient,
		testReplyAdmitParams(f, firstRoot.MessageID, "client-owned-loop"))
	require.NoError(t, err)
	firstReply := firstReplyRaw.(AdmitMessageResult)

	otherRootParams := testAdmitParams(f)
	otherRootParams.ClientMsgID = "client-other-root"
	otherRootParams.Recipients = []MessageRecipient{{
		Agent: third, DeliveryMode: protocol.DeliveryModeWhenSafe, RequiresAck: true,
	}}
	otherRootRaw, err := callAdmit(t, f, otherRootParams)
	require.NoError(t, err)
	otherRoot := otherRootRaw.(AdmitMessageResult)

	_, err = callAdmitAs(t, f, third,
		testReplyAdmitParams(f, otherRoot.MessageID, "client-outsider-budget"))
	_ = requireCallCode(t, err, codeInvalidRequest)
	var mode protocol.LoopMode
	var messageCount, maxMessages, approvals, events int64
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(), `SELECT mode, message_count,
		max_messages FROM communication_loops WHERE loop_id = ?`, firstReply.AdmissionResult.LoopID).
		Scan(&mode, &messageCount, &maxMessages))
	require.Equal(t, protocol.LoopModeAuto, mode)
	require.Equal(t, maxMessages, messageCount)
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
		"SELECT count(*) FROM approval_requests WHERE source = 'loop_budget'").Scan(&approvals))
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
		"SELECT count(*) FROM events WHERE event_type = 'loop.budget_exhausted'").Scan(&events))
	require.Zero(t, approvals)
	require.Zero(t, events)
}

func TestHubReplySoftTurnLimitAdmitsHeldAndReservesNextTurn(t *testing.T) {
	f := newHubFixture(t)
	requestedMaxTurns := int64(1)
	rootParams := testAdmitParams(f)
	rootParams.RequestedBudget = &protocol.RequestedBudget{MaxTurns: &requestedMaxTurns}
	rootRaw, err := callAdmit(t, f, rootParams)
	require.NoError(t, err)
	root := rootRaw.(AdmitMessageResult)
	firstRaw, err := callAdmitAs(t, f, testHubRecipient,
		testReplyAdmitParams(f, root.MessageID, "client-turn-first"))
	require.NoError(t, err)
	first := firstRaw.(AdmitMessageResult)
	secondParams := AdmitMessageParams{
		ClientMsgID: "client-turn-soft-limit", OriginNode: testHubNode, DestinationNode: testHubNode,
		Recipients: []MessageRecipient{{
			Agent: testHubRecipient, DeliveryMode: protocol.DeliveryModeWhenSafe, RequiresAck: true,
		}},
		ThreadID: testHubThread, Priority: protocol.PriorityNormal,
		CreatedAt: f.clock.now, ExpiresAt: f.clock.now.Add(time.Hour), ParentMessageID: first.MessageID,
	}
	secondRaw, err := callAdmitAs(t, f, testHubSender, secondParams)
	require.NoError(t, err)
	second := secondRaw.(AdmitMessageResult)
	require.Equal(t, protocol.AdmissionStatusAcceptedHeld, second.AdmissionResult.Result)
	require.Equal(t, protocol.DecisionReasonLoopBudgetExhausted, second.AdmissionResult.ControlReason)
	require.Equal(t, int64(3), second.ThreadSequence)
	var mode protocol.LoopMode
	var turns, maxTurns, messages int64
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(), `SELECT mode, turn_count,
		max_turns, message_count FROM communication_loops WHERE loop_id = ?`,
		first.AdmissionResult.LoopID).Scan(&mode, &turns, &maxTurns, &messages))
	require.Equal(t, protocol.LoopModePaused, mode)
	require.Equal(t, maxTurns+1, turns)
	require.Equal(t, maxTurns+1, *second.AdmissionResult.TurnIndex)
	require.Equal(t, int64(2), messages)
	var approvals, events int64
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(), `SELECT count(*) FROM approval_requests
		WHERE source = 'loop_budget' AND object_id = ? AND state = 'PENDING'`,
		first.AdmissionResult.LoopID).Scan(&approvals))
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(), `SELECT count(*) FROM events
		WHERE event_type = 'loop.budget_exhausted'`).Scan(&events))
	require.Equal(t, int64(1), approvals)
	require.Equal(t, int64(1), events)
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
	listedRaw, err := f.service.ListPendingMessages(t.Context(), testHubRecipient, json.RawMessage(`{}`))
	require.NoError(t, err)
	require.Empty(t, listedRaw.(ListPendingMessagesResult).Messages)
	for range 2 {
		_, err = f.service.ClaimMessage(t.Context(), testHubRecipient, claimParams)
		got := requireCallCode(t, err, codeDomainError)
		require.Equal(t, protocol.ErrorCodeNotFound, got.domain)
		require.ErrorContains(t, got, "already acknowledged")
	}
}

func TestAckMessageClosesOnlyAnUnclaimedAcknowledgedNotice(t *testing.T) {
	setup := func(t *testing.T) (*hubFixture, AdmitMessageResult, protocol.ApprovalRequest) {
		t.Helper()
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
		tx, err := f.store.DB().BeginTx(t.Context(), nil)
		require.NoError(t, err)
		approval, created, err := f.controller.EnsureOrchestratorApproval(
			t.Context(), tx, admitted.MessageID, testHubRecipient, "automatic delivery deferred",
		)
		require.NoError(t, err)
		require.True(t, created)
		require.NoError(t, tx.Commit())
		return f, admitted, approval
	}
	ack := func(t *testing.T, f *hubFixture, messageID protocol.ULID, status AckMessageStatus) {
		t.Helper()
		raw, err := json.Marshal(AckMessageParams{
			MessageID: messageID, DeliveryGeneration: 1, Status: status,
		})
		require.NoError(t, err)
		_, err = f.service.AckMessage(t.Context(), testHubRecipient, raw)
		require.NoError(t, err)
	}
	queueState := func(t *testing.T, f *hubFixture, messageID protocol.ULID) string {
		t.Helper()
		var state string
		require.NoError(t, f.store.DB().QueryRowContext(t.Context(), `SELECT state
			FROM delivery_queue WHERE message_id = ? AND agent_id = ? AND delivery_generation = 1`,
			messageID, testHubRecipient).Scan(&state))
		return state
	}

	t.Run("acknowledged cancels and supersedes atomically", func(t *testing.T) {
		f, admitted, approval := setup(t)
		ack(t, f, admitted.MessageID, AckMessageAcknowledged)
		ack(t, f, admitted.MessageID, AckMessageAcknowledged)

		require.Equal(t, "CANCELLED", queueState(t, f, admitted.MessageID))
		resolved, err := f.controller.Approval(t.Context(), approval.ApprovalID)
		require.NoError(t, err)
		require.Equal(t, protocol.ApprovalStateSuperseded, resolved.State)
		require.Equal(t, "orchestrator", resolved.ResolvedBy)
		var acks, attempts int
		require.NoError(t, f.store.DB().QueryRowContext(t.Context(), `SELECT count(*) FROM acks
			WHERE message_id = ? AND agent_id = ? AND delivery_generation = 1
				AND status = 'ACKNOWLEDGED'`, admitted.MessageID, testHubRecipient).Scan(&acks))
		require.Equal(t, 1, acks)
		require.NoError(t, f.store.DB().QueryRowContext(t.Context(), `SELECT count(*)
			FROM delivery_attempts WHERE message_id = ?`, admitted.MessageID).Scan(&attempts))
		require.Zero(t, attempts)
		var messageState string
		require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
			`SELECT state FROM messages WHERE message_id = ?`, admitted.MessageID).Scan(&messageState))
		require.Equal(t, messageStatePending, messageState)
	})

	t.Run("idempotent retry repairs a preexisting ack", func(t *testing.T) {
		f, admitted, approval := setup(t)
		_, err := f.store.DB().ExecContext(t.Context(), `INSERT INTO acks
			(message_id, agent_id, delivery_generation, status, acked_at)
			VALUES (?, ?, 1, 'ACKNOWLEDGED', ?)`, admitted.MessageID, testHubRecipient,
			eventstore.FormatTime(f.clock.Now()))
		require.NoError(t, err)

		ack(t, f, admitted.MessageID, AckMessageAcknowledged)
		require.Equal(t, "CANCELLED", queueState(t, f, admitted.MessageID))
		resolved, err := f.controller.Approval(t.Context(), approval.ApprovalID)
		require.NoError(t, err)
		require.Equal(t, protocol.ApprovalStateSuperseded, resolved.State)
	})

	t.Run("processing leaves the notice pending", func(t *testing.T) {
		f, admitted, approval := setup(t)
		ack(t, f, admitted.MessageID, AckMessageProcessing)
		require.Equal(t, "QUEUED", queueState(t, f, admitted.MessageID))
		pending, err := f.controller.Approval(t.Context(), approval.ApprovalID)
		require.NoError(t, err)
		require.Equal(t, protocol.ApprovalStatePending, pending.State)
	})

	for _, state := range []string{"CLAIMED", "DELIVERING"} {
		t.Run(strings.ToLower(state)+" belongs to the active delivery", func(t *testing.T) {
			f, admitted, approval := setup(t)
			_, err := f.store.DB().ExecContext(t.Context(), `UPDATE delivery_queue SET state = ?
				WHERE message_id = ? AND agent_id = ? AND delivery_generation = 1`,
				state, admitted.MessageID, testHubRecipient)
			require.NoError(t, err)
			ack(t, f, admitted.MessageID, AckMessageAcknowledged)
			require.Equal(t, state, queueState(t, f, admitted.MessageID))
			pending, err := f.controller.Approval(t.Context(), approval.ApprovalID)
			require.NoError(t, err)
			require.Equal(t, protocol.ApprovalStatePending, pending.State)
		})
	}

	t.Run("uncertain recovery keeps its queue and approval", func(t *testing.T) {
		f, admitted, approval := setup(t)
		stamp := eventstore.FormatTime(f.clock.Now())
		_, err := f.store.DB().ExecContext(t.Context(), `INSERT INTO delivery_attempts
			(delivery_id, message_id, agent_id, delivery_generation, adapter_session_id,
			 orchestrator_decision_id, channel_decision_id, draft_generation,
			 input_guard_version, control_versions_json, state, terminal_reason,
			 created_at, updated_at, uncertain_approval_id)
			VALUES ('dlv-uncertain', ?, ?, 1, 'session-agent-recipient',
			 'decision-uncertain', 'channel-uncertain', 0, 0, '{}', 'UNCERTAIN',
			 'TIMED_OUT', ?, ?, ?)`, admitted.MessageID, testHubRecipient, stamp, stamp,
			approval.ApprovalID)
		require.NoError(t, err)

		ack(t, f, admitted.MessageID, AckMessageAcknowledged)
		require.Equal(t, "QUEUED", queueState(t, f, admitted.MessageID))
		pending, err := f.controller.Approval(t.Context(), approval.ApprovalID)
		require.NoError(t, err)
		require.Equal(t, protocol.ApprovalStatePending, pending.State)
	})
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
	_, err = f.service.ClaimMessage(t.Context(), testHubRecipient, claim)
	require.NoError(t, err)
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(), `SELECT control_state,
		control_reason FROM delivery_queue WHERE message_id = ? AND agent_id = ?`,
		admitted.MessageID, testHubRecipient).Scan(&controlState, &controlReason))
	require.Equal(t, protocol.ControlStateAllowed, controlState)
	require.Equal(t, protocol.DecisionReasonHoldReleased, controlReason)
	explanation, err := f.controller.Explain(t.Context(), protocol.ExplainParams{MessageID: admitted.MessageID})
	require.NoError(t, err)
	require.Equal(t, protocol.DecisionReasonHoldReleased, explanation.Reason)
}

func TestHubApprovedOnceExposesOnlyReleasedDeliveryGeneration(t *testing.T) {
	f := newHubFixture(t)
	actor := channel.Actor{Type: protocol.ActorTypeHuman, ID: testOperatorID}
	_, err := f.controller.SetAgentMode(t.Context(), protocol.SetAgentModeParams{
		AgentID: testHubRecipient, Mode: protocol.AgentModeManual, ExpectedVersion: 0,
	}, actor)
	require.NoError(t, err)
	params := testAdmitParams(f)
	answer, err := callAdmit(t, f, params)
	require.NoError(t, err)
	admitted := answer.(AdmitMessageResult)
	envelope := signedEnvelopeFor(t, f, admitted, params)
	rawEnvelope, err := json.Marshal(envelope)
	require.NoError(t, err)
	_, err = f.service.SubmitMessageEnvelope(t.Context(), testHubSender, rawEnvelope)
	require.NoError(t, err)
	claimOne, err := json.Marshal(ClaimMessageParams{
		MessageID: admitted.MessageID, DeliveryGeneration: 1,
	})
	require.NoError(t, err)

	listedRaw, err := f.service.ListPendingMessages(
		t.Context(), testHubRecipient, json.RawMessage(`{}`),
	)
	require.NoError(t, err)
	require.Empty(t, listedRaw.(ListPendingMessagesResult).Messages)
	_, err = f.service.ClaimMessage(t.Context(), testHubRecipient, claimOne)
	held := requireCallCode(t, err, codeDomainError)
	require.ErrorContains(t, held, string(protocol.DecisionReasonAgentAutoDeliveryOff))

	tx, err := f.store.DB().BeginTx(t.Context(), nil)
	require.NoError(t, err)
	approval, created, err := f.controller.EnsureOrchestratorApproval(
		t.Context(), tx, admitted.MessageID, testHubRecipient, "automatic delivery deferred",
	)
	require.NoError(t, err)
	require.True(t, created)
	message := channel.Message{
		ID: admitted.MessageID, From: testHubSender, FromNode: testHubNode,
		To: []channel.Recipient{{
			Agent: testHubRecipient, Deliverable: true, Policy: channel.PolicyAllowed,
		}},
		ToNode: testHubNode, ThreadID: params.ThreadID, HopCount: params.HopCount,
	}
	decided, err := f.service.engine.AuthorizePTYDelivery(
		t.Context(), tx, message, 1, approval.ApprovalID,
	)
	require.NoError(t, err)
	require.True(t, decided.Allowed())
	require.Equal(t, protocol.DecisionReasonApprovedOnce, decided.Reason())
	resolved, err := f.controller.ResolveOrchestratorApproval(
		t.Context(), tx, protocol.ResolveApprovalParams{
			ApprovalID: approval.ApprovalID, Option: "deliver_now", Reason: "human selected deliver_now",
		}, actor,
	)
	require.NoError(t, err)
	require.Equal(t, protocol.ApprovalStateApproved, resolved.State)
	require.NoError(t, tx.Commit())

	tx, err = f.store.DB().BeginTx(t.Context(), nil)
	require.NoError(t, err)
	_, err = tx.ExecContext(t.Context(), `INSERT INTO delivery_queue
		(message_id, agent_id, delivery_generation, from_agent, thread_ref, state,
		 priority, created_at, expires_at, updated_at, parent_message_id, thread_sequence,
		 loop_id, loop_generation, turn_index, hop_count, route_digest)
		SELECT message_id, agent_id, 2, from_agent, thread_ref, 'QUEUED', priority,
		 created_at, expires_at, updated_at, parent_message_id, thread_sequence,
		 loop_id, loop_generation, turn_index, hop_count, route_digest
		FROM delivery_queue WHERE message_id = ? AND agent_id = ? AND delivery_generation = 1`,
		admitted.MessageID, testHubRecipient)
	require.NoError(t, err)
	require.NoError(t, f.service.engine.InitializeDelivery(
		t.Context(), tx, admitted.MessageID, testHubRecipient, 2,
	))
	require.NoError(t, tx.Commit())

	listedRaw, err = f.service.ListPendingMessages(
		t.Context(), testHubRecipient, json.RawMessage(`{}`),
	)
	require.NoError(t, err)
	listed := listedRaw.(ListPendingMessagesResult).Messages
	require.Len(t, listed, 1)
	require.Equal(t, int64(1), listed[0].DeliveryGeneration)
	claimTwo, err := json.Marshal(ClaimMessageParams{
		MessageID: admitted.MessageID, DeliveryGeneration: 2,
	})
	require.NoError(t, err)
	_, err = f.service.ClaimMessage(t.Context(), testHubRecipient, claimTwo)
	held = requireCallCode(t, err, codeDomainError)
	require.ErrorContains(t, held, string(protocol.DecisionReasonAgentAutoDeliveryOff))
	claimed, err := f.service.ClaimMessage(t.Context(), testHubRecipient, claimOne)
	require.NoError(t, err)
	require.Equal(t, int64(1), claimed.(ClaimMessageResult).DeliveryGeneration)

	rows, err := f.store.DB().QueryContext(t.Context(), `SELECT delivery_generation,
		control_state, control_reason FROM delivery_queue WHERE message_id = ? AND agent_id = ?
		ORDER BY delivery_generation`, admitted.MessageID, testHubRecipient)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	type control struct {
		generation int64
		state      protocol.ControlState
		reason     protocol.DecisionReason
	}
	var controls []control
	for rows.Next() {
		var got control
		require.NoError(t, rows.Scan(&got.generation, &got.state, &got.reason))
		controls = append(controls, got)
	}
	require.NoError(t, rows.Err())
	require.Equal(t, []control{
		{generation: 1, state: protocol.ControlStateAllowed, reason: protocol.DecisionReasonApprovedOnce},
		{generation: 2, state: protocol.ControlStateHeld, reason: protocol.DecisionReasonAgentAutoDeliveryOff},
	}, controls)
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

	actor := channel.Actor{Type: protocol.ActorTypeHuman, ID: testOperatorID}
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
	response := dispatch(t.Context(), testHubSender, 1, Handlers{Admit: handler}, rpcRequest{
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
