package hub

import (
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

const (
	testBridgeSource  = "source-node-a"
	testBridgeCreated = "2026-10-29T11:59:00.123456Z"
	testBridgeRead    = "2026-10-29T12:00:05.654321Z"
)

func bridgeContext(value string) *string { return &value }

func testBridgeMirrorParams() BridgeMirrorMessageParams {
	return BridgeMirrorMessageParams{
		SourceNodeID:    testBridgeSource,
		SourceMessageID: "73402",
		FromAgent:       testHubSender,
		ToAgent:         testHubRecipient,
		SourceCreatedAt: testBridgeCreated,
		SourceContextID: bridgeContext("context-bridge-1"),
	}
}

func callBridgeMirror(
	t *testing.T,
	f *hubFixture,
	params BridgeMirrorMessageParams,
) (BridgeMirrorMessageResult, error) {
	t.Helper()
	raw, err := json.Marshal(params)
	require.NoError(t, err)
	answer, err := f.service.MirrorMessage(t.Context(), raw)
	if err != nil {
		return BridgeMirrorMessageResult{}, err
	}
	return answer.(BridgeMirrorMessageResult), nil
}

func callBridgeRead(
	t *testing.T,
	f *hubFixture,
	params BridgeMarkReadParams,
) (BridgeMarkReadResult, error) {
	t.Helper()
	raw, err := json.Marshal(params)
	require.NoError(t, err)
	answer, err := f.service.MarkRead(t.Context(), raw)
	if err != nil {
		return BridgeMarkReadResult{}, err
	}
	return answer.(BridgeMarkReadResult), nil
}

func requireBridgeDomainError(
	t *testing.T,
	err error,
	code protocol.ErrorCode,
) {
	t.Helper()
	var callErr *callError
	require.ErrorAs(t, err, &callErr)
	require.Equal(t, codeDomainError, callErr.code)
	require.Equal(t, messageRejected, callErr.message)
	require.Equal(t, code, callErr.domain)
}

func requireBridgeInvalidError(t *testing.T, err error) {
	t.Helper()
	var callErr *callError
	require.ErrorAs(t, err, &callErr)
	require.Equal(t, codeInvalidRequest, callErr.code)
	require.Equal(t, messageInvalid, callErr.message)
	require.Empty(t, callErr.domain)
}

func TestBridgeMirrorCreatesDurableMetadataAndExactRetry(t *testing.T) {
	f := newHubFixture(t)
	wake := make(chan struct{}, 2)
	f.service.notificationWake = wake
	params := testBridgeMirrorParams()

	created, err := callBridgeMirror(t, f, params)
	require.NoError(t, err)
	require.True(t, created.GiffordMessageID.Valid())
	require.True(t, created.GiffordThreadID.Valid())
	require.Equal(t, BridgeDispositionCreated, created.Disposition)
	require.Len(t, wake, 1)

	var clientID, namespace, origin, destination, fromAgent, threadID string
	var state, priority, createdAt, expiresAt string
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(), `SELECT client_msg_id,
		client_msg_namespace, origin_node, destination_node, from_agent, thread_id,
		state, priority, created_at, expires_at FROM messages WHERE message_id = ?`,
		created.GiffordMessageID).Scan(
		&clientID, &namespace, &origin, &destination, &fromAgent, &threadID,
		&state, &priority, &createdAt, &expiresAt,
	))
	require.Equal(t, params.SourceMessageID, clientID)
	require.Equal(t, params.SourceNodeID, namespace)
	require.Equal(t, string(testHubNode), origin)
	require.Equal(t, string(testHubNode), destination)
	require.Equal(t, string(testHubSender), fromAgent)
	require.Equal(t, string(created.GiffordThreadID), threadID)
	require.Equal(t, messageStatePending, state)
	require.Equal(t, string(protocol.PriorityNormal), priority)
	sourceTime, err := time.Parse(time.RFC3339Nano, testBridgeCreated)
	require.NoError(t, err)
	require.Equal(t, eventstore.FormatTime(sourceTime), createdAt)
	require.Equal(t, bridgeNoExpiry, expiresAt)

	var mappedCreated, mappedContext string
	var mappedRead sql.NullString
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(), `SELECT source_created_at,
		source_context_id, source_read_at FROM bridge_message_mappings
		WHERE source_node_id = ? AND source_message_id = 73402`, testBridgeSource).Scan(
		&mappedCreated, &mappedContext, &mappedRead,
	))
	require.Equal(t, testBridgeCreated, mappedCreated)
	require.Equal(t, *params.SourceContextID, mappedContext)
	require.False(t, mappedRead.Valid)

	var envelopes int
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
		"SELECT count(*) FROM message_envelopes WHERE message_id = ?", created.GiffordMessageID).
		Scan(&envelopes))
	require.Zero(t, envelopes, "a bridge mirror must never invent content")
	var queueState string
	var queueControl protocol.ControlState
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(), `SELECT state, control_state
		FROM delivery_queue WHERE message_id = ? AND agent_id = ? AND delivery_generation = 1`,
		created.GiffordMessageID, testHubRecipient).Scan(&queueState, &queueControl))
	require.Equal(t, "QUEUED", queueState)
	require.Equal(t, protocol.ControlStateHeld, queueControl)
	_, err = f.store.DB().ExecContext(t.Context(), `UPDATE messages
		SET client_msg_namespace = 'other-node' WHERE message_id = ?`, created.GiffordMessageID)
	require.ErrorContains(t, err, "messages.client_msg_namespace is immutable")
	_, err = f.store.DB().ExecContext(t.Context(), `UPDATE bridge_message_mappings
		SET from_agent = 'agent-third' WHERE source_node_id = ? AND source_message_id = 73402`,
		testBridgeSource)
	require.ErrorContains(t, err, "bridge message projection is immutable")

	restarted, err := NewService(ServiceOptions{
		DB: f.store.DB(), NodeID: testHubNode, Engine: f.service.engine,
		Approvals: f.controller, Registry: f.keys, Clock: f.clock, EventStore: f.store,
	})
	require.NoError(t, err)
	f.service = restarted
	duplicate, err := callBridgeMirror(t, f, params)
	require.NoError(t, err)
	require.Equal(t, created.GiffordMessageID, duplicate.GiffordMessageID)
	require.Equal(t, created.GiffordThreadID, duplicate.GiffordThreadID)
	require.Equal(t, BridgeDispositionDuplicate, duplicate.Disposition)
	require.Len(t, wake, 1, "an exact retry must not wake another notification")
}

func TestBridgeMirrorNamespacesDedupeAndMapsContexts(t *testing.T) {
	f := newHubFixture(t)
	firstParams := testBridgeMirrorParams()
	first, err := callBridgeMirror(t, f, firstParams)
	require.NoError(t, err)
	restarted, err := NewService(ServiceOptions{
		DB: f.store.DB(), NodeID: testHubNode, Engine: f.service.engine,
		Approvals: f.controller, Registry: f.keys, Clock: f.clock, EventStore: f.store,
	})
	require.NoError(t, err)
	f.service = restarted

	secondParams := firstParams
	secondParams.SourceMessageID = "73403"
	second, err := callBridgeMirror(t, f, secondParams)
	require.NoError(t, err)
	require.NotEqual(t, first.GiffordMessageID, second.GiffordMessageID)
	require.Equal(t, first.GiffordThreadID, second.GiffordThreadID)

	otherNodeParams := firstParams
	otherNodeParams.SourceNodeID = "source-node-b"
	otherNode, err := callBridgeMirror(t, f, otherNodeParams)
	require.NoError(t, err)
	require.NotEqual(t, first.GiffordMessageID, otherNode.GiffordMessageID)
	require.NotEqual(t, first.GiffordThreadID, otherNode.GiffordThreadID,
		"a source context is namespaced by its source node")

	nullOneParams := firstParams
	nullOneParams.SourceMessageID = "73404"
	nullOneParams.SourceContextID = nil
	nullOne, err := callBridgeMirror(t, f, nullOneParams)
	require.NoError(t, err)
	nullTwoParams := nullOneParams
	nullTwoParams.SourceMessageID = "73405"
	nullTwo, err := callBridgeMirror(t, f, nullTwoParams)
	require.NoError(t, err)
	require.NotEqual(t, nullOne.GiffordThreadID, nullTwo.GiffordThreadID,
		"each null context receives its own thread")

	ordinary := testAdmitParams(f)
	ordinary.ClientMsgID = protocol.OpaqueID(firstParams.SourceMessageID)
	ordinary.ThreadID = "ordinary-same-client-id"
	answer, err := callAdmit(t, f, ordinary)
	require.NoError(t, err)
	require.NotEqual(t, first.GiffordMessageID, answer.(AdmitMessageResult).MessageID,
		"ordinary admission keeps its pre-bridge idempotency namespace")

	rows, err := f.store.DB().QueryContext(t.Context(), `SELECT source_message_id,
		message.thread_sequence FROM bridge_message_mappings AS mapping
		JOIN messages AS message ON message.message_id = mapping.gifford_message_id
		WHERE mapping.source_node_id = ? AND mapping.source_context_id = ?
		ORDER BY source_message_id`, testBridgeSource, *firstParams.SourceContextID)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()
	var sequences []int64
	for rows.Next() {
		var sourceID, sequence int64
		require.NoError(t, rows.Scan(&sourceID, &sequence))
		sequences = append(sequences, sequence)
	}
	require.NoError(t, rows.Err())
	require.Equal(t, []int64{1, 2}, sequences)
}

func TestBridgeMirrorConflictsAndAgentAdmission(t *testing.T) {
	f := newHubFixture(t)
	params := testBridgeMirrorParams()
	_, err := callBridgeMirror(t, f, params)
	require.NoError(t, err)

	drifted := params
	drifted.SourceCreatedAt = "2026-10-29T11:59:01Z"
	_, err = callBridgeMirror(t, f, drifted)
	requireBridgeDomainError(t, err, protocol.ErrorCodeControlVersionConflict)

	unknown := params
	unknown.SourceMessageID = "73403"
	unknown.FromAgent = testUnknownAgent
	_, err = callBridgeMirror(t, f, unknown)
	requireBridgeDomainError(t, err, protocol.ErrorCodeNotFound)

	_, err = f.store.DB().ExecContext(t.Context(), `UPDATE sessions SET state = 'DISCONNECTED',
		ended_at = ? WHERE agent_id = ?`, eventstore.FormatTime(f.clock.Now()), testHubRecipient)
	require.NoError(t, err)
	inactive := params
	inactive.SourceMessageID = "73404"
	_, err = callBridgeMirror(t, f, inactive)
	requireBridgeDomainError(t, err, protocol.ErrorCodeRecipientNotDeliverable)

	var mirrors int
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
		"SELECT count(*) FROM bridge_message_mappings").Scan(&mirrors))
	require.Equal(t, 1, mirrors, "refusals must have no partial effects")
}

func TestBridgeMirrorRejectsNonCanonicalProjection(t *testing.T) {
	f := newHubFixture(t)
	valid := testBridgeMirrorParams()
	tests := map[string]json.RawMessage{}

	leadingZero := valid
	leadingZero.SourceMessageID = "073402"
	tests["leading zero id"] = mustJSON(t, leadingZero)
	overflow := valid
	overflow.SourceMessageID = "9223372036854775808"
	tests["overflow id"] = mustJSON(t, overflow)
	nonUTC := valid
	nonUTC.SourceCreatedAt = "2026-10-29T11:59:00.123456+00:00"
	tests["noncanonical UTC"] = mustJSON(t, nonUTC)
	tests["missing nullable field"] = json.RawMessage(`{
		"source_node_id":"source-node-a","source_message_id":"73402",
		"from_agent":"agent-sender","to_agent":"agent-recipient",
		"source_created_at":"2026-10-29T11:59:00.123456Z"}`)
	tests["unknown field"] = json.RawMessage(`{
		"source_node_id":"source-node-a","source_message_id":"73402",
		"from_agent":"agent-sender","to_agent":"agent-recipient",
		"source_created_at":"2026-10-29T11:59:00.123456Z",
		"source_context_id":null,"priority":"URGENT"}`)
	for _, field := range []string{"author", "agent_id", "role", "uid"} {
		var object map[string]any
		require.NoError(t, json.Unmarshal(mustJSON(t, valid), &object))
		object[field] = "attacker-asserted-identity"
		tests["client asserted "+field] = mustJSON(t, object)
	}

	for name, raw := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := f.service.MirrorMessage(t.Context(), raw)
			requireBridgeInvalidError(t, err)
		})
	}
	var mirrors int
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
		"SELECT count(*) FROM bridge_message_mappings").Scan(&mirrors))
	require.Zero(t, mirrors)
}

func TestOrdinaryAgentDispatchDoesNotOfferBridgeMethods(t *testing.T) {
	f := newHubFixture(t)
	response := dispatch(t.Context(), testHubSender, 1234, f.service.Handlers(), rpcRequest{
		JSONRPC: jsonRPCVersion,
		Method:  MethodBridgeMirrorMessage,
		Params:  mustJSON(t, testBridgeMirrorParams()),
		ID:      json.RawMessage(`1`),
	})
	require.Nil(t, response.Result)
	require.NotNil(t, response.Error)
	require.Equal(t, codeMethodNotFound, response.Error.Code)
	var mirrors int
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
		"SELECT count(*) FROM bridge_message_mappings").Scan(&mirrors))
	require.Zero(t, mirrors, "a crossed method must have no effects")

	handlers := f.service.Handlers()
	require.NotNil(t, handlers.Bridge)
	interfaceType := reflect.TypeOf((*BridgeHandler)(nil)).Elem()
	require.Equal(t, 2, interfaceType.NumMethod())
	require.Equal(t, "MarkRead", interfaceType.Method(0).Name)
	require.Equal(t, "MirrorMessage", interfaceType.Method(1).Name)
}

func TestBridgeMarkReadAcknowledgesAndCancelsPendingNotice(t *testing.T) {
	f := newHubFixture(t)
	mirrorParams := testBridgeMirrorParams()
	mirrored, err := callBridgeMirror(t, f, mirrorParams)
	require.NoError(t, err)
	readParams := BridgeMarkReadParams{
		SourceNodeID: testBridgeSource, SourceMessageID: mirrorParams.SourceMessageID,
		RecipientAgent: testHubRecipient, ReadAt: testBridgeRead,
	}

	read, err := callBridgeRead(t, f, readParams)
	require.NoError(t, err)
	require.Equal(t, mirrored.GiffordMessageID, read.GiffordMessageID)
	require.Equal(t, AckMessageAcknowledged, read.Status)
	require.Equal(t, testBridgeRead, read.ReadAt)
	var sourceReadAt, ackedAt, queueState string
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(), `SELECT source_read_at
		FROM bridge_message_mappings WHERE source_node_id = ? AND source_message_id = 73402`,
		testBridgeSource).Scan(&sourceReadAt))
	require.Equal(t, testBridgeRead, sourceReadAt)
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(), `SELECT acked_at FROM acks
		WHERE message_id = ? AND agent_id = ? AND delivery_generation = 1
		AND status = 'ACKNOWLEDGED'`, mirrored.GiffordMessageID, testHubRecipient).Scan(&ackedAt))
	parsedRead, err := time.Parse(time.RFC3339Nano, testBridgeRead)
	require.NoError(t, err)
	require.Equal(t, eventstore.FormatTime(parsedRead), ackedAt)
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(), `SELECT state FROM delivery_queue
		WHERE message_id = ? AND agent_id = ? AND delivery_generation = 1`,
		mirrored.GiffordMessageID, testHubRecipient).Scan(&queueState))
	require.Equal(t, "CANCELLED", queueState)

	retry, err := callBridgeRead(t, f, readParams)
	require.NoError(t, err)
	require.Equal(t, read, retry)
	var acks int
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(), `SELECT count(*) FROM acks
		WHERE message_id = ? AND agent_id = ? AND status = 'ACKNOWLEDGED'`,
		mirrored.GiffordMessageID, testHubRecipient).Scan(&acks))
	require.Equal(t, 1, acks)

	drifted := readParams
	drifted.ReadAt = "2026-10-29T12:00:06Z"
	_, err = callBridgeRead(t, f, drifted)
	requireBridgeDomainError(t, err, protocol.ErrorCodeControlVersionConflict)
	wrongRecipient := readParams
	wrongRecipient.RecipientAgent = testHubSender
	_, err = callBridgeRead(t, f, wrongRecipient)
	requireBridgeDomainError(t, err, protocol.ErrorCodeControlVersionConflict)
}

func TestBridgeMarkReadRequiresMirrorAndPreservesDeliveredNotice(t *testing.T) {
	f := newHubFixture(t)
	params := BridgeMarkReadParams{
		SourceNodeID: testBridgeSource, SourceMessageID: "73402",
		RecipientAgent: testHubRecipient, ReadAt: testBridgeRead,
	}
	_, err := callBridgeRead(t, f, params)
	requireBridgeDomainError(t, err, protocol.ErrorCodeNotFound)

	mirrored, err := callBridgeMirror(t, f, testBridgeMirrorParams())
	require.NoError(t, err)
	_, err = f.store.DB().ExecContext(t.Context(), `UPDATE delivery_queue SET state = 'DELIVERED'
		WHERE message_id = ? AND agent_id = ?`, mirrored.GiffordMessageID, testHubRecipient)
	require.NoError(t, err)
	_, err = callBridgeRead(t, f, params)
	require.NoError(t, err)
	var state string
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(), `SELECT state FROM delivery_queue
		WHERE message_id = ? AND agent_id = ?`, mirrored.GiffordMessageID, testHubRecipient).Scan(&state))
	require.Equal(t, "DELIVERED", state)
}

func TestBridgeAcknowledgementIsScopedToOneRecipient(t *testing.T) {
	f := newHubFixture(t)
	otherRecipient := protocol.AgentID("agent-third")
	insertHubAgent(t, f.store, otherRecipient)

	firstParams := testBridgeMirrorParams()
	first, err := callBridgeMirror(t, f, firstParams)
	require.NoError(t, err)
	secondParams := firstParams
	secondParams.SourceMessageID = "73403"
	secondParams.ToAgent = otherRecipient
	second, err := callBridgeMirror(t, f, secondParams)
	require.NoError(t, err)
	require.Equal(t, first.GiffordThreadID, second.GiffordThreadID)

	_, err = callBridgeRead(t, f, BridgeMarkReadParams{
		SourceNodeID: testBridgeSource, SourceMessageID: firstParams.SourceMessageID,
		RecipientAgent: testHubRecipient, ReadAt: testBridgeRead,
	})
	require.NoError(t, err)
	var firstState, secondState string
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(), `SELECT state
		FROM delivery_queue WHERE message_id = ? AND agent_id = ?`,
		first.GiffordMessageID, testHubRecipient).Scan(&firstState))
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(), `SELECT state
		FROM delivery_queue WHERE message_id = ? AND agent_id = ?`,
		second.GiffordMessageID, otherRecipient).Scan(&secondState))
	require.Equal(t, "CANCELLED", firstState)
	require.Equal(t, "QUEUED", secondState)
	var otherAcks int
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(), `SELECT count(*) FROM acks
		WHERE message_id = ? OR agent_id = ?`, second.GiffordMessageID, otherRecipient).Scan(&otherAcks))
	require.Zero(t, otherAcks, "reading A must not acknowledge B")
}

func TestBridgeMessagesStayOutOfOrdinaryContentRPCs(t *testing.T) {
	f := newHubFixture(t)
	mirrored, err := callBridgeMirror(t, f, testBridgeMirrorParams())
	require.NoError(t, err)

	pending, err := f.service.ListPendingMessages(t.Context(), testHubRecipient, json.RawMessage(`{}`))
	require.NoError(t, err)
	require.Empty(t, pending.(ListPendingMessagesResult).Messages)
	claimRaw := mustJSON(t, ClaimMessageParams{
		MessageID: mirrored.GiffordMessageID, DeliveryGeneration: 1,
	})
	_, err = f.service.ClaimMessage(t.Context(), testHubRecipient, claimRaw)
	var callErr *callError
	require.ErrorAs(t, err, &callErr)
	require.Equal(t, protocol.ErrorCodeNotFound, callErr.domain)
	statusRaw := mustJSON(t, GetDeliveryStatusParams{MessageID: mirrored.GiffordMessageID})
	_, err = f.service.GetDeliveryStatus(t.Context(), testHubSender, statusRaw)
	require.ErrorAs(t, err, &callErr)
	require.Equal(t, protocol.ErrorCodeNotFound, callErr.domain)
	ackRaw := mustJSON(t, AckMessageParams{
		MessageID: mirrored.GiffordMessageID, DeliveryGeneration: 1,
		Status: AckMessageAcknowledged,
	})
	_, err = f.service.AckMessage(t.Context(), testHubRecipient, ackRaw)
	require.ErrorAs(t, err, &callErr)
	require.Equal(t, protocol.ErrorCodeNotFound, callErr.domain)
}

func TestBridgeMirrorConcurrentRetryHasOneCreator(t *testing.T) {
	f := newHubFixture(t)
	raw := mustJSON(t, testBridgeMirrorParams())
	start := make(chan struct{})
	results := make([]BridgeMirrorMessageResult, 2)
	errs := make([]error, 2)
	var wait sync.WaitGroup
	for index := range results {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			answer, err := f.service.MirrorMessage(t.Context(), raw)
			errs[index] = err
			if err == nil {
				results[index] = answer.(BridgeMirrorMessageResult)
			}
		}()
	}
	close(start)
	wait.Wait()
	require.NoError(t, errors.Join(errs...))
	require.Equal(t, results[0].GiffordMessageID, results[1].GiffordMessageID)
	require.Equal(t, results[0].GiffordThreadID, results[1].GiffordThreadID)
	require.ElementsMatch(t,
		[]BridgeDisposition{BridgeDispositionCreated, BridgeDispositionDuplicate},
		[]BridgeDisposition{results[0].Disposition, results[1].Disposition})
}

func mustJSON(t *testing.T, value any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(value)
	require.NoError(t, err)
	return raw
}
