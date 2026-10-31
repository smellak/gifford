package triage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/analyst"
	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/gateway"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/redact"
	"github.com/smellak/gifford/internal/redact/redacttest"
)

// All messages below are invented.
const (
	sender      protocol.AgentID = "agent-a"
	recipientB  protocol.AgentID = "agent-b"
	recipientC  protocol.AgentID = "agent-c"
	threadRef                    = "thr_01JB0000000000000000000010"
	routeDigest                  = "sha256:0000000000000000000000000000000000000000000000000000000000000000"
	queued                       = "QUEUED"
	nodeA                        = "node-a"
	nodeB                        = "node-b"
	call1                        = "call-1"
	call7                        = "call-7"
)

var t0 = time.Date(2026, 10, 31, 3, 0, 0, 0, time.UTC)

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

func testStore(t *testing.T) *eventstore.Store {
	t.Helper()
	store, err := eventstore.Open(t.Context(), filepath.Join(t.TempDir(), "gifford.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func msgID(n int) protocol.ULID { return protocol.ULID(fmt.Sprintf("01JB00000000000000000000%02d", n)) }

// message is one message to seed.
type message struct {
	id          protocol.ULID
	subject     string
	body        string
	sensitivity protocol.MessageSensitivity
	profile     protocol.EncryptionProfile // of the envelope row; empty: none
	envelope    []byte                     // replaces the encoded envelope
	recipients  []protocol.AgentID
	state       string    // of the queue rows; empty: QUEUED
	expiresAt   time.Time // zero: an hour after t0
}

// seed stores a message, its profile-none envelope and one queue row per
// recipient. Plain inserts leave every control triple at its HELD default.
func seed(t *testing.T, db *sql.DB, m message) {
	t.Helper()
	m = withDefaults(m)
	created := t0.Add(-time.Minute)
	client := "cli-" + string(m.id)
	if m.envelope == nil {
		m.envelope = encodeEnvelope(t, m, created, client)
	}
	stamp, expires := eventstore.FormatTime(created), eventstore.FormatTime(m.expiresAt)
	_, err := db.ExecContext(t.Context(), `INSERT INTO messages
		(message_id, client_msg_id, origin_node, from_agent, destination_node,
		 thread_ref, priority, state, created_at, expires_at, updated_at,
		 hop_count, route_digest)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		string(m.id), client, nodeA, string(sender), nodeB, threadRef,
		"NORMAL", "PENDING", stamp, expires, stamp, 0, routeDigest)
	require.NoError(t, err)
	recipients, err := json.Marshal(m.recipients)
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), `INSERT INTO message_envelopes
		(message_id, envelope, protocol, version, client_msg_id, origin_node,
		 origin_agent, destination_node, recipient_agents_json, created_at,
		 expires_at, priority, thread_ref, encryption_profile,
		 sender_signing_key_id, ciphertext_sha256, stored_at, hop_count,
		 route_digest, wire_control_version)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		string(m.id), m.envelope, protocol.WireProtocol, protocol.WireVersion, client, nodeA,
		string(sender), nodeB, string(recipients), stamp, expires, "NORMAL", threadRef,
		string(m.profile), "ed25519:agent-a:v1", "sha256:ciphertext", stamp, 0, routeDigest, 0)
	require.NoError(t, err)
	for _, r := range m.recipients {
		_, err = db.ExecContext(t.Context(), `INSERT INTO delivery_queue
			(message_id, agent_id, from_agent, thread_ref, state, priority,
			 created_at, expires_at, updated_at, hop_count, route_digest)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			string(m.id), string(r), string(sender), threadRef, m.state, 2,
			stamp, expires, stamp, 0, routeDigest)
		require.NoError(t, err)
	}
}

func withDefaults(m message) message {
	if m.subject == "" {
		m.subject = "parser review"
	}
	if m.body == "" {
		m.body = "please review the parser change"
	}
	if m.sensitivity == "" {
		m.sensitivity = protocol.MessageSensitivityInternal
	}
	if m.profile == "" {
		m.profile = protocol.EncryptionProfileNone
	}
	if len(m.recipients) == 0 {
		m.recipients = []protocol.AgentID{recipientB}
	}
	if m.state == "" {
		m.state = queued
	}
	if m.expiresAt.IsZero() {
		m.expiresAt = t0.Add(time.Hour)
	}
	return m
}

// encodeEnvelope is the profile-none envelope of m, with a placeholder
// signature: triage reads the envelope, it does not verify it.
func encodeEnvelope(t *testing.T, m message, created time.Time, client string) []byte {
	t.Helper()
	logical := protocol.LogicalMessage{
		MessageID: m.id, ClientMsgID: protocol.OpaqueID(client), ThreadID: "thread-1",
		FromAgent: sender, ToAgents: m.recipients, OriginNode: nodeA, DestinationNode: nodeB,
		Subject: m.subject, Body: m.body, Priority: protocol.PriorityNormal,
		DeliveryMode: protocol.DeliveryModeWhenSafe, CreatedAt: created, ExpiresAt: m.expiresAt,
		Sensitivity: m.sensitivity, ThreadSequence: 1, RouteDigest: routeDigest,
	}
	require.NoError(t, logical.Validate())
	plain, err := json.Marshal(logical)
	require.NoError(t, err)
	sum := sha256.Sum256(plain)
	wire := protocol.WireEnvelope{
		Protocol: protocol.WireProtocol, Version: protocol.WireVersion, MessageID: m.id,
		ClientMsgID: protocol.OpaqueID(client), OriginNode: nodeA, OriginAgent: sender,
		DestinationNode: nodeB, RecipientAgents: m.recipients, CreatedAt: created, ExpiresAt: m.expiresAt,
		Priority: protocol.PriorityNormal, ThreadRef: threadRef, RouteDigest: routeDigest,
		ContentType: protocol.WireContentType, EncryptionProfile: protocol.EncryptionProfileNone,
		SenderSigningKeyID: "ed25519:agent-a:v1", RecipientKeyEnvelopes: []protocol.RecipientKeyEnvelope{},
		Ciphertext: plain, CiphertextSHA256: protocol.SHA256Hex(hex.EncodeToString(sum[:])),
		Signature: make([]byte, 64),
	}
	require.NoError(t, wire.Validate())
	out, err := json.Marshal(wire)
	require.NoError(t, err)
	return out
}

// fakeChannel answers every evaluation alike, reading through the
// caller's transaction as a real evaluation would.
type fakeChannel struct {
	mu     sync.Mutex
	allow  bool
	err    error
	asked  []Delivery
	onAsk  func(Delivery)
	readTx bool // the transaction could be read
}

func (c *fakeChannel) AllowsNow(ctx context.Context, tx *sql.Tx, d Delivery) (bool, error) {
	var n int
	err := tx.QueryRowContext(ctx, `SELECT count(*) FROM delivery_queue WHERE message_id = ?`, string(d.MessageID)).Scan(&n)
	c.mu.Lock()
	c.asked = append(c.asked, d)
	c.readTx = err == nil && n > 0
	allow, cerr, onAsk := c.allow, c.err, c.onAsk
	c.mu.Unlock()
	if onAsk != nil {
		onAsk(d)
	}
	return allow, cerr
}

func (c *fakeChannel) calls() []Delivery {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]Delivery(nil), c.asked...)
}

// fakeModel answers every call alike and keeps the requests. During each
// call it notes how many database connections are in use.
type fakeModel struct {
	mu      sync.Mutex
	db      *sql.DB
	reqs    []protocol.InferRequest
	inUse   []int
	res     protocol.InferResult
	err     error
	entered chan struct{} // when set, signalled on each call
	release chan struct{} // when set, each call waits for it
	cancel  func()        // when set, called during each call
}

func (m *fakeModel) Infer(_ context.Context, req protocol.InferRequest) (protocol.InferResult, error) {
	m.mu.Lock()
	m.reqs = append(m.reqs, req)
	if m.db != nil {
		m.inUse = append(m.inUse, m.db.Stats().InUse)
	}
	res, err, entered, release, cancel := m.res, m.err, m.entered, m.release, m.cancel
	m.mu.Unlock()
	if entered != nil {
		entered <- struct{}{}
	}
	if release != nil {
		<-release
	}
	if cancel != nil {
		cancel()
	}
	return res, err
}

func (m *fakeModel) calls() []protocol.InferRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]protocol.InferRequest(nil), m.reqs...)
}

func okTriage(label protocol.TriageLabel) protocol.InferResult {
	return protocol.InferResult{
		Contract: protocol.ModelContractTriageMessage, InferenceProfile: protocol.InferenceProfileBase,
		Label: string(label), Probabilities: map[string]float64{string(label): 1},
		CallRecord: protocol.GlimmerCallRecord{CallID: call1, Status: protocol.GlimmerCallStatusOK},
	}
}

func newTriager(t *testing.T, db *sql.DB, model Inferrer, ch Channel) *Triager {
	t.Helper()
	tr, err := New(db, model, ch, fixedClock{t0})
	require.NoError(t, err)
	return tr
}

func delivery(id protocol.ULID, agent protocol.AgentID) Delivery {
	return Delivery{MessageID: id, AgentID: agent, DeliveryGeneration: 1}
}

func verdictCount(t *testing.T, db *sql.DB) int {
	t.Helper()
	var n int
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT count(*) FROM message_triage`).Scan(&n))
	return n
}

// payloadText is the text a triage request carries.
func payloadText(t *testing.T, req protocol.InferRequest) string {
	t.Helper()
	var in struct {
		Text string `json:"text"`
	}
	dec := json.NewDecoder(bytes.NewReader(req.Payload))
	dec.DisallowUnknownFields()
	require.NoError(t, dec.Decode(&in))
	return in.Text
}

func TestTriageClassifiesADeliverableMessage(t *testing.T) {
	t.Parallel()
	db := testStore(t).DB()
	seed(t, db, message{id: msgID(1)})
	model := &fakeModel{db: db, res: okTriage(protocol.TriageLabelConflictOrUrgent)}
	ch := &fakeChannel{allow: true}

	v, ok, err := newTriager(t, db, model, ch).Triage(t.Context(), delivery(msgID(1), recipientB))
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, Verdict{
		MessageID: msgID(1), Label: protocol.TriageLabelConflictOrUrgent, Outcome: OutcomeClassified,
		GlimmerCallID: call1, CreatedAt: t0,
	}, v)
	require.True(t, v.Classified())
	stored, found, err := Lookup(t.Context(), db, msgID(1))
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, v, stored)

	require.Equal(t, []Delivery{delivery(msgID(1), recipientB)}, ch.calls())
	require.True(t, ch.readTx, "the channel reads through the caller's transaction")
	reqs := model.calls()
	require.Len(t, reqs, 1)
	require.Equal(t, protocol.ModelContractTriageMessage, reqs[0].Contract)
	require.Empty(t, reqs[0].AgentID, "the message is triaged, not an agent")
	require.Equal(t, "Subject: parser review\n\nplease review the parser change", payloadText(t, reqs[0]))
	require.Equal(t, []int{0}, model.inUse, "no connection, so no transaction, is held during the call")
}

// The fresh evaluation decides, never the stored triple: every seeded row
// keeps its HELD default. This proves the interface contract only; the
// real channel's evaluation comes with its adapter.
func TestTriageAsksTheChannelNotTheStoredTriple(t *testing.T) {
	t.Parallel()
	db := testStore(t).DB()
	seed(t, db, message{id: msgID(1)})
	var state string
	require.NoError(t, db.QueryRowContext(t.Context(),
		`SELECT control_state FROM delivery_queue WHERE message_id = ?`, string(msgID(1))).Scan(&state))
	require.Equal(t, "HELD", state)

	model := &fakeModel{res: okTriage(protocol.TriageLabelNormal)}
	ch := &fakeChannel{allow: false}
	tr := newTriager(t, db, model, ch)
	v, ok, err := tr.Triage(t.Context(), delivery(msgID(1), recipientB))
	require.NoError(t, err)
	require.False(t, ok, "a channel that holds or is off now")
	require.Equal(t, Verdict{}, v)
	require.Empty(t, model.calls())
	require.Zero(t, verdictCount(t, db))

	ch.mu.Lock()
	ch.allow = true
	ch.mu.Unlock()
	v, ok, err = tr.Triage(t.Context(), delivery(msgID(1), recipientB))
	require.NoError(t, err)
	require.True(t, ok, "a channel that allows now, whatever the stored triple says")
	require.Equal(t, protocol.TriageLabelNormal, v.Label)
	require.Len(t, model.calls(), 1)
}

func TestTriageSkipsADeliveryThatCannotGoNow(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		msg     message
		d       Delivery
		prepare func(t *testing.T, store *eventstore.Store)
	}{
		"claimed":            {msg: message{id: msgID(1), state: "CLAIMED"}, d: delivery(msgID(1), recipientB)},
		"another generation": {msg: message{id: msgID(1)}, d: Delivery{MessageID: msgID(1), AgentID: recipientB, DeliveryGeneration: 2}},
		"another recipient":  {msg: message{id: msgID(1)}, d: delivery(msgID(1), recipientC)},
		"expired now":        {msg: message{id: msgID(1), expiresAt: t0}, d: delivery(msgID(1), recipientB)},
		"no such message":    {msg: message{id: msgID(1)}, d: delivery(msgID(2), recipientB)},
		"a pending orchestrator approval": {
			msg: message{id: msgID(1)}, d: delivery(msgID(1), recipientB),
			prepare: func(t *testing.T, store *eventstore.Store) {
				t.Helper()
				pendingApproval(t, store, msgID(1), recipientB)
			},
		},
		"an unresolved UNCERTAIN attempt of the agent": {
			msg: message{id: msgID(1)}, d: delivery(msgID(1), recipientB),
			prepare: func(t *testing.T, store *eventstore.Store) {
				t.Helper()
				seed(t, store.DB(), message{id: msgID(9)})
				uncertainAttempt(t, store.DB(), msgID(9), recipientB, nil)
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			store := testStore(t)
			seed(t, store.DB(), tc.msg)
			if tc.prepare != nil {
				tc.prepare(t, store)
			}
			model := &fakeModel{res: okTriage(protocol.TriageLabelNormal)}
			ch := &fakeChannel{allow: true}
			v, ok, err := newTriager(t, store.DB(), model, ch).Triage(t.Context(), tc.d)
			require.NoError(t, err)
			require.False(t, ok)
			require.Equal(t, Verdict{}, v)
			require.Empty(t, model.calls(), "nothing is sent")
			require.Empty(t, ch.calls(), "the channel is asked only about a delivery that could go")
			require.Zero(t, verdictCount(t, store.DB()), "nothing is written")
		})
	}
}

// pendingApproval opens an orchestrator approval through internal/channel,
// the only writer of approval_requests.
func pendingApproval(t *testing.T, store *eventstore.Store, id protocol.ULID, agent protocol.AgentID) protocol.ApprovalRequest {
	t.Helper()
	engine, err := channel.New(channel.Options{Clock: clock.System{}})
	require.NoError(t, err)
	controller, err := channel.NewController(channel.ControllerOptions{
		DB: store.DB(), Engine: engine, Clock: clock.System{}, NodeID: nodeA,
		Entropy: bytes.NewReader(make([]byte, 1024)),
	})
	require.NoError(t, err)
	tx, err := store.DB().BeginTx(t.Context(), nil)
	require.NoError(t, err)
	request, created, err := controller.EnsureOrchestratorApproval(t.Context(), tx, id, agent, "automatic delivery deferred")
	require.NoError(t, err)
	require.True(t, created)
	require.NoError(t, tx.Commit())
	return request
}

// uncertainAttempt records an UNCERTAIN delivery attempt with no
// resolution for agent.
func uncertainAttempt(t *testing.T, db *sql.DB, id protocol.ULID, agent protocol.AgentID, resolution any) {
	t.Helper()
	stamp := eventstore.FormatTime(t0)
	_, err := db.ExecContext(t.Context(), `INSERT INTO delivery_attempts
		(delivery_id, message_id, agent_id, delivery_generation, adapter_session_id,
		 orchestrator_decision_id, channel_decision_id, draft_generation,
		 input_guard_version, control_versions_json, state, created_at, updated_at,
		 resolution_approval_id)
		VALUES (?, ?, ?, 1, ?, ?, ?, 0, 0, ?, 'UNCERTAIN', ?, ?, ?)`,
		"dlv-"+string(id), string(id), string(agent), "sess-1", "dec-1", "chd-1", "{}", stamp, stamp, resolution)
	require.NoError(t, err)
}

// An UNCERTAIN attempt with a resolution, or another agent's, does not
// hold the delivery back.
func TestTriageIgnoresResolvedOrOtherUncertainAttempts(t *testing.T) {
	t.Parallel()
	for name, prepare := range map[string]func(t *testing.T, store *eventstore.Store){
		"resolved": func(t *testing.T, store *eventstore.Store) {
			t.Helper()
			seed(t, store.DB(), message{id: msgID(9)})
			resolution := pendingApproval(t, store, msgID(9), recipientB)
			uncertainAttempt(t, store.DB(), msgID(9), recipientB, string(resolution.ApprovalID))
		},
		"another agent's": func(t *testing.T, store *eventstore.Store) {
			t.Helper()
			seed(t, store.DB(), message{id: msgID(9), recipients: []protocol.AgentID{recipientC}})
			uncertainAttempt(t, store.DB(), msgID(9), recipientC, nil)
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			store := testStore(t)
			seed(t, store.DB(), message{id: msgID(1)})
			prepare(t, store)
			model := &fakeModel{res: okTriage(protocol.TriageLabelNormal)}
			_, ok, err := newTriager(t, store.DB(), model, &fakeChannel{allow: true}).
				Triage(t.Context(), delivery(msgID(1), recipientB))
			require.NoError(t, err)
			require.True(t, ok)
			require.Len(t, model.calls(), 1)
		})
	}
}

// A missing or failing evaluation fails closed: no call, no verdict.
func TestTriageFailsClosedWithoutTheChannel(t *testing.T) {
	t.Parallel()
	db := testStore(t).DB()
	seed(t, db, message{id: msgID(1)})
	model := &fakeModel{res: okTriage(protocol.TriageLabelNormal)}
	sentinel := errors.New("channel unavailable")
	v, ok, err := newTriager(t, db, model, &fakeChannel{allow: true, err: sentinel}).
		Triage(t.Context(), delivery(msgID(1), recipientB))
	require.ErrorIs(t, err, sentinel)
	require.False(t, ok)
	require.Equal(t, Verdict{}, v)
	require.Empty(t, model.calls())
	require.Zero(t, verdictCount(t, db))

	for name, deps := range map[string]struct {
		db    *sql.DB
		model Inferrer
		ch    Channel
		clk   clock.Clock
	}{
		"no database": {nil, model, &fakeChannel{}, fixedClock{t0}},
		"no model":    {db, nil, &fakeChannel{}, fixedClock{t0}},
		"no channel":  {db, model, nil, fixedClock{t0}},
		"no clock":    {db, model, &fakeChannel{}, nil},
	} {
		_, err := New(deps.db, deps.model, deps.ch, deps.clk)
		require.ErrorIs(t, err, ErrInvalidConfig, name)
	}
}

// What must not leave the node is never sent: it gets its verdict, and no
// call is made.
func TestTriageNeverSendsASealedMessage(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		msg  message
		want Outcome
	}{
		"end-to-end encrypted": {message{id: msgID(1), profile: protocol.EncryptionProfileGiffordE2EV1}, OutcomeEncrypted},
		"confidential":         {message{id: msgID(1), sensitivity: protocol.MessageSensitivityConfidential}, OutcomeConfidential},
		"unreadable envelope":  {message{id: msgID(1), envelope: []byte(`{"protocol":"gifford-message"}`)}, OutcomeUnreadable},
		"an envelope of another message": {
			message{id: msgID(1), envelope: renamed(t, envelopeOf(t, msgID(1)), msgID(2))},
			OutcomeUnreadable,
		},
		"another message inside the envelope": {
			message{id: msgID(1), envelope: renamed(t, envelopeOf(t, msgID(2)), msgID(1))},
			OutcomeUnreadable,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			db := testStore(t).DB()
			seed(t, db, tc.msg)
			model := &fakeModel{res: okTriage(protocol.TriageLabelNormal)}
			v, ok, err := newTriager(t, db, model, &fakeChannel{allow: true}).
				Triage(t.Context(), delivery(msgID(1), recipientB))
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, Verdict{MessageID: msgID(1), Outcome: tc.want, CreatedAt: t0}, v)
			require.False(t, v.Classified())
			require.Empty(t, model.calls(), "nothing is sent")
		})
	}
}

// envelopeOf is the default envelope of message id.
func envelopeOf(t *testing.T, id protocol.ULID) []byte {
	t.Helper()
	return encodeEnvelope(t, withDefaults(message{id: id}), t0.Add(-time.Minute), "cli-"+string(id))
}

// renamed is an envelope whose header names message id instead, the
// message inside left as it was.
func renamed(t *testing.T, raw []byte, id protocol.ULID) []byte {
	t.Helper()
	var doc map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &doc))
	doc["message_id"] = json.RawMessage(`"` + string(id) + `"`)
	out, err := json.Marshal(doc)
	require.NoError(t, err)
	return out
}

// A failed call leaves an unclassified verdict with its call, and the
// message is not asked about again.
func TestTriageKeepsAFailedCall(t *testing.T) {
	t.Parallel()
	failed := func(status protocol.GlimmerCallStatus) protocol.InferResult {
		return protocol.InferResult{
			Contract:   protocol.ModelContractTriageMessage,
			CallRecord: protocol.GlimmerCallRecord{CallID: call7, Status: status},
		}
	}
	for name, tc := range map[string]struct {
		res  protocol.InferResult
		err  error
		want Outcome
		call protocol.OpaqueID
	}{
		"timeout":           {failed(protocol.GlimmerCallStatusTimeout), gateway.ErrTimeout, OutcomeTimeout, call7},
		"invalid answer":    {failed(protocol.GlimmerCallStatusInvalid), gateway.ErrInvalidOutput, OutcomeInvalid, call7},
		"open breaker":      {failed(protocol.GlimmerCallStatusError), gateway.ErrCircuitOpen, OutcomeCircuitOpen, call7},
		"redaction failed":  {failed(protocol.GlimmerCallStatusRedactionFailed), gateway.ErrRedactionFailed, OutcomeRedactionFailed, call7},
		"call failed":       {failed(protocol.GlimmerCallStatusError), gateway.ErrCallFailed, OutcomeError, call7},
		"record not stored": {protocol.InferResult{}, gateway.ErrRecordFailed, OutcomeError, ""},
		"a label outside the contract": {
			protocol.InferResult{Label: "BACKGROUND_WORK", CallRecord: protocol.GlimmerCallRecord{CallID: call7}},
			nil, OutcomeInvalid, call7,
		},
		"a label without a call": {okTriage(protocol.TriageLabelNormal), nil, OutcomeInvalid, ""},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if name == "a label without a call" {
				tc.res.CallRecord.CallID = ""
			}
			db := testStore(t).DB()
			seed(t, db, message{id: msgID(1)})
			model := &fakeModel{res: tc.res, err: tc.err}
			tr := newTriager(t, db, model, &fakeChannel{allow: true})
			v, ok, err := tr.Triage(t.Context(), delivery(msgID(1), recipientB))
			require.NoError(t, err, "a failed call is a verdict, not an error")
			require.True(t, ok)
			require.Equal(t, Verdict{MessageID: msgID(1), Outcome: tc.want, GlimmerCallID: tc.call, CreatedAt: t0}, v)

			again, ok, err := tr.Triage(t.Context(), delivery(msgID(1), recipientB))
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, v, again)
			require.Len(t, model.calls(), 1, "no retry")
		})
	}
}

// A call the caller cancels leaves no verdict, so a later triage asks
// again.
func TestTriageCancelledCallLeavesNoVerdict(t *testing.T) {
	t.Parallel()
	db := testStore(t).DB()
	seed(t, db, message{id: msgID(1)})
	ctx, cancel := context.WithCancel(t.Context())
	model := &fakeModel{err: gateway.ErrCallFailed, cancel: cancel}
	tr := newTriager(t, db, model, &fakeChannel{allow: true})
	_, ok, err := tr.Triage(ctx, delivery(msgID(1), recipientB))
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, ok)
	require.Zero(t, verdictCount(t, db))

	model.mu.Lock()
	model.cancel, model.err, model.res = nil, nil, okTriage(protocol.TriageLabelLowPriority)
	model.mu.Unlock()
	v, ok, err := tr.Triage(t.Context(), delivery(msgID(1), recipientB))
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, protocol.TriageLabelLowPriority, v.Label)
	require.Len(t, model.calls(), 2)
}

// A verdict the caller stops waiting for is still stored.
func TestTriageStoresAVerdictTheCallerLeft(t *testing.T) {
	t.Parallel()
	db := testStore(t).DB()
	seed(t, db, message{id: msgID(1)})
	ctx, cancel := context.WithCancel(t.Context())
	model := &fakeModel{res: okTriage(protocol.TriageLabelNormal), cancel: cancel}
	v, ok, err := newTriager(t, db, model, &fakeChannel{allow: true}).Triage(ctx, delivery(msgID(1), recipientB))
	require.NoError(t, err)
	require.True(t, ok)
	stored, found, err := Lookup(t.Context(), db, msgID(1))
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, v, stored)
}

// Two recipients of one message, triaged at once, share one call.
func TestTriageOneCallPerMessage(t *testing.T) {
	t.Parallel()
	db := testStore(t).DB()
	seed(t, db, message{id: msgID(1), recipients: []protocol.AgentID{recipientB, recipientC}})
	model := &fakeModel{
		res: okTriage(protocol.TriageLabelNormal), entered: make(chan struct{}, 2), release: make(chan struct{}),
	}
	secondChecked := make(chan struct{})
	ch := &fakeChannel{allow: true, onAsk: func(d Delivery) {
		if d.AgentID == recipientC {
			close(secondChecked)
		}
	}}
	tr := newTriager(t, db, model, ch)

	type outcome struct {
		v   Verdict
		ok  bool
		err error
	}
	results := make(chan outcome, 2)
	run := func(agent protocol.AgentID) {
		v, ok, err := tr.Triage(t.Context(), delivery(msgID(1), agent))
		results <- outcome{v, ok, err}
	}
	go run(recipientB)
	<-model.entered
	go run(recipientC)
	<-secondChecked
	waitForWaiter(t, tr, msgID(1))
	close(model.release)
	first, second := <-results, <-results
	require.NoError(t, first.err)
	require.NoError(t, second.err)
	require.True(t, first.ok)
	require.True(t, second.ok)
	require.Equal(t, first.v, second.v)
	require.Len(t, model.calls(), 1, "one call for both recipients")
	require.Equal(t, 1, verdictCount(t, db))
}

// waitForWaiter returns once a triage waits for the flight of message id.
func waitForWaiter(t *testing.T, tr *Triager, id protocol.ULID) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		tr.mu.Lock()
		f := tr.flights[id]
		waiting := f != nil && f.waiters > 0
		tr.mu.Unlock()
		if waiting {
			return
		}
		require.True(t, time.Now().Before(deadline), "no triage waits for the flight")
		time.Sleep(time.Millisecond)
	}
}

// A flight that starts just after another one stored its verdict reads
// that verdict instead of calling again.
func TestTriageLateFlightReadsTheStoredVerdict(t *testing.T) {
	t.Parallel()
	db := testStore(t).DB()
	seed(t, db, message{id: msgID(1)})
	insertVerdict(t, db, msgID(1), "NORMAL", "classified", "call-3")
	model := &fakeModel{res: okTriage(protocol.TriageLabelLowPriority)}
	tr := newTriager(t, db, model, &fakeChannel{allow: true})
	v, err := tr.once(t.Context(), msgID(1), envelope{profile: protocol.EncryptionProfileNone, raw: envelopeOf(t, msgID(1))})
	require.NoError(t, err)
	require.Equal(t, protocol.TriageLabelNormal, v.Label)
	require.Empty(t, model.calls())
}

// A stored verdict is returned as it is, without asking anyone.
func TestTriageReturnsAStoredVerdict(t *testing.T) {
	t.Parallel()
	db := testStore(t).DB()
	seed(t, db, message{id: msgID(1), state: "DELIVERED"})
	insertVerdict(t, db, msgID(1), "REQUIRES_HUMAN", "classified", "call-3")
	model := &fakeModel{}
	ch := &fakeChannel{}
	v, ok, err := newTriager(t, db, model, ch).Triage(t.Context(), delivery(msgID(1), recipientB))
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, Verdict{
		MessageID: msgID(1), Label: protocol.TriageLabelRequiresHuman, Outcome: OutcomeClassified,
		GlimmerCallID: "call-3", CreatedAt: t0,
	}, v)
	require.Empty(t, model.calls())
	require.Empty(t, ch.calls())

	tx, err := db.BeginTx(t.Context(), &sql.TxOptions{ReadOnly: true})
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	inTx, found, err := Lookup(t.Context(), tx, msgID(1))
	require.NoError(t, err)
	require.True(t, found, "the Orchestrator reads it in its own transaction")
	require.Equal(t, v, inTx)
	_, found, err = Lookup(t.Context(), tx, msgID(2))
	require.NoError(t, err)
	require.False(t, found)
}

func insertVerdict(t *testing.T, db *sql.DB, id protocol.ULID, label, outcome, call any) {
	t.Helper()
	_, err := db.ExecContext(t.Context(), `INSERT INTO message_triage
		(message_id, label, outcome, glimmer_call_id, created_at) VALUES (?, ?, ?, ?, ?)`,
		string(id), label, outcome, call, eventstore.FormatTime(t0))
	require.NoError(t, err)
}

// The text is the subject and the body, cut to maxTextRunes runes.
func TestTriageCutsTheText(t *testing.T) {
	t.Parallel()
	db := testStore(t).DB()
	body := strings.Repeat("é", maxTextRunes)
	seed(t, db, message{id: msgID(1), body: body})
	model := &fakeModel{res: okTriage(protocol.TriageLabelNormal)}
	_, _, err := newTriager(t, db, model, &fakeChannel{allow: true}).Triage(t.Context(), delivery(msgID(1), recipientB))
	require.NoError(t, err)
	text := payloadText(t, model.calls()[0])
	require.Equal(t, maxTextRunes, utf8.RuneCountInString(text))
	require.True(t, utf8.ValidString(text))
	require.True(t, strings.HasPrefix(text, "Subject: parser review\n\néé"))

	require.Equal(t, "ab", cutRunes("abc", 2))
	require.Equal(t, "abc", cutRunes("abc", 3))
	require.Equal(t, "éé", cutRunes("ééé", 2))
	require.Empty(t, cutRunes("abc", 0))
}

const gatewayYAML = `
endpoint:
  url_env: U
  token_env: T
base_model:
  repo: example.invalid/glimmer
  revision: v1
runtime: vllm
profiles:
  - contract: ANALYZE_AGENT_STATE
    inference_profile: base
  - contract: TRIAGE_MESSAGE
    inference_profile: base
  - contract: CHOOSE_ORCHESTRATION_ACTION
    inference_profile: base
`

// gatewayClock reads t0, and its timers never fire.
type gatewayClock struct{}

func (gatewayClock) Now() time.Time                       { return t0 }
func (gatewayClock) After(time.Duration) <-chan time.Time { return make(chan time.Time) }

// endpoint is a fake Glimmer endpoint that keeps the bodies it gets.
type endpoint struct {
	mu     sync.Mutex
	bodies []string
	answer string
}

func (e *endpoint) Send(_ context.Context, body []byte) ([]byte, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.bodies = append(e.bodies, string(body))
	return []byte(e.answer), nil
}

// allText is every column of every table but message_envelopes, which
// holds the message, as text.
func allText(t *testing.T, db *sql.DB) string {
	t.Helper()
	var b strings.Builder
	for _, table := range tableNames(t, db) {
		if table != "message_envelopes" {
			tableText(t, db, table, &b)
		}
	}
	return b.String()
}

func tableNames(t *testing.T, db *sql.DB) []string {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), `SELECT name FROM sqlite_schema
		WHERE type = 'table' AND name NOT LIKE 'sqlite_%'`)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	var tables []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		tables = append(tables, name)
	}
	require.NoError(t, rows.Err())
	return tables
}

func tableText(t *testing.T, db *sql.DB, table string, b *strings.Builder) {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), "SELECT * FROM "+table)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	cols, err := rows.Columns()
	require.NoError(t, err)
	for rows.Next() {
		vals := make([]sql.NullString, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		require.NoError(t, rows.Scan(ptrs...))
		for _, v := range vals {
			b.WriteString(v.String + "\n")
		}
	}
	require.NoError(t, rows.Err())
}

// Through the real gateway, its redactor and its SQL recorder: the text
// reaches the endpoint, masked, and is stored nowhere but in the envelope.
func TestTriageThroughTheGateway(t *testing.T) {
	t.Parallel()
	store := testStore(t)
	db := store.DB()
	secret, err := redacttest.Value("github_pat")
	require.NoError(t, err)
	const canary = "body-canary-7f3"
	seed(t, db, message{id: msgID(1), body: canary + " please rotate " + secret})

	cfg, err := gateway.ParseConfig(strings.NewReader(gatewayYAML), func(k string) string {
		return map[string]string{"U": "http://127.0.0.1:1", "T": "test-token"}[k]
	})
	require.NoError(t, err)
	red, err := redact.New(redact.Options{})
	require.NoError(t, err)
	rec, err := gateway.NewSQLRecorder(db)
	require.NoError(t, err)
	ep := &endpoint{answer: `{"label":"REQUIRES_HUMAN","probabilities":{"REQUIRES_HUMAN":0.8,"NORMAL":0.2},"letter_mass":0.9}`}
	gw, err := gateway.New(cfg, gateway.Deps{
		Transport: ep, Redactor: red, Recorder: rec, Clock: gatewayClock{},
		IDs: gateway.RandomCallIDs{}, Prompts: analyst.Prompts{},
	})
	require.NoError(t, err)

	v, ok, err := newTriager(t, db, gw, &fakeChannel{allow: true}).Triage(t.Context(), delivery(msgID(1), recipientB))
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, protocol.TriageLabelRequiresHuman, v.Label)
	var status, contract string
	require.NoError(t, db.QueryRowContext(t.Context(),
		`SELECT status, contract FROM glimmer_calls WHERE call_id = ?`, string(v.GlimmerCallID)).Scan(&status, &contract))
	require.Equal(t, "ok", status, "the verdict names the recorded call")
	require.Equal(t, string(protocol.ModelContractTriageMessage), contract)

	require.Len(t, ep.bodies, 1)
	require.Contains(t, ep.bodies[0], canary, "the text reaches the endpoint")
	require.NotContains(t, ep.bodies[0], secret, "masked before it leaves")
	var wire struct {
		PromptVersion string            `json:"prompt_version"`
		Messages      []gateway.Message `json:"messages"`
	}
	require.NoError(t, json.Unmarshal([]byte(ep.bodies[0]), &wire))
	require.Equal(t, analyst.TriagePromptVersion, wire.PromptVersion)
	require.Len(t, wire.Messages, 2)
	require.Contains(t, wire.Messages[1].Content, "Subject: parser review\n\n"+canary, "the prompt frames the text")
	stored := allText(t, db)
	require.NotContains(t, stored, canary, "no table but the envelope's holds the text")
	require.NotContains(t, stored, secret)
}

func TestMessageTriageTable(t *testing.T) {
	t.Parallel()
	store := testStore(t)
	db := store.DB()
	seed(t, db, message{id: msgID(1)})
	for name, row := range map[string][]any{
		"classified without a label":         {msgID(1), nil, string(OutcomeClassified), call1},
		"classified without a call":          {msgID(1), "NORMAL", string(OutcomeClassified), nil},
		"a label on a failed call":           {msgID(1), "NORMAL", string(OutcomeTimeout), call1},
		"an encrypted message with a call":   {msgID(1), nil, string(OutcomeEncrypted), call1},
		"a confidential message with a call": {msgID(1), nil, string(OutcomeConfidential), call1},
		"an unreadable message with a call":  {msgID(1), nil, string(OutcomeUnreadable), call1},
		"an unknown label":                   {msgID(1), "URGENT", string(OutcomeClassified), call1},
		"an unknown outcome":                 {msgID(1), nil, "low_mass", call1},
		"no message id":                      {nil, nil, string(OutcomeEncrypted), nil},
		"a message that does not exist":      {msgID(2), nil, string(OutcomeEncrypted), nil},
	} {
		_, err := db.ExecContext(t.Context(), `INSERT INTO message_triage
			(message_id, label, outcome, glimmer_call_id, created_at) VALUES (?, ?, ?, ?, ?)`,
			append(row, eventstore.FormatTime(t0))...)
		require.Error(t, err, name)
	}
	require.Zero(t, verdictCount(t, db))

	insertVerdict(t, db, msgID(1), nil, string(OutcomeTimeout), call1)
	_, err := db.ExecContext(t.Context(), `UPDATE message_triage SET outcome = 'invalid' WHERE message_id = ?`, string(msgID(1)))
	require.ErrorContains(t, err, "message_triage is written once")
	_, err = db.ExecContext(t.Context(), `INSERT INTO message_triage
		(message_id, label, outcome, glimmer_call_id, created_at) VALUES (?, NULL, 'error', NULL, ?)`,
		string(msgID(1)), eventstore.FormatTime(t0))
	require.Error(t, err, "one verdict per message")

	objects := func() int {
		var n int
		require.NoError(t, db.QueryRowContext(t.Context(), `SELECT count(*) FROM sqlite_schema
			WHERE name IN ('message_triage', 'message_triage_once')`).Scan(&n))
		return n
	}
	require.Equal(t, 2, objects())
	require.NoError(t, store.MigrateDown(t.Context(), 1))
	require.Zero(t, objects(), "Down drops the table and its trigger")
	require.NoError(t, store.MigrateUp(t.Context()))
	require.Equal(t, 2, objects())
}
