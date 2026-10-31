package triage

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

// realChannel is internal/channel as the node runs it: the engine whose
// preview the adapter asks, and the controller an operator sets controls
// through.
type realChannel struct {
	store      *eventstore.Store
	engine     *channel.Engine
	controller *channel.Controller
}

func newRealChannel(t *testing.T) realChannel {
	t.Helper()
	store := testStore(t)
	engine, err := channel.New(channel.Options{Clock: clock.System{}})
	require.NoError(t, err)
	controller, err := channel.NewController(channel.ControllerOptions{
		DB: store.DB(), Engine: engine, Clock: clock.System{}, NodeID: nodeA,
		Entropy: bytes.NewReader(make([]byte, 4096)),
	})
	require.NoError(t, err)
	return realChannel{store: store, engine: engine, controller: controller}
}

// admitAndQueue admits m through the channel and initializes its delivery to
// recipientB while the channel allows, so the queued triple says ALLOWED.
// The recipient has a live, deliverable session. Every other row is a plain
// insert.
func (c realChannel) admitAndQueue(t *testing.T, m message) {
	t.Helper()
	m = withDefaults(m)
	db := c.store.DB()
	stamp := eventstore.FormatTime(t0.Add(-time.Minute))
	_, err := db.ExecContext(t.Context(), `INSERT INTO agents
		(agent_id, node_id, harness, created_at, updated_at) VALUES (?, ?, 'codex', ?, ?)`,
		string(recipientB), nodeA, stamp, stamp)
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), `INSERT INTO sessions
		(session_id, agent_id, state, capture_mode, deliverable, manifest_json,
		 process_json, started_at, ended_at, updated_at)
		VALUES ('session-b', ?, 'CONNECTED', 'pty_proxy', 1, '{}', NULL, ?, NULL, ?)`,
		string(recipientB), stamp, stamp)
	require.NoError(t, err)

	admitted := channel.Message{
		ID: m.id, From: sender, FromNode: nodeA, ToNode: nodeB, ThreadID: "thread-1",
		To: []channel.Recipient{{Agent: recipientB, Deliverable: true, Policy: channel.PolicyAllowed}},
	}
	tx, err := db.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	_, _, err = c.engine.Admit(t.Context(), tx, admitted, func(ctx context.Context, result protocol.AdmissionResult) error {
		raw, err := json.Marshal(result)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO messages
			(message_id, client_msg_id, origin_node, from_agent, destination_node,
			 thread_ref, thread_id, priority, state, created_at, expires_at,
			 updated_at, hop_count, route_digest, admission_result_json)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			string(m.id), "cli-"+string(m.id), nodeA, string(sender), nodeB, threadRef,
			"thread-1", "NORMAL", "ADMITTED", stamp, eventstore.FormatTime(m.expiresAt), stamp,
			0, routeDigest, string(raw))
		return err
	})
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	seedEnvelopeAndQueue(t, db, m)
	tx, err = db.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	require.NoError(t, c.engine.InitializeDelivery(t.Context(), tx, m.id, recipientB, 1))
	require.NoError(t, tx.Commit())
	require.Equal(t, string(protocol.ControlStateAllowed), queuedState(t, db, m.id))
}

func queuedState(t *testing.T, db *sql.DB, id protocol.ULID) string {
	t.Helper()
	var state string
	require.NoError(t, db.QueryRowContext(t.Context(),
		`SELECT control_state FROM delivery_queue WHERE message_id = ?`, string(id)).Scan(&state))
	return state
}

func decisionCount(t *testing.T, db *sql.DB, id protocol.ULID) int {
	t.Helper()
	var n int
	require.NoError(t, db.QueryRowContext(t.Context(),
		`SELECT count(*) FROM channel_control_decisions WHERE object_id = ?`, string(id)).Scan(&n))
	return n
}

func realChannelTriager(t *testing.T, c realChannel, model Inferrer) *Triager {
	t.Helper()
	ch, err := NewChannel(c.engine)
	require.NoError(t, err)
	return newTriager(t, c.store.DB(), model, ch)
}

// While the channel allows, the adapter lets the triage call the model.
func TestRealChannelAllowsTheTriage(t *testing.T) {
	t.Parallel()
	c := newRealChannel(t)
	c.admitAndQueue(t, message{id: msgID(1)})
	model := &fakeModel{res: okTriage(protocol.TriageLabelNormal)}
	v, ok, err := realChannelTriager(t, c, model).Triage(t.Context(), delivery(msgID(1), recipientB))
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, OutcomeClassified, v.Outcome)
	require.Len(t, model.calls(), 1)
}

// A control set after the message was queued counts, though the queued row
// still says ALLOWED: the adapter asks the channel of now. Nothing is called,
// written or decided, and the queued triple stays as it was.
func TestRealChannelStopsTheTriageAfterAControlChange(t *testing.T) {
	t.Parallel()
	actor := channel.Actor{Type: protocol.ActorTypeHuman, ID: "operator"}
	for name, restrict := range map[string]func(*testing.T, realChannel){
		"global off": func(t *testing.T, c realChannel) {
			t.Helper()
			_, err := c.controller.SetGlobal(t.Context(), protocol.SetGlobalParams{
				Mode: protocol.ChannelModeOff, ExpectedVersion: 0,
			}, actor)
			require.NoError(t, err)
		},
		"link hold": func(t *testing.T, c realChannel) {
			t.Helper()
			_, err := c.controller.SetLink(t.Context(), protocol.SetLinkParams{
				FromAgent: sender, ToAgent: recipientB, Mode: protocol.LinkModeHold,
				Plane: protocol.PlaneMessage,
				ExpectedVersions: protocol.ControlVersions{
					"link:" + string(sender) + "→" + string(recipientB) + ":message": 0,
				},
			}, actor)
			require.NoError(t, err)
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			c := newRealChannel(t)
			c.admitAndQueue(t, message{id: msgID(1)})
			restrict(t, c)
			db := c.store.DB()
			decisions := decisionCount(t, db, msgID(1))

			model := &fakeModel{res: okTriage(protocol.TriageLabelNormal)}
			v, ok, err := realChannelTriager(t, c, model).Triage(t.Context(), delivery(msgID(1), recipientB))
			require.NoError(t, err)
			require.False(t, ok)
			require.Equal(t, Verdict{}, v)
			require.Empty(t, model.calls(), "no call")
			require.Zero(t, verdictCount(t, db), "no verdict")
			require.Equal(t, decisions, decisionCount(t, db, msgID(1)), "no decision minted")
			require.Equal(t, string(protocol.ControlStateAllowed), queuedState(t, db, msgID(1)),
				"the queued triple is not refreshed")
		})
	}
}

type recordingPreviewer struct {
	tx         *sql.Tx
	messageID  protocol.ULID
	recipient  protocol.AgentID
	generation int64
	allowed    bool
	err        error
}

func (p *recordingPreviewer) PreviewQueuedDelivery(
	_ context.Context,
	tx *sql.Tx,
	messageID protocol.ULID,
	recipient protocol.AgentID,
	generation int64,
) (bool, error) {
	p.tx, p.messageID, p.recipient, p.generation = tx, messageID, recipient, generation
	return p.allowed, p.err
}

// The adapter asks about exactly the delivery, through the caller's
// transaction, and a failed preview is an error, never an allow.
func TestChannelAdapterAsksAboutTheDelivery(t *testing.T) {
	t.Parallel()
	_, err := NewChannel(nil)
	require.ErrorIs(t, err, ErrInvalidConfig)

	db := testStore(t).DB()
	tx, err := db.BeginTx(t.Context(), &sql.TxOptions{ReadOnly: true})
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	d := Delivery{MessageID: msgID(7), AgentID: recipientC, DeliveryGeneration: 3}
	for _, allowed := range []bool{true, false} {
		p := &recordingPreviewer{allowed: allowed}
		ch, err := NewChannel(p)
		require.NoError(t, err)
		got, err := ch.AllowsNow(t.Context(), tx, d)
		require.NoError(t, err)
		require.Equal(t, allowed, got)
		require.Same(t, tx, p.tx)
		require.Equal(t, Delivery{MessageID: p.messageID, AgentID: p.recipient, DeliveryGeneration: p.generation}, d)
	}
	failure := errors.New("preview failed")
	ch, err := NewChannel(&recordingPreviewer{allowed: true, err: failure})
	require.NoError(t, err)
	got, err := ch.AllowsNow(t.Context(), tx, d)
	require.ErrorIs(t, err, failure)
	require.False(t, got)
}
