package renotify

import (
	"bytes"
	"context"
	"database/sql"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/fallback"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
)

var loadT0 = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

// humanActor is the operator driving the fixture's channel controls.
var humanActor = channel.Actor{Type: protocol.ActorTypeHuman, ID: "op-1"}

type scriptChannels struct {
	view protocol.AgentChannel
	err  error
}

func (s scriptChannels) Agent(context.Context, protocol.AgentID) (protocol.AgentChannel, error) {
	return s.view, s.err
}

// errScriptChannels simulates a channel lookup failure; the loader must fail closed.
var errScriptChannels = errScriptChannelsSentinel{}

type errScriptChannelsSentinel struct{}

func (errScriptChannelsSentinel) Error() string { return "script channels unavailable" }

type seedMessage struct {
	id        protocol.ULID
	agent     protocol.AgentID
	pending   time.Time
	state     string
	notices   []time.Time
	acked     bool
	uncertain bool
	escalated bool
	// allowed drives the delivery through the channel API, so the row
	// carries ALLOWED. Without it the row keeps its HELD default.
	allowed bool
	// setup runs after admission: agent controls through the channel
	// API, never raw control writes.
	setup func(t *testing.T, ctl *channel.Controller)
}

func seedLoopDB(t *testing.T, msgs ...seedMessage) (*sql.DB, *Loader) {
	t.Helper()
	ctx := context.Background()
	store, err := eventstore.Open(ctx, filepath.Join(t.TempDir(), "loop.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	db := store.DB()
	engine, err := channel.New(channel.Options{Clock: fixedClock{at: loadT0}})
	require.NoError(t, err)
	ctl, err := channel.NewController(channel.ControllerOptions{
		DB: db, Engine: engine, Clock: fixedClock{at: loadT0}, NodeID: protocol.NodeID("node-1"),
	})
	require.NoError(t, err)
	require.NoError(t, ctl.Reload(ctx))
	for _, m := range msgs {
		_, err := db.ExecContext(ctx, `INSERT INTO agents(agent_id, node_id, harness, created_at, updated_at)
			VALUES(?, 'node-1', 'test', ?, ?)`,
			string(m.agent), eventstore.FormatTime(loadT0), eventstore.FormatTime(loadT0))
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, `INSERT INTO sessions(session_id, agent_id, state, capture_mode,
			deliverable, manifest_json, started_at, updated_at)
			VALUES('sess-`+string(m.agent)+`', ?, 'CONNECTED', 'pty_proxy', 1, '{}', ?, ?)`,
			string(m.agent), eventstore.FormatTime(loadT0), eventstore.FormatTime(loadT0))
		require.NoError(t, err)
		admit := channel.Message{
			ID: loadMsg, From: protocol.AgentID("agent-sender"), FromNode: protocol.NodeID("node-1"),
			To:     []channel.Recipient{{Agent: m.agent, Deliverable: true, Policy: channel.PolicyAllowed}},
			ToNode: protocol.NodeID("node-1"), ThreadID: protocol.ThreadID("thread-1"),
		}
		admit.ID = m.id
		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		_, _, err = engine.Admit(ctx, tx, admit, func(ctx context.Context, _ protocol.AdmissionResult) error {
			_, err := tx.ExecContext(ctx, `INSERT INTO messages(message_id, client_msg_id, origin_node,
				from_agent, destination_node, thread_ref, thread_id, priority, state,
				created_at, expires_at, updated_at, hop_count, route_digest)
				VALUES(?, ?, 'node-1', 'agent-sender', 'node-1', 'thr_00000000000000000000000001',
				'thread-1', 'NORMAL', ?, ?, ?, ?, 0, 'sha256:00')`,
				string(m.id), "client-"+string(m.id), m.state,
				eventstore.FormatTime(loadT0), eventstore.FormatTime(time.Now().UTC().Add(24*time.Hour)),
				eventstore.FormatTime(loadT0))
			return err
		})
		require.NoError(t, err)
		_, err = tx.ExecContext(ctx, `INSERT INTO delivery_queue(message_id, agent_id, delivery_generation,
			from_agent, thread_ref, state, priority, created_at, expires_at, updated_at,
			hop_count, route_digest)
			VALUES(?, ?, 1, 'agent-sender', 'thr_00000000000000000000000001', 'DELIVERED',
			'NORMAL', ?, ?, ?, 0, 'sha256:00')`,
			string(m.id), string(m.agent),
			eventstore.FormatTime(m.pending), eventstore.FormatTime(time.Now().UTC().Add(24*time.Hour)),
			eventstore.FormatTime(m.pending))
		require.NoError(t, err)
		if m.allowed {
			require.NoError(t, engine.InitializeDelivery(ctx, tx, m.id, m.agent, 1))
		}
		require.NoError(t, tx.Commit())
		if m.setup != nil {
			m.setup(t, ctl)
		}
		for i, at := range m.notices {
			_, err = db.ExecContext(ctx, `INSERT INTO delivery_attempts(delivery_id, message_id, agent_id,
				delivery_generation, adapter_session_id, orchestrator_decision_id, channel_decision_id,
				draft_generation, input_guard_version, control_versions_json, state, created_at, updated_at)
				VALUES(?, ?, ?, 1, 'pane-1', 'dec-1', 'ch-1', 0, 0, '{}', 'COMMITTED', ?, ?)`,
				"att-"+string(m.id)+"-"+string(rune('a'+i)), string(m.id), string(m.agent),
				eventstore.FormatTime(at), eventstore.FormatTime(at))
			require.NoError(t, err)
		}
		if m.acked {
			_, err = db.ExecContext(ctx, `INSERT INTO acks(message_id, agent_id, delivery_generation, status, acked_at)
				VALUES(?, ?, 1, 'ACKNOWLEDGED', ?)`,
				string(m.id), string(m.agent), eventstore.FormatTime(loadT0.Add(time.Hour)))
			require.NoError(t, err)
		}
		if m.uncertain {
			_, err = db.ExecContext(ctx, `INSERT INTO delivery_attempts(delivery_id, message_id, agent_id,
				delivery_generation, adapter_session_id, orchestrator_decision_id, channel_decision_id,
				draft_generation, input_guard_version, control_versions_json, state, created_at, updated_at)
				VALUES(?, ?, ?, 1, 'pane-1', 'dec-1', 'ch-1', 0, 0, '{}', 'UNCERTAIN', ?, ?)`,
				"att-unc-"+string(m.id), string(m.id), string(m.agent),
				eventstore.FormatTime(loadT0.Add(50*time.Minute)), eventstore.FormatTime(loadT0.Add(50*time.Minute)))
			require.NoError(t, err)
		}
		if m.escalated {
			_, err = db.ExecContext(ctx, `INSERT INTO escalations(message_id, agent_id, operator_agent, created_at)
				VALUES(?, ?, 'agent-operator', ?)`,
				string(m.id), string(m.agent), eventstore.FormatTime(loadT0.Add(31*time.Minute)))
			require.NoError(t, err)
		}
	}
	// The clock stays fixed: expiry is seeded relative to the real now
	// and every pending stamp is older, so the check is deterministic.
	return db, &Loader{DB: db, Channels: ctl, Clock: fixedClock{at: loadT0}}
}

func setupManual(t *testing.T, ctl *channel.Controller) {
	t.Helper()
	off := false
	_, err := ctl.SetAgent(context.Background(), protocol.SetAgentChannelParams{
		AgentID: loadAgent, AutoDelivery: &off, ExpectedVersion: 0,
	}, humanActor)
	require.NoError(t, err)
}

func setupOff(t *testing.T, ctl *channel.Controller) {
	t.Helper()
	_, err := ctl.SetAgentMode(context.Background(), protocol.SetAgentModeParams{
		AgentID: loadAgent, Mode: protocol.AgentModeOff, ExpectedVersion: 0,
	}, humanActor)
	require.NoError(t, err)
}

const (
	loadMsg   = protocol.ULID("01ARZ3NDEKTSV4RRFFQ69G0001")
	loadAgent = protocol.AgentID("agent-1")

	loadMsg2   = protocol.ULID("01ARZ3NDEKTSV4RRFFQ69G0002")
	loadAgent2 = protocol.AgentID("agent-2")
)

func secondSeed() seedMessage {
	seed := baseSeed()
	seed.id, seed.agent = loadMsg2, loadAgent2
	return seed
}

func loadItem(t *testing.T, loader *Loader) Item {
	t.Helper()
	items, err := loader.Items(context.Background())
	require.NoError(t, err)
	require.Len(t, items, 1)
	return items[0]
}

// seedRawDelivery files one queue row for a hand-written message: the
// loader reads rows, never how they were admitted.
func seedRawDelivery(t *testing.T, db *sql.DB, id protocol.ULID, from, to, clientKey string, pending time.Time) {
	t.Helper()
	ctx := context.Background()
	_, err := db.ExecContext(ctx, `INSERT INTO messages(message_id, client_msg_id, origin_node,
		from_agent, destination_node, thread_ref, thread_id, priority, state,
		created_at, expires_at, updated_at, hop_count, route_digest)
		VALUES(?, ?, 'node-1', ?, 'node-1', 'thr_00000000000000000000000001',
		'thread-1', 'NORMAL', 'PENDING', ?, ?, ?, 0, 'sha256:00')`,
		string(id), clientKey, from,
		eventstore.FormatTime(pending), eventstore.FormatTime(pending.Add(48*time.Hour)),
		eventstore.FormatTime(pending))
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `INSERT INTO delivery_queue(message_id, agent_id, delivery_generation,
		from_agent, thread_ref, state, priority, created_at, expires_at, updated_at,
		hop_count, route_digest)
		VALUES(?, ?, 1, ?, 'thr_00000000000000000000000001', 'QUEUED',
		2, ?, ?, ?, 0, 'sha256:00')`,
		string(id), to, from,
		eventstore.FormatTime(pending), eventstore.FormatTime(pending.Add(48*time.Hour)),
		eventstore.FormatTime(pending))
	require.NoError(t, err)
}

// Rows sent by the node itself are never loop items: the operator
// notice is filed, never renotified. Only the sender differs from an
// ordinary queued row.
func TestLoaderSkipsNodeSentRows(t *testing.T) {
	t.Parallel()

	db, loader := seedLoopDB(t, baseSeed())
	seedRawDelivery(t, db, protocol.ULID("01ARZ3NDEKTSV4RRFFQ69G00E1"),
		string(registry.EmitterAgentID), string(loadAgent), "operator-notice:"+string(loadMsg), loadT0)
	items, err := loader.Items(context.Background())
	require.NoError(t, err)
	require.Len(t, items, 1, "only the ordinary message lists; the node-sent row never does")
	require.Equal(t, loadMsg, items[0].MessageID)
}

// An agent-chosen client id never opts a message out of renotify,
// even under the escalation prefix: the exclusion keys on the node
// sender, which agents cannot choose.
func TestLoaderListsPrefixedAgentClientID(t *testing.T) {
	t.Parallel()

	db, loader := seedLoopDB(t, baseSeed())
	prefixed := protocol.ULID("01ARZ3NDEKTSV4RRFFQ69G00E2")
	seedRawDelivery(t, db, prefixed, "agent-sender", string(loadAgent2), "escalate:my-own-key", loadT0)
	items, err := loader.Items(context.Background())
	require.NoError(t, err)
	byID := map[protocol.ULID]bool{}
	for _, item := range items {
		byID[item.MessageID] = true
	}
	require.True(t, byID[prefixed], "a message with an agent-chosen escalate: client id still lists")
}

func baseSeed() seedMessage {
	return seedMessage{
		id: loadMsg, agent: loadAgent, pending: loadT0,
		state: "PENDING", allowed: true,
	}
}

func TestLoaderMapsRealFacts(t *testing.T) {
	t.Parallel()

	withNotices := baseSeed()
	withNotices.notices = []time.Time{loadT0}
	_, loader := seedLoopDB(t, withNotices)
	item := loadItem(t, loader)
	require.Equal(t, loadT0, item.Facts.PendingSince.UTC())
	require.False(t, item.Facts.Read)
	require.False(t, item.Facts.Uncertain)
	require.False(t, item.Facts.Escalated)
	require.Len(t, item.Facts.Notices, 1)
	require.True(t, item.Facts.GuardsPassed)
	require.Equal(t, fallback.PaneIdle, item.Facts.Pane)
}

func TestLoaderHonoursDeliveryStates(t *testing.T) {
	t.Parallel()

	held := baseSeed()
	held.allowed = false
	_, loader := seedLoopDB(t, held)
	item := loadItem(t, loader)
	require.True(t, item.Facts.Controls.Hold)
	require.False(t, item.Facts.GuardsPassed)
}

func TestLoaderHonoursAgentControls(t *testing.T) {
	t.Parallel()

	manual := baseSeed()
	manual.setup = setupManual
	_, loader := seedLoopDB(t, manual)
	require.True(t, loadItem(t, loader).Facts.Controls.Manual)

	off := baseSeed()
	off.setup = setupOff
	_, loader = seedLoopDB(t, off)
	require.True(t, loadItem(t, loader).Facts.Controls.Off)

	_, loader = seedLoopDB(t, baseSeed())
	loader.Channels = scriptChannels{err: errScriptChannels}
	item := loadItem(t, loader)
	require.True(t, item.Facts.Controls.Hold, "missing channel view fails closed")
	require.False(t, item.Facts.GuardsPassed)
}

// TestGatesMapsDeliveryStates covers the delivery states the channel API
// never produces in a fixture: denial drops the attempt, approval waits,
// and an unknown state fails closed. None of them notifies or escalates.
func TestGatesMapsDeliveryStates(t *testing.T) {
	t.Parallel()

	db, loader := seedLoopDB(t, baseSeed())
	bare := &Loader{DB: db}
	for _, tc := range []struct {
		name    string
		control string
		check   func(*testing.T, fallback.Controls)
	}{
		{"denied", "DENIED", func(t *testing.T, c fallback.Controls) {
			require.True(t, c.Rejected)
		}},
		{"approval", "APPROVAL_REQUIRED", func(t *testing.T, c fallback.Controls) {
			require.True(t, c.ApprovalPending)
		}},
		{"unknown", "WEIRD", func(t *testing.T, c fallback.Controls) {
			require.True(t, c.Hold, "unknown control fails closed")
		}},
	} {
		controls, _ := bare.gates(context.Background(), loadAgent, tc.control)
		tc.check(t, controls)
	}

	allowed, guards := loader.gates(context.Background(), loadAgent, "ALLOWED")
	require.True(t, guards)
	require.Equal(t, fallback.Controls{}, allowed)
}

// An unreadable message is recorded with its ids and skipped alone:
// the good message of the same turn still loads.
func TestLoaderSkipsUnreadableMessageAndKeepsTheRest(t *testing.T) {
	t.Parallel()

	db, loader := seedLoopDB(t, baseSeed(), secondSeed())
	_, err := db.ExecContext(t.Context(),
		`UPDATE delivery_queue SET created_at = 'not-a-time' WHERE message_id = ?`, string(loadMsg))
	require.NoError(t, err)
	var buf bytes.Buffer
	loader.Logger = slog.New(slog.NewTextHandler(&buf, nil))
	items, err := loader.Items(t.Context())
	require.NoError(t, err)
	require.Len(t, items, 1)
	require.Equal(t, loadMsg2, items[0].MessageID)
	recorded := buf.String()
	require.Contains(t, recorded, string(loadMsg))
	require.Contains(t, recorded, string(loadAgent))
}

func TestLoaderTracksReadUncertainEscalated(t *testing.T) {
	t.Parallel()

	read := baseSeed()
	read.acked = true
	_, loader := seedLoopDB(t, read)
	require.True(t, loadItem(t, loader).Facts.Read)

	uncertain := baseSeed()
	uncertain.uncertain = true
	_, loader = seedLoopDB(t, uncertain)
	require.True(t, loadItem(t, loader).Facts.Uncertain)

	escalated := baseSeed()
	escalated.escalated = true
	_, loader = seedLoopDB(t, escalated)
	require.True(t, loadItem(t, loader).Facts.Escalated)
}
