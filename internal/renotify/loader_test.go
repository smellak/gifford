package renotify

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/fallback"
	"github.com/smellak/gifford/internal/protocol"
)

var loadT0 = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

type scriptChannels struct {
	view protocol.AgentChannel
	err  error
}

// errScriptChannels simulates a channel lookup failure; the loader must fail closed.
var errScriptChannels = errScriptChannelsSentinel{}

type errScriptChannelsSentinel struct{}

func (errScriptChannelsSentinel) Error() string { return "script channels unavailable" }

func (s scriptChannels) Agent(context.Context, protocol.AgentID) (protocol.AgentChannel, error) {
	return s.view, s.err
}

func allowingChannels() scriptChannels {
	return scriptChannels{view: protocol.AgentChannel{
		TX: true, RX: true, AutoDelivery: true, Deliverable: true,
	}}
}

type seedMessage struct {
	id        protocol.ULID
	agent     protocol.AgentID
	pending   time.Time
	expires   time.Time
	state     string
	control   string
	notices   []time.Time
	acked     bool
	uncertain bool
	escalated bool
}

func seedLoopDB(t *testing.T, channels ChannelReader, msgs ...seedMessage) (*sql.DB, *Loader) {
	t.Helper()
	store, err := eventstore.Open(t.Context(), filepath.Join(t.TempDir(), "loop.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	db := store.DB()
	for _, m := range msgs {
		_, err := db.Exec(`INSERT INTO agents(agent_id, node_id, harness, created_at, updated_at)
			VALUES(?, 'node-1', 'test', ?, ?)`,
			string(m.agent), eventstore.FormatTime(loadT0), eventstore.FormatTime(loadT0))
		require.NoError(t, err)
		_, err = db.Exec(`INSERT INTO sessions(session_id, agent_id, state, capture_mode,
			deliverable, manifest_json, started_at, updated_at)
			VALUES('sess-`+string(m.agent)+`', ?, 'CONNECTED', 'pty_proxy', 1, '{}', ?, ?)`,
			string(m.agent), eventstore.FormatTime(loadT0), eventstore.FormatTime(loadT0))
		require.NoError(t, err)
		_, err = db.Exec(`INSERT INTO messages(message_id, client_msg_id, origin_node, from_agent,
			destination_node, thread_ref, priority, state, created_at, expires_at, updated_at,
			hop_count, route_digest)
			VALUES(?, ?, 'node-1', 'agent-sender', 'node-1', 'thr_00000000000000000000000001',
			'NORMAL', ?, ?, ?, ?, 0, 'sha256:00')`,
			string(m.id), "client-"+string(m.id), m.state,
			eventstore.FormatTime(loadT0), eventstore.FormatTime(time.Now().UTC().Add(24*time.Hour)),
			eventstore.FormatTime(loadT0))
		require.NoError(t, err)
		_, err = db.Exec(`INSERT INTO delivery_queue(message_id, agent_id, delivery_generation,
			from_agent, thread_ref, state, priority, created_at, expires_at, updated_at,
			hop_count, route_digest, control_state)
			VALUES(?, ?, 1, 'agent-sender', 'thr_00000000000000000000000001', 'DELIVERED',
			'NORMAL', ?, ?, ?, 0, 'sha256:00', ?)`,
			string(m.id), string(m.agent),
			eventstore.FormatTime(m.pending), eventstore.FormatTime(time.Now().UTC().Add(24*time.Hour)),
			eventstore.FormatTime(m.pending),
			m.control)
		require.NoError(t, err)
		for i, at := range m.notices {
			_, err = db.Exec(`INSERT INTO delivery_attempts(delivery_id, message_id, agent_id,
				delivery_generation, adapter_session_id, orchestrator_decision_id, channel_decision_id,
				draft_generation, input_guard_version, control_versions_json, state, created_at, updated_at)
				VALUES(?, ?, ?, 1, 'pane-1', 'dec-1', 'ch-1', 0, 0, '{}', 'COMMITTED', ?, ?)`,
				"att-"+string(m.id)+"-"+string(rune('a'+i)), string(m.id), string(m.agent),
				eventstore.FormatTime(at), eventstore.FormatTime(at))
			require.NoError(t, err)
		}
		if m.acked {
			_, err = db.Exec(`INSERT INTO acks(message_id, agent_id, delivery_generation, status, acked_at)
				VALUES(?, ?, 1, 'ACKNOWLEDGED', ?)`,
				string(m.id), string(m.agent), eventstore.FormatTime(loadT0.Add(time.Hour)))
			require.NoError(t, err)
		}
		if m.uncertain {
			_, err = db.Exec(`INSERT INTO delivery_attempts(delivery_id, message_id, agent_id,
				delivery_generation, adapter_session_id, orchestrator_decision_id, channel_decision_id,
				draft_generation, input_guard_version, control_versions_json, state, created_at, updated_at)
				VALUES(?, ?, ?, 1, 'pane-1', 'dec-1', 'ch-1', 0, 0, '{}', 'UNCERTAIN', ?, ?)`,
				"att-unc-"+string(m.id), string(m.id), string(m.agent),
				eventstore.FormatTime(loadT0.Add(50*time.Minute)), eventstore.FormatTime(loadT0.Add(50*time.Minute)))
			require.NoError(t, err)
		}
		if m.escalated {
			_, err = db.Exec(`INSERT INTO escalations(message_id, agent_id, operator_agent, created_at)
				VALUES(?, ?, 'agent-operator', ?)`,
				string(m.id), string(m.agent), eventstore.FormatTime(loadT0.Add(31*time.Minute)))
			require.NoError(t, err)
		}
	}
	return db, &Loader{DB: db, Channels: channels}
}

const (
	loadMsg   = protocol.ULID("01ARZ3NDEKTSV4RRFFQ69G0001")
	loadAgent = protocol.AgentID("agent-1")
)

func loadItem(t *testing.T, loader *Loader) Item {
	t.Helper()
	items, err := loader.Items(context.Background())
	require.NoError(t, err)
	require.Len(t, items, 1)
	return items[0]
}

func baseSeed() seedMessage {
	return seedMessage{
		id: loadMsg, agent: loadAgent, pending: loadT0,
		expires: loadT0.Add(24 * time.Hour), state: "PENDING", control: "ALLOWED",
	}
}

func TestLoaderMapsRealFacts(t *testing.T) {
	t.Parallel()

	withNotices := baseSeed()
	withNotices.notices = []time.Time{loadT0}
	_, loader := seedLoopDB(t, allowingChannels(), withNotices)
	item := loadItem(t, loader)
	require.Equal(t, loadT0, item.Facts.PendingSince.UTC())
	require.False(t, item.Facts.Read)
	require.False(t, item.Facts.Uncertain)
	require.False(t, item.Facts.Escalated)
	require.Len(t, item.Facts.Notices, 1)
	require.True(t, item.Facts.GuardsPassed)
	require.Equal(t, fallback.PaneIdle, item.Facts.Pane)
}

func TestLoaderHonoursMessageControls(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		control string
		check   func(*testing.T, fallback.Controls)
	}{
		{"held", "HELD", func(t *testing.T, c fallback.Controls) { require.True(t, c.Hold) }},
		{"approval", "APPROVAL_REQUIRED", func(t *testing.T, c fallback.Controls) {
			require.True(t, c.ApprovalPending)
		}},
		{"denied", "DENIED", func(t *testing.T, c fallback.Controls) {
			require.True(t, c.Rejected)
		}},
		{"unknown", "WEIRD", func(t *testing.T, c fallback.Controls) {
			require.True(t, c.Hold, "unknown control fails closed")
		}},
	} {
		seed := baseSeed()
		seed.control = tc.control
		_, loader := seedLoopDB(t, allowingChannels(), seed)
		item := loadItem(t, loader)
		tc.check(t, item.Facts.Controls)
		require.False(t, item.Facts.GuardsPassed, tc.name)
	}
}

func TestLoaderHonoursAgentControls(t *testing.T) {
	t.Parallel()

	manual := allowingChannels()
	manual.view.AutoDelivery = false
	_, loader := seedLoopDB(t, manual, baseSeed())
	require.True(t, loadItem(t, loader).Facts.Controls.Manual)

	off := allowingChannels()
	off.view.Mode = protocol.AgentModeOff
	_, loader = seedLoopDB(t, off, baseSeed())
	require.True(t, loadItem(t, loader).Facts.Controls.Off)

	_, loader = seedLoopDB(t, scriptChannels{err: errScriptChannels}, baseSeed())
	item := loadItem(t, loader)
	require.True(t, item.Facts.Controls.Hold, "missing channel view fails closed")
	require.False(t, item.Facts.GuardsPassed)
}

func TestLoaderTracksReadUncertainEscalated(t *testing.T) {
	t.Parallel()

	read := baseSeed()
	read.acked = true
	_, loader := seedLoopDB(t, allowingChannels(), read)
	require.True(t, loadItem(t, loader).Facts.Read)

	uncertain := baseSeed()
	uncertain.uncertain = true
	_, loader = seedLoopDB(t, allowingChannels(), uncertain)
	require.True(t, loadItem(t, loader).Facts.Uncertain)

	escalated := baseSeed()
	escalated.escalated = true
	_, loader = seedLoopDB(t, allowingChannels(), escalated)
	require.True(t, loadItem(t, loader).Facts.Escalated)
}
