package tmuxattach

import (
	"database/sql"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

const (
	testInstanceOne  protocol.ULID    = "01JB0000000000000000000001"
	testInstanceTwo  protocol.ULID    = "01JB0000000000000000000002"
	testSessionAgent protocol.AgentID = "agent-one"
	testSessionNode  protocol.NodeID  = "node-test"
)

var testSessionTime = time.Date(2026, time.November, 2, 20, 0, 0, 0, time.UTC)

type sessionClock struct{ now time.Time }

func (c *sessionClock) Now() time.Time { return c.now }

func openSessionStore(t *testing.T) (*SessionStore, *sql.DB, *sessionClock) {
	t.Helper()
	store, err := eventstore.Open(t.Context(), filepath.Join(t.TempDir(), "gifford.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	clk := &sessionClock{now: testSessionTime}
	sessions, err := NewSessionStore(SessionStoreOptions{
		DB: store.DB(), NodeID: testSessionNode, Clock: clk,
	})
	require.NoError(t, err)
	return sessions, store.DB(), clk
}

func validSessionSpec(instance protocol.ULID) SessionSpec {
	return SessionSpec{
		InstanceID: instance, AgentID: testSessionAgent,
		Harness: protocol.HarnessCodex, HarnessVersion: "1.2.3",
		Process: protocol.ProcessIdentity{
			PID: 4242, ProcessStartTime: 100, UID: 1001, GID: 1001,
		},
		BootID: "boot-one",
	}
}

func TestSessionStoreBindsVerifiedTmuxSession(t *testing.T) {
	t.Parallel()
	sessions, db, _ := openSessionStore(t)
	spec := validSessionSpec(testInstanceOne)

	sessionID, err := sessions.Bind(t.Context(), spec)
	require.NoError(t, err)
	require.Equal(t, protocol.OpaqueID("tmux-"+string(testInstanceOne)), sessionID)

	var agent, node, harness string
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT agent_id, node_id, harness
		FROM agents WHERE agent_id = ?`, spec.AgentID).Scan(&agent, &node, &harness))
	require.Equal(t, string(spec.AgentID), agent)
	require.Equal(t, string(testSessionNode), node)
	require.Equal(t, string(spec.Harness), harness)

	var state, capture, manifestJSON, processJSON, bootID, startedAt, updatedAt string
	var deliverable bool
	var endedAt sql.NullString
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT state, capture_mode,
		deliverable, manifest_json, process_json, boot_id, started_at, ended_at, updated_at
		FROM sessions WHERE session_id = ?`, sessionID).Scan(&state, &capture, &deliverable,
		&manifestJSON, &processJSON, &bootID, &startedAt, &endedAt, &updatedAt))
	require.Equal(t, string(protocol.SessionStateConnected), state)
	require.Equal(t, string(protocol.CaptureModeTmuxAttach), capture)
	require.True(t, deliverable)
	require.Equal(t, spec.BootID, bootID)
	require.False(t, endedAt.Valid)
	require.Equal(t, eventstore.FormatTime(testSessionTime), startedAt)
	require.Equal(t, startedAt, updatedAt)

	var manifest protocol.CapabilityManifest
	require.NoError(t, json.Unmarshal([]byte(manifestJSON), &manifest))
	require.NoError(t, manifest.Validate())
	require.Equal(t, protocol.InputGuardProviderTmuxAttach, manifest.InputGuardProvider)
	require.Equal(t, protocol.CaptureModeTmuxAttach, manifest.CaptureMode)
	require.False(t, manifest.Capabilities.MCP,
		"tmux attachment cannot assert MCP before an authoritative registry source exists")
	var process protocol.ProcessIdentity
	require.NoError(t, json.Unmarshal([]byte(processJSON), &process))
	require.Equal(t, spec.Process, process)
}

func TestSessionStoreInvalidateIsIdempotentAndReconnectsExactInstance(t *testing.T) {
	t.Parallel()
	sessions, db, clk := openSessionStore(t)
	spec := validSessionSpec(testInstanceOne)
	sessionID, err := sessions.Bind(t.Context(), spec)
	require.NoError(t, err)

	clk.now = testSessionTime.Add(time.Minute)
	require.NoError(t, sessions.Invalidate(t.Context(), spec.AgentID, sessionID))
	var state, endedAt, updatedAt string
	var deliverable bool
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT state, deliverable,
		ended_at, updated_at FROM sessions WHERE session_id = ?`, sessionID).Scan(
		&state, &deliverable, &endedAt, &updatedAt))
	require.Equal(t, string(protocol.SessionStateDisconnected), state)
	require.False(t, deliverable)
	require.Equal(t, eventstore.FormatTime(clk.now), endedAt)
	require.Equal(t, endedAt, updatedAt)

	clk.now = clk.now.Add(time.Minute)
	require.NoError(t, sessions.Invalidate(t.Context(), spec.AgentID, sessionID))
	var unchanged string
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT updated_at FROM sessions
		WHERE session_id = ?`, sessionID).Scan(&unchanged))
	require.Equal(t, updatedAt, unchanged)

	clk.now = clk.now.Add(time.Minute)
	rebound, err := sessions.Bind(t.Context(), spec)
	require.NoError(t, err)
	require.Equal(t, sessionID, rebound)
	var reconnectedEnded sql.NullString
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT state, deliverable,
		ended_at, updated_at FROM sessions WHERE session_id = ?`, sessionID).Scan(
		&state, &deliverable, &reconnectedEnded, &updatedAt))
	require.Equal(t, string(protocol.SessionStateConnected), state)
	require.True(t, deliverable)
	require.False(t, reconnectedEnded.Valid)
	require.Equal(t, eventstore.FormatTime(clk.now), updatedAt)
}

func TestSessionStoreReplacementDisconnectsOldInstanceMonotonically(t *testing.T) {
	t.Parallel()
	sessions, db, clk := openSessionStore(t)
	first := validSessionSpec(testInstanceOne)
	firstID, err := sessions.Bind(t.Context(), first)
	require.NoError(t, err)

	clk.now = testSessionTime.Add(-time.Hour)
	second := validSessionSpec(testInstanceTwo)
	second.Process.ProcessStartTime++
	secondID, err := sessions.Bind(t.Context(), second)
	require.NoError(t, err)
	require.NotEqual(t, firstID, secondID)

	var state, endedAt, updatedAt string
	var deliverable bool
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT state, deliverable,
		ended_at, updated_at FROM sessions WHERE session_id = ?`, firstID).Scan(
		&state, &deliverable, &endedAt, &updatedAt))
	require.Equal(t, string(protocol.SessionStateDisconnected), state)
	require.False(t, deliverable)
	require.Equal(t, endedAt, updatedAt)
	previous, err := eventstore.ParseTime(eventstore.FormatTime(testSessionTime))
	require.NoError(t, err)
	updated, err := eventstore.ParseTime(updatedAt)
	require.NoError(t, err)
	require.True(t, updated.After(previous))
}

func TestSessionStoreCollisionRollsBackReplacement(t *testing.T) {
	t.Parallel()
	sessions, db, clk := openSessionStore(t)
	first := validSessionSpec(testInstanceOne)
	firstID, err := sessions.Bind(t.Context(), first)
	require.NoError(t, err)

	collision := validSessionSpec(testInstanceTwo)
	manifest, processJSON, _, err := encodeSessionSpec(collision)
	require.NoError(t, err)
	manifestJSON, err := json.Marshal(manifest)
	require.NoError(t, err)
	stamp := eventstore.FormatTime(clk.now)
	_, err = db.ExecContext(t.Context(), `INSERT INTO sessions
		(session_id, agent_id, state, capture_mode, deliverable, manifest_json,
		 process_json, started_at, ended_at, updated_at, boot_id)
		VALUES (?, ?, 'CONNECTED', ?, 1, ?, ?, ?, NULL, ?, 'different-boot')`,
		collision.SessionID(), collision.AgentID, protocol.CaptureModeTmuxAttach,
		string(manifestJSON), string(processJSON), stamp, stamp)
	require.NoError(t, err)

	clk.now = clk.now.Add(time.Minute)
	_, err = sessions.Bind(t.Context(), collision)
	require.ErrorIs(t, err, ErrSessionCollision)

	var state string
	var deliverable bool
	var endedAt sql.NullString
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT state, deliverable, ended_at
		FROM sessions WHERE session_id = ?`, firstID).Scan(&state, &deliverable, &endedAt))
	require.Equal(t, string(protocol.SessionStateConnected), state)
	require.True(t, deliverable)
	require.False(t, endedAt.Valid)
}

func TestSessionStoreRejectsInvalidSpecsAndForeignSessions(t *testing.T) {
	t.Parallel()
	sessions, db, _ := openSessionStore(t)
	invalid := validSessionSpec(testInstanceOne)
	invalid.HarnessVersion = ""
	_, err := sessions.Bind(t.Context(), invalid)
	require.ErrorIs(t, err, ErrInvalidOptions)

	stamp := eventstore.FormatTime(testSessionTime)
	_, err = db.ExecContext(t.Context(), `INSERT INTO agents
		(agent_id, node_id, harness, created_at, updated_at) VALUES
		('pty-agent', ?, ?, ?, ?)`, testSessionNode, protocol.HarnessCodex, stamp, stamp)
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), `INSERT INTO sessions
		(session_id, agent_id, state, capture_mode, deliverable, manifest_json,
		 process_json, started_at, ended_at, updated_at, boot_id)
		VALUES ('pty-session', 'pty-agent', 'CONNECTED', ?, 1, '{}', '{}', ?, NULL, ?, 'boot')`,
		protocol.CaptureModePTYProxy, stamp, stamp)
	require.NoError(t, err)
	require.NoError(t, sessions.Invalidate(t.Context(), "pty-agent", "pty-session"))

	var state string
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT state FROM sessions
		WHERE session_id = 'pty-session'`).Scan(&state))
	require.Equal(t, string(protocol.SessionStateConnected), state)
}

func TestSessionStoreRefusesToReplaceLivePTYSession(t *testing.T) {
	t.Parallel()
	sessions, db, _ := openSessionStore(t)
	spec := validSessionSpec(testInstanceOne)
	stamp := eventstore.FormatTime(testSessionTime)
	_, err := db.ExecContext(t.Context(), `INSERT INTO agents
		(agent_id, node_id, harness, created_at, updated_at) VALUES (?, ?, ?, ?, ?)`,
		spec.AgentID, testSessionNode, spec.Harness, stamp, stamp)
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), `INSERT INTO sessions
		(session_id, agent_id, state, capture_mode, deliverable, manifest_json,
		 process_json, started_at, ended_at, updated_at, boot_id)
		VALUES ('pty-live', ?, 'CONNECTED', ?, 1, '{}', '{}', ?, NULL, ?, 'boot')`,
		spec.AgentID, protocol.CaptureModePTYProxy, stamp, stamp)
	require.NoError(t, err)

	_, err = sessions.Bind(t.Context(), spec)
	require.ErrorIs(t, err, ErrSessionCollision)
	var state string
	var deliverable bool
	var endedAt sql.NullString
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT state, deliverable, ended_at
		FROM sessions WHERE session_id = 'pty-live'`).Scan(&state, &deliverable, &endedAt))
	require.Equal(t, string(protocol.SessionStateConnected), state)
	require.True(t, deliverable)
	require.False(t, endedAt.Valid)
	var count int
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM sessions
		WHERE capture_mode = ?`, protocol.CaptureModeTmuxAttach).Scan(&count))
	require.Zero(t, count)
}

func TestSessionStoreRefusesToRepairMalformedLiveTmuxSession(t *testing.T) {
	t.Parallel()
	sessions, db, _ := openSessionStore(t)
	spec := validSessionSpec(testInstanceTwo)
	stamp := eventstore.FormatTime(testSessionTime)
	_, err := db.ExecContext(t.Context(), `INSERT INTO agents
		(agent_id, node_id, harness, created_at, updated_at) VALUES (?, ?, ?, ?, ?)`,
		spec.AgentID, testSessionNode, spec.Harness, stamp, stamp)
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), `INSERT INTO sessions
		(session_id, agent_id, state, capture_mode, deliverable, manifest_json,
		 process_json, started_at, ended_at, updated_at, boot_id)
		VALUES (?, ?, 'DISCONNECTED', ?, 0, '{}', '{}', ?, NULL, ?, 'boot')`,
		validSessionSpec(testInstanceOne).SessionID(), spec.AgentID,
		protocol.CaptureModeTmuxAttach, stamp, stamp)
	require.NoError(t, err)

	_, err = sessions.Bind(t.Context(), spec)
	require.ErrorIs(t, err, ErrSessionCollision)
	var state string
	var deliverable bool
	var endedAt sql.NullString
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT state, deliverable, ended_at
		FROM sessions WHERE session_id = ?`, validSessionSpec(testInstanceOne).SessionID()).Scan(
		&state, &deliverable, &endedAt))
	require.Equal(t, string(protocol.SessionStateDisconnected), state)
	require.False(t, deliverable)
	require.False(t, endedAt.Valid)
	var created int
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM sessions
		WHERE session_id = ?`, spec.SessionID()).Scan(&created))
	require.Zero(t, created)
}

func TestSessionStoreDisconnectsSessionsOfOtherBoots(t *testing.T) {
	t.Parallel()
	sessions, db, clk := openSessionStore(t)
	earlier := validSessionSpec(testInstanceOne)
	earlierID, err := sessions.Bind(t.Context(), earlier)
	require.NoError(t, err)
	current := validSessionSpec(testInstanceTwo)
	current.AgentID = "agent-two"
	current.BootID = "boot-two"
	currentID, err := sessions.Bind(t.Context(), current)
	require.NoError(t, err)
	stamp := eventstore.FormatTime(testSessionTime)
	_, err = db.ExecContext(t.Context(), `INSERT INTO agents
		(agent_id, node_id, harness, created_at, updated_at) VALUES ('agent-pty', ?, ?, ?, ?)`,
		testSessionNode, protocol.HarnessCodex, stamp, stamp)
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), `INSERT INTO sessions
		(session_id, agent_id, state, capture_mode, deliverable, manifest_json,
		 process_json, started_at, ended_at, updated_at, boot_id)
		VALUES ('pty-live', 'agent-pty', 'CONNECTED', ?, 1, '{}', '{}', ?, NULL, ?, 'boot-one')`,
		protocol.CaptureModePTYProxy, stamp, stamp)
	require.NoError(t, err)

	clk.now = testSessionTime.Add(-time.Hour)
	closed, err := sessions.DisconnectOtherBoots(t.Context(), "boot-two")
	require.NoError(t, err)
	require.Equal(t, 1, closed)
	for id, want := range map[protocol.OpaqueID]protocol.SessionState{
		earlierID:  protocol.SessionStateDisconnected,
		currentID:  protocol.SessionStateConnected,
		"pty-live": protocol.SessionStateConnected,
	} {
		var state string
		var deliverable bool
		var endedAt sql.NullString
		var updatedAt string
		require.NoError(t, db.QueryRowContext(t.Context(), `SELECT state, deliverable,
			ended_at, updated_at FROM sessions WHERE session_id = ?`, id).Scan(
			&state, &deliverable, &endedAt, &updatedAt))
		require.Equal(t, string(want), state, id)
		require.Equal(t, want == protocol.SessionStateConnected, deliverable, id)
		require.Equal(t, want == protocol.SessionStateDisconnected, endedAt.Valid, id)
		if endedAt.Valid {
			updated, err := eventstore.ParseTime(updatedAt)
			require.NoError(t, err)
			require.True(t, updated.After(testSessionTime), "lifecycle stamps never go back")
		}
	}

	closed, err = sessions.DisconnectOtherBoots(t.Context(), "boot-two")
	require.NoError(t, err)
	require.Zero(t, closed)
	_, err = sessions.DisconnectOtherBoots(t.Context(), "")
	require.ErrorIs(t, err, ErrInvalidOptions)
}
