package agentlink

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

var testNow = time.Date(2026, 10, 30, 18, 0, 0, 0, time.UTC)

const (
	testAgentID   protocol.AgentID  = "agent-1"
	testSessionID protocol.OpaqueID = "session-1"
)

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time                     { return c.now }
func (fixedClock) After(time.Duration) <-chan time.Time { return nil }

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	store, err := eventstore.Open(t.Context(), filepath.Join(t.TempDir(), "gifford.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return store.DB()
}

func testRegistration(agent protocol.AgentID, process protocol.ProcessIdentity) protocol.RegisterAgent {
	return protocol.RegisterAgent{
		Manifest: protocol.CapabilityManifest{
			SchemaVersion: protocol.SchemaVersion, AgentID: agent,
			Harness: protocol.HarnessCodex, HarnessVersion: "1.2.3",
			AdapterType:        protocol.AdapterTypeGenericPTY,
			InputGuardProvider: protocol.InputGuardProviderPTYProxy,
			CaptureMode:        protocol.CaptureModePTYProxy,
			Deliverable:        true,
			Capabilities:       protocol.Capabilities{MCP: true, PTYCapture: true},
		},
		Process: &process,
	}
}

func insertTestSession(
	t *testing.T,
	db *sql.DB,
	session protocol.OpaqueID,
	registration protocol.RegisterAgent,
) {
	t.Helper()
	manifest, err := json.Marshal(registration.Manifest)
	require.NoError(t, err)
	process, err := json.Marshal(registration.Process)
	require.NoError(t, err)
	stamp := eventstore.FormatTime(testNow)
	_, err = db.ExecContext(t.Context(), `INSERT INTO agents
		(agent_id, node_id, harness, created_at, updated_at) VALUES (?, ?, ?, ?, ?)`,
		registration.Manifest.AgentID, "node-test", registration.Manifest.Harness, stamp, stamp)
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), `INSERT INTO sessions
		(session_id, agent_id, state, capture_mode, deliverable, manifest_json,
		 process_json, started_at, ended_at, updated_at)
		VALUES (?, ?, 'CONNECTED', 'pty_proxy', 1, ?, ?, ?, NULL, ?)`,
		session, registration.Manifest.AgentID, string(manifest), string(process), stamp, stamp)
	require.NoError(t, err)
}

func TestInputGuardStoreWritesManifestProfileAndOnlyRisingVersions(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	registration := testRegistration(testAgentID, protocol.ProcessIdentity{
		PID: 1, ProcessStartTime: 1, UID: 1, GID: 1,
	})
	insertTestSession(t, db, testSessionID, registration)
	store, err := NewInputGuardStore(db, fixedClock{now: testNow})
	require.NoError(t, err)
	humanKeyAt := testNow.Add(-time.Second)
	report := protocol.InputGuardReport{
		AgentID: testAgentID, SessionID: testSessionID,
		DraftState: protocol.DraftStateOpen, ActivityState: protocol.ActivityStateActive,
		DraftGeneration: 3, InputGuardVersion: 5, LastHumanKeyAt: humanKeyAt,
		DetectionSource: protocol.InputGuardProviderPTYProxy, Confidence: 1,
	}
	written, err := store.WriteInputGuard(t.Context(), report)
	require.NoError(t, err)
	require.True(t, written)

	var (
		agent, draftState, activityState, humanAt, source, profile, updated string
		generation, version                                                 int64
		confidence                                                          float64
		draftOpened, lastSubmit, lastCancel                                 sql.NullString
	)
	err = db.QueryRowContext(t.Context(), `SELECT agent_id, draft_generation, draft_state,
		activity_state, draft_opened_at, last_human_key_at, last_submit_at, last_cancel_at,
		detection_source, submit_detector_profile, confidence, version, updated_at
		FROM input_guards WHERE session_id = ?`, report.SessionID).Scan(
		&agent, &generation, &draftState, &activityState, &draftOpened, &humanAt,
		&lastSubmit, &lastCancel, &source, &profile, &confidence, &version, &updated)
	require.NoError(t, err)
	require.Equal(t, string(testAgentID), agent)
	require.Equal(t, int64(3), generation)
	require.Equal(t, string(protocol.DraftStateOpen), draftState)
	require.Equal(t, string(protocol.ActivityStateActive), activityState)
	require.False(t, draftOpened.Valid)
	require.Equal(t, eventstore.FormatTime(humanKeyAt), humanAt)
	require.False(t, lastSubmit.Valid)
	require.False(t, lastCancel.Valid)
	require.Equal(t, string(protocol.InputGuardProviderPTYProxy), source)
	require.Equal(t, "profiles/codex.yaml", profile)
	require.InDelta(t, 1, confidence, 0)
	require.Equal(t, int64(5), version)
	require.Equal(t, eventstore.FormatTime(testNow), updated)

	stale := report
	stale.InputGuardVersion = 5
	stale.DraftGeneration = 99
	written, err = store.WriteInputGuard(t.Context(), stale)
	require.NoError(t, err)
	require.False(t, written)
	stale.InputGuardVersion = 4
	written, err = store.WriteInputGuard(t.Context(), stale)
	require.NoError(t, err)
	require.False(t, written)

	newer := report
	newer.InputGuardVersion = 6
	newer.DraftGeneration = 4
	newer.LastHumanKeyAt = time.Time{}
	// A report already received by the authenticated link remains durable
	// when Hub shutdown marks its session disconnected before this write.
	ended := eventstore.FormatTime(testNow.Add(time.Second))
	_, err = db.ExecContext(t.Context(), `UPDATE sessions SET state = 'DISCONNECTED',
		ended_at = ?, updated_at = ? WHERE session_id = ?`, ended, ended, testSessionID)
	require.NoError(t, err)
	written, err = store.WriteInputGuard(t.Context(), newer)
	require.NoError(t, err)
	require.True(t, written)
	var gotGeneration, gotVersion int64
	var gotHuman sql.NullString
	err = db.QueryRowContext(t.Context(), `SELECT draft_generation, version, last_human_key_at
		FROM input_guards WHERE session_id = ?`, report.SessionID).
		Scan(&gotGeneration, &gotVersion, &gotHuman)
	require.NoError(t, err)
	require.Equal(t, int64(4), gotGeneration)
	require.Equal(t, int64(6), gotVersion)
	require.False(t, gotHuman.Valid)
}

func TestInputGuardStoreRejectsCrossedSession(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	insertTestSession(t, db, testSessionID, testRegistration(testAgentID, protocol.ProcessIdentity{
		PID: 1, ProcessStartTime: 1, UID: 1, GID: 1,
	}))
	store, err := NewInputGuardStore(db, fixedClock{now: testNow})
	require.NoError(t, err)
	report := protocol.InputGuardReport{
		AgentID: "agent-2", SessionID: testSessionID,
		DraftState: protocol.DraftStateClosed, ActivityState: protocol.ActivityStateIdle,
		DetectionSource: protocol.InputGuardProviderPTYProxy, Confidence: 1,
	}
	_, err = store.WriteInputGuard(context.Background(), report)
	require.ErrorIs(t, err, ErrSessionUnavailable)
}
