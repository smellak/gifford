package channel

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

// testInsertControl stores one row with controls_insert.sql.
func testInsertControl(t *testing.T, ctx context.Context, db *sql.DB, root protocol.ChannelControl) {
	t.Helper()
	params, err := controlParams(root)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, testQueryText(t, "controls_insert.sql"), params...)
	require.NoError(t, err)
}

// updateParams encodes the controls_update_cas.sql parameters for a row
// carrying its new version, with the expected version last.
func updateParams(t *testing.T, root protocol.ChannelControl, expected int64) []any {
	t.Helper()
	params, err := controlParams(root)
	require.NoError(t, err)
	// Insert order: id, scope, key, plane, mode, tx, rx, autoDeliv,
	// autoCont, from, to, reason, actor, createdBy, expires, restore,
	// thenMode, version, createdAt, updatedAt.
	return []any{
		params[4], params[5], params[6], params[7], params[8], params[11],
		params[12], params[13], params[14], params[15], params[16],
		root.Version, params[19],
		params[1], params[2], params[3], expected,
	}
}

// testReloadEngine reloads a fresh engine or fails the test.
func testReloadEngine(t *testing.T, ctx context.Context, db *sql.DB) *Engine {
	t.Helper()
	engine, err := New(Options{Clock: testClock{now: testEngineTime}})
	require.NoError(t, err)
	require.NoError(t, engine.Reload(ctx, db))
	return engine
}

// TestReload materializes inserted rows into the cache, honours the
// compare-and-swap version on update, and fails closed on an invalid row.
func TestReload(t *testing.T) {
	t.Parallel()

	store := testChannelStore(t)
	ctx := t.Context()
	db := store.DB()
	global := testEngineRoot(testControlID(1))
	global.Mode = protocol.ControlMode(protocol.ChannelModePaused)
	global.Version = 2
	link := testLinkRoot(testControlID(2), testAgent1, testAgent2, protocol.PlaneMessage, protocol.LinkModeHold)
	link.Version = 7
	agent := testAgentRoot(testControlID(3), testAgent1, false)
	testInsertControl(t, ctx, db, global)
	testInsertControl(t, ctx, db, link)
	testInsertControl(t, ctx, db, agent)

	engine := testReloadEngine(t, ctx, db)
	query := testDeliveryQuery()
	verdict, _, versions, err := engine.evaluate(query)
	require.NoError(t, err)
	require.Equal(t, Hold, verdict.outcome)
	require.Equal(t, protocol.DecisionReasonChannelPaused, verdict.reason)
	require.Equal(t, int64(2), versions[testGlobalKey()])
	require.Equal(t, int64(7), versions[testLinkVersionKey])
	require.Equal(t, int64(1), versions[testAgentKey(testAgent1)])

	second := testEngineRoot(testControlID(9))
	second.Mode = protocol.ControlMode(protocol.ChannelModeOff)
	params, err := controlParams(second)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, testQueryText(t, "controls_insert.sql"), params...)
	require.ErrorContains(t, err, "UNIQUE")

	link.Version = 8
	result, err := db.ExecContext(ctx, testQueryText(t, "controls_update_cas.sql"), updateParams(t, link, 7)...)
	require.NoError(t, err)
	requireRowsAffected(t, result, 1)
	result, err = db.ExecContext(ctx, testQueryText(t, "controls_update_cas.sql"), updateParams(t, link, 7)...)
	require.NoError(t, err)
	requireRowsAffected(t, result, 0)

	engine = testReloadEngine(t, ctx, db)
	_, _, versions, err = engine.evaluate(query)
	require.NoError(t, err)
	require.Equal(t, int64(8), versions[testLinkVersionKey])

	stale, err := New(Options{Clock: testClock{now: testEngineTime}})
	require.NoError(t, err)
	require.NoError(t, stale.Reload(ctx, db))
	invalid := testLinkRoot(testControlID(4), testAgent1, testAgent2, protocol.PlaneAll, protocol.LinkModeHold)
	testInsertControl(t, ctx, db, invalid)
	require.ErrorIs(t, stale.Reload(ctx, db), ErrInvalidControl)
	failed, _, _, err := stale.evaluate(query)
	require.NoError(t, err)
	require.Equal(t, Hold, failed.outcome)
	require.Equal(t, protocol.DecisionReasonControlUnavailable, failed.reason)
	_, err = stale.Effective(query)
	require.ErrorIs(t, err, ErrControlUnavailable)

	result, err = db.ExecContext(ctx, testQueryText(t, "controls_delete_cas.sql"),
		string(invalid.ScopeType), invalid.ScopeKey, string(invalid.Plane), invalid.Version)
	require.NoError(t, err)
	requireRowsAffected(t, result, 1)
	testReloadEngine(t, ctx, db)
}

// requireRowsAffected requires an exact affected row count.
func requireRowsAffected(t *testing.T, result sql.Result, want int64) {
	t.Helper()
	got, err := result.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, want, got)
}

// decodeDecisionRow reads one decisions_select_by_object row back into a
// record.
func decodeDecisionRow(t *testing.T, rows *sql.Rows) protocol.DecisionRecord {
	t.Helper()
	require.True(t, rows.Next())
	var rec protocol.DecisionRecord
	var from, to, actor sql.NullString
	var matchedJSON, effectiveJSON, versionsJSON, explanationJSON, createdAt string
	require.NoError(t, rows.Scan(
		&rec.DecisionID, &rec.ObjectType, &rec.ObjectID, &rec.Stage, &rec.Plane,
		&from, &to,
		&rec.Outcome, &rec.Reason,
		&matchedJSON, &effectiveJSON, &versionsJSON, &explanationJSON,
		&actor, &createdAt,
	))
	rec.FromAgent = protocol.AgentID(from.String)
	rec.ToAgent = protocol.AgentID(to.String)
	require.NoError(t, json.Unmarshal([]byte(matchedJSON), &rec.MatchedControlIDs))
	require.NoError(t, json.Unmarshal([]byte(effectiveJSON), &rec.EffectiveControl))
	require.NoError(t, json.Unmarshal([]byte(versionsJSON), &rec.ControlVersions))
	require.NoError(t, json.Unmarshal([]byte(explanationJSON), &rec.Explanation))
	rec.ActorID = actor.String
	parsed, err := parseChannelTime(createdAt)
	require.NoError(t, err)
	rec.CreatedAt = parsed
	require.False(t, rows.Next())
	require.NoError(t, rows.Err())
	return rec
}

// TestDecisionRecord stores the link_hold decision and reads it back
// unchanged.
func TestDecisionRecord(t *testing.T) {
	t.Parallel()

	store := testChannelStore(t)
	ctx := t.Context()
	db := store.DB()
	link := testLinkRoot(testControlID(3), testAgent1, testAgent2, protocol.PlaneMessage, protocol.LinkModeHold)
	link.Version = 7
	link.Reason = "pairing session"
	link.ExpiresAt = testEngineTime.Add(10 * time.Minute)
	engine := goldenEngine(t)
	applyStored(t, engine, link)
	query := goldenQuery()
	verdict, eff, versions, err := engine.evaluate(query)
	require.NoError(t, err)
	explained, err := engine.Explain(query)
	require.NoError(t, err)
	rec, err := recordOf("chd_00000000000000000000000001", query, verdict, eff, versions, explained, testEngineTime)
	require.NoError(t, err)
	decided, err := newDecision(rec, verdict.class, verdict.line)
	require.NoError(t, err)

	params, err := decisionRecord(decided)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, testQueryText(t, "decisions_insert.sql"), params...)
	require.NoError(t, err)

	rows, err := db.QueryContext(ctx, testQueryText(t, "decisions_select_by_object.sql"),
		string(rec.ObjectType), string(rec.ObjectID))
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	decoded := decodeDecisionRow(t, rows)
	require.Equal(t, decided.Record(), decoded)
	require.NoError(t, decoded.Validate())
}

// TestTimeLayoutMatchesEventStore requires the copied time layout to stay
// equal to the event store's canonical layout.
func TestTimeLayoutMatchesEventStore(t *testing.T) {
	t.Parallel()

	require.Equal(t, eventstore.TimeLayout, timeLayout)
}
