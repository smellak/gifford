package channel

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

// TestReloadRestoreJSONTrailing verifies that a restore_json with trailing
// data after a valid document fails closed, leaves the cache invalidated,
// and does not copy the raw data into the error.
func TestReloadRestoreJSONTrailing(t *testing.T) {
	t.Parallel()

	suffixes := []struct {
		name   string
		suffix string
	}{
		{"space_brace", " {}"},
		{"space_null", " null"},
		{"space_bang", " !"},
	}

	for _, tc := range suffixes {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := testChannelStore(t)
			ctx := context.Background()
			db := store.DB()

			base := testNodeRoot(testControlID(1), testNodeA, protocol.ChannelModeActive)
			base.Version = 1
			base.ExpiresAt = testEngineTime.Add(time.Hour)
			base.Restore = testSnapshot(protocol.ControlModePaused)
			testInsertControl(ctx, t, db, base)

			engine, err := New(Options{Clock: testClock{now: testEngineTime}})
			require.NoError(t, err)
			require.NoError(t, engine.Reload(ctx, db))

			// Verify cache is loaded.
			_, err = engine.Effective(testDeliveryQuery())
			require.NoError(t, err)

			// Insert a second control whose restore_json is valid JSON plus trailing suffix.
			trailing := testNodeRoot(testControlID(2), "node-b", protocol.ChannelModeActive)
			trailing.Version = 1
			trailing.ExpiresAt = testEngineTime.Add(time.Hour)
			trailing.Restore = testSnapshot(protocol.ControlModePaused)
			params, err := controlParams(trailing)
			require.NoError(t, err)
			// params[15] is restore_json.
			require.NotNil(t, params[15])
			raw, ok := params[15].(string)
			require.True(t, ok)
			params[15] = raw + tc.suffix

			_, err = db.ExecContext(ctx, testQueryText(t, "controls_insert.sql"), params...)
			require.NoError(t, err)

			err = engine.Reload(ctx, db)
			require.Error(t, err)
			// Error must be wrapped and must not copy the trailing raw data.
			require.ErrorContains(t, err, "bad JSON")
			// Ensure the error does not leak the full raw suffix with surrounding JSON.
			// At most it may mention the unexpected token, but not the whole trailing document.
			if tc.suffix == " {}" || tc.suffix == " null" {
				require.NotContains(t, err.Error(), raw+tc.suffix)
			}
			// Cache must be invalidated: loaded false, entries/stale empty, fail closed.
			require.False(t, engine.loaded)
			require.Empty(t, engine.entries)
			require.Empty(t, engine.stale)
			_, err = engine.Effective(testDeliveryQuery())
			require.ErrorIs(t, err, ErrControlUnavailable)

			verdict, _, _, err := engine.evaluate(testDeliveryQuery())
			require.NoError(t, err)
			require.Equal(t, Hold, verdict.outcome)
			require.Equal(t, protocol.DecisionReasonControlUnavailable, verdict.reason)

			// Ensure the DB still holds the bad row (no rollback of insert), but cache is cold.
			var count int
			require.NoError(t, db.QueryRowContext(ctx, "SELECT count(*) FROM channel_controls").Scan(&count))
			require.Equal(t, 2, count)

			// Recovery: removing the bad row and reloading with valid data must succeed.
			_, err = db.ExecContext(ctx, testQueryText(t, "controls_delete_cas.sql"),
				string(trailing.ScopeType), trailing.ScopeKey, string(trailing.Plane), trailing.Version)
			require.NoError(t, err)
			require.NoError(t, engine.Reload(ctx, db))
			require.True(t, engine.loaded)
			_, err = engine.Effective(testDeliveryQuery())
			require.NoError(t, err)
			verdict, _, _, err = engine.evaluate(testDeliveryQuery())
			require.NoError(t, err)
			require.NotEqual(t, protocol.DecisionReasonControlUnavailable, verdict.reason)
		})
	}
}

func TestReloadRestoreJSONWhitespaceAllowed(t *testing.T) {
	t.Parallel()
	store := testChannelStore(t)
	ctx := context.Background()
	db := store.DB()

	base := testNodeRoot(testControlID(1), testNodeA, protocol.ChannelModeActive)
	base.Version = 1
	base.ExpiresAt = testEngineTime.Add(time.Hour)
	base.Restore = testSnapshot(protocol.ControlModePaused)
	testInsertControl(ctx, t, db, base)

	engine, err := New(Options{Clock: testClock{now: testEngineTime}})
	require.NoError(t, err)
	require.NoError(t, engine.Reload(ctx, db))

	whitespace := testNodeRoot(testControlID(2), "node-b", protocol.ChannelModeActive)
	whitespace.Version = 1
	whitespace.ExpiresAt = testEngineTime.Add(time.Hour)
	whitespace.Restore = testSnapshot(protocol.ControlModePaused)
	params, err := controlParams(whitespace)
	require.NoError(t, err)
	raw, ok := params[15].(string)
	require.True(t, ok)
	params[15] = raw + "   \n\t  "
	_, err = db.ExecContext(ctx, testQueryText(t, "controls_insert.sql"), params...)
	require.NoError(t, err)

	require.NoError(t, engine.Reload(ctx, db))
	_, err = engine.Effective(testDeliveryQuery())
	require.NoError(t, err)
	verdict, _, _, err := engine.evaluate(testDeliveryQuery())
	require.NoError(t, err)
	// With two valid rows, verdict should not be CONTROL_UNAVAILABLE.
	require.NotEqual(t, protocol.DecisionReasonControlUnavailable, verdict.reason)
}

func TestReloadRestoreJSONUnknownFieldStillRejected(t *testing.T) {
	t.Parallel()
	store := testChannelStore(t)
	ctx := context.Background()
	db := store.DB()

	base := testNodeRoot(testControlID(1), testNodeA, protocol.ChannelModeActive)
	base.Version = 1
	base.ExpiresAt = testEngineTime.Add(time.Hour)
	base.Restore = testSnapshot(protocol.ControlModePaused)
	testInsertControl(ctx, t, db, base)

	engine, err := New(Options{Clock: testClock{now: testEngineTime}})
	require.NoError(t, err)
	require.NoError(t, engine.Reload(ctx, db))

	unknown := testNodeRoot(testControlID(2), "node-b", protocol.ChannelModeActive)
	unknown.Version = 1
	unknown.ExpiresAt = testEngineTime.Add(time.Hour)
	unknown.Restore = testSnapshot(protocol.ControlModePaused)
	params, err := controlParams(unknown)
	require.NoError(t, err)
	raw, ok := params[15].(string)
	require.True(t, ok)
	// Inject unknown field into valid JSON.
	bad := `{"mode":"PAUSED","actor_type":"system","created_by":"system","version":4,"unknown_field":1}`
	// Ensure raw is valid and replace with bad JSON (use raw with unknown field plus no trailing, just unknown).
	_ = raw
	params[15] = bad
	_, err = db.ExecContext(ctx, testQueryText(t, "controls_insert.sql"), params...)
	require.NoError(t, err)

	err = engine.Reload(ctx, db)
	require.Error(t, err)
	require.ErrorContains(t, err, "bad JSON")
	require.False(t, engine.loaded)
	_, err = engine.Effective(testDeliveryQuery())
	require.ErrorIs(t, err, ErrControlUnavailable)
}

// Ensure sql import is used.
var _ = sql.ErrNoRows
