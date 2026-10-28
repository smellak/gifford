package channel

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

// testControlTime is the fixed clock of the control rows.
var testControlTime = time.Date(2026, time.October, 28, 14, 0, 0, 0, time.UTC)

// testRow builds a valid global ACTIVE row.
func testRow() control {
	return control{
		id:        "ctl_00000000000000000000000001",
		scope:     protocol.ScopeTypeGlobal,
		key:       "*",
		plane:     protocol.PlaneAll,
		mode:      protocol.ControlModeActive,
		actor:     protocol.ActorTypeHuman,
		createdBy: testOperator,
		createdAt: testControlTime,
		version:   1,
		updatedAt: testControlTime,
	}
}

// TestValidateControl checks one row per scoped rule.
func TestValidateControl(t *testing.T) {
	t.Parallel()

	on := true
	cases := []struct {
		name  string
		valid bool
		rows  func() []control
	}{
		{name: "global passes", valid: true, rows: func() []control { return []control{testRow()} }},
		{name: "global key is star", rows: func() []control {
			row := testRow()
			row.key = testNodeA
			return []control{row}
		}},
		{name: "global plane is all", rows: func() []control {
			row := testRow()
			row.plane = protocol.PlaneMessage
			return []control{row}
		}},
		{name: "global mode is a channel mode", rows: func() []control {
			row := testRow()
			row.mode = protocol.ControlModeAllow
			return []control{row}
		}},
		{name: "node passes", valid: true, rows: func() []control {
			row := testRow()
			row.scope = protocol.ScopeTypeNode
			row.key = testNodeA
			return []control{row}
		}},
		{name: "node needs a key", rows: func() []control {
			row := testRow()
			row.scope = protocol.ScopeTypeNode
			row.key = ""
			return []control{row}
		}},
		{name: "agent passes", valid: true, rows: func() []control {
			row := testRow()
			row.scope = protocol.ScopeTypeAgent
			row.key = testAgent1
			row.mode = ""
			row.tx = &on
			return []control{row}
		}},
		{name: "agent carries no mode", rows: func() []control {
			row := testRow()
			row.scope = protocol.ScopeTypeAgent
			row.key = testAgent1
			row.tx = &on
			return []control{row}
		}},
		{name: "agent needs a flag", rows: func() []control {
			row := testRow()
			row.scope = protocol.ScopeTypeAgent
			row.key = testAgent1
			row.mode = ""
			return []control{row}
		}},
		{name: "link passes", valid: true, rows: func() []control {
			row := testRow()
			row.scope = protocol.ScopeTypeLink
			row.key = linkKey(testAgent1, testAgent2)
			row.plane = protocol.PlaneMessage
			row.mode = protocol.ControlModeHold
			row.from = testAgent1
			row.to = testAgent2
			return []control{row}
		}},
		{name: "link key joins endpoints", rows: func() []control {
			row := testRow()
			row.scope = protocol.ScopeTypeLink
			row.key = linkKey(testAgent1, testAgent2)
			row.plane = protocol.PlaneMessage
			row.mode = protocol.ControlModeHold
			row.from = testAgent1
			row.to = testAgent1
			return []control{row}
		}},
		{name: "thread passes", valid: true, rows: func() []control {
			row := testRow()
			row.scope = protocol.ScopeTypeThread
			row.key = testThreadID
			row.mode = protocol.ControlModeHold
			return []control{row}
		}},
		{name: "thread plane is all", rows: func() []control {
			row := testRow()
			row.scope = protocol.ScopeTypeThread
			row.key = testThreadID
			row.plane = protocol.PlaneMessage
			row.mode = protocol.ControlModeHold
			return []control{row}
		}},
		{name: "loop override passes", valid: true, rows: func() []control {
			row := testRow()
			row.scope = protocol.ScopeTypeLoop
			row.key = testLoopID
			row.mode = protocol.ControlModePaused
			return []control{row}
		}},
		{name: "loop override is paused or stopped", rows: func() []control {
			row := testRow()
			row.scope = protocol.ScopeTypeLoop
			row.key = testLoopID
			row.mode = protocol.ControlModeAllow
			return []control{row}
		}},
		{name: "restore needs expiry", rows: func() []control {
			row := testRow()
			row.restore = &protocol.ControlSnapshot{Version: 1}
			return []control{row}
		}},
		{name: "then off needs draining", rows: func() []control {
			row := testRow()
			row.thenMode = protocol.ControlModeOff
			return []control{row}
		}},
		{name: "then mode only off", rows: func() []control {
			row := testRow()
			row.mode = protocol.ControlModeDraining
			row.thenMode = protocol.ControlModePaused
			return []control{row}
		}},
		{name: "updated never before created", rows: func() []control {
			row := testRow()
			row.updatedAt = row.createdAt.Add(-time.Second)
			return []control{row}
		}},
		{name: "duplicate keys", rows: func() []control {
			return []control{testRow(), testRow()}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := validateControl(tc.rows())
			if tc.valid {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, protocol.ErrInvalid)
		})
	}
}

// TestRestoreTarget checks the three restore shapes.
// testSnapshot builds a legal restore snapshot with the given mode.
func testSnapshot(mode protocol.ControlMode) *protocol.ControlSnapshot {
	return &protocol.ControlSnapshot{
		Mode:      mode,
		ActorType: protocol.ActorTypeSystem,
		CreatedBy: "system",
		ExpiresAt: testControlTime.Add(time.Hour),
		Version:   4,
	}
}

func TestRestoreTarget(t *testing.T) {
	t.Parallel()

	t.Run("no restore", func(t *testing.T) {
		t.Parallel()

		target, err := restoreTarget(testRow())
		require.NoError(t, err)
		require.Nil(t, target)
	})
	t.Run("snapshot copy", func(t *testing.T) {
		t.Parallel()

		row := testRow()
		row.expiresAt = testControlTime.Add(time.Hour)
		row.restore = testSnapshot(protocol.ControlModePaused)
		target, err := restoreTarget(row)
		require.NoError(t, err)
		require.NotNil(t, target)
		require.Equal(t, row.id, target.id)
		require.Equal(t, row.scope, target.scope)
		require.Equal(t, row.key, target.key)
		require.Equal(t, protocol.ControlModePaused, target.mode)
		require.Equal(t, protocol.ActorTypeSystem, target.actor)
		require.Equal(t, "system", target.createdBy)
		require.Equal(t, row.restore.ExpiresAt, target.expiresAt)
		require.Equal(t, int64(4), target.version)
		require.Nil(t, target.restore)
	})
	t.Run("expired snapshot returned", func(t *testing.T) {
		t.Parallel()

		row := testRow()
		row.expiresAt = testControlTime.Add(-time.Minute)
		row.restore = testSnapshot(protocol.ControlModePaused)
		row.restore.ExpiresAt = testControlTime.Add(-time.Minute)
		target, err := restoreTarget(row)
		require.NoError(t, err)
		require.NotNil(t, target)
		require.Equal(t, protocol.ControlModePaused, target.mode)
	})
	t.Run("illegal snapshot", func(t *testing.T) {
		t.Parallel()

		link := controlFromRoot(testLinkRoot(testControlID(21), testAgent2, testAgent1, protocol.PlaneMessage, protocol.LinkModeDeny))
		for _, mode := range []protocol.ControlMode{protocol.ControlModePaused, protocol.ControlModeActive} {
			link.restore = testSnapshot(mode)
			_, err := restoreTarget(link)
			require.ErrorIs(t, err, ErrInvalidControl, "link restoring %s", mode)
		}
		global := testRow()
		global.restore = testSnapshot(protocol.ControlModeHold)
		_, err := restoreTarget(global)
		require.ErrorIs(t, err, ErrInvalidControl, "global restoring HOLD")
	})
	t.Run("load rejects illegal restore", func(t *testing.T) {
		t.Parallel()

		bad := testLinkRoot(testControlID(22), testAgent1, testAgent2, protocol.PlaneMessage, protocol.LinkModeDeny)
		bad.ExpiresAt = testControlTime.Add(time.Hour)
		bad.Restore = testSnapshot(protocol.ControlModePaused)
		engine, err := New(Options{Clock: testClock{now: testEngineTime}})
		require.NoError(t, err)
		require.ErrorIs(t, engine.load([]protocol.ChannelControl{bad}), ErrInvalidControl)
	})
}
