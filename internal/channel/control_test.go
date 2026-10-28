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
		createdBy: "operator",
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
			row.key = "node-a"
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
			row.key = "node-a"
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
			row.key = "agent-1"
			row.mode = ""
			row.tx = &on
			return []control{row}
		}},
		{name: "agent carries no mode", rows: func() []control {
			row := testRow()
			row.scope = protocol.ScopeTypeAgent
			row.key = "agent-1"
			row.tx = &on
			return []control{row}
		}},
		{name: "agent needs a flag", rows: func() []control {
			row := testRow()
			row.scope = protocol.ScopeTypeAgent
			row.key = "agent-1"
			row.mode = ""
			return []control{row}
		}},
		{name: "link passes", valid: true, rows: func() []control {
			row := testRow()
			row.scope = protocol.ScopeTypeLink
			row.key = linkKey("agent-1", "agent-2")
			row.plane = protocol.PlaneMessage
			row.mode = protocol.ControlModeHold
			row.from = "agent-1"
			row.to = "agent-2"
			return []control{row}
		}},
		{name: "link key joins endpoints", rows: func() []control {
			row := testRow()
			row.scope = protocol.ScopeTypeLink
			row.key = linkKey("agent-1", "agent-2")
			row.plane = protocol.PlaneMessage
			row.mode = protocol.ControlModeHold
			row.from = "agent-1"
			row.to = "agent-1"
			return []control{row}
		}},
		{name: "thread passes", valid: true, rows: func() []control {
			row := testRow()
			row.scope = protocol.ScopeTypeThread
			row.key = "thread-1"
			row.mode = protocol.ControlModeHold
			return []control{row}
		}},
		{name: "thread plane is all", rows: func() []control {
			row := testRow()
			row.scope = protocol.ScopeTypeThread
			row.key = "thread-1"
			row.plane = protocol.PlaneMessage
			row.mode = protocol.ControlModeHold
			return []control{row}
		}},
		{name: "loop override passes", valid: true, rows: func() []control {
			row := testRow()
			row.scope = protocol.ScopeTypeLoop
			row.key = "loop_01JB0000000000000000000003"
			row.mode = protocol.ControlModePaused
			return []control{row}
		}},
		{name: "loop override is paused or stopped", rows: func() []control {
			row := testRow()
			row.scope = protocol.ScopeTypeLoop
			row.key = "loop_01JB0000000000000000000003"
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
func TestRestoreTarget(t *testing.T) {
	t.Parallel()

	t.Run("no restore", func(t *testing.T) {
		t.Parallel()

		require.Nil(t, restoreTarget(testRow(), testControlTime))
	})
	t.Run("snapshot while due", func(t *testing.T) {
		t.Parallel()

		row := testRow()
		row.expiresAt = testControlTime.Add(time.Hour)
		row.restore = &protocol.ControlSnapshot{
			Mode:      protocol.ControlModePaused,
			ActorType: protocol.ActorTypeSystem,
			CreatedBy: "system",
			Version:   4,
		}
		target := restoreTarget(row, testControlTime)
		require.NotNil(t, target)
		require.Equal(t, row.id, target.id)
		require.Equal(t, row.scope, target.scope)
		require.Equal(t, row.key, target.key)
		require.Equal(t, protocol.ControlModePaused, target.mode)
		require.Equal(t, int64(4), target.version)
	})
	t.Run("then mode once expired", func(t *testing.T) {
		t.Parallel()

		row := testRow()
		row.mode = protocol.ControlModeDraining
		row.thenMode = protocol.ControlModeOff
		row.expiresAt = testControlTime.Add(-time.Minute)
		row.restore = &protocol.ControlSnapshot{Version: 4}
		target := restoreTarget(row, testControlTime)
		require.NotNil(t, target)
		require.Equal(t, protocol.ControlModeOff, target.mode)
		require.Equal(t, int64(4), target.version)
		require.Nil(t, target.restore)
	})
	t.Run("expired without then", func(t *testing.T) {
		t.Parallel()

		row := testRow()
		row.expiresAt = testControlTime.Add(-time.Minute)
		row.restore = &protocol.ControlSnapshot{Version: 4}
		require.Nil(t, restoreTarget(row, testControlTime))
	})
}
