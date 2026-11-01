package protocol

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Sample control identifiers and times shared by the channel tables.
const testControlID = "ctl_01JB0000000000000000000000"

func testControlTime() time.Time {
	return time.Date(2026, time.October, 28, 13, 0, 0, 0, time.UTC)
}

func validGlobalControl() ChannelControl {
	at := testControlTime()
	return ChannelControl{
		ControlID: testControlID,
		ScopeType: ScopeTypeGlobal,
		ScopeKey:  "*",
		Plane:     PlaneAll,
		Mode:      ControlModeActive,
		ActorType: ActorTypeHuman,
		CreatedBy: "op-1",
		Version:   18,
		CreatedAt: at,
		UpdatedAt: at,
	}
}

// TestChannelControlRules covers the legal fields per scope of §6.2.
func TestChannelControlRules(t *testing.T) {
	t.Parallel()

	on := true
	cases := []struct {
		name  string
		valid bool
		patch func(*ChannelControl)
	}{
		{name: "global passes", valid: true, patch: func(*ChannelControl) {}},
		{name: "global key is star", patch: func(c *ChannelControl) { c.ScopeKey = testNode }},
		{name: "global plane is all", patch: func(c *ChannelControl) { c.Plane = PlaneMessage }},
		{name: "global mode required", patch: func(c *ChannelControl) { c.Mode = "" }},
		{name: "global rejects flags", patch: func(c *ChannelControl) { c.TxEnabled = &on }},
		{name: "draining with then off passes", valid: true, patch: func(c *ChannelControl) {
			c.Mode = ControlModeDraining
			c.ThenMode = ControlModeOff
		}},
		{name: "then off needs draining", patch: func(c *ChannelControl) { c.ThenMode = ControlModeOff }},
		{name: "then mode only off", patch: func(c *ChannelControl) {
			c.Mode = ControlModeDraining
			c.ThenMode = ControlModePaused
		}},
		{name: "node passes", valid: true, patch: func(c *ChannelControl) {
			c.ScopeType = ScopeTypeNode
			c.ScopeKey = testNode
		}},
		{name: "agent passes", valid: true, patch: func(c *ChannelControl) {
			c.ScopeType = ScopeTypeAgent
			c.ScopeKey = testAgentID
			c.Mode = ""
			c.TxEnabled = &on
		}},
		{name: "agent needs a flag", patch: func(c *ChannelControl) {
			c.ScopeType = ScopeTypeAgent
			c.ScopeKey = testAgentID
			c.Mode = ""
		}},
		{name: "agent carries no mode", patch: func(c *ChannelControl) {
			c.ScopeType = ScopeTypeAgent
			c.ScopeKey = testAgentID
			c.TxEnabled = &on
		}},
		{name: "link passes", valid: true, patch: func(c *ChannelControl) {
			c.ScopeType = ScopeTypeLink
			c.ScopeKey = testAgentID + linkArrow + testAgent2
			c.Plane = PlaneMessage
			c.Mode = ControlModeAllow
			c.FromAgent = testAgentID
			c.ToAgent = testAgent2
		}},
		{name: "link key joins endpoints", patch: func(c *ChannelControl) {
			c.ScopeType = ScopeTypeLink
			c.ScopeKey = testAgentID + linkArrow + testAgent2
			c.Plane = PlaneMessage
			c.Mode = ControlModeAllow
			c.FromAgent = testAgentID
			c.ToAgent = testAgentID
		}},
		{name: "link needs link mode", patch: func(c *ChannelControl) {
			c.ScopeType = ScopeTypeLink
			c.ScopeKey = testAgentID + linkArrow + testAgent2
			c.Plane = PlaneMessage
			c.Mode = ControlModeActive
			c.FromAgent = testAgentID
			c.ToAgent = testAgent2
		}},
		{name: "thread passes", valid: true, patch: func(c *ChannelControl) {
			c.ScopeType = ScopeTypeThread
			c.ScopeKey = testThread
			c.Mode = ControlModeHold
		}},
		{name: "loop override passes", valid: true, patch: func(c *ChannelControl) {
			c.ScopeType = ScopeTypeLoop
			c.ScopeKey = testLoop
			c.Mode = ControlModePaused
		}},
		{name: "loop override is paused or stopped", patch: func(c *ChannelControl) {
			c.ScopeType = ScopeTypeLoop
			c.ScopeKey = testLoop
			c.Mode = ControlModeAllow
		}},
		{name: "restore needs expiry", patch: func(c *ChannelControl) {
			c.Restore = &ControlSnapshot{ActorType: ActorTypeSystem, CreatedBy: "system", Version: 1}
		}},
		{name: "updated never before created", patch: func(c *ChannelControl) {
			c.UpdatedAt = c.CreatedAt.Add(-time.Second)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			control := validGlobalControl()
			tc.patch(&control)
			if tc.valid {
				require.NoError(t, control.Validate())
				return
			}
			require.ErrorIs(t, control.Validate(), ErrInvalid)
		})
	}
}

// TestChannelStateRules covers the global and node views.
func TestChannelStateRules(t *testing.T) {
	t.Parallel()

	control := validGlobalControl()
	state := ChannelState{
		ScopeType:     ScopeTypeGlobal,
		ScopeKey:      "*",
		Mode:          ChannelModeActive,
		EffectiveMode: ChannelModeActive,
		Version:       18,
		Control:       &control,
	}
	require.NoError(t, state.Validate())

	global := state
	global.EffectiveMode = ChannelModePaused
	require.ErrorIs(t, global.Validate(), ErrInvalid)

	absent := state
	absent.Control = nil
	require.ErrorIs(t, absent.Validate(), ErrInvalid)

	noversion := state
	noversion.Version = 0
	require.ErrorIs(t, noversion.Validate(), ErrInvalid)

	node := state
	node.ScopeType = ScopeTypeNode
	node.ScopeKey = testNode
	node.Control = nil
	node.Version = 0
	require.NoError(t, node.Validate())
}

// TestAgentChannelRules covers the macro and the control presence.
func TestAgentChannelRules(t *testing.T) {
	t.Parallel()

	channel := AgentChannel{
		AgentID:      testAgentID,
		TX:           true,
		RX:           true,
		AutoDelivery: true,
		AutoContinue: true,
		Mode:         AgentModeFull,
		Deliverable:  true,
		Effective: ChannelEffective{
			Global: ChannelModeActive,
			Node:   ChannelModeActive,
			Tx:     true,
			Rx:     true,
		},
	}
	require.NoError(t, channel.Validate())

	manual := channel
	manual.AutoDelivery = false
	manual.AutoContinue = false
	manual.Mode = AgentModeManual
	require.NoError(t, manual.Validate())

	mismatch := channel
	mismatch.Mode = AgentModeOff
	require.ErrorIs(t, mismatch.Validate(), ErrInvalid)

	nomacro := channel
	nomacro.Mode = ""
	nomacro.AutoContinue = false
	require.NoError(t, nomacro.Validate())
}

// TestLinkStateRules covers the directional link view.
func TestLinkStateRules(t *testing.T) {
	t.Parallel()

	link := LinkState{
		FromAgent:       testAgentID,
		ToAgent:         testAgent2,
		MessageMode:     LinkModeAllow,
		ArtifactMode:    LinkModeAllow,
		MessageVersion:  0,
		ArtifactVersion: 0,
	}
	require.NoError(t, link.Validate())

	self := link
	self.ToAgent = testAgentID
	require.ErrorIs(t, self.Validate(), ErrInvalid)

	claimed := link
	claimed.MessageVersion = 7
	require.ErrorIs(t, claimed.Validate(), ErrInvalid)
}
