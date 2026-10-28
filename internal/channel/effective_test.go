package channel

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

// testEffective builds an Effective with two nodes, a thread and a loop
// with an override.
func testEffective() Effective {
	return Effective{
		Plane:     protocol.PlaneMessage,
		FromAgent: "agent-1",
		ToAgent:   "agent-2",
		Global:    ModeLevel{Mode: protocol.ControlModeActive},
		Nodes: []NodeLevel{
			{NodeID: "node-a", ModeLevel: ModeLevel{Mode: protocol.ControlModeActive}},
			{NodeID: "node-b", ModeLevel: ModeLevel{Mode: protocol.ControlModePaused}},
		},
		Sender:   AgentLevel{AgentID: "agent-1", TX: true, RX: true, AutoDelivery: true, AutoContinue: true},
		Receiver: AgentLevel{AgentID: "agent-2", TX: true, RX: true, AutoDelivery: true, AutoContinue: true},
		Link:     ModeLevel{Mode: protocol.ControlModeAllow},
		Thread:   &ThreadLevel{ThreadID: "thread-1", ModeLevel: ModeLevel{Mode: protocol.ControlModeHold}},
		Loop: &LoopLevel{
			LoopID:         "loop_01JB0000000000000000000003",
			Generation:     1,
			Mode:           protocol.LoopModeAuto,
			ControlVersion: 5,
			ExpiresAt:      testRecordTime,
			Override:       &ModeLevel{Mode: protocol.ControlModePaused},
		},
	}
}

// TestEffectiveDeepCopy proves the copy owns its nodes slice and its
// thread, loop and override pointers.
func TestEffectiveDeepCopy(t *testing.T) {
	t.Parallel()

	eff := testEffective()
	copied := eff.deepCopy()

	copied.Nodes[0].Mode = protocol.ControlModeOff
	copied.Nodes = append(copied.Nodes, NodeLevel{NodeID: "node-c"})
	copied.Thread.Mode = protocol.ControlModeDeny
	copied.Loop.Mode = protocol.LoopModeStep
	copied.Loop.Override.Mode = protocol.ControlModeStopped

	require.Equal(t, protocol.ControlModeActive, eff.Nodes[0].Mode)
	require.Len(t, eff.Nodes, 2)
	require.Equal(t, protocol.ControlModeHold, eff.Thread.Mode)
	require.Equal(t, protocol.LoopModeAuto, eff.Loop.Mode)
	require.Equal(t, protocol.ControlModePaused, eff.Loop.Override.Mode)
}
