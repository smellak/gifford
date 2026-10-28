package channel

import (
	"time"

	"github.com/smellak/gifford/internal/protocol"
)

// This file holds the effective-control level types: the resolved control
// for one query, level by level. The flat projection to
// protocol.EffectiveControl lands with the engine.

// ControlRef describes the control row behind one level. The zero value
// means that no row exists and the default applies.
type ControlRef struct {
	ControlID protocol.ControlID   `json:"control_id,omitempty"`
	Version   int64                `json:"version,omitempty"`
	Reason    string               `json:"reason,omitempty"` // operator text of the row
	ActorType protocol.ActorType   `json:"actor_type,omitempty"`
	CreatedBy string               `json:"created_by,omitempty"`
	ExpiresAt time.Time            `json:"expires_at,omitzero"`
	Expired   bool                 `json:"expired,omitempty"` // expires_at <= now, not restored yet
}

// ModeLevel is the value of the global, node, link or thread scope.
type ModeLevel struct {
	Mode     protocol.ControlMode `json:"mode,omitempty"`      // empty only for a thread without a control
	ThenMode protocol.ControlMode `json:"then_mode,omitempty"` // global and node only
	ControlRef
}

// NodeLevel is the mode of one node.
type NodeLevel struct {
	NodeID protocol.NodeID `json:"node_id"`
	ModeLevel
}

// AgentLevel holds the four flags of one agent after defaults.
type AgentLevel struct {
	AgentID      protocol.AgentID `json:"agent_id"`
	TX           bool             `json:"tx"`
	RX           bool             `json:"rx"`
	AutoDelivery bool             `json:"auto_delivery"`
	AutoContinue bool             `json:"auto_continue"`
	ControlRef
}

// ThreadLevel is the thread scope of a query that has a thread.
type ThreadLevel struct {
	ThreadID protocol.ThreadID `json:"thread_id"`
	ModeLevel
}

// LoopLevel is the loop replica plus the optional local loop-scope control.
type LoopLevel struct {
	LoopID         protocol.LoopID          `json:"loop_id"`
	Generation     int64                    `json:"loop_generation"`
	Mode           protocol.LoopMode        `json:"mode"`
	StateReason    protocol.LoopStateReason `json:"state_reason,omitempty"`
	ControlVersion int64                    `json:"control_version"`
	Budget         protocol.LoopBudget      `json:"budget"`
	Counters       protocol.LoopCounters    `json:"counters"`
	ExpiresAt      time.Time                `json:"expires_at"`
	Override       *ModeLevel               `json:"override,omitempty"`
}

// Effective is the resolved control for one query, level by level.
type Effective struct {
	Plane     protocol.Plane   `json:"plane"`
	FromAgent protocol.AgentID `json:"from_agent"`
	ToAgent   protocol.AgentID `json:"to_agent"`
	Global    ModeLevel        `json:"global"`
	Nodes     []NodeLevel      `json:"nodes"` // sender's node first; receiver's only if different
	Sender    AgentLevel       `json:"sender"`
	Receiver  AgentLevel       `json:"receiver"`
	Link      ModeLevel        `json:"link"`
	Thread    *ThreadLevel     `json:"thread,omitempty"`
	Loop      *LoopLevel       `json:"loop,omitempty"`
}

// deepCopy returns an Effective with its own nodes slice and thread, loop
// and override pointers.
func (e Effective) deepCopy() Effective {
	out := e
	out.Nodes = append([]NodeLevel(nil), e.Nodes...)
	if e.Thread != nil {
		thread := *e.Thread
		out.Thread = &thread
	}
	if e.Loop != nil {
		loop := *e.Loop
		out.Loop = &loop
		if e.Loop.Override != nil {
			override := *e.Loop.Override
			out.Loop.Override = &override
		}
	}
	return out
}
